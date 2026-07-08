//! Prune-only job. Lists matching snapshots per filesystem, evaluates
//! the configured keep-rule chain, destroys victims. Never creates
//! snapshots — that's the snap job's responsibility.
//!
//! Use case: a receiver host that gets snapshots from a push sender
//! still needs retention. zrepl handled this with
//! `push.pruning.keep_receiver`; arctern's push doesn't manage
//! receiver-side retention, so the receiver defines a `prune` job over
//! the received subtree with the desired grid.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration as StdDuration;

use arctern_config::{PruneJobConfig, filter::resolve_all};
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use time::OffsetDateTime;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};

use super::{Job, JobContext, JobStatusInner};

pub const KIND: &str = "prune";

pub struct PruneJob {
    config: PruneJobConfig,
    status: Mutex<JobStatusInner>,
    wakeup: Arc<tokio::sync::Notify>,
}

impl PruneJob {
    pub fn new(config: PruneJobConfig) -> Self {
        Self {
            config,
            status: Mutex::new(JobStatusInner::default()),
            wakeup: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn interval(&self) -> StdDuration {
        self.config.interval
    }

    fn record_cycle(&self, last_error: Option<String>, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.last_run = Some(now);
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.last_error = last_error;
        s.running = false;
    }
}

impl Job for PruneJob {
    fn name(&self) -> &str {
        &self.config.name
    }
    fn kind(&self) -> &'static str {
        KIND
    }
    fn status(&self) -> JobStatusInner {
        self.status.lock().unwrap().clone()
    }
    fn wakeup(&self) {
        self.wakeup.notify_one();
    }
    fn run(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let span = info_span!("prune_job", name = %self.config.name);
        let job_name = self.config.name.clone();
        Box::pin(
            async move {
                let interval = self.interval();
                run_and_record(&self, &ctx, &job_name, interval).await;
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = sleep(interval) => {}
                        _ = self.wakeup.notified() => {}
                    }
                    run_and_record(&self, &ctx, &job_name, interval).await;
                }
            }
            .instrument(span),
        )
    }
}

async fn run_and_record(job: &PruneJob, ctx: &JobContext, job_name: &str, interval: StdDuration) {
    job.status.lock().unwrap().running = true;
    let started_at = OffsetDateTime::now_utc().unix_timestamp();
    if let Some(pool) = ctx.state.as_ref() {
        let _ = crate::state::job_runs::record_start(pool, job_name, started_at).await;
    }
    let outcome = job.run_cycle(ctx).await;
    let finished_at = OffsetDateTime::now_utc().unix_timestamp();
    let (status, error_message) = match &outcome {
        Ok(()) => (crate::state::job_runs::STATUS_OK, None),
        Err(e) => (crate::state::job_runs::STATUS_ERROR, Some(e.as_str())),
    };
    if let Some(pool) = ctx.state.as_ref() {
        let _ = crate::state::job_runs::record_finish(
            pool,
            job_name,
            started_at,
            finished_at,
            status,
            error_message,
            None,
        )
        .await;
    }
    job.record_cycle(outcome.err(), interval);
}

impl PruneJob {
    async fn run_cycle(&self, ctx: &JobContext) -> Result<(), String> {
        let runner = ctx.runner.as_ref();
        let mut pools: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for f in &self.config.filesystems {
            let pool = f.path.split('/').next().unwrap_or(&f.path).to_string();
            pools.insert(pool);
        }
        let roots: Vec<String> = pools.into_iter().collect();
        let list_opts = ListOptions {
            recursive: true,
            types: vec![DatasetType::Filesystem, DatasetType::Volume],
            roots,
            ..ListOptions::default()
        };
        let entries = palimpsest::dataset::list(runner, &list_opts)
            .await
            .map_err(|e| format!("list datasets: {e}"))?;
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        let targets = resolve_all(&self.config.filesystems, &names);
        if targets.is_empty() {
            return Ok(());
        }
        let mut errors: Vec<String> = Vec::new();
        for ds in &targets {
            if let Err(e) = super::prune_dataset(runner, &self.config.pruning().keep, ds).await {
                warn!(dataset = %ds, error = %e, "prune cycle errored");
                errors.push(format!("prune {ds}: {e}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}
