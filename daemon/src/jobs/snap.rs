//! Periodic-snapshot job. Loops on `interval`, snapshots every matched
//! filesystem, then prunes per the configured `KeepRule` chain.
//!
//! Algorithm matches zrepl's snap-job behaviour:
//! - On startup, run one cycle immediately (rather than waiting a full
//!   `interval`) so a daemon restart doesn't skip its window. A snapshot
//!   taken within the same second as an existing one is an idempotent
//!   no-op (SnapshotExists).
//! - Each cycle: list datasets (recursive), resolve filters, for each
//!   matched dataset: snapshot (idempotent on SnapshotExists); list
//!   snapshots with `creation`; build SnapshotEntry vec; evaluate
//!   keep-rules; destroy each victim (idempotent on SnapshotHeld).
//! - Snapshot tag is `<prefix><RFC3339-utc-no-colons>` — wire-compatible
//!   with zrepl per constitution VII.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration as StdDuration;

use arctern_config::{SnapJobConfig, filter::resolve_all};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};
use zfskit::dataset::{ListOptions, SnapshotOptions};
use zfskit::models::DatasetType;

use super::{Job, JobContext, JobStatusInner};

pub const KIND: &str = "snap";

pub struct SnapJob {
    config: SnapJobConfig,
    status: Mutex<JobStatusInner>,
    wakeup: Arc<tokio::sync::Notify>,
}

impl SnapJob {
    pub fn new(config: SnapJobConfig) -> Self {
        Self {
            config,
            status: Mutex::new(JobStatusInner::default()),
            wakeup: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn interval(&self) -> StdDuration {
        self.config.snapshotting().interval
    }

    fn prefix(&self) -> &str {
        &self.config.snapshotting().prefix
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

impl Job for SnapJob {
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
        let span = info_span!("snap_job", name = %self.config.name);
        let job_name = self.config.name.clone();
        Box::pin(
            async move {
                let interval = self.interval();
                // Startup-immediate: run a cycle now instead of waiting a
                // full `interval` after a restart.
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

async fn run_and_record(job: &SnapJob, ctx: &JobContext, job_name: &str, interval: StdDuration) {
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

impl SnapJob {
    /// One snapshot+prune pass. Returns Err only if the cycle failed in
    /// a way the operator should see at the per-job status level.
    /// Per-dataset failures are logged and accumulated into a summary
    /// string; the cycle still completes the work it can.
    async fn run_cycle(&self, ctx: &JobContext) -> Result<(), String> {
        let runner = ctx.runner.as_ref();
        // 1. List every filesystem + volume under any pool a filter
        //    references. Scoping to those pools (rather than a global
        //    list) keeps unrelated pools out of the result and makes
        //    integration tests robust against parallel test pools.
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
        let entries = zfskit::dataset::list(runner, &list_opts)
            .await
            .map_err(|e| format!("list datasets: {e}"))?;
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        let targets = resolve_all(&self.config.filesystems, &names);
        if targets.is_empty() {
            tracing::info!("no datasets matched filesystem filter");
            return Ok(());
        }

        let tag = snapshot_tag(self.prefix());
        let mut errors: Vec<String> = Vec::new();
        for ds in &targets {
            let full = format!("{ds}@{tag}");
            tracing::info!(dataset = %ds, snapshot = %tag, "creating snapshot");
            match zfskit::dataset::snapshot(runner, &full, &SnapshotOptions::new()).await {
                Ok(()) => {}
                Err(zfskit::ZfsError::SnapshotExists { .. }) => {
                    warn!(snapshot = %full, "snapshot already exists; treating as no-op");
                }
                Err(e) => {
                    let msg = format!("snapshot {full}: {e}");
                    warn!(error = %msg);
                    errors.push(msg);
                }
            }
        }

        // 2. Prune. Per-dataset to keep the algorithm's "now" reference
        //    local (so a stale dataset's youngest snapshot does not
        //    skew the bucket math for an active dataset).
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

/// `<prefix><RFC3339-utc-no-colons>` at second precision, e.g.
/// `arctern_2026-07-08T182612Z`. Colons stripped because some
/// downstream tooling chokes on them; sub-second digits dropped —
/// they added 10 chars of noise to every snapshot name and the
/// same-second collision they'd prevent is already an idempotent
/// no-op (SnapshotExists).
fn snapshot_tag(prefix: &str) -> String {
    let now = OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .expect("0 is a valid nanosecond");
    let formatted = now
        .format(&Rfc3339)
        .expect("Rfc3339 format always succeeds");
    let stripped: String = formatted.chars().filter(|c| *c != ':').collect();
    format!("{prefix}{stripped}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_tag_strips_colons() {
        let t = snapshot_tag("zrepl_");
        assert!(t.starts_with("zrepl_"));
        assert!(!t.contains(':'));
    }

    #[test]
    fn snapshot_tag_is_second_precision() {
        let t = snapshot_tag("arctern_");
        // No fractional-second part: arctern_2026-07-08T182612Z.
        assert!(!t.contains('.'), "got: {t}");
        assert!(t.ends_with('Z'));
    }
}
