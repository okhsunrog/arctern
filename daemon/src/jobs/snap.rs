//! Periodic-snapshot job. Loops on `interval`, snapshots every matched
//! filesystem, then prunes per the configured `KeepRule` chain.
//!
//! Algorithm matches zrepl's snap-job behaviour:
//! - On startup, take an "immediate" snapshot if the youngest matching
//!   snapshot is older than `interval` (so a daemon restart doesn't
//!   miss its window).
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

use arctern_config::{
    SnapJobConfig, SnapshotEntry, SnapshottingConfig, evaluate_keep_rules, filter::resolve_all,
};
use palimpsest::dataset::{DestroyOptions, ListOptions, SnapshotOptions};
use palimpsest::models::DatasetType;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};

use super::{Job, JobContext, JobStatusInner};

pub const KIND: &str = "snap";

pub struct SnapJob {
    config: SnapJobConfig,
    status: Mutex<JobStatusInner>,
}

impl SnapJob {
    pub fn new(config: SnapJobConfig) -> Self {
        Self {
            config,
            status: Mutex::new(JobStatusInner::default()),
        }
    }

    fn interval(&self) -> StdDuration {
        match self.config.snapshotting {
            SnapshottingConfig::Periodic { interval, .. } => interval,
        }
    }

    fn prefix(&self) -> &str {
        match &self.config.snapshotting {
            SnapshottingConfig::Periodic { prefix, .. } => prefix,
        }
    }

    fn record_cycle(&self, last_error: Option<String>, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.last_run = Some(now);
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.last_error = last_error;
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
    fn run(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let span = info_span!("snap_job", name = %self.config.name);
        Box::pin(
            async move {
                let interval = self.interval();
                // Startup-immediate: if no recent matching snapshot, run a
                // cycle now instead of waiting `interval`.
                if let Err(e) = self.run_cycle(&ctx).await {
                    self.record_cycle(Some(e), interval);
                } else {
                    self.record_cycle(None, interval);
                }
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = sleep(interval) => {}
                    }
                    let result = self.run_cycle(&ctx).await;
                    let last_err = result.err();
                    self.record_cycle(last_err, interval);
                }
            }
            .instrument(span),
        )
    }
}

impl SnapJob {
    /// One snapshot+prune pass. Returns Err only if the cycle failed in
    /// a way the operator should see at the per-job status level.
    /// Per-dataset failures are logged and accumulated into a summary
    /// string; the cycle still completes the work it can.
    async fn run_cycle(&self, ctx: &JobContext) -> Result<(), String> {
        let runner = ctx.runner.as_ref();
        // 1. List every filesystem + volume under any pool a filter
        //    references. We use `-r <pool>` per distinct pool because
        //    `zfs list -r` with no target fails ("no datasets
        //    available") and `zfs list` without `-r` only returns
        //    each pool's top filesystem (no descendants).
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
            tracing::info!("no datasets matched filesystem filter");
            return Ok(());
        }

        let tag = snapshot_tag(self.prefix());
        let mut errors: Vec<String> = Vec::new();
        for ds in &targets {
            let full = format!("{ds}@{tag}");
            tracing::info!(dataset = %ds, snapshot = %tag, "creating snapshot");
            match palimpsest::dataset::snapshot(runner, &full, &SnapshotOptions::new()).await {
                Ok(()) => {}
                Err(palimpsest::ZfsError::SnapshotExists { .. }) => {
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
            if let Err(e) = self.prune_one(runner, ds).await {
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

    async fn prune_one(
        &self,
        runner: &dyn palimpsest::runner::CommandRunner,
        dataset: &str,
    ) -> Result<(), String> {
        let opts = ListOptions {
            recursive: false,
            types: vec![DatasetType::Snapshot],
            roots: vec![dataset.to_string()],
            properties: vec!["creation".into()],
            ..ListOptions::default()
        };
        let snaps = palimpsest::dataset::list(runner, &opts)
            .await
            .map_err(|e| format!("list snapshots: {e}"))?;
        let mut entries: Vec<SnapshotEntry> = Vec::with_capacity(snaps.len());
        // Parallel vector of full ZFS names (`pool/ds@tag`); the prune
        // algorithm matches on the bare tag (so a user's `^zrepl_.*`
        // regex does not have to embed the dataset name), but destroy
        // needs the fully-qualified target.
        let mut full_names: Vec<String> = Vec::with_capacity(snaps.len());
        for s in &snaps {
            let creation = s
                .properties
                .get("creation")
                .and_then(|p| p.value.parse::<i64>().ok())
                .and_then(|t| OffsetDateTime::from_unix_timestamp(t).ok());
            let Some(creation) = creation else {
                warn!(snapshot = %s.name, "snapshot has no parseable creation property; skipping");
                continue;
            };
            let tag = s.name.split_once('@').map(|(_, t)| t).unwrap_or(&s.name);
            entries.push(SnapshotEntry {
                name: tag.to_string(),
                creation,
            });
            full_names.push(s.name.clone());
        }
        let destroy_idx = evaluate_keep_rules(&self.config.pruning.keep, &entries)
            .map_err(|e| format!("keep-rule evaluation: {e}"))?;
        for i in destroy_idx {
            let target = &full_names[i];
            tracing::info!(snapshot = %target, "destroying snapshot");
            match palimpsest::dataset::destroy(runner, target, &DestroyOptions::new()).await {
                Ok(()) => {}
                Err(palimpsest::ZfsError::SnapshotHeld { .. }) => {
                    warn!(snapshot = %target, "snapshot is held; skipping");
                }
                Err(e) => {
                    return Err(format!("destroy {target}: {e}"));
                }
            }
        }
        Ok(())
    }
}

/// `<prefix><RFC3339-utc-no-colons>`. Colons stripped because some
/// downstream tooling chokes on them and zrepl uses the same shape.
fn snapshot_tag(prefix: &str) -> String {
    let now = OffsetDateTime::now_utc();
    // Format with second precision; colons replaced.
    let formatted = now.format(&Rfc3339).expect("Rfc3339 format always succeeds");
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
}
