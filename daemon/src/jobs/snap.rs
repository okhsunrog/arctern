//! Periodic-snapshot cycle: snapshot every matched filesystem, then
//! prune per the configured `KeepRule` chain. Snapshot names are
//! `<prefix><RFC3339-utc-no-colons>`, the zrepl idiom.

use std::time::Duration;

use arctern_api::JobKind;
use arctern_config::{SnapJobConfig, filter::resolve_all};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::warn;
use zfskit::dataset::SnapshotOptions;

use super::periodic::{Cycle, PeriodicJob};
use super::{CycleErrors, DatasetError, JobContext, UnmatchedFilters};

pub struct SnapCycle {
    config: SnapJobConfig,
    unmatched: UnmatchedFilters,
}

pub type SnapJob = PeriodicJob<SnapCycle>;

impl SnapCycle {
    pub fn job(config: SnapJobConfig) -> SnapJob {
        PeriodicJob::new(Self {
            config,
            unmatched: UnmatchedFilters::default(),
        })
    }
}

impl Cycle for SnapCycle {
    const KIND: JobKind = JobKind::Snap;

    fn name(&self) -> &str {
        &self.config.name
    }

    fn interval(&self) -> Duration {
        self.config.snapshotting().interval
    }

    async fn run(&self, ctx: &JobContext) -> Result<(), CycleErrors> {
        let entries =
            crate::inventory::list_filesystems(ctx.zfs.command_runner(), &self.config.filesystems)
                .await
                .map_err(|e| CycleErrors(vec![DatasetError::ListDatasets(e)]))?;
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        self.unmatched
            .report(&self.config.name, &self.config.filesystems, &names);
        let targets = resolve_all(&self.config.filesystems, &names);
        if targets.is_empty() {
            tracing::info!("no datasets matched filesystem filter");
            return Ok(());
        }

        let tag = snapshot_tag(&self.config.snapshotting().prefix);
        let mut errors: Vec<DatasetError> = Vec::new();
        for ds in &targets {
            let full = format!("{ds}@{tag}");
            tracing::info!(dataset = %ds, snapshot = %tag, "creating snapshot");
            let result = match ctx.zfs.dataset(*ds) {
                Ok(dataset) => dataset
                    .snapshot(&tag, &SnapshotOptions::new())
                    .await
                    .map(|_| ()),
                Err(e) => Err(e.into()),
            };
            match result {
                Ok(()) => {}
                // A restart within the same second re-requests the same
                // name; that is the idempotent no-op it looks like.
                Err(zfskit::ZfsError::SnapshotExists { .. }) => {
                    warn!(snapshot = %full, "snapshot already exists; treating as no-op");
                }
                Err(source) => {
                    let e = DatasetError::Snapshot {
                        snapshot: full,
                        source,
                    };
                    warn!(error = %e);
                    errors.push(e);
                }
            }
        }

        // Per dataset so the grid's "now" (the youngest snapshot) stays
        // local: a stale dataset must not skew an active one's buckets.
        for ds in &targets {
            if let Err(source) =
                super::prune_dataset(&ctx.zfs, &self.config.pruning().keep, ds).await
            {
                warn!(dataset = %ds, error = %source, "prune cycle errored");
                errors.push(DatasetError::Prune {
                    dataset: ds.to_string(),
                    source,
                });
            }
        }
        CycleErrors::from_vec(errors)
    }
}

/// `<prefix><RFC3339-utc-no-colons>` at second precision, e.g.
/// `arctern_2026-07-08T182612Z`. Colons stripped because some
/// downstream tooling chokes on them; sub-second digits dropped since
/// a same-second collision is already an idempotent no-op.
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
        assert!(!t.contains('.'), "got: {t}");
        assert!(t.ends_with('Z'));
    }
}
