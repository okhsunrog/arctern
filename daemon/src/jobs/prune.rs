//! Prune-only cycle: evaluate the keep-rule chain per matched
//! filesystem, destroy victims. Never creates snapshots. This is how a
//! receiver keeps retention over what it received: arctern's push does
//! not manage receiver-side retention, so the receiver defines a
//! `prune` job over the received subtree with the desired grid.

use std::time::Duration;

use arctern_api::JobKind;
use arctern_config::{PruneJobConfig, filter::resolve_all};
use tracing::warn;

use super::periodic::{Cycle, PeriodicJob};
use super::{CycleErrors, DatasetError, JobContext, UnmatchedFilters};

pub struct PruneCycle {
    config: PruneJobConfig,
    unmatched: UnmatchedFilters,
}

pub type PruneJob = PeriodicJob<PruneCycle>;

impl PruneCycle {
    pub fn job(config: PruneJobConfig) -> PruneJob {
        PeriodicJob::new(Self {
            config,
            unmatched: UnmatchedFilters::default(),
        })
    }
}

impl Cycle for PruneCycle {
    const KIND: JobKind = JobKind::Prune;

    fn name(&self) -> &str {
        &self.config.name
    }

    fn interval(&self) -> Duration {
        self.config.interval
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
        let mut errors: Vec<DatasetError> = Vec::new();
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
