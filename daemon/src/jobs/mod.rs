//! Background-job runtime. The daemon spawns one tokio task per
//! configured job; each task owns a `CancellationToken` for graceful
//! shutdown. Status is read by `GET /api/v1/jobs` over the same Arc.

pub mod periodic;
pub mod prune;
pub mod push;
pub mod snap;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use arctern_api::{JobStatus, PeriodicJobStatus};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zfskit::ZfsError;

/// Latches the set of `filesystems` entries that currently match no
/// dataset, so the warning is emitted on change instead of every cycle.
/// A standing misconfiguration that reprints hourly gets tuned out.
#[derive(Default)]
pub struct UnmatchedFilters(Mutex<Vec<String>>);

impl UnmatchedFilters {
    fn take_change(
        &self,
        filters: &[arctern_config::FilesystemFilter],
        candidates: &[&str],
    ) -> Option<Vec<String>> {
        let current: Vec<String> = arctern_config::filter::unmatched(filters, candidates)
            .into_iter()
            .map(str::to_string)
            .collect();
        let mut previous = self.0.lock().expect("unmatched filters mutex");
        if *previous == current {
            return None;
        }
        *previous = current.clone();
        Some(current)
    }

    pub fn report(
        &self,
        job: &str,
        filters: &[arctern_config::FilesystemFilter],
        candidates: &[&str],
    ) {
        let Some(current) = self.take_change(filters, candidates) else {
            return;
        };
        if current.is_empty() {
            tracing::info!(job = %job, "every configured filesystem matches a dataset again");
        } else {
            tracing::warn!(
                job = %job,
                filesystems = %current.join(", "),
                "configured filesystems match no dataset; the job reports success but does nothing for them"
            );
        }
    }
}

/// The scheduling fields every job kind keeps between cycles.
#[derive(Debug, Clone, Default)]
pub struct PeriodicStatus {
    pub last_run: Option<OffsetDateTime>,
    pub next_run: Option<OffsetDateTime>,
    pub last_error: Option<String>,
    pub running: bool,
}

impl PeriodicStatus {
    pub fn render(&self, name: &str) -> PeriodicJobStatus {
        PeriodicJobStatus {
            name: name.to_string(),
            last_run: self.last_run.and_then(|t| t.format(&Rfc3339).ok()),
            next_run: self.next_run.and_then(|t| t.format(&Rfc3339).ok()),
            last_error: self.last_error.clone(),
            running: self.running,
        }
    }
}

#[derive(Clone)]
pub struct JobContext {
    pub zfs: zfskit::Zfs,
    /// Per-daemon SQLite pool. None inside test-only `JobManager` setups
    /// that don't care about persistence; production code paths always
    /// pass `Some(pool)`.
    pub state: Option<Arc<sqlx::SqlitePool>>,
}

/// What a control request (cancel / pause / resume) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlOutcome {
    Applied,
    /// The job kind has nothing to cancel, pause or resume.
    Unsupported,
    NoSuchJob,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PushRequestError {
    #[error("no such job")]
    NoSuchJob,
    #[error("job kind does not support manual push")]
    Unsupported,
    #[error("peer {peer:?} is not a target of job {job:?}")]
    NotATarget { job: String, peer: String },
}

pub trait Job: Send + Sync + 'static {
    fn name(&self) -> &str;
    /// The kind is the `JobStatus` variant.
    fn status(&self) -> JobStatus;
    /// Runs until cancelled. Implementations MUST honour `cancel`
    /// inside any sleep / await they perform.
    fn run(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
    /// Wake the job's cycle loop early.
    fn wakeup(&self) {}
    /// Abort the in-flight transfer (resumable via `recv -s` partial
    /// state).
    fn cancel_current(&self) -> ControlOutcome {
        ControlOutcome::Unsupported
    }
    /// Abort the in-flight transfer AND suspend scheduled cycles until
    /// `resume`.
    fn pause(&self) -> ControlOutcome {
        ControlOutcome::Unsupported
    }
    /// Clear the paused flag and wake the cycle loop (a paused transfer
    /// continues from its resume token).
    fn resume(&self) -> ControlOutcome {
        ControlOutcome::Unsupported
    }
    /// Queue a manual replication to `peer` and wake the cycle loop.
    fn request_push(&self, _peer: &str) -> Result<(), PushRequestError> {
        Err(PushRequestError::Unsupported)
    }
}

struct JobHandle {
    name: String,
    cancel: CancellationToken,
    task: JoinHandle<()>,
    job: Arc<dyn Job>,
}

/// Why one snapshot could not be pruned.
#[derive(Debug, thiserror::Error)]
pub enum PruneError {
    #[error("list snapshots: {0}")]
    ListSnapshots(#[source] ZfsError),
    #[error("keep-rule evaluation: {0}")]
    KeepRules(#[from] arctern_config::PruneError),
    #[error("destroy {snapshot}: {source}")]
    Destroy {
        snapshot: String,
        #[source]
        source: ZfsError,
    },
}

/// One dataset's failure inside a snap or prune cycle. The cycle
/// finishes the rest of its work and reports all of them together.
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("list datasets: {0}")]
    ListDatasets(#[source] ZfsError),
    #[error("snapshot {snapshot}: {source}")]
    Snapshot {
        snapshot: String,
        #[source]
        source: ZfsError,
    },
    #[error("prune {dataset}: {source}")]
    Prune {
        dataset: String,
        #[source]
        source: PruneError,
    },
}

/// Every per-dataset failure of one cycle. Non-empty by construction.
#[derive(Debug)]
pub struct CycleErrors(pub Vec<DatasetError>);

impl std::fmt::Display for CycleErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, e) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for CycleErrors {}

impl CycleErrors {
    pub fn from_vec(errors: Vec<DatasetError>) -> Result<(), Self> {
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Self(errors))
        }
    }
}

/// One prune pass over a single dataset: list snapshots with
/// `creation`, evaluate the keep-rule chain against the bare tags,
/// destroy the victims. Held and busy snapshots are skipped, not fatal.
pub(crate) async fn prune_dataset(
    zfs: &zfskit::Zfs,
    keep: &[arctern_config::KeepRule],
    dataset: &str,
) -> Result<(), PruneError> {
    use zfskit::dataset::{DestroyOptions, ListOptions};
    use zfskit::models::DatasetType;

    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![dataset.to_string()],
        properties: vec!["creation".into()],
        ..ListOptions::default()
    };
    let snaps = zfs
        .list_datasets(&opts)
        .await
        .map_err(PruneError::ListSnapshots)?;
    let mut entries: Vec<arctern_config::SnapshotEntry> = Vec::with_capacity(snaps.len());
    // The keep rules match on the bare tag so a user's `^zrepl_.*`
    // regex need not embed the dataset name; destroy needs the full
    // name, hence the parallel vector.
    let mut full_names: Vec<String> = Vec::with_capacity(snaps.len());
    for s in &snaps {
        let creation = s
            .properties
            .get("creation")
            .and_then(|p| p.value.parse::<i64>().ok())
            .and_then(|t| OffsetDateTime::from_unix_timestamp(t).ok());
        let Some(creation) = creation else {
            tracing::warn!(snapshot = %s.name, "snapshot has no parseable creation property; skipping");
            continue;
        };
        let tag = s.name.split_once('@').map(|(_, t)| t).unwrap_or(&s.name);
        entries.push(arctern_config::SnapshotEntry {
            name: tag.to_string(),
            creation,
        });
        full_names.push(s.name.clone());
    }
    let destroy_idx = arctern_config::evaluate_keep_rules(keep, &entries)?;
    for i in destroy_idx {
        let target = &full_names[i];
        tracing::info!(snapshot = %target, "destroying snapshot");
        let snapshot = zfs
            .snapshot(target.clone())
            .map_err(|e| PruneError::Destroy {
                snapshot: target.clone(),
                source: ZfsError::from(e),
            })?;
        match snapshot.destroy(&DestroyOptions::new()).await {
            Ok(()) => {}
            Err(ZfsError::SnapshotHeld { .. }) => {
                tracing::warn!(snapshot = %target, "snapshot is held; skipping");
            }
            // A `zfs send -I` in flight keeps its intermediate snapshots
            // busy without a hold.
            Err(ZfsError::Busy { .. }) => {
                tracing::warn!(snapshot = %target, "snapshot is busy (a send is using it); skipping");
            }
            Err(source) => {
                return Err(PruneError::Destroy {
                    snapshot: target.clone(),
                    source,
                });
            }
        }
    }
    Ok(())
}

/// Owned by the daemon's `AppState`; cloned (via `Arc`) into the HTTP
/// handler that serves `/api/v1/jobs`.
pub struct JobManager {
    handles: Mutex<Vec<JobHandle>>,
}

impl Default for JobManager {
    fn default() -> Self {
        Self::new()
    }
}

impl JobManager {
    pub fn new() -> Self {
        Self {
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Spawn `job` as a background task and keep a handle for status +
    /// cancellation.
    pub fn spawn<J: Job + 'static>(&self, job: Arc<J>, ctx: JobContext) {
        let name = job.name().to_string();
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let job_for_task = job.clone();
        let task = tokio::spawn(async move {
            job_for_task.run(ctx, cancel_for_task).await;
        });
        let job_dyn: Arc<dyn Job> = job;
        self.handles.lock().unwrap().push(JobHandle {
            name,
            cancel,
            task,
            job: job_dyn,
        });
    }

    pub fn statuses(&self) -> Vec<JobStatus> {
        self.handles
            .lock()
            .unwrap()
            .iter()
            .map(|h| h.job.status())
            .collect()
    }

    fn with_job<T>(&self, name: &str, f: impl FnOnce(&dyn Job) -> T) -> Option<T> {
        let handles = self.handles.lock().unwrap();
        handles
            .iter()
            .find(|h| h.name == name)
            .map(|h| f(h.job.as_ref()))
    }

    /// Trigger the named job's `wakeup()`. False if no job with that
    /// name is registered.
    pub fn wakeup_by_name(&self, name: &str) -> bool {
        self.with_job(name, |job| job.wakeup()).is_some()
    }

    pub fn cancel_by_name(&self, name: &str) -> ControlOutcome {
        self.with_job(name, |job| job.cancel_current())
            .unwrap_or(ControlOutcome::NoSuchJob)
    }

    pub fn pause_by_name(&self, name: &str) -> ControlOutcome {
        self.with_job(name, |job| job.pause())
            .unwrap_or(ControlOutcome::NoSuchJob)
    }

    pub fn resume_by_name(&self, name: &str) -> ControlOutcome {
        self.with_job(name, |job| job.resume())
            .unwrap_or(ControlOutcome::NoSuchJob)
    }

    pub fn request_push_by_name(&self, name: &str, peer: &str) -> Result<(), PushRequestError> {
        self.with_job(name, |job| job.request_push(peer))
            .unwrap_or(Err(PushRequestError::NoSuchJob))
    }

    /// Trigger every cancellation token, then wait up to `deadline`
    /// for tasks to join. Tasks that miss the deadline are left to be
    /// aborted by the runtime.
    pub async fn shutdown(&self, deadline: Duration) {
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        for h in &handles {
            h.cancel.cancel();
        }
        let join_all = async {
            for h in handles {
                let _ = h.task.await;
            }
        };
        if tokio::time::timeout(deadline, join_all).await.is_err() {
            tracing::warn!("job shutdown deadline exceeded; tasks will be aborted");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use zfskit::runner::CommandRunner;

    struct NoopJob {
        flag: AtomicBool,
        woken: AtomicBool,
    }

    impl Job for NoopJob {
        fn name(&self) -> &str {
            "noop"
        }
        fn status(&self) -> JobStatus {
            JobStatus::Snap(PeriodicJobStatus {
                name: "noop".into(),
                ..Default::default()
            })
        }
        fn wakeup(&self) {
            self.woken.store(true, Ordering::SeqCst);
        }
        fn run(
            self: Arc<Self>,
            _ctx: JobContext,
            cancel: CancellationToken,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            Box::pin(async move {
                cancel.cancelled().await;
                self.flag.store(true, Ordering::SeqCst);
            })
        }
    }

    struct FakeRunner;
    #[async_trait::async_trait]
    impl CommandRunner for FakeRunner {
        async fn run(
            &self,
            _cmd: zfskit::runner::Cmd,
        ) -> Result<std::process::Output, std::io::Error> {
            unreachable!()
        }
    }

    fn noop_manager() -> (JobManager, Arc<NoopJob>) {
        let mgr = JobManager::new();
        let job = Arc::new(NoopJob {
            flag: AtomicBool::new(false),
            woken: AtomicBool::new(false),
        });
        mgr.spawn(
            job.clone(),
            JobContext {
                zfs: zfskit::Zfs::with_runner(FakeRunner),
                state: None,
            },
        );
        (mgr, job)
    }

    #[tokio::test]
    async fn cancellation_joins_cleanly() {
        let (mgr, job) = noop_manager();
        mgr.shutdown(Duration::from_secs(2)).await;
        assert!(job.flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn wakeup_by_name_dispatches_to_named_job() {
        let (mgr, job) = noop_manager();
        assert!(mgr.wakeup_by_name("noop"));
        assert!(!mgr.wakeup_by_name("does-not-exist"));
        assert!(job.woken.load(Ordering::SeqCst));
        mgr.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn control_outcomes_tell_missing_from_unsupported() {
        let (mgr, _job) = noop_manager();
        assert_eq!(mgr.cancel_by_name("noop"), ControlOutcome::Unsupported);
        assert_eq!(mgr.pause_by_name("noop"), ControlOutcome::Unsupported);
        assert_eq!(mgr.resume_by_name("nowhere"), ControlOutcome::NoSuchJob);
        assert_eq!(
            mgr.request_push_by_name("noop", "mira"),
            Err(PushRequestError::Unsupported)
        );
        assert_eq!(
            mgr.request_push_by_name("nowhere", "mira"),
            Err(PushRequestError::NoSuchJob)
        );
        mgr.shutdown(Duration::from_secs(2)).await;
    }

    fn exact(path: &str) -> arctern_config::FilesystemFilter {
        arctern_config::FilesystemFilter {
            path: path.into(),
            recursive: false,
            exclude: Vec::new(),
        }
    }

    #[test]
    fn unmatched_filters_reports_once_then_stays_quiet() {
        let filters = [exact("tank/data/home"), exact("tank/data/root")];
        let present = ["tank/data/home_new", "tank/data/root"];
        let watch = UnmatchedFilters::default();

        assert_eq!(
            watch.take_change(&filters, &present),
            Some(vec!["tank/data/home".to_string()])
        );
        assert_eq!(watch.take_change(&filters, &present), None);
        assert_eq!(watch.take_change(&filters, &present), None);
    }

    #[test]
    fn unmatched_filters_announces_recovery_after_a_rename() {
        let filters = [exact("tank/data/home"), exact("tank/data/root")];
        let watch = UnmatchedFilters::default();
        watch.take_change(&filters, &["tank/data/home_new", "tank/data/root"]);

        assert_eq!(
            watch.take_change(&filters, &["tank/data/home", "tank/data/root"]),
            Some(Vec::new())
        );
        assert_eq!(
            watch.take_change(&filters, &["tank/data/home", "tank/data/root"]),
            None
        );
    }

    #[test]
    fn unmatched_filters_reports_again_when_the_set_grows() {
        let filters = [exact("tank/data/home"), exact("tank/data/root")];
        let watch = UnmatchedFilters::default();
        watch.take_change(&filters, &["tank/data/root"]);

        assert_eq!(
            watch.take_change(&filters, &[]),
            Some(vec![
                "tank/data/home".to_string(),
                "tank/data/root".to_string()
            ])
        );
    }

    #[test]
    fn a_healthy_config_never_reports() {
        let filters = [exact("tank/data/home")];
        let watch = UnmatchedFilters::default();
        assert_eq!(watch.take_change(&filters, &["tank/data/home"]), None);
    }
}
