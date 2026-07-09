//! Background-job runtime. The daemon spawns one tokio task per
//! configured job; each task owns a `CancellationToken` for graceful
//! shutdown. Status is read by `GET /api/v1/jobs` over the same Arc.
//!
//! Slice 003 introduces this; only `SnapJob` implements it. Future
//! slices add push/pull/source/sink as siblings.

pub mod prune;
pub mod push;
pub mod snap;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zfskit::runner::CommandRunner;

#[derive(Debug, Clone, Default)]
pub struct JobStatusInner {
    pub last_run: Option<OffsetDateTime>,
    pub next_run: Option<OffsetDateTime>,
    pub last_error: Option<String>,
    /// True while a cycle is executing. Long-running cycles (a full
    /// send) would otherwise be indistinguishable from idle-with-stale-
    /// status in the UI.
    pub running: bool,
    /// True while the job is paused (current transfer aborted resumably,
    /// scheduled cycles suspended).
    pub paused: bool,
    /// In-flight transfer progress (push jobs only), one entry per
    /// parallel send slot.
    pub transfers: Vec<arctern_api::TransferInfo>,
    /// Per-target replication policy + last outcome (push jobs only).
    pub targets: Vec<arctern_api::TargetStatus>,
}

#[derive(Clone)]
pub struct JobContext {
    pub runner: Arc<dyn CommandRunner>,
    /// Per-daemon SQLite pool. None inside test-only `JobManager` setups
    /// that don't care about persistence; production code paths always
    /// pass `Some(pool)` from `daemon::main::run_daemon`.
    pub state: Option<Arc<sqlx::SqlitePool>>,
}

pub trait Job: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn kind(&self) -> &'static str;
    fn status(&self) -> JobStatusInner;
    /// Runs until cancelled. Implementations MUST honour `cancel`
    /// inside any sleep / await they perform.
    fn run(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
    /// Wake the job's cycle loop early. Default no-op so kinds that
    /// don't have a cycle (sink — event-driven) absorb the call
    /// harmlessly. Snap and push override.
    fn wakeup(&self) {}
    /// Abort the in-flight transfer (resumable via `recv -s` partial
    /// state). Returns false when the job kind has nothing to cancel.
    fn cancel_current(&self) -> bool {
        false
    }
    /// Pause: abort the in-flight transfer AND suspend scheduled cycles
    /// until `resume`. Returns false when unsupported.
    fn pause(&self) -> bool {
        false
    }
    /// Clear the paused flag and wake the cycle loop (a paused transfer
    /// continues from its resume token). Returns false when unsupported.
    fn resume(&self) -> bool {
        false
    }
    /// Queue a manual replication to `peer` and wake the cycle loop.
    /// Push jobs validate the peer against their targets.
    fn request_push(&self, _peer: &str) -> Result<(), String> {
        Err("job kind does not support manual push".into())
    }
}

struct JobHandle {
    name: String,
    kind: &'static str,
    cancel: CancellationToken,
    task: JoinHandle<()>,
    job: Arc<dyn Job>,
}

/// One prune pass over a single dataset: list snapshots with
/// `creation`, evaluate the keep-rule chain against the bare tags,
/// destroy the victims. Shared by the snap job (post-snapshot prune)
/// and the standalone prune job. Held snapshots are skipped, not fatal.
pub(crate) async fn prune_dataset(
    runner: &dyn CommandRunner,
    keep: &[arctern_config::KeepRule],
    dataset: &str,
) -> Result<(), String> {
    use zfskit::dataset::{DestroyOptions, ListOptions};
    use zfskit::models::DatasetType;

    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![dataset.to_string()],
        properties: vec!["creation".into()],
        ..ListOptions::default()
    };
    let snaps = zfskit::dataset::list(runner, &opts)
        .await
        .map_err(|e| format!("list snapshots: {e}"))?;
    let mut entries: Vec<arctern_config::SnapshotEntry> = Vec::with_capacity(snaps.len());
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
    let destroy_idx = arctern_config::evaluate_keep_rules(keep, &entries)
        .map_err(|e| format!("keep-rule evaluation: {e}"))?;
    for i in destroy_idx {
        let target = &full_names[i];
        tracing::info!(snapshot = %target, "destroying snapshot");
        match zfskit::dataset::destroy(runner, target, &DestroyOptions::new()).await {
            Ok(()) => {}
            Err(zfskit::ZfsError::SnapshotHeld { .. }) => {
                tracing::warn!(snapshot = %target, "snapshot is held; skipping");
            }
            Err(e) => {
                return Err(format!("destroy {target}: {e}"));
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

    /// Spawn `job` as a background task. The returned `JobManager` keeps
    /// a handle for status + cancellation.
    pub fn spawn<J: Job + 'static>(&self, job: Arc<J>, ctx: JobContext) {
        let name = job.name().to_string();
        let kind = job.kind();
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let job_for_task = job.clone();
        let task = tokio::spawn(async move {
            job_for_task.run(ctx, cancel_for_task).await;
        });
        let job_dyn: Arc<dyn Job> = job;
        self.handles.lock().unwrap().push(JobHandle {
            name,
            kind,
            cancel,
            task,
            job: job_dyn,
        });
    }

    pub fn statuses(&self) -> Vec<(String, &'static str, JobStatusInner)> {
        self.handles
            .lock()
            .unwrap()
            .iter()
            .map(|h| (h.name.clone(), h.kind, h.job.status()))
            .collect()
    }

    /// Trigger the named job's `wakeup()`. Returns false if no job
    /// with that name is registered. Cheap to call (microseconds for
    /// the lookup + a `Notify::notify_one`).
    pub fn wakeup_by_name(&self, name: &str) -> bool {
        let handles = self.handles.lock().unwrap();
        if let Some(h) = handles.iter().find(|h| h.name == name) {
            h.job.wakeup();
            true
        } else {
            false
        }
    }

    /// `None` = no such job; `Some(supported)` otherwise.
    pub fn cancel_by_name(&self, name: &str) -> Option<bool> {
        let handles = self.handles.lock().unwrap();
        handles
            .iter()
            .find(|h| h.name == name)
            .map(|h| h.job.cancel_current())
    }

    pub fn pause_by_name(&self, name: &str) -> Option<bool> {
        let handles = self.handles.lock().unwrap();
        handles
            .iter()
            .find(|h| h.name == name)
            .map(|h| h.job.pause())
    }

    pub fn resume_by_name(&self, name: &str) -> Option<bool> {
        let handles = self.handles.lock().unwrap();
        handles
            .iter()
            .find(|h| h.name == name)
            .map(|h| h.job.resume())
    }

    /// `None` = no such job; `Some(result)` otherwise.
    pub fn request_push_by_name(&self, name: &str, peer: &str) -> Option<Result<(), String>> {
        let handles = self.handles.lock().unwrap();
        handles
            .iter()
            .find(|h| h.name == name)
            .map(|h| h.job.request_push(peer))
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

    struct NoopJob {
        flag: AtomicBool,
        woken: AtomicBool,
    }

    impl Job for NoopJob {
        fn name(&self) -> &str {
            "noop"
        }
        fn kind(&self) -> &'static str {
            "snap"
        }
        fn status(&self) -> JobStatusInner {
            JobStatusInner::default()
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

    #[tokio::test]
    async fn cancellation_joins_cleanly() {
        // No real runner needed; the test job ignores it. Use a sentinel
        // CommandRunner via a placeholder Arc.
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
        let mgr = JobManager::new();
        let job = Arc::new(NoopJob {
            flag: AtomicBool::new(false),
            woken: AtomicBool::new(false),
        });
        mgr.spawn(
            job.clone(),
            JobContext {
                runner: Arc::new(FakeRunner) as Arc<dyn CommandRunner>,
                state: None,
            },
        );
        mgr.shutdown(Duration::from_secs(2)).await;
        assert!(job.flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn wakeup_by_name_dispatches_to_named_job() {
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
        let mgr = JobManager::new();
        let job = Arc::new(NoopJob {
            flag: AtomicBool::new(false),
            woken: AtomicBool::new(false),
        });
        mgr.spawn(
            job.clone(),
            JobContext {
                runner: Arc::new(FakeRunner) as Arc<dyn CommandRunner>,
                state: None,
            },
        );
        assert!(mgr.wakeup_by_name("noop"));
        assert!(!mgr.wakeup_by_name("does-not-exist"));
        assert!(job.woken.load(Ordering::SeqCst));
        mgr.shutdown(Duration::from_secs(2)).await;
    }
}
