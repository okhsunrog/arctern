//! Background-job runtime. The daemon spawns one tokio task per
//! configured job; each task owns a `CancellationToken` for graceful
//! shutdown. Status is read by `GET /api/v1/jobs` over the same Arc.
//!
//! Slice 003 introduces this; only `SnapJob` implements it. Future
//! slices add push/pull/source/sink as siblings.

pub mod sink;
pub mod snap;

use std::sync::Arc;
use std::time::Duration;

use palimpsest::runner::CommandRunner;
use parking_lot_or_std::Mutex;
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// `parking_lot` would be nicer but adding a dep just for this single
// short-lived mutex is overkill. std::sync::Mutex is fine — held for
// microseconds (status read/write).
mod parking_lot_or_std {
    pub use std::sync::Mutex;
}

#[derive(Debug, Clone, Default)]
pub struct JobStatusInner {
    pub last_run: Option<OffsetDateTime>,
    pub next_run: Option<OffsetDateTime>,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct JobContext {
    pub runner: Arc<dyn CommandRunner>,
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
}

struct JobHandle {
    name: String,
    kind: &'static str,
    cancel: CancellationToken,
    task: JoinHandle<()>,
    status_ref: Arc<dyn StatusRead>,
}

trait StatusRead: Send + Sync {
    fn read(&self) -> JobStatusInner;
}

struct JobStatusFromJob<J: Job + ?Sized>(Arc<J>);
impl<J: Job + ?Sized> StatusRead for JobStatusFromJob<J> {
    fn read(&self) -> JobStatusInner {
        self.0.status()
    }
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
        let status_ref: Arc<dyn StatusRead> = Arc::new(JobStatusFromJob(job));
        self.handles.lock().unwrap().push(JobHandle {
            name,
            kind,
            cancel,
            task,
            status_ref,
        });
    }

    pub fn statuses(&self) -> Vec<(String, &'static str, JobStatusInner)> {
        self.handles
            .lock()
            .unwrap()
            .iter()
            .map(|h| (h.name.clone(), h.kind, h.status_ref.read()))
            .collect()
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
                _cmd: palimpsest::runner::Cmd,
            ) -> Result<std::process::Output, std::io::Error> {
                unreachable!()
            }
        }
        let mgr = JobManager::new();
        let job = Arc::new(NoopJob {
            flag: AtomicBool::new(false),
        });
        mgr.spawn(
            job.clone(),
            JobContext {
                runner: Arc::new(FakeRunner) as Arc<dyn CommandRunner>,
            },
        );
        mgr.shutdown(Duration::from_secs(2)).await;
        assert!(job.flag.load(Ordering::SeqCst));
    }
}
