//! The loop shared by snap and prune jobs: run one cycle at startup,
//! then every `interval` measured from the previous cycle's START so
//! the cadence does not drift by the cycle's own duration. A wakeup
//! runs a cycle immediately; each cycle is recorded in `job_runs`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arctern_api::{JobKind, JobStatus, RunStatus};
use time::OffsetDateTime;
use tokio::time::sleep_until;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use super::{CycleErrors, Job, JobContext, PeriodicStatus};

/// One kind of periodic work.
pub trait Cycle: Send + Sync + 'static {
    const KIND: JobKind;
    fn name(&self) -> &str;
    fn interval(&self) -> Duration;
    /// Err only for failures the operator should see at the job level;
    /// per-dataset failures are accumulated inside and the cycle still
    /// completes the work it can.
    fn run(&self, ctx: &JobContext) -> impl Future<Output = Result<(), CycleErrors>> + Send;
}

pub struct PeriodicJob<C> {
    cycle: C,
    status: Mutex<PeriodicStatus>,
    wakeup: tokio::sync::Notify,
}

impl<C: Cycle> PeriodicJob<C> {
    pub fn new(cycle: C) -> Self {
        Self {
            cycle,
            status: Mutex::new(PeriodicStatus::default()),
            wakeup: tokio::sync::Notify::new(),
        }
    }

    async fn run_and_record(&self, ctx: &JobContext) {
        self.status.lock().unwrap().running = true;
        let started = OffsetDateTime::now_utc();
        let run_id = match ctx.state.as_ref() {
            Some(pool) => crate::state::job_runs::record_start(
                pool,
                self.cycle.name(),
                started.unix_timestamp(),
            )
            .await
            .ok(),
            None => None,
        };
        let outcome = self.cycle.run(ctx).await;
        let finished_at = OffsetDateTime::now_utc().unix_timestamp();
        let (status, message) = match &outcome {
            Ok(()) => (RunStatus::Ok, None),
            Err(e) => (RunStatus::Error, Some(e.to_string())),
        };
        if let (Some(pool), Some(run_id)) = (ctx.state.as_ref(), run_id) {
            let _ = crate::state::job_runs::record_finish(
                pool,
                run_id,
                finished_at,
                status,
                message.as_deref(),
                None,
            )
            .await;
        }
        let mut s = self.status.lock().unwrap();
        s.last_run = Some(OffsetDateTime::now_utc());
        s.next_run = Some(
            started
                + time::Duration::try_from(self.cycle.interval()).unwrap_or(time::Duration::ZERO),
        );
        s.last_error = message;
        s.running = false;
    }
}

impl<C: Cycle> Job for PeriodicJob<C> {
    fn name(&self) -> &str {
        self.cycle.name()
    }

    fn status(&self) -> JobStatus {
        let status = self.status.lock().unwrap().render(self.cycle.name());
        match C::KIND {
            JobKind::Snap => JobStatus::Snap(status),
            JobKind::Prune => JobStatus::Prune(status),
            JobKind::Push => unreachable!("push is not a periodic job"),
        }
    }

    fn wakeup(&self) {
        self.wakeup.notify_one();
    }

    fn run(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let span = info_span!("periodic_job", kind = %C::KIND, name = %self.cycle.name());
        Box::pin(
            async move {
                let interval = self.cycle.interval();
                let mut due = tokio::time::Instant::now();
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = sleep_until(due) => {}
                        _ = self.wakeup.notified() => {}
                    }
                    let started = tokio::time::Instant::now();
                    self.run_and_record(&ctx).await;
                    due = started + interval;
                }
            }
            .instrument(span),
        )
    }
}
