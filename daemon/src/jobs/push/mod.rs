//! Push job — active sender. Each cycle, for every configured filesystem:
//! list local matching snapshots, ask the receiver via the SSH control
//! channel what it has, intersect by GUID, then open a recv channel and
//! pipe `zfs send`'s stdout into it.
//!
//! The planner (`plan`) is pure; `step` executes one planned send over
//! `peer::PeerLink`; `holds` owns the step holds and the cursor
//! bookmark; `limiter` is the shared token bucket. This file is the job
//! itself: scheduling, target selection and status.
//!
//! Holds and cursor bookmarks (ARCHITECTURE.md "Holds and replication
//! cursor"):
//!
//!   - Step hold tag `arctern_step_J_<jobname>_P_<peer>` is placed on
//!     the `to` snapshot (and the `from` snapshot of an incremental)
//!     before the send begins. On success the tag is swept from every
//!     filtered snapshot of the dataset; on failure it stays so a retry
//!     can find the snapshot regardless of intervening prune.
//!   - Cursor bookmark `<dataset>#arctern_cursor_G_<guid>_J_<job>_P_<peer>`
//!     is created from the new `to` snapshot on success; previous
//!     cursors (same job/peer suffix, different GUID) are destroyed
//!     after the new one lands, so the transition is crash-safe.
//!     When sender and receiver share no common snapshot, the planner
//!     falls back to an incremental send based on any bookmark whose
//!     GUID the receiver still has (see `plan::apply_bookmark_fallback`).

mod holds;
mod limiter;
pub mod plan;
mod step;

pub use plan::{CompiledFilter, PlanError};
pub use step::StepError;

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use arctern_api::{
    JobStatus, PushJobStatus, RunStatus, TargetStatus, TransferInfo, TransferKind, TransferPhase,
};
use arctern_config::{PeerConfig, PeerMode, PushJobConfig};
use arctern_transport::regex;
use time::OffsetDateTime;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};
use zfskit::ZfsError;
use zfskit::runner::CommandRunner;

use super::{ControlOutcome, Job, JobContext, PeriodicStatus, PushRequestError};
use crate::peer::state::PeersState;
use crate::peer::{PeerLink, RpcError};
use limiter::RateLimiter;
use plan::{SnapshotPlan, build_send_args, plan_one_filesystem};
use step::{StepCtx, run_one_filesystem};

/// One thing that went wrong in a push cycle. Per-peer attempts collect
/// these; the run's error message is all of them joined.
#[derive(Debug, thiserror::Error)]
pub enum CycleError {
    #[error("cancelled")]
    Cancelled,
    #[error("plan {dataset}: {source}")]
    Plan {
        dataset: String,
        #[source]
        source: PlanError,
    },
    #[error("discard partial receive {target}: {source}")]
    DiscardPartial {
        target: String,
        #[source]
        source: RpcError,
    },
    #[error("execute {dataset}: {source}")]
    Step {
        dataset: String,
        #[source]
        source: StepError,
    },
    #[error("list datasets: {0}")]
    ListDatasets(#[source] ZfsError),
    /// The ancestor's first full send failed this cycle. Receiving the
    /// descendant would create the ancestor as a placeholder that its
    /// own full stream can then never land on.
    #[error("{dataset}: skipped, the first full send of {ancestor} failed this cycle")]
    AncestorFailed { dataset: String, ancestor: String },
    #[error("manual push to {peer:?}: peer not connected")]
    PeerNotConnected { peer: String },
    #[error(
        "auto target {peer:?} has no auto-eligible route and last successful sync is {hours}h old"
    )]
    AutoTargetStale { peer: String, hours: i64 },
}

impl CycleError {
    fn is_cancelled(&self) -> bool {
        matches!(
            self,
            CycleError::Cancelled
                | CycleError::Step {
                    source: StepError::Cancelled,
                    ..
                }
        )
    }
}

fn join_errors(errors: &[CycleError]) -> String {
    errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Per-peer scheduling anchor plus the most recent attempt shown in the UI.
#[derive(Debug, Clone, Default)]
struct PeerOutcome {
    last_success: Option<i64>,
    last_attempt: Option<i64>,
    outcome: Option<RunStatus>,
    message: Option<String>,
}

/// One peer's attempt, as recorded in `push_syncs` and shown per target.
///
/// Cancellation wins over any accumulated messages: calling a cancelled
/// step a failure is how a routine `systemctl restart` painted the job
/// red. `cancelled` covers the case where the token fired between
/// steps, so no step got to report it. A clean dry run is `DryRun`, not
/// `Ok`: `Ok` becomes `last_success_at`, which drives the auto schedule,
/// and a plan-only cycle has replicated nothing.
fn classify_peer_attempt(
    cancelled: bool,
    dry_run: bool,
    errors: &[CycleError],
) -> (RunStatus, Option<String>) {
    if cancelled || errors.iter().any(CycleError::is_cancelled) {
        (RunStatus::Cancelled, None)
    } else if errors.is_empty() {
        (
            if dry_run {
                RunStatus::DryRun
            } else {
                RunStatus::Ok
            },
            None,
        )
    } else {
        (RunStatus::Error, Some(join_errors(errors)))
    }
}

/// Group dataset paths by depth, shallowest first, keeping the input
/// order within a level. Every ancestor of a path sits in an earlier
/// level, so running the levels in sequence replicates parents before
/// children whatever the concurrency within a level.
fn depth_levels(paths: &[String]) -> Vec<Vec<&str>> {
    let mut levels: std::collections::BTreeMap<usize, Vec<&str>> = Default::default();
    for p in paths {
        levels
            .entry(p.matches('/').count())
            .or_default()
            .push(p.as_str());
    }
    levels.into_values().collect()
}

fn is_ancestor(ancestor: &str, path: &str) -> bool {
    path.len() > ancestor.len()
        && path.starts_with(ancestor)
        && path.as_bytes()[ancestor.len()] == b'/'
}

/// Safety-net poll when nothing is due and no signal arrives.
const FALLBACK_POLL: StdDuration = StdDuration::from_secs(15 * 60);
/// Retry cadence while a target is due but blocked (manual-only route
/// active / peer unreachable) — waking sooner would just spin.
const BLOCKED_RETRY: StdDuration = StdDuration::from_secs(5 * 60);

pub struct PushJob {
    config: PushJobConfig,
    /// Shared token bucket built from `bandwidth_limit`; all parallel
    /// sends of this job draw from the same bucket.
    limiter: Option<Arc<RateLimiter>>,
    /// Filesystems replicated concurrently per target peer.
    parallel: usize,
    /// `[[peers]]` entries for this job's targets (mode, auto_interval).
    peer_configs: Vec<PeerConfig>,
    filter: CompiledFilter,
    status: Mutex<PeriodicStatus>,
    wakeup: Arc<tokio::sync::Notify>,
    unmatched: crate::jobs::UnmatchedFilters,
    /// Shared peers state, maintained by the reconnect tasks. None in
    /// unit tests without a network.
    peers: Option<PeersState>,
    /// In-flight transfer progress, mirrored into `status()`. Keyed by
    /// `peer:dataset` — one entry per parallel send slot.
    transfers: Arc<Mutex<HashMap<String, TransferInfo>>>,
    /// Pause = abort the current transfer (resumable) + suspend
    /// scheduled cycles until `resume`.
    paused: AtomicBool,
    /// Peers queued for a one-shot manual replication.
    manual_requests: Mutex<BTreeSet<String>>,
    /// Cancellation token of the currently running cycle (child of the
    /// job's own token), so cancel/pause can abort mid-transfer.
    cycle_cancel: Mutex<Option<CancellationToken>>,
    /// Filesystems this cycle has not started yet. Cancelling is
    /// meaningful while any remain, even when every running slot has
    /// already handed off to `zfs recv`.
    queued_filesystems: AtomicUsize,
    /// Last known per-peer success and most recent attempt outcome.
    /// Seeded from SQLite on the first cycle, updated after every sync.
    peer_outcomes: Mutex<HashMap<String, PeerOutcome>>,
    outcomes_loaded: AtomicBool,
}

/// The whole cycle's verdict, across every selected peer.
enum CycleOutcome {
    Ok,
    Cancelled,
    Failed(Vec<CycleError>),
}

/// What one replication step did to the receiver.
enum StepOutcome {
    /// Receiver already holds the sender's head: nothing was sent.
    Done,
    /// A stream landed; the receiver may still be behind, re-plan.
    Advanced,
    Failed {
        error: CycleError,
        /// The failed plan was a first full send, so the target dataset
        /// still does not exist on the receiver.
        target_absent: bool,
    },
}

impl PushJob {
    pub fn new(
        config: PushJobConfig,
        peers: Option<PeersState>,
        all_peer_configs: &[PeerConfig],
    ) -> Result<Self, regex::Error> {
        let filter = CompiledFilter::from_config(config.snapshot_filter())?;
        // Validated at config load; a parse failure here is unreachable.
        let limiter = config
            .bandwidth_limit
            .as_deref()
            .and_then(|s| arctern_config::parse_bytes_per_sec(s).ok())
            .map(|rate| Arc::new(RateLimiter::new(rate)));
        let parallel = config.parallel.unwrap_or(1).clamp(1, 4) as usize;
        let peer_configs = all_peer_configs
            .iter()
            .filter(|p| config.targets.contains(&p.name))
            .cloned()
            .collect();
        Ok(Self {
            config,
            limiter,
            parallel,
            peer_configs,
            filter,
            status: Mutex::new(PeriodicStatus::default()),
            wakeup: Arc::new(tokio::sync::Notify::new()),
            unmatched: crate::jobs::UnmatchedFilters::default(),
            peers,
            transfers: Arc::new(Mutex::new(HashMap::new())),
            paused: AtomicBool::new(false),
            manual_requests: Mutex::new(BTreeSet::new()),
            cycle_cancel: Mutex::new(None),
            queued_filesystems: AtomicUsize::new(0),
            peer_outcomes: Mutex::new(HashMap::new()),
            outcomes_loaded: AtomicBool::new(false),
        })
    }

    fn peer_mode(&self, name: &str) -> PeerMode {
        self.peer_configs
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.mode)
            .unwrap_or_default()
    }

    fn peer_auto_interval(&self, name: &str) -> Option<StdDuration> {
        self.peer_configs
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| p.auto_interval)
    }

    /// The cadence a peer is scheduled on: its own `auto_interval`,
    /// else the job's `interval`, else the fallback poll.
    fn cadence_secs(&self, name: &str) -> i64 {
        self.peer_auto_interval(name)
            .or(self.config.interval)
            .unwrap_or(FALLBACK_POLL)
            .as_secs() as i64
    }

    /// A user-cancelled attempt suppresses an immediate automatic retry.
    /// Treat the cancellation time as the cadence anchor while retaining
    /// last_success as the actual recovery point for incremental planning.
    fn peer_schedule_anchor(outcome: &PeerOutcome) -> Option<i64> {
        if outcome.outcome == Some(RunStatus::Cancelled) {
            outcome.last_attempt
        } else {
            outcome.last_success
        }
    }

    /// Live link + active-route snapshot for one named target, if
    /// connected. The bool is the active route's `auto` eligibility.
    fn link_for(&self, name: &str) -> Option<(Arc<PeerLink>, String, bool)> {
        let entry = self.peers.as_ref()?.get(name)?;
        let link = entry.link.clone()?;
        let route = entry.active_route()?;
        Some((link, route.name.clone(), route.auto))
    }

    /// True while any target is connected — used only for the startup
    /// grace wait.
    fn any_link(&self) -> bool {
        let Some(peers) = self.peers.as_ref() else {
            return false;
        };
        self.config
            .targets
            .iter()
            .any(|name| peers.link(name).is_some())
    }

    fn record_cycle(&self, last_error: Option<String>, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.last_run = Some(now);
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.last_error = last_error;
        s.running = false;
    }

    /// A tick where nothing was due. Only `next_run` moves: `last_run`
    /// keeps meaning "last cycle that actually replicated" and
    /// `last_error` must survive idle ticks.
    fn record_idle_tick(&self, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.running = false;
    }

    fn mark_running(&self) {
        self.status.lock().unwrap().running = true;
    }

    async fn expand_filesystems(
        &self,
        runner: &dyn CommandRunner,
    ) -> Result<Vec<String>, ZfsError> {
        let entries = crate::inventory::list_filesystems(runner, &self.config.filesystems).await?;
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        self.unmatched
            .report(&self.config.name, &self.config.filesystems, &names);
        Ok(
            arctern_config::filter::resolve_all(&self.config.filesystems, &names)
                .into_iter()
                .map(str::to_string)
                .collect(),
        )
    }

    /// Seed the per-peer outcome cache from SQLite once per process.
    async fn ensure_outcomes_loaded(&self, ctx: &JobContext) {
        if self.outcomes_loaded.swap(true, Ordering::Relaxed) {
            return;
        }
        let Some(pool) = ctx.state.as_ref() else {
            return;
        };
        if let Ok(rows) = crate::state::push_syncs::for_job(pool, &self.config.name).await {
            let mut o = self.peer_outcomes.lock().unwrap();
            for r in rows {
                o.insert(
                    r.peer,
                    PeerOutcome {
                        last_success: r.last_success_at,
                        last_attempt: Some(r.finished_at),
                        outcome: r.status,
                        message: r.error,
                    },
                );
            }
        }
    }

    /// Decide which targets this cycle replicates to.
    /// - manual requests: always, over whatever route is active (error
    ///   if the peer is unreachable);
    /// - auto peers: when connected over an auto-eligible route AND the
    ///   cadence has elapsed since the last success. A peer without an
    ///   auto-eligible active route is skipped silently: route
    ///   reachability IS the locality policy. Once the last success is
    ///   more than 3x the cadence old that becomes a visible error.
    ///
    /// A queued "send now" outranks pause, but only for the peer it
    /// names: the auto schedule stays suspended.
    fn select_targets(
        &self,
        errors: &mut Vec<CycleError>,
    ) -> Vec<(String, Arc<PeerLink>, &'static str)> {
        let manual: BTreeSet<String> = std::mem::take(&mut *self.manual_requests.lock().unwrap());
        let mut selected = Vec::new();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let paused = self.paused.load(Ordering::Relaxed);
        for name in &self.config.targets {
            let link = self.link_for(name);
            if manual.contains(name) {
                match link {
                    Some((l, route, _)) => {
                        tracing::info!(peer = %name, route = %route, "manual push queued");
                        selected.push((name.clone(), l, "manual"));
                    }
                    None => errors.push(CycleError::PeerNotConnected { peer: name.clone() }),
                }
                continue;
            }
            if paused || self.peer_mode(name) != PeerMode::Auto {
                continue;
            }
            let last_success = self
                .peer_outcomes
                .lock()
                .unwrap()
                .get(name)
                .and_then(Self::peer_schedule_anchor);
            let cadence = self.cadence_secs(name);
            match link {
                Some((l, _route, true)) => {
                    let due = last_success.is_none_or(|ts| now - ts >= cadence);
                    if due {
                        selected.push((name.clone(), l, "auto"));
                    }
                }
                // Connected over a manual-only route, or not connected.
                Some((_, _, false)) | None => {
                    if let Some(ts) = last_success
                        && now - ts > cadence.saturating_mul(3)
                    {
                        errors.push(CycleError::AutoTargetStale {
                            peer: name.clone(),
                            hours: (now - ts) / 3600,
                        });
                    }
                }
            }
        }
        selected
    }

    async fn run_cycle(
        &self,
        ctx: &JobContext,
        cancel: &CancellationToken,
        selected: Vec<(String, Arc<PeerLink>, &'static str)>,
        mut errors: Vec<CycleError>,
    ) -> (u64, CycleOutcome) {
        let mut total_bytes: u64 = 0;
        for (peer_name, link, reason) in selected {
            if cancel.is_cancelled() {
                break;
            }
            tracing::info!(peer = %peer_name, reason, "push: replicating to target");
            let mut peer_errors: Vec<CycleError> = Vec::new();
            let bytes = self
                .run_for_peer(ctx, cancel, &peer_name, &link, &mut peer_errors)
                .await;
            total_bytes += bytes;
            let finished = OffsetDateTime::now_utc().unix_timestamp();
            let (status, message) =
                classify_peer_attempt(cancel.is_cancelled(), self.config.dry_run, &peer_errors);
            if let Some(pool) = ctx.state.as_ref() {
                let _ = crate::state::push_syncs::record(
                    pool,
                    &self.config.name,
                    &peer_name,
                    finished,
                    status,
                    message.as_deref(),
                )
                .await;
            }
            {
                let mut o = self.peer_outcomes.lock().unwrap();
                let entry = o.entry(peer_name.clone()).or_default();
                entry.last_attempt = Some(finished);
                entry.outcome = Some(status);
                entry.message = message;
                if status == RunStatus::Ok {
                    entry.last_success = Some(finished);
                }
            }
            errors.extend(peer_errors);
        }
        let outcome = if errors.iter().any(CycleError::is_cancelled) {
            CycleOutcome::Cancelled
        } else if errors.is_empty() {
            CycleOutcome::Ok
        } else {
            CycleOutcome::Failed(errors)
        };
        (total_bytes, outcome)
    }

    /// Replicate every configured filesystem to one peer. Returns bytes
    /// streamed; errors accumulate into `errors`.
    async fn run_for_peer(
        &self,
        ctx: &JobContext,
        cancel: &CancellationToken,
        peer_name: &str,
        peer: &Arc<PeerLink>,
        errors: &mut Vec<CycleError>,
    ) -> u64 {
        let runner = ctx.zfs.command_runner();
        let sender_paths = match self.expand_filesystems(runner).await {
            Ok(p) => p,
            Err(e) => {
                errors.push(CycleError::ListDatasets(e));
                return 0;
            }
        };
        // Up to `parallel` filesystems replicate concurrently, each on
        // its own recv channel, one depth level at a time: a receive
        // creates its target's missing parent as a placeholder, and a
        // full stream cannot land on an existing dataset, so ancestors
        // finish before any descendant starts. The futures run on this
        // task (no spawn), so borrowing &self is fine.
        let errs = tokio::sync::Mutex::new(Vec::new());
        // Ancestors whose first full send failed this cycle: their
        // descendants are skipped, or they would leave that placeholder.
        let blocked = tokio::sync::Mutex::new(Vec::<String>::new());
        let cycle_bytes = std::sync::atomic::AtomicU64::new(0);
        self.queued_filesystems
            .store(sender_paths.len(), Ordering::Relaxed);
        for level in depth_levels(&sender_paths) {
            futures_util::StreamExt::for_each_concurrent(
                futures_util::stream::iter(level),
                self.parallel,
                |sender_path| {
                    let errs = &errs;
                    let blocked = &blocked;
                    let cycle_bytes = &cycle_bytes;
                    async move {
                        // Claimed here rather than on completion: what makes
                        // cancelling worthwhile is work not yet STARTED.
                        self.queued_filesystems.fetch_sub(1, Ordering::Relaxed);
                        if cancel.is_cancelled() {
                            return;
                        }
                        let ancestor = blocked
                            .lock()
                            .await
                            .iter()
                            .find(|a| is_ancestor(a, sender_path))
                            .cloned();
                        if let Some(ancestor) = ancestor {
                            let e = CycleError::AncestorFailed {
                                dataset: sender_path.to_string(),
                                ancestor,
                            };
                            warn!(error = %e);
                            errs.lock().await.push(e);
                            return;
                        }
                        let (bytes, failed) = self
                            .replicate_one(ctx, cancel, peer_name, peer, sender_path)
                            .await;
                        cycle_bytes.fetch_add(bytes, Ordering::Relaxed);
                        if let Some((error, target_absent)) = failed {
                            if target_absent {
                                blocked.lock().await.push(sender_path.to_string());
                            }
                            errs.lock().await.push(error);
                        }
                    }
                },
            )
            .await;
        }
        self.queued_filesystems.store(0, Ordering::Relaxed);
        errors.extend(errs.into_inner());
        cycle_bytes.into_inner()
    }

    /// Replicate one filesystem to one peer until the receiver holds the
    /// sender's head or a step fails. Returns bytes actually sent and at
    /// most one error, with whether the target still does not exist.
    ///
    /// A step moves the receiver by one plan: a resume finishes only the
    /// snapshot the token names, a full send lands one snapshot, and in
    /// `all` mode a `-I` stream stops at the head that existed when it
    /// was planned. The loop re-plans after every successful step so the
    /// receiver does not sit at an old snapshot until the next scheduled
    /// cycle. Bounded so a snap job racing ahead of the push cannot keep
    /// the cycle alive forever.
    async fn replicate_one(
        &self,
        ctx: &JobContext,
        cancel: &CancellationToken,
        peer_name: &str,
        peer: &Arc<PeerLink>,
        sender_path: &str,
    ) -> (u64, Option<(CycleError, bool)>) {
        const MAX_STEPS_PER_CYCLE: usize = 16;
        let mut total = 0u64;
        for step in 1.. {
            let (bytes, outcome) = self
                .replicate_step(ctx, cancel, peer_name, peer, sender_path)
                .await;
            total += bytes;
            match outcome {
                StepOutcome::Done => return (total, None),
                StepOutcome::Failed {
                    error,
                    target_absent,
                } => return (total, Some((error, target_absent))),
                StepOutcome::Advanced => {}
            }
            if self.config.dry_run || cancel.is_cancelled() {
                return (total, None);
            }
            if step >= MAX_STEPS_PER_CYCLE {
                warn!(
                    sender = %sender_path,
                    steps = step,
                    "push: still behind the sender's head after the per-cycle step limit; continuing next cycle"
                );
                return (total, None);
            }
        }
        unreachable!("the step loop returns from inside");
    }

    /// Plan + execute one step for one filesystem against one peer.
    async fn replicate_step(
        &self,
        ctx: &JobContext,
        cancel: &CancellationToken,
        peer_name: &str,
        peer: &Arc<PeerLink>,
        sender_path: &str,
    ) -> (u64, StepOutcome) {
        let runner = ctx.zfs.command_runner();
        // Literal concat: target = root_fs/sender_path.
        let target = format!("{}/{}", self.config.target.root_fs, sender_path);
        tracing::info!(sender = %sender_path, target = %target, "push: planning");
        let (plan, sender_snaps) = match plan_one_filesystem(
            runner,
            peer.as_ref(),
            sender_path,
            &target,
            &self.filter,
            self.config.replicate,
        )
        .await
        {
            Ok(p) => p,
            Err(source) => {
                let error = CycleError::Plan {
                    dataset: sender_path.to_string(),
                    source,
                };
                warn!(error = %error);
                return (
                    0,
                    StepOutcome::Failed {
                        error,
                        target_absent: false,
                    },
                );
            }
        };
        let target_absent = matches!(plan, SnapshotPlan::Full { .. });
        let failed = |error: CycleError| StepOutcome::Failed {
            error,
            target_absent,
        };
        // If the planner picked discard, send the explicit RPC before
        // opening the recv channel — it's idempotent and makes the recv
        // channel's first action a fresh, clean recv.
        let needs_discard = matches!(
            plan,
            SnapshotPlan::Full {
                discard_partial_recv: true,
                ..
            } | SnapshotPlan::Incremental {
                discard_partial_recv: true,
                ..
            } | SnapshotPlan::IncrementalAll {
                discard_partial_recv: true,
                ..
            } | SnapshotPlan::IncrementalFromBookmark {
                discard_partial_recv: true,
                ..
            }
        );
        if needs_discard {
            if self.config.dry_run {
                tracing::info!(target = %target, "push: dry-run would discard partial receive state");
            } else if let Err(source) = peer.discard_partial_recv(target.clone()).await {
                let error = CycleError::DiscardPartial {
                    target: target.clone(),
                    source,
                };
                warn!(error = %error, "refusing to open recv stream");
                return (0, failed(error));
            }
        }
        let kind = match &plan {
            SnapshotPlan::Nothing => {
                tracing::info!(sender = %sender_path, "push: nothing to do");
                return (0, StepOutcome::Done);
            }
            SnapshotPlan::Full { to, .. } => {
                tracing::info!(sender = %sender_path, to = %to.name, "push: full send");
                TransferKind::Full
            }
            SnapshotPlan::Incremental { from, to, .. } => {
                tracing::info!(
                    sender = %sender_path, from = %from.name, to = %to.name,
                    "push: incremental send"
                );
                TransferKind::Incremental
            }
            SnapshotPlan::IncrementalAll { from, to, .. } => {
                tracing::info!(
                    sender = %sender_path, from = %from.name, to = %to.name,
                    "push: incremental send with every snapshot in between"
                );
                TransferKind::Incremental
            }
            SnapshotPlan::IncrementalFromBookmark { from, to, .. } => {
                tracing::info!(
                    sender = %sender_path, from_bookmark = %from.name, to = %to.name,
                    "push: incremental send from bookmark"
                );
                TransferKind::Incremental
            }
            SnapshotPlan::Resume { decoded, .. } => {
                tracing::info!(
                    sender = %sender_path,
                    to = %decoded.to_name,
                    bytes = decoded.bytes_received,
                    "push: resuming from token"
                );
                TransferKind::Resume
            }
        };
        if self.config.dry_run {
            tracing::info!(sender = %sender_path, target = %target, "push: dry-run skipping execution");
            return (0, StepOutcome::Advanced);
        }
        // Total is a dry-run estimate; resume streams have no cheap one.
        let total = match build_send_args(&plan, sender_path, &self.config.send) {
            Some(args) if kind != TransferKind::Resume => zfskit::send::dry_run(runner, &args)
                .await
                .ok()
                .map(|d| d.total_bytes),
            _ => None,
        };
        let key = format!("{peer_name}:{sender_path}");
        let now = OffsetDateTime::now_utc().unix_timestamp();
        self.transfers.lock().unwrap().insert(
            key.clone(),
            TransferInfo {
                dataset: sender_path.to_string(),
                peer: peer_name.to_string(),
                kind,
                bytes_sent: 0,
                total_bytes: total,
                started_at: now,
                phase: TransferPhase::Sending,
                phase_since: now,
            },
        );
        let step = StepCtx {
            runner,
            peer: peer.as_ref(),
            job_name: &self.config.name,
            peer_name,
            flags: &self.config.send,
            limiter: self.limiter.as_deref(),
            cancel,
            transfers: &self.transfers,
        };
        let res = run_one_filesystem(&step, sender_path, &target, &plan, &sender_snaps, &key).await;
        let bytes = self
            .transfers
            .lock()
            .unwrap()
            .remove(&key)
            .map(|t| t.bytes_sent)
            .unwrap_or(0);
        match res {
            Ok(()) => (bytes, StepOutcome::Advanced),
            Err(StepError::Cancelled) => {
                tracing::info!(sender = %sender_path, "push: transfer cancelled");
                (bytes, failed(CycleError::Cancelled))
            }
            Err(source) => {
                let error = CycleError::Step {
                    dataset: sender_path.to_string(),
                    source,
                };
                warn!(error = %error);
                (bytes, failed(error))
            }
        }
    }

    /// Whether aborting the cycle would still cut short real work. Once
    /// every slot has handed off to `zfs recv` there is nothing left to
    /// interrupt, so cancel/pause degrade to no-ops. Filesystems the
    /// cycle has not started yet count: stopping now spares them.
    fn cancellable_now(&self) -> bool {
        if self.queued_filesystems.load(Ordering::Relaxed) > 0 {
            return true;
        }
        let transfers = self.transfers.lock().unwrap();
        transfers.is_empty()
            || transfers.values().any(|t| {
                !matches!(
                    t.phase,
                    TransferPhase::Finalizing
                        | TransferPhase::Committing
                        | TransferPhase::Cancelling
                )
            })
    }

    /// How long to sleep before the next scheduling decision. Earliest
    /// auto-target due time wins; a due-but-blocked target degrades to
    /// a fixed retry so the loop doesn't spin; no auto targets = the
    /// fallback (manual requests wake us via Notify anyway).
    fn next_wake(&self, fallback: StdDuration) -> StdDuration {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let outcomes = self.peer_outcomes.lock().unwrap();
        let mut earliest: Option<i64> = None;
        for name in &self.config.targets {
            if self.peer_mode(name) != PeerMode::Auto {
                continue;
            }
            let due_at = match outcomes.get(name).and_then(Self::peer_schedule_anchor) {
                Some(ts) => ts + self.cadence_secs(name),
                None => now,
            };
            earliest = Some(earliest.map_or(due_at, |e| e.min(due_at)));
        }
        let nap = match earliest {
            None => fallback,
            Some(at) if at <= now => BLOCKED_RETRY,
            Some(at) => StdDuration::from_secs((at - now) as u64).min(fallback),
        };
        nap.max(StdDuration::from_secs(10))
    }

    async fn run_and_record(
        &self,
        ctx: &JobContext,
        cancel: &CancellationToken,
        interval: StdDuration,
    ) {
        // While paused, scheduled ticks are no-ops, but queued manual
        // requests still run.
        if self.paused.load(Ordering::Relaxed) && self.manual_requests.lock().unwrap().is_empty() {
            return;
        }
        let job_name = &self.config.name;
        self.ensure_outcomes_loaded(ctx).await;
        let mut errors: Vec<CycleError> = Vec::new();
        let selected = self.select_targets(&mut errors);
        // A tick where nothing is due records no job_runs row: a 15m
        // cycle against a 1d auto_interval would otherwise write 96
        // no-op rows a day into the history.
        if selected.is_empty() && errors.is_empty() {
            self.record_idle_tick(interval);
            return;
        }
        self.mark_running();
        // Child token: cancel/pause abort just this cycle, daemon
        // shutdown (the parent) still cancels everything.
        let cycle_token = cancel.child_token();
        *self.cycle_cancel.lock().unwrap() = Some(cycle_token.clone());
        let started_at = OffsetDateTime::now_utc().unix_timestamp();
        let run_id = match ctx.state.as_ref() {
            Some(pool) => crate::state::job_runs::record_start(pool, job_name, started_at)
                .await
                .ok(),
            None => None,
        };
        let (bytes, outcome) = self.run_cycle(ctx, &cycle_token, selected, errors).await;
        *self.cycle_cancel.lock().unwrap() = None;
        let finished_at = OffsetDateTime::now_utc().unix_timestamp();
        let (status, message) = match &outcome {
            CycleOutcome::Ok if self.config.dry_run => (RunStatus::DryRun, None),
            CycleOutcome::Ok => (RunStatus::Ok, None),
            CycleOutcome::Cancelled => (RunStatus::Cancelled, None),
            CycleOutcome::Failed(errors) => (RunStatus::Error, Some(join_errors(errors))),
        };
        if let (Some(pool), Some(run_id)) = (ctx.state.as_ref(), run_id) {
            let _ = crate::state::job_runs::record_finish(
                pool,
                run_id,
                finished_at,
                status,
                message.as_deref(),
                Some(bytes as i64),
            )
            .await;
        }
        self.record_cycle(message, interval);
    }
}

impl Job for PushJob {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn status(&self) -> JobStatus {
        let base = self.status.lock().unwrap().render(&self.config.name);
        let transfers = {
            let g = self.transfers.lock().unwrap();
            let mut v: Vec<TransferInfo> = g.values().cloned().collect();
            v.sort_by(|a, b| (a.started_at, &a.dataset).cmp(&(b.started_at, &b.dataset)));
            v
        };
        let cancellable = self.cycle_cancel.lock().unwrap().is_some() && self.cancellable_now();
        let queued: BTreeSet<String> = self.manual_requests.lock().unwrap().clone();
        let outcomes = self.peer_outcomes.lock().unwrap();
        let targets = self
            .config
            .targets
            .iter()
            .map(|name| {
                let outcome = outcomes.get(name).cloned().unwrap_or_default();
                let entry = self.peers.as_ref().and_then(|p| p.get(name));
                let route = entry.as_ref().and_then(|e| e.active_route().cloned());
                TargetStatus {
                    peer: name.clone(),
                    mode: match self.peer_mode(name) {
                        PeerMode::Auto => arctern_api::PeerMode::Auto,
                        PeerMode::Manual => arctern_api::PeerMode::Manual,
                    },
                    connected: entry.as_ref().is_some_and(|e| e.link.is_some()),
                    route: route.as_ref().map(|r| r.name.clone()),
                    route_auto: route.as_ref().is_some_and(|r| r.auto),
                    manual_queued: queued.contains(name),
                    auto_interval_secs: self.peer_auto_interval(name).map(|d| d.as_secs()),
                    last_success: outcome.last_success,
                    last_attempt: outcome.last_attempt,
                    last_outcome: outcome.outcome,
                    last_message: outcome.message.clone(),
                    last_error: (outcome.outcome == Some(RunStatus::Error))
                        .then_some(outcome.message)
                        .flatten(),
                }
            })
            .collect();
        JobStatus::Push(PushJobStatus {
            name: base.name,
            last_run: base.last_run,
            next_run: base.next_run,
            last_error: base.last_error,
            running: base.running,
            paused: self.paused.load(Ordering::Relaxed),
            cancellable,
            dry_run: self.config.dry_run,
            transfers,
            targets,
        })
    }

    fn wakeup(&self) {
        self.wakeup.notify_one();
    }

    fn cancel_current(&self) -> ControlOutcome {
        let token = self.cycle_cancel.lock().unwrap().clone();
        match token {
            Some(token) if self.cancellable_now() => {
                token.cancel();
                ControlOutcome::Applied
            }
            _ => ControlOutcome::Unsupported,
        }
    }

    fn pause(&self) -> ControlOutcome {
        self.paused.store(true, Ordering::Relaxed);
        if self.cancellable_now()
            && let Some(tok) = self.cycle_cancel.lock().unwrap().as_ref()
        {
            tok.cancel();
        }
        ControlOutcome::Applied
    }

    fn resume(&self) -> ControlOutcome {
        self.paused.store(false, Ordering::Relaxed);
        self.wakeup.notify_one();
        ControlOutcome::Applied
    }

    fn request_push(&self, peer: &str) -> Result<(), PushRequestError> {
        if !self.config.targets.iter().any(|t| t == peer) {
            return Err(PushRequestError::NotATarget {
                job: self.config.name.clone(),
                peer: peer.to_string(),
            });
        }
        self.manual_requests
            .lock()
            .unwrap()
            .insert(peer.to_string());
        self.wakeup.notify_one();
        Ok(())
    }

    fn run(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let span = info_span!("push_job", name = %self.config.name);
        Box::pin(
            async move {
                let interval = self.config.interval.unwrap_or(FALLBACK_POLL);
                // A push cycle needs a connected peer: give the eager
                // reconnect tasks a short grace so a daemon restart does
                // not immediately record a "peer not connected" run. If
                // nothing connects within the grace, run anyway; the
                // error is accurate and visible.
                const CONNECT_GRACE: StdDuration = StdDuration::from_secs(30);
                let deadline = tokio::time::Instant::now() + CONNECT_GRACE;
                while !self.any_link() && tokio::time::Instant::now() < deadline {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = sleep(StdDuration::from_secs(1)) => {}
                    }
                }
                self.run_and_record(&ctx, &cancel, interval).await;
                // Event-driven: sleep until the earliest auto target is
                // due; wake early on a manual request or a peer
                // connectivity change. `interval` only bounds the blind
                // sleep.
                let mut peers_rx = self.peers.as_ref().map(PeersState::subscribe);
                loop {
                    let nap = self.next_wake(interval);
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = sleep(nap) => {}
                        _ = self.wakeup.notified() => {}
                        _ = async {
                            match peers_rx.as_mut() {
                                Some(rx) => { let _ = rx.changed().await; }
                                None => std::future::pending::<()>().await,
                            }
                        } => {}
                    }
                    self.run_and_record(&ctx, &cancel, interval).await;
                }
            }
            .instrument(span),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(targets: &[&str]) -> PushJobConfig {
        let list = targets
            .iter()
            .map(|t| format!("{t:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        toml::from_str(&format!(
            r#"
name = "push_test"
targets = [{list}]
filesystems = {{ "novafs/data<" = true }}
target = {{ root_fs = "backup/nova" }}
"#
        ))
        .expect("test config parses")
    }

    fn test_job(targets: &[&str]) -> PushJob {
        PushJob::new(test_config(targets), None, &[]).expect("job builds")
    }

    fn test_job_with_interval(interval: &str) -> PushJob {
        let cfg: PushJobConfig = toml::from_str(&format!(
            r#"
name = "push_test"
targets = ["mira"]
interval = "{interval}"
filesystems = {{ "novafs/data<" = true }}
target = {{ root_fs = "backup/nova" }}
"#
        ))
        .expect("test config parses");
        PushJob::new(cfg, None, &[]).expect("job builds")
    }

    fn record_success(job: &PushJob, peer: &str, at: i64) {
        let mut o = job.peer_outcomes.lock().unwrap();
        let e = o.entry(peer.to_string()).or_default();
        e.last_success = Some(at);
        e.last_attempt = Some(at);
        e.outcome = Some(RunStatus::Ok);
    }

    fn push_status(job: &PushJob) -> PushJobStatus {
        match job.status() {
            JobStatus::Push(s) => s,
            other => panic!("push job reported {other:?}"),
        }
    }

    // A peer with no `auto_interval` was due on every wake, so it
    // replicated on the loop's retry floor and the job's own `interval`
    // never applied.
    #[tokio::test]
    async fn a_peer_without_its_own_interval_follows_the_jobs_interval() {
        let job = test_job_with_interval("1h");
        let now = OffsetDateTime::now_utc().unix_timestamp();
        record_success(&job, "mira", now - 60);

        let nap = job.next_wake(StdDuration::from_secs(24 * 3600));
        assert!(
            nap > BLOCKED_RETRY,
            "slept {nap:?}, which is the retry floor rather than the configured interval"
        );
        assert!(
            nap <= StdDuration::from_secs(3600),
            "slept too long: {nap:?}"
        );
    }

    #[tokio::test]
    async fn a_peer_that_has_never_synced_is_due_at_once() {
        let job = test_job_with_interval("1h");
        assert_eq!(
            job.next_wake(StdDuration::from_secs(24 * 3600)),
            BLOCKED_RETRY
        );
    }

    // No PeerLink can be built without a real SSH session, so the auto
    // branch is observed through its other side effect: an auto target
    // whose last success is far past its cadence reports staleness.
    #[tokio::test]
    async fn a_manual_push_does_not_resume_a_paused_jobs_schedule() {
        let long_ago = OffsetDateTime::now_utc().unix_timestamp() - 10 * 86_400;

        let running = test_job(&["mira", "elsewhere"]);
        record_success(&running, "elsewhere", long_ago);
        let mut errors = Vec::new();
        running.select_targets(&mut errors);
        assert!(
            errors.iter().any(
                |e| matches!(e, CycleError::AutoTargetStale { peer, .. } if peer == "elsewhere")
            ),
            "a running job must still consider its auto targets: {errors:?}"
        );

        let paused = test_job(&["mira", "elsewhere"]);
        record_success(&paused, "elsewhere", long_ago);
        paused.pause();
        paused.request_push("mira").expect("mira is a target");
        let mut errors = Vec::new();
        let selected = paused.select_targets(&mut errors);

        let names: Vec<&str> = selected.iter().map(|(n, _, _)| n.as_str()).collect();
        assert!(
            names.is_empty(),
            "paused job selected {names:?} for a scheduled sync"
        );
        assert!(
            !errors
                .iter()
                .any(|e| matches!(e, CycleError::AutoTargetStale { .. })),
            "paused job still evaluated its auto schedule: {errors:?}"
        );
    }

    fn transfer(peer: &str, phase: TransferPhase) -> TransferInfo {
        TransferInfo {
            dataset: "novafs/data".into(),
            peer: peer.into(),
            kind: TransferKind::Incremental,
            bytes_sent: 1,
            total_bytes: None,
            started_at: 0,
            phase,
            phase_since: 0,
        }
    }

    #[tokio::test]
    async fn manual_request_is_visible_in_status_until_the_cycle_drains_it() {
        let job = test_job(&["mira"]);
        assert!(!push_status(&job).targets[0].manual_queued);

        job.request_push("mira").expect("mira is a target");
        assert!(push_status(&job).targets[0].manual_queued);

        let mut errors = Vec::new();
        job.select_targets(&mut errors);
        assert!(
            !push_status(&job).targets[0].manual_queued,
            "select_targets drains the request set, so the flag must clear with it"
        );
    }

    #[test]
    fn manual_request_rejects_a_peer_that_is_not_a_target() {
        let job = test_job(&["mira"]);
        assert_eq!(
            job.request_push("nowhere"),
            Err(PushRequestError::NotATarget {
                job: "push_test".into(),
                peer: "nowhere".into()
            })
        );
        assert!(!push_status(&job).targets[0].manual_queued);
    }

    #[test]
    fn cancellable_is_false_without_a_running_cycle() {
        let job = test_job(&["mira"]);
        assert!(!push_status(&job).cancellable);
        assert_eq!(job.cancel_current(), ControlOutcome::Unsupported);
    }

    // With several filesystems in a cycle, the last running slot can sit
    // in finalizing while the rest of the queue has not started.
    // Cancelling then spares everything still queued.
    #[test]
    fn a_cycle_with_queued_filesystems_can_still_be_stopped() {
        let job = test_job(&["mira"]);
        *job.cycle_cancel.lock().unwrap() = Some(CancellationToken::new());
        job.transfers
            .lock()
            .unwrap()
            .insert("a".into(), transfer("mira", TransferPhase::Finalizing));

        assert!(!push_status(&job).cancellable);

        job.queued_filesystems.store(2, Ordering::Relaxed);
        assert!(
            push_status(&job).cancellable,
            "queued filesystems make cancelling worthwhile"
        );
        assert_eq!(job.cancel_current(), ControlOutcome::Applied);
    }

    #[test]
    fn cancellable_tracks_the_transfer_phases() {
        let job = test_job(&["mira"]);
        *job.cycle_cancel.lock().unwrap() = Some(CancellationToken::new());

        assert!(push_status(&job).cancellable);

        job.transfers
            .lock()
            .unwrap()
            .insert("a".into(), transfer("mira", TransferPhase::Sending));
        assert!(push_status(&job).cancellable);

        job.transfers
            .lock()
            .unwrap()
            .insert("a".into(), transfer("mira", TransferPhase::Finalizing));
        assert!(!push_status(&job).cancellable);
        assert_eq!(job.cancel_current(), ControlOutcome::Unsupported);

        job.transfers
            .lock()
            .unwrap()
            .insert("b".into(), transfer("mira", TransferPhase::WaitingReceiver));
        assert!(push_status(&job).cancellable);
        assert_eq!(job.cancel_current(), ControlOutcome::Applied);
    }

    // With `parallel >= 2` a child could start before its parent and create
    // the parent as a placeholder, after which the parent's full stream
    // was refused forever. Depth levels run in sequence, so that cannot
    // happen; order within a level is the listing order.
    #[test]
    fn filesystems_are_scheduled_parents_first() {
        let paths: Vec<String> = [
            "tank/a/b/c",
            "tank/a",
            "tank/z",
            "tank",
            "tank/a/b",
            "tank/z/y",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            depth_levels(&paths),
            vec![
                vec!["tank"],
                vec!["tank/a", "tank/z"],
                vec!["tank/a/b", "tank/z/y"],
                vec!["tank/a/b/c"],
            ]
        );
        assert!(depth_levels(&[]).is_empty());
    }

    #[test]
    fn ancestry_is_by_path_component() {
        assert!(is_ancestor("tank/a", "tank/a/b"));
        assert!(is_ancestor("tank", "tank/a/b/c"));
        assert!(!is_ancestor("tank/a", "tank/ab"));
        assert!(!is_ancestor("tank/a", "tank/a"));
        assert!(!is_ancestor("tank/a/b", "tank/a"));
    }

    #[test]
    fn cancellation_is_not_a_peer_error() {
        assert_eq!(
            classify_peer_attempt(true, false, &[CycleError::Cancelled]),
            (RunStatus::Cancelled, None)
        );
    }

    // The token can clear between the step reporting and the classifier
    // reading it, so the variant has to carry the verdict on its own.
    #[test]
    fn a_cancelled_step_outranks_a_token_that_already_cleared() {
        assert_eq!(
            classify_peer_attempt(
                false,
                false,
                &[CycleError::Step {
                    dataset: "tank/a".into(),
                    source: StepError::Cancelled
                }]
            ),
            (RunStatus::Cancelled, None)
        );
    }

    // `ok` becomes `last_success_at`, which schedules the next auto sync
    // and paints the target "synced". A plan-only cycle sent nothing and
    // must not earn either.
    #[test]
    fn a_clean_dry_run_is_not_a_sync() {
        assert_eq!(
            classify_peer_attempt(false, true, &[]),
            (RunStatus::DryRun, None)
        );
        assert_eq!(
            classify_peer_attempt(false, true, &[CycleError::Cancelled]),
            (RunStatus::Cancelled, None)
        );
        assert_eq!(
            classify_peer_attempt(
                false,
                true,
                &[CycleError::PeerNotConnected {
                    peer: "mira".into()
                }]
            )
            .0,
            RunStatus::Error
        );
    }

    #[test]
    fn a_dry_run_job_says_so_in_its_status() {
        let cfg: PushJobConfig = toml::from_str(
            r#"
name = "push_test"
targets = ["mira"]
dry_run = true
filesystems = { "novafs/data" = true }
target = { root_fs = "backup/nova" }
"#,
        )
        .expect("test config parses");
        let job = PushJob::new(cfg, None, &[]).expect("job builds");
        assert!(push_status(&job).dry_run);
        assert!(!push_status(&test_job(&["mira"])).dry_run);
    }

    // A receiver's own wording that happens to contain "cancelled" must
    // stay an error: the verdict lives in the variant, not the text.
    #[test]
    fn a_failure_mentioning_cancellation_is_still_a_failure() {
        let (status, message) = classify_peer_attempt(
            false,
            false,
            &[CycleError::Step {
                dataset: "tank/a".into(),
                source: StepError::Receiver {
                    code: arctern_transport::ErrorCode::Zfs,
                    message: "cancelled".into(),
                },
            }],
        );
        assert_eq!(status, RunStatus::Error);
        assert_eq!(
            message.as_deref(),
            Some("execute tank/a: receiver: cancelled")
        );
    }

    #[test]
    fn several_failures_are_reported_together() {
        let (status, message) = classify_peer_attempt(
            false,
            false,
            &[
                CycleError::PeerNotConnected {
                    peer: "mira".into(),
                },
                CycleError::AncestorFailed {
                    dataset: "tank/a/b".into(),
                    ancestor: "tank/a".into(),
                },
            ],
        );
        assert_eq!(status, RunStatus::Error);
        assert_eq!(
            message.as_deref(),
            Some(
                "manual push to \"mira\": peer not connected; tank/a/b: skipped, the first full \
                 send of tank/a failed this cycle"
            )
        );
    }

    #[test]
    fn a_clean_peer_attempt_is_ok() {
        assert_eq!(
            classify_peer_attempt(false, false, &[]),
            (RunStatus::Ok, None)
        );
    }
}
