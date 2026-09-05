//! Push job — active sender. Each cycle, for every configured filesystem:
//! list local matching snapshots, ask the receiver via the SSH control
//! channel what it has, intersect by GUID, then open a recv channel and
//! pipe `zfs send`'s stdout into it.
//!
//! The planner (`pick_plan`, `pick_plan_with_token`,
//! `apply_bookmark_fallback`, `build_send_header`, `build_send_args`,
//! `CompiledFilter`) is pure; the executor drives it over
//! `peer::PeerLink`.
//!
//! Holds and cursor bookmarks (ARCHITECTURE.md "Holds and replication
//! cursor"):
//!
//!   - Step hold tag `arctern_step_J_<jobname>_P_<peer>` is placed on
//!     the `to` snapshot before the send begins. On success the tag is
//!     swept from every filtered snapshot of the dataset (the current
//!     `to` plus stale holds left by earlier failed cycles); on failure
//!     it stays so a retry can find the snapshot regardless of
//!     intervening prune.
//!   - Cursor bookmark `<dataset>#arctern_cursor_G_<guid>_J_<job>_P_<peer>`
//!     is created from the new `to` snapshot on success; previous
//!     cursors (same job/peer suffix, different GUID) are destroyed
//!     after the new one lands, so the transition is crash-safe.
//!     When sender and receiver share no common snapshot, the planner
//!     falls back to an incremental send based on any bookmark whose
//!     GUID the receiver still has (see `apply_bookmark_fallback`).
//!
//! Split across `plan` (pure planning), `step` (executing one
//! planned send), `holds` (step holds + cursor bookmark) and
//! `limiter` (the shared token bucket); this file is the job itself:
//! scheduling, target selection and status.

mod holds;
mod limiter;
pub mod plan;
mod step;

pub use plan::CompiledFilter;

use arctern_transport::regex;

use plan::{SnapshotPlan, build_send_args};
pub use step::StepError;

use limiter::RateLimiter;
use plan::plan_one_filesystem;
use step::{StepCtx, run_one_filesystem};

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use arctern_api::{TargetStatus, TransferInfo};
use arctern_config::{PeerConfig, PeerMode, PushJobConfig};
use time::OffsetDateTime;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};
use zfskit::dataset::ListOptions;
use zfskit::models::DatasetType;
use zfskit::runner::CommandRunner;

use super::{Job, JobContext, JobStatusInner};
use crate::peer::PeerLink;
use crate::peer::state::PeersState;

/// Per-peer scheduling anchor plus the most recent attempt shown in the UI.
#[derive(Debug, Clone, Default)]
struct PeerOutcome {
    last_success: Option<i64>,
    last_attempt: Option<i64>,
    outcome: Option<String>,
    message: Option<String>,
}

type PeerOutcomes = HashMap<String, PeerOutcome>;

/// One peer's attempt, as recorded in `push_syncs` and shown per target.
///
/// Cancellation wins over any accumulated messages: a cancelled step
/// reports whatever it managed to say on the way out, and calling that a
/// failure is how a routine `systemctl restart` used to paint the job
/// red. `cancelled` covers the case where the token fired between steps,
/// so no step got to report it.
///
/// A dry run that went through cleanly is `dry_run`, not `ok`: `ok` is
/// what `push_syncs` turns into `last_success_at`, which drives the auto
/// schedule and the "synced 2h ago" the console shows. A job in plan-only
/// mode has replicated nothing, and used to report exactly that as a
/// healthy sync.
fn classify_peer_attempt(
    cancelled: bool,
    dry_run: bool,
    errors: &[StepError],
) -> (&'static str, Option<String>) {
    if cancelled || errors.iter().any(|e| matches!(e, StepError::Cancelled)) {
        ("cancelled", None)
    } else if errors.is_empty() {
        (if dry_run { "dry_run" } else { "ok" }, None)
    } else {
        (
            "error",
            Some(
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; "),
            ),
        )
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

/// Safety-net poll when nothing is due and no signal arrives.
pub const KIND: &str = arctern_api::JOB_KIND_PUSH;

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
    /// Bumped by the reconnect tasks on any peer state change so the
    /// scheduler re-evaluates due-ness the moment a link appears.
    peers_changed: Option<tokio::sync::watch::Receiver<u64>>,
    /// `[[peers]]` entries for this job's targets (mode, auto_interval).
    peer_configs: Vec<PeerConfig>,
    filter: CompiledFilter,
    status: Mutex<JobStatusInner>,
    wakeup: Arc<tokio::sync::Notify>,
    unmatched: crate::jobs::UnmatchedFilters,
    /// Shared peers state. Each cycle looks up the configured peer name
    /// here so that a reconnect performed by the background task takes
    /// effect on the next cycle without restarting the job.
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
    /// already handed off to `zfs recv` — it stops the rest from
    /// starting.
    queued_filesystems: AtomicUsize,
    /// Last known per-peer success and most recent attempt outcome.
    /// Seeded from SQLite on the first cycle, updated after every sync.
    peer_outcomes: Mutex<PeerOutcomes>,
    outcomes_loaded: AtomicBool,
}

impl PushJob {
    pub fn new(
        config: PushJobConfig,
        peers: Option<PeersState>,
        all_peer_configs: &[PeerConfig],
        peers_changed: Option<tokio::sync::watch::Receiver<u64>>,
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
            peers_changed,
            peer_configs,
            filter,
            status: Mutex::new(JobStatusInner::default()),
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

    /// A user-cancelled attempt suppresses an immediate automatic retry.
    /// Treat the cancellation time as the cadence anchor while retaining
    /// last_success as the actual recovery point for incremental planning.
    fn peer_schedule_anchor(outcome: &PeerOutcome) -> Option<i64> {
        if outcome.outcome.as_deref() == Some("cancelled") {
            outcome.last_attempt
        } else {
            outcome.last_success
        }
    }

    /// Live link + active-route snapshot for one named target, if
    /// connected. The bool is the active route's `auto` eligibility.
    async fn link_for(&self, name: &str) -> Option<(Arc<PeerLink>, String, bool)> {
        let peers = self.peers.as_ref()?;
        let g = peers.read().await;
        let entry = g.get(name)?;
        let link = entry.link.clone()?;
        let route = entry.active_route()?;
        Some((link, route.name.clone(), route.auto))
    }

    /// True while any target is connected — used only for the startup
    /// grace wait.
    async fn any_link(&self) -> bool {
        let Some(peers) = self.peers.as_ref() else {
            return false;
        };
        let g = peers.read().await;
        self.config
            .targets
            .iter()
            .any(|name| g.get(name).is_some_and(|e| e.link.is_some()))
    }

    fn record_cycle(&self, last_error: Option<String>, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.last_run = Some(now);
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.last_error = last_error;
        s.running = false;
    }

    /// A tick where nothing was due. Only `next_run` moves — `last_run`
    /// keeps meaning "last cycle that actually replicated" and
    /// `last_error` must survive idle ticks (overwriting it here would
    /// silently clear a real failure 15 minutes later).
    fn record_idle_tick(&self, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.running = false;
    }

    fn mark_running(&self) {
        self.status.lock().unwrap().running = true;
    }

    async fn expand_filesystems(&self, runner: &dyn CommandRunner) -> Result<Vec<String>, String> {
        let mut pools: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for f in &self.config.filesystems {
            let pool = f.path.split('/').next().unwrap_or(&f.path).to_string();
            pools.insert(pool);
        }
        let opts = ListOptions {
            recursive: true,
            types: vec![DatasetType::Filesystem, DatasetType::Volume],
            roots: pools.into_iter().collect(),
            ..ListOptions::default()
        };
        let entries = zfskit::dataset::list(runner, &opts)
            .await
            .map_err(|e| format!("list datasets: {e}"))?;
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
                        outcome: Some(r.status),
                        message: r.error,
                    },
                );
            }
        }
    }

    /// Decide which targets this cycle replicates to.
    /// - manual requests: always, over whatever route is active (error
    ///   if the peer is unreachable);
    /// - auto peers: when connected over an auto-eligible route AND
    ///   `auto_interval` has elapsed since the last success. A peer
    ///   without an auto-eligible active route is skipped silently —
    ///   route reachability IS the locality policy (a LAN-only route is
    ///   "am I home?"; a metered WG route carries manual pushes only) —
    ///   unless the last success is more than 3x the expected cadence
    ///   old, which becomes a visible error regardless of why auto
    ///   couldn't run.
    async fn select_targets(
        &self,
        errors: &mut Vec<StepError>,
    ) -> Vec<(String, Arc<PeerLink>, &'static str)> {
        let manual: BTreeSet<String> = std::mem::take(&mut *self.manual_requests.lock().unwrap());
        let mut selected = Vec::new();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        // A queued "send now" outranks pause, but only for the peer it
        // names: the cycle it wakes used to sweep up every due auto
        // target too, so one manual push to one peer quietly resumed a
        // paused job's whole schedule.
        let paused = self.paused.load(Ordering::Relaxed);
        for name in &self.config.targets {
            let link = self.link_for(name).await;
            if manual.contains(name) {
                match link {
                    Some((l, route, _)) => {
                        tracing::info!(peer = %name, route = %route, "manual push queued");
                        selected.push((name.clone(), l, "manual"));
                    }
                    None => errors.push(StepError::Failed(format!(
                        "manual push to {name:?}: peer not connected"
                    ))),
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
            let cadence = self
                .peer_auto_interval(name)
                .or(self.config.interval)
                .unwrap_or(FALLBACK_POLL)
                .as_secs() as i64;
            let auto_link = match link {
                Some((l, _route, true)) => Some(l),
                // Connected, but the active route is manual-only.
                Some((_, _, false)) | None => None,
            };
            match auto_link {
                Some(l) => {
                    // `cadence` already falls back to the job's `interval`
                    // and then to FALLBACK_POLL. Reading only
                    // `auto_interval` here made a peer without one due on
                    // every wake, so it replicated on the loop's retry
                    // floor and the job's own `interval` never applied.
                    let due = match last_success {
                        None => true,
                        Some(ts) => now - ts >= cadence,
                    };
                    if due {
                        selected.push((name.clone(), l, "auto"));
                    }
                }
                None => {
                    if let Some(ts) = last_success
                        && now - ts > cadence.saturating_mul(3)
                    {
                        errors.push(StepError::Failed(format!(
                            "auto target {name:?} has no auto-eligible route and last successful sync is {}h old",
                            (now - ts) / 3600
                        )));
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
        mut errors: Vec<StepError>,
    ) -> (u64, Result<(), StepError>) {
        let mut total_bytes: u64 = 0;
        for (peer_name, link, reason) in selected {
            if cancel.is_cancelled() {
                break;
            }
            tracing::info!(peer = %peer_name, reason, "push: replicating to target");
            let mut peer_errors: Vec<StepError> = Vec::new();
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
                entry.outcome = Some(status.into());
                entry.message = message;
                if status == "ok" {
                    entry.last_success = Some(finished);
                }
            }
            errors.extend(peer_errors);
        }
        // The run is cancelled if any step was; otherwise its message is
        // everything that went wrong, joined.
        let result = if errors.iter().any(|e| matches!(e, StepError::Cancelled)) {
            Err(StepError::Cancelled)
        } else if errors.is_empty() {
            Ok(())
        } else {
            Err(StepError::Failed(
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; "),
            ))
        };
        (total_bytes, result)
    }

    /// Replicate every configured filesystem to one peer. Returns bytes
    /// streamed; errors accumulate into `errors`.
    async fn run_for_peer(
        &self,
        ctx: &JobContext,
        cancel: &CancellationToken,
        peer_name: &str,
        peer: &Arc<PeerLink>,
        errors: &mut Vec<StepError>,
    ) -> u64 {
        let runner = ctx.zfs.command_runner();
        let sender_paths = match self.expand_filesystems(runner).await {
            Ok(p) => p,
            Err(e) => {
                errors.push(StepError::Failed(e));
                return 0;
            }
        };
        // Up to `parallel` filesystems replicate concurrently, each on
        // its own recv channel. The futures run on this task (no
        // spawn), so borrowing &self is fine; the shared RateLimiter
        // keeps the aggregate under bandwidth_limit.
        //
        // One depth level at a time. A receive creates its target's parent
        // when that is missing, so a child that starts before its parent
        // leaves a placeholder where the parent's own full stream has to
        // land — and `zfs recv` refuses a full stream over an existing
        // dataset, every cycle. Ancestors therefore finish before any
        // descendant starts; within a level nothing depends on anything.
        let errs = tokio::sync::Mutex::new(Vec::new());
        let cycle_bytes = std::sync::atomic::AtomicU64::new(0);
        self.queued_filesystems
            .store(sender_paths.len(), Ordering::Relaxed);
        for level in depth_levels(&sender_paths) {
            futures_util::StreamExt::for_each_concurrent(
                futures_util::stream::iter(level),
                self.parallel,
                |sender_path| {
                    let errs = &errs;
                    let cycle_bytes = &cycle_bytes;
                    async move {
                        // Claimed here rather than on completion: what makes
                        // cancelling worthwhile is work not yet STARTED.
                        self.queued_filesystems.fetch_sub(1, Ordering::Relaxed);
                        if cancel.is_cancelled() {
                            return;
                        }
                        let (bytes, err) = self
                            .replicate_one(ctx, cancel, peer_name, peer, sender_path)
                            .await;
                        cycle_bytes.fetch_add(bytes, Ordering::Relaxed);
                        if let Some(e) = err {
                            errs.lock().await.push(e);
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
    /// most one error message.
    ///
    /// A step moves the receiver by one plan: a resume finishes only the
    /// snapshot the token names, a full send lands one snapshot, and in
    /// `all` mode a `-I` stream stops at the head that existed when it
    /// was planned. Each of those used to be the whole cycle, so after a
    /// resume or a long first sync the receiver sat at an old snapshot
    /// until the next scheduled cycle — up to `auto_interval` later. The
    /// loop re-plans after every successful step instead. Bounded so a
    /// snap job racing ahead of the push cannot keep the cycle alive
    /// forever.
    async fn replicate_one(
        &self,
        ctx: &JobContext,
        cancel: &CancellationToken,
        peer_name: &str,
        peer: &Arc<PeerLink>,
        sender_path: &str,
    ) -> (u64, Option<StepError>) {
        const MAX_STEPS_PER_CYCLE: usize = 16;
        let mut total = 0u64;
        for step in 1.. {
            let (bytes, outcome) = self
                .replicate_step(ctx, cancel, peer_name, peer, sender_path)
                .await;
            total += bytes;
            match outcome {
                StepOutcome::Done => return (total, None),
                StepOutcome::Failed(e) => return (total, Some(e)),
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
        // FR-005: literal concat — target = root_fs/sender_path.
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
            Err(e) => {
                let msg = format!("plan {sender_path}: {e}");
                warn!(error = %msg);
                return (0, StepOutcome::Failed(StepError::Failed(msg)));
            }
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
            } else if let Err(e) = peer.discard_partial_recv(target.clone()).await {
                let msg = format!("discard partial receive {target}: {e}");
                warn!(target = %target, error = %e, "discard_partial_recv RPC failed; refusing to open recv stream");
                return (0, StepOutcome::Failed(StepError::Failed(msg)));
            }
        }
        match &plan {
            SnapshotPlan::Nothing => {
                tracing::info!(sender = %sender_path, "push: nothing to do");
                return (0, StepOutcome::Done);
            }
            SnapshotPlan::Full { to, .. } => {
                tracing::info!(sender = %sender_path, to = %to.name, "push: full send");
            }
            SnapshotPlan::Incremental { from, to, .. } => {
                tracing::info!(
                    sender = %sender_path, from = %from.name, to = %to.name,
                    "push: incremental send"
                );
            }
            SnapshotPlan::IncrementalAll { from, to, .. } => {
                tracing::info!(
                    sender = %sender_path, from = %from.name, to = %to.name,
                    "push: incremental send with every snapshot in between"
                );
            }
            SnapshotPlan::IncrementalFromBookmark { from, to, .. } => {
                tracing::info!(
                    sender = %sender_path, from_bookmark = %from.name, to = %to.name,
                    "push: incremental send from bookmark"
                );
            }
            SnapshotPlan::Resume { decoded, .. } => {
                tracing::info!(
                    sender = %sender_path,
                    to = %decoded.to_name,
                    bytes = decoded.bytes_received,
                    "push: resuming from token"
                );
            }
        }
        if self.config.dry_run {
            tracing::info!(sender = %sender_path, target = %target, "push: dry-run skipping execution");
            return (0, StepOutcome::Advanced);
        }
        // Publish transfer info for the UI. Total is a dry-run
        // estimate; resume streams have no cheap estimate.
        let kind = match &plan {
            SnapshotPlan::Full { .. } => "full",
            SnapshotPlan::Incremental { .. }
            | SnapshotPlan::IncrementalAll { .. }
            | SnapshotPlan::IncrementalFromBookmark { .. } => "incremental",
            SnapshotPlan::Resume { .. } => "resume",
            SnapshotPlan::Nothing => unreachable!("filtered above"),
        };
        let total = match build_send_args(&plan, sender_path, &self.config.send) {
            Some(args) if kind != "resume" => zfskit::send::dry_run(runner, &args)
                .await
                .ok()
                .map(|d| d.total_bytes),
            _ => None,
        };
        let key = format!("{peer_name}:{sender_path}");
        self.transfers.lock().unwrap().insert(
            key.clone(),
            TransferInfo {
                dataset: sender_path.to_string(),
                peer: peer_name.to_string(),
                kind: kind.to_string(),
                bytes_sent: 0,
                total_bytes: total,
                started_at: OffsetDateTime::now_utc().unix_timestamp(),
                phase: "sending".into(),
                phase_since: OffsetDateTime::now_utc().unix_timestamp(),
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
                (bytes, StepOutcome::Failed(StepError::Cancelled))
            }
            Err(e) => {
                // Context is added to the message, never around the
                // variant — wrapping used to erase the cancelled case.
                let msg = format!("execute {sender_path}: {e}");
                warn!(error = %msg);
                (bytes, StepOutcome::Failed(StepError::Failed(msg)))
            }
        }
    }
}

/// What one replication step did to the receiver.
enum StepOutcome {
    /// Receiver already holds the sender's head: nothing was sent.
    Done,
    /// A stream landed; the receiver may still be behind, re-plan.
    Advanced,
    Failed(StepError),
}

impl Job for PushJob {
    fn name(&self) -> &str {
        &self.config.name
    }
    fn kind(&self) -> &'static str {
        KIND
    }
    fn status(&self) -> JobStatusInner {
        let mut s = self.status.lock().unwrap().clone();
        s.paused = self.paused.load(Ordering::Relaxed);
        s.dry_run = self.config.dry_run;
        s.transfers = {
            let g = self.transfers.lock().unwrap();
            let mut v: Vec<TransferInfo> = g.values().cloned().collect();
            v.sort_by(|a, b| (a.started_at, &a.dataset).cmp(&(b.started_at, &b.dataset)));
            v
        };
        s.cancellable = self.cycle_cancel.lock().unwrap().is_some() && self.cancellable_now();
        // Best-effort snapshot via try_read: status() is sync and the
        // peers map is an async RwLock; a missed read shows the peer as
        // disconnected for one 5s poll — harmless.
        type RouteSnap = (bool, Option<String>, bool);
        let connected: HashMap<String, RouteSnap> = match self.peers.as_ref() {
            Some(p) => match p.try_read() {
                Ok(g) => g
                    .iter()
                    .map(|(name, e)| {
                        let route = e.active_route();
                        (
                            name.clone(),
                            (
                                e.link.is_some(),
                                route.map(|r| r.name.clone()),
                                route.is_some_and(|r| r.auto),
                            ),
                        )
                    })
                    .collect(),
                Err(_) => HashMap::new(),
            },
            None => HashMap::new(),
        };
        let queued: BTreeSet<String> = self.manual_requests.lock().unwrap().clone();
        let outcomes = self.peer_outcomes.lock().unwrap();
        s.targets = self
            .config
            .targets
            .iter()
            .map(|name| {
                let outcome = outcomes.get(name).cloned().unwrap_or_default();
                let (is_connected, route, route_auto) =
                    connected.get(name).cloned().unwrap_or((false, None, false));
                TargetStatus {
                    peer: name.clone(),
                    mode: match self.peer_mode(name) {
                        PeerMode::Auto => "auto".into(),
                        PeerMode::Manual => "manual".into(),
                    },
                    connected: is_connected,
                    route,
                    route_auto,
                    manual_queued: queued.contains(name),
                    auto_interval_secs: self.peer_auto_interval(name).map(|d| d.as_secs()),
                    last_success: outcome.last_success,
                    last_attempt: outcome.last_attempt,
                    last_outcome: outcome.outcome.clone(),
                    last_message: outcome.message.clone(),
                    last_error: (outcome.outcome.as_deref() == Some("error"))
                        .then_some(outcome.message)
                        .flatten(),
                }
            })
            .collect();
        s
    }
    fn wakeup(&self) {
        self.wakeup.notify_one();
    }
    fn cancel_current(&self) -> bool {
        let token = self.cycle_cancel.lock().unwrap().clone();
        let Some(token) = token else {
            return false;
        };
        if !self.cancellable_now() {
            return false;
        }
        token.cancel();
        true
    }
    fn pause(&self) -> bool {
        self.paused.store(true, Ordering::Relaxed);
        if self.cancellable_now()
            && let Some(tok) = self.cycle_cancel.lock().unwrap().as_ref()
        {
            tok.cancel();
        }
        true
    }
    fn resume(&self) -> bool {
        self.paused.store(false, Ordering::Relaxed);
        self.wakeup.notify_one();
        true
    }
    fn request_push(&self, peer: &str) -> Result<(), String> {
        if !self.config.targets.iter().any(|t| t == peer) {
            return Err(format!(
                "peer {peer:?} is not a target of job {:?}",
                self.config.name
            ));
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
                // Startup-immediate like the snap job, but a push cycle
                // needs a connected peer: give the eager-reconnect tasks
                // a short grace to establish the first link so a daemon
                // restart doesn't immediately record a "none of targets
                // connected" error run. If nothing connects within the
                // grace, run anyway — the error is accurate and visible.
                const CONNECT_GRACE: StdDuration = StdDuration::from_secs(30);
                let deadline = tokio::time::Instant::now() + CONNECT_GRACE;
                while !self.any_link().await && tokio::time::Instant::now() < deadline {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = sleep(StdDuration::from_secs(1)) => {}
                    }
                }
                self.run_and_record(&ctx, &cancel, interval).await;
                // Event-driven: sleep exactly until the earliest auto
                // target is due; wake early on a manual request or a
                // peer connectivity change. `interval` is only the
                // upper bound on how long we may sleep blind.
                let mut peers_rx = self.peers_changed.clone();
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

impl PushJob {
    /// Whether aborting the cycle would still cut short real work. Once
    /// every slot has handed off to `zfs recv` there is nothing left to
    /// interrupt, so cancel/pause degrade to no-ops. Shared by
    /// `cancel_current`, `pause` and `status` so the daemon and every UI
    /// surface answer this question identically.
    fn cancellable_now(&self) -> bool {
        // Filesystems the cycle has not started yet: stopping now spares
        // them, whatever the running slots are doing. A job replicating
        // several filesystems otherwise refused to stop for as long as
        // its last running slot sat in finalizing, even with the rest of
        // the queue untouched.
        if self.queued_filesystems.load(Ordering::Relaxed) > 0 {
            return true;
        }
        let transfers = self.transfers.lock().unwrap();
        transfers.is_empty()
            || transfers
                .values()
                .any(|t| !matches!(t.phase.as_str(), "finalizing" | "committing" | "cancelling"))
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
            // Same cadence `select_targets` schedules on, or the sleep
            // and the selection disagree: the loop would wake on the
            // retry floor for a peer that is not actually due yet.
            let cadence = self
                .peer_auto_interval(name)
                .or(self.config.interval)
                .unwrap_or(FALLBACK_POLL)
                .as_secs() as i64;
            let due_at = match outcomes.get(name).and_then(Self::peer_schedule_anchor) {
                Some(ts) => ts + cadence,
                // No history: due immediately.
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
        // While paused, scheduled ticks are no-ops — but queued manual
        // requests still run (an explicit "send now" outranks pause).
        if self.paused.load(Ordering::Relaxed) && self.manual_requests.lock().unwrap().is_empty() {
            return;
        }
        let job_name = &self.config.name;
        self.ensure_outcomes_loaded(ctx).await;
        let mut errors: Vec<StepError> = Vec::new();
        let selected = self.select_targets(&mut errors).await;
        // A tick where nothing is due (auto_interval not elapsed, no
        // manual request, nothing to report) records no job_runs row —
        // otherwise a 15m cycle against a 1d auto_interval writes 96
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
        let run_id = if let Some(pool) = ctx.state.as_ref() {
            crate::state::job_runs::record_start(pool, job_name, started_at)
                .await
                .ok()
        } else {
            None
        };
        let (bytes, outcome) = self.run_cycle(ctx, &cycle_token, selected, errors).await;
        *self.cycle_cancel.lock().unwrap() = None;
        let finished_at = OffsetDateTime::now_utc().unix_timestamp();
        // Cancellation is read off the variant, so this agrees with the
        // per-peer classifier by construction. It used to be
        // `cycle_token.is_cancelled() && !cancel.is_cancelled()`, which
        // excluded daemon shutdown and therefore recorded a routine
        // restart mid-transfer as an error run while `push_syncs` recorded
        // the very same event as cancelled.
        let rendered;
        let (status, err_msg) = match &outcome {
            Ok(()) if self.config.dry_run => (crate::state::job_runs::STATUS_DRY_RUN, None),
            Ok(()) => (crate::state::job_runs::STATUS_OK, None),
            Err(StepError::Cancelled) => (crate::state::job_runs::STATUS_CANCELLED, None),
            Err(e) => {
                rendered = e.to_string();
                (
                    crate::state::job_runs::STATUS_ERROR,
                    Some(rendered.as_str()),
                )
            }
        };
        if let (Some(pool), Some(run_id)) = (ctx.state.as_ref(), run_id) {
            let _ = crate::state::job_runs::record_finish(
                pool,
                run_id,
                finished_at,
                status,
                err_msg,
                Some(bytes as i64),
            )
            .await;
        }
        self.record_cycle(
            match status {
                "error" => outcome.err().map(|e| e.to_string()),
                _ => None,
            },
            interval,
        );
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
        PushJob::new(test_config(targets), None, &[], None).expect("job builds")
    }

    /// A job whose config carries an explicit `interval`.
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
        PushJob::new(cfg, None, &[], None).expect("job builds")
    }

    fn record_success(job: &PushJob, peer: &str, at: i64) {
        let mut o = job.peer_outcomes.lock().unwrap();
        let e = o.entry(peer.to_string()).or_default();
        e.last_success = Some(at);
        e.last_attempt = Some(at);
        e.outcome = Some("ok".into());
    }

    // A peer with no `auto_interval` was due on every wake, so it
    // replicated on the loop's retry floor and the job's own `interval`
    // never applied: an operator asking for hourly syncs got them every
    // five minutes.
    #[tokio::test]
    async fn a_peer_without_its_own_interval_follows_the_jobs_interval() {
        let job = test_job_with_interval("1h");
        let now = OffsetDateTime::now_utc().unix_timestamp();
        record_success(&job, "mira", now - 60);

        // The sleep must reach roughly the remaining hour, not the floor.
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

    // Pause means the schedule stops. A queued "send now" outranks it —
    // but only for the peer it names; the cycle it wakes used to sweep up
    // every due auto target as well.
    //
    // No PeerLink can be built without a real SSH session, so the auto
    // branch is observed through its other side effect: an auto target
    // whose last success is far past its cadence reports staleness. That
    // report happens only if the branch was entered at all.
    #[tokio::test]
    async fn a_manual_push_does_not_resume_a_paused_jobs_schedule() {
        let long_ago = OffsetDateTime::now_utc().unix_timestamp() - 10 * 86_400;

        let running = test_job(&["mira", "elsewhere"]);
        record_success(&running, "elsewhere", long_ago);
        let mut errors = Vec::new();
        running.select_targets(&mut errors).await;
        assert!(
            errors.iter().any(|e| e.to_string().contains("elsewhere")),
            "a running job must still consider its auto targets: {errors:?}"
        );

        let paused = test_job(&["mira", "elsewhere"]);
        record_success(&paused, "elsewhere", long_ago);
        paused.pause();
        paused.request_push("mira").expect("mira is a target");
        let mut errors = Vec::new();
        let selected = paused.select_targets(&mut errors).await;

        let names: Vec<&str> = selected.iter().map(|(n, _, _)| n.as_str()).collect();
        assert!(
            names.is_empty(),
            "paused job selected {names:?} for a scheduled sync"
        );
        assert!(
            !errors.iter().any(|e| e.to_string().contains("elsewhere")),
            "paused job still evaluated its auto schedule: {errors:?}"
        );
    }

    fn transfer(peer: &str, phase: &str) -> TransferInfo {
        TransferInfo {
            dataset: "novafs/data".into(),
            peer: peer.into(),
            kind: "incremental".into(),
            bytes_sent: 1,
            total_bytes: None,
            started_at: 0,
            phase: phase.into(),
            phase_since: 0,
        }
    }

    // The console shows a "send now" button per target; before
    // `manual_queued` existed it had no way to say the request had been
    // taken, so pressing it during a running cycle looked like a no-op.
    #[tokio::test]
    async fn manual_request_is_visible_in_status_until_the_cycle_drains_it() {
        let job = test_job(&["mira"]);
        assert!(!job.status().targets[0].manual_queued);

        job.request_push("mira").expect("mira is a target");
        assert!(job.status().targets[0].manual_queued);

        let mut errors = Vec::new();
        job.select_targets(&mut errors).await;
        assert!(
            !job.status().targets[0].manual_queued,
            "select_targets drains the request set, so the flag must clear with it"
        );
    }

    #[test]
    fn manual_request_rejects_a_peer_that_is_not_a_target() {
        let job = test_job(&["mira"]);
        assert!(job.request_push("nowhere").is_err());
        assert!(!job.status().targets[0].manual_queued);
    }

    // `cancellable` is what every UI surface draws its stop button from,
    // so it has to agree with what `cancel_current` would actually do.
    #[test]
    fn cancellable_is_false_without_a_running_cycle() {
        let job = test_job(&["mira"]);
        assert!(!job.status().cancellable);
        assert!(!job.cancel_current(), "nothing to cancel when idle");
    }

    // With several filesystems in a cycle, the last running slot can sit
    // in finalizing while the rest of the queue has not started.
    // Cancelling then is not a no-op — it spares everything still queued
    // — but the job refused to stop, and the UI hid the button entirely.
    #[test]
    fn a_cycle_with_queued_filesystems_can_still_be_stopped() {
        let job = test_job(&["mira"]);
        *job.cycle_cancel.lock().unwrap() = Some(CancellationToken::new());
        job.transfers
            .lock()
            .unwrap()
            .insert("a".into(), transfer("mira", "finalizing"));

        // Nothing left to start: the running slot is past the point where
        // cancelling changes anything.
        assert!(!job.status().cancellable);

        job.queued_filesystems.store(2, Ordering::Relaxed);
        assert!(
            job.status().cancellable,
            "queued filesystems make cancelling worthwhile"
        );
        assert!(job.cancel_current());
    }

    #[test]
    fn cancellable_tracks_the_transfer_phases() {
        let job = test_job(&["mira"]);
        *job.cycle_cancel.lock().unwrap() = Some(CancellationToken::new());

        // A cycle with no transfer yet is still interruptible.
        assert!(job.status().cancellable);

        job.transfers
            .lock()
            .unwrap()
            .insert("a".into(), transfer("mira", "sending"));
        assert!(job.status().cancellable);

        // Past the hand-off to zfs recv, cancelling does nothing.
        job.transfers
            .lock()
            .unwrap()
            .insert("a".into(), transfer("mira", "finalizing"));
        assert!(!job.status().cancellable);
        assert!(!job.cancel_current());

        // One live slot among finished ones is enough to keep it useful.
        job.transfers
            .lock()
            .unwrap()
            .insert("b".into(), transfer("mira", "waiting_receiver"));
        assert!(job.status().cancellable);
        assert!(job.cancel_current());
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
    fn cancellation_is_not_a_peer_error() {
        assert_eq!(
            classify_peer_attempt(true, false, &[StepError::Cancelled]),
            ("cancelled", None)
        );
    }

    // The token can clear between the step reporting and the classifier
    // reading it, so the variant has to carry the verdict on its own.
    #[test]
    fn a_cancelled_step_outranks_a_token_that_already_cleared() {
        assert_eq!(
            classify_peer_attempt(false, false, &[StepError::Cancelled]),
            ("cancelled", None)
        );
    }

    // `ok` becomes `last_success_at`, which schedules the next auto sync
    // and paints the target "synced". A plan-only cycle sent nothing and
    // must not earn either.
    #[test]
    fn a_clean_dry_run_is_not_a_sync() {
        assert_eq!(classify_peer_attempt(false, true, &[]), ("dry_run", None));
        // Errors and cancellation still win over the dry-run label.
        assert_eq!(
            classify_peer_attempt(false, true, &[StepError::Cancelled]),
            ("cancelled", None)
        );
        assert_eq!(
            classify_peer_attempt(false, true, &[StepError::Failed("plan: boom".into())]).0,
            "error"
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
        let job = PushJob::new(cfg, None, &[], None).expect("job builds");
        assert!(job.status().dry_run);
        assert!(!test_job(&["mira"]).status().dry_run);
    }

    // The sentinel this replaced was a bare "cancelled" string, so any
    // caller adding context turned an interruption into a failure. A
    // message that merely CONTAINS the word must stay an error.
    #[test]
    fn a_failure_mentioning_cancellation_is_still_a_failure() {
        let (status, message) = classify_peer_attempt(
            false,
            false,
            &[StepError::Failed(
                "execute tank/a: receiver: cancelled".into(),
            )],
        );
        assert_eq!(status, "error");
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
                StepError::Failed("plan tank/a: boom".into()),
                StepError::Failed("plan tank/b: bang".into()),
            ],
        );
        assert_eq!(status, "error");
        assert_eq!(
            message.as_deref(),
            Some("plan tank/a: boom; plan tank/b: bang")
        );
    }

    #[test]
    fn a_clean_peer_attempt_is_ok() {
        assert_eq!(classify_peer_attempt(false, false, &[]), ("ok", None));
    }
}
