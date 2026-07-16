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

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use arctern_api::{TargetStatus, TransferInfo};
use arctern_config::{PeerConfig, PeerMode, PushJobConfig, SendFlagsConfig, SnapshotFilterConfig};
use arctern_transport::{
    PROTOCOL_VERSION, RecvHeader, Response, SendFlagsWire, SendHeader, SendKind, SnapshotEntry,
    SnapshotRef, compile_prefix_regex, regex,
};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};
use zfskit::dataset::ListOptions;
use zfskit::models::DatasetType;
use zfskit::runner::CommandRunner;
use zfskit::send::{SendArgs, send as zfs_send};

use super::{Job, JobContext, JobStatusInner};
use crate::peer::PeerLink;
use crate::peer::state::PeersState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPlan {
    Nothing,
    Full {
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    Incremental {
        from: SnapshotRef,
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    /// `zfs send -i <dataset>#<bookmark> <dataset>@<to>` — incremental
    /// whose base is a bookmark instead of a snapshot. Picked when the
    /// receiver and sender share no common *snapshot* (the sender's
    /// copy was pruned) but a sender bookmark's GUID is still present
    /// on the receiver. This is what makes cursor bookmarks (arctern's
    /// own, or zrepl's `#zrepl_CURSOR_*` during migration) load-bearing:
    /// an offline gap longer than the sender's retention window resyncs
    /// incrementally instead of forcing a full resend.
    /// `from.name` carries the bookmark leaf (part after `#`).
    IncrementalFromBookmark {
        from: SnapshotRef,
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    Resume {
        token: String,
        decoded: zfskit::resume_token::ResumeToken,
    },
}

/// One sender-side bookmark, as listed for the fallback planner.
/// `leaf` is the part after `#`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookmarkRef {
    pub leaf: String,
    pub guid: u64,
    pub createtxg: u64,
}

#[derive(Debug, Clone)]
pub struct CompiledFilter {
    re: Option<regex::Regex>,
}

impl CompiledFilter {
    pub fn from_config(cfg: &SnapshotFilterConfig) -> Result<Self, regex::Error> {
        let re = compile_prefix_regex(cfg.as_regex_str().as_deref())?;
        Ok(Self { re })
    }

    pub fn matches(&self, snap_name: &str) -> bool {
        match &self.re {
            None => true,
            Some(r) => r.is_match(snap_name),
        }
    }
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("list sender snapshots on {dataset}: {source}")]
    SenderList {
        dataset: String,
        #[source]
        source: zfskit::ZfsError,
    },
}

pub async fn list_sender_snaps(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    filter: &CompiledFilter,
) -> Result<Vec<SnapshotEntry>, PlanError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![sender_dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(runner, &opts)
        .await
        .map_err(|source| PlanError::SenderList {
            dataset: sender_dataset.to_string(),
            source,
        })?;
    let mut snaps: Vec<SnapshotEntry> = entries
        .into_iter()
        .filter_map(|e| {
            let snap_name = e.snapshot_name.clone()?;
            if !filter.matches(&snap_name) {
                return None;
            }
            let guid = e
                .properties
                .get("guid")
                .and_then(|p| p.value.parse::<u64>().ok())?;
            let createtxg = e.createtxg.parse::<u64>().ok()?;
            Some(SnapshotEntry {
                name: snap_name,
                guid,
                createtxg,
            })
        })
        .collect();
    snaps.sort_by_key(|s| s.createtxg);
    Ok(snaps)
}

fn snap_ref(s: &SnapshotEntry) -> SnapshotRef {
    SnapshotRef {
        name: s.name.clone(),
        guid: s.guid,
    }
}

pub fn pick_plan(sender: &[SnapshotEntry], receiver: &[SnapshotEntry]) -> SnapshotPlan {
    pick_plan_with_discard(sender, receiver, false)
}

fn pick_plan_with_discard(
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    discard_partial_recv: bool,
) -> SnapshotPlan {
    let Some(latest) = sender.last() else {
        return SnapshotPlan::Nothing;
    };
    if receiver.is_empty() {
        return SnapshotPlan::Full {
            to: snap_ref(latest),
            discard_partial_recv,
        };
    }
    use std::collections::BTreeSet;
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    let mut from: Option<&SnapshotEntry> = None;
    for s in sender.iter().rev() {
        if recv_guids.contains(&s.guid) {
            from = Some(s);
            break;
        }
    }
    match from {
        None => SnapshotPlan::Full {
            to: snap_ref(latest),
            discard_partial_recv,
        },
        Some(f) if f.guid == latest.guid => SnapshotPlan::Nothing,
        Some(f) => SnapshotPlan::Incremental {
            from: snap_ref(f),
            to: snap_ref(latest),
            discard_partial_recv,
        },
    }
}

/// Prefer a sender bookmark as the incremental base when it is a
/// STRICTLY newer common point than any common snapshot (or when there
/// is no common snapshot at all and the plan degraded to Full). Any
/// bookmark qualifies — arctern's own cursors, zrepl's
/// `#zrepl_CURSOR_*` left over from a migration, or a hand-made
/// `zfs bookmark`.
///
/// The Incremental upgrade matters: after retention prunes the
/// sender's copy of the receiver's newest snapshot, an OLDER snapshot
/// may still be common — but an incremental from it is unreceivable
/// (the receiver's head is newer than the base, so `zfs recv` refuses
/// with "destination has been modified"). The cursor bookmark IS the
/// receiver's head; sending from it is the only plan that applies.
/// Resume / Nothing plans pass through untouched.
pub fn apply_bookmark_fallback(
    plan: SnapshotPlan,
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    bookmarks: &[BookmarkRef],
) -> SnapshotPlan {
    // First replication — Full is correct, not a degraded case.
    if receiver.is_empty() {
        return plan;
    }
    let (to, discard_partial_recv, base_txg) = match &plan {
        SnapshotPlan::Full {
            to,
            discard_partial_recv,
        } => (to.clone(), *discard_partial_recv, None),
        SnapshotPlan::Incremental {
            from,
            to,
            discard_partial_recv,
        } => {
            let txg = sender
                .iter()
                .find(|s| s.guid == from.guid)
                .map(|s| s.createtxg);
            (to.clone(), *discard_partial_recv, txg)
        }
        _ => return plan,
    };
    use std::collections::BTreeSet;
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    let best = bookmarks
        .iter()
        .filter(|b| recv_guids.contains(&b.guid))
        .max_by_key(|b| b.createtxg);
    match (best, base_txg) {
        // A common snapshot base at least as new as the bookmark wins:
        // snapshots can carry holds, bookmarks cannot.
        (Some(b), Some(txg)) if b.createtxg <= txg => plan,
        (Some(b), _) if b.guid == to.guid => SnapshotPlan::Nothing,
        (Some(b), _) => SnapshotPlan::IncrementalFromBookmark {
            from: SnapshotRef {
                name: b.leaf.clone(),
                guid: b.guid,
            },
            to,
            discard_partial_recv,
        },
        (None, _) => plan,
    }
}

/// List every bookmark of `sender_dataset` with its GUID. Unfiltered by
/// name on purpose — the fallback matches by GUID, and foreign bookmarks
/// (zrepl cursors) are exactly the migration case.
pub async fn list_sender_bookmarks(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
) -> Result<Vec<BookmarkRef>, PlanError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Bookmark],
        roots: vec![sender_dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(runner, &opts)
        .await
        .map_err(|source| PlanError::SenderList {
            dataset: sender_dataset.to_string(),
            source,
        })?;
    Ok(entries
        .into_iter()
        .filter_map(|e| {
            let leaf = e.name.split_once('#').map(|(_, l)| l.to_string())?;
            let guid = e
                .properties
                .get("guid")
                .and_then(|p| p.value.parse::<u64>().ok())?;
            let createtxg = e.createtxg.parse::<u64>().ok()?;
            Some(BookmarkRef {
                leaf,
                guid,
                createtxg,
            })
        })
        .collect())
}

pub fn pick_plan_with_token(
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    token: Option<&str>,
    decoded: Option<&zfskit::resume_token::ResumeToken>,
    sender_bookmarks: &[BookmarkRef],
) -> SnapshotPlan {
    let (Some(token), Some(decoded)) = (token, decoded) else {
        return pick_plan(sender, receiver);
    };
    use std::collections::BTreeSet;
    let sender_guids: BTreeSet<u64> = sender.iter().map(|s| s.guid).collect();
    let to_live = sender_guids.contains(&decoded.to_guid);
    // The `from` base of an interrupted send is a BOOKMARK guid
    // whenever the send was cursor-based — which is the normal daily
    // case once the retention grid has thinned the sender's copy of
    // the receiver's newest snapshot. Checking snapshots alone here
    // discarded perfectly resumable partial receives.
    let from_live = decoded
        .from_guid
        .map(|g| sender_guids.contains(&g) || sender_bookmarks.iter().any(|b| b.guid == g))
        .unwrap_or(true);
    if to_live && from_live {
        SnapshotPlan::Resume {
            token: token.to_string(),
            decoded: decoded.clone(),
        }
    } else {
        pick_plan_with_discard(sender, receiver, true)
    }
}

pub fn build_send_header(plan: &SnapshotPlan, flags: &SendFlagsConfig) -> Option<SendHeader> {
    let wire_flags = SendFlagsWire {
        raw: flags.encrypted,
        embedded: flags.embedded_data,
        compressed: flags.compressed,
        large_blocks: flags.large_blocks,
    };
    let (send_kind, from_snap, to_snap, discard_partial_recv) = match plan {
        SnapshotPlan::Nothing => return None,
        SnapshotPlan::Full {
            to,
            discard_partial_recv,
        } => (SendKind::Full, None, to.clone(), *discard_partial_recv),
        SnapshotPlan::Incremental {
            from,
            to,
            discard_partial_recv,
        }
        // On the wire a bookmark base is indistinguishable from a
        // snapshot base: the receiver only logs `from_snap` — the
        // stream itself carries the real base identity.
        | SnapshotPlan::IncrementalFromBookmark {
            from,
            to,
            discard_partial_recv,
        } => (
            SendKind::Incremental,
            Some(from.clone()),
            to.clone(),
            *discard_partial_recv,
        ),
        SnapshotPlan::Resume { decoded, .. } => (
            SendKind::Resume,
            None,
            SnapshotRef {
                // The token's toname is the full sender-side
                // `dataset@snap`; the wire carries only the leaf — the
                // receiver validates it as a snapshot leaf (no `/` or
                // `@`) and names its own `target_dataset@<leaf>` with it.
                name: decoded
                    .to_name
                    .split_once('@')
                    .map(|(_, leaf)| leaf)
                    .unwrap_or(&decoded.to_name)
                    .to_string(),
                guid: decoded.to_guid,
            },
            // Resume MUST NOT discard the partial — that IS the partial
            // we are continuing.
            false,
        ),
    };
    debug_assert!(
        !(matches!(plan, SnapshotPlan::Resume { .. }) && discard_partial_recv),
        "Resume plan must not set discard_partial_recv"
    );
    Some(SendHeader {
        send_kind,
        from_snap,
        to_snap,
        flags: wire_flags,
        discard_partial_recv,
    })
}

pub fn build_send_args(
    plan: &SnapshotPlan,
    sender_dataset: &str,
    flags: &SendFlagsConfig,
) -> Option<SendArgs> {
    if let SnapshotPlan::Resume { token, .. } = plan {
        return Some(SendArgs::resume(token));
    }
    let to_full = match plan {
        SnapshotPlan::Nothing => return None,
        SnapshotPlan::Full { to, .. }
        | SnapshotPlan::Incremental { to, .. }
        | SnapshotPlan::IncrementalFromBookmark { to, .. } => {
            format!("{sender_dataset}@{}", to.name)
        }
        SnapshotPlan::Resume { .. } => unreachable!("handled above"),
    };
    let mut args = SendArgs::new(to_full);
    if flags.encrypted {
        args = args.raw();
    }
    if flags.embedded_data {
        args = args.embedded();
    }
    if flags.compressed {
        args = args.compressed();
    }
    if flags.large_blocks {
        args = args.large_blocks();
    }
    match plan {
        SnapshotPlan::Incremental { from, .. } => {
            args = args.incremental(format!("{sender_dataset}@{}", from.name));
        }
        SnapshotPlan::IncrementalFromBookmark { from, .. } => {
            args = args.incremental(format!("{sender_dataset}#{}", from.name));
        }
        _ => {}
    }
    Some(args)
}

/// Naming conventions pinned in ARCHITECTURE.md. Peer-namespaced so a
/// multi-target push job tracks each receiver's cursor independently:
/// peer A can be a week behind peer B and still catch up cleanly from
/// its own bookmark instead of triggering a full resend.
fn step_hold_tag(job_name: &str, peer: &str) -> String {
    format!("arctern_step_J_{job_name}_P_{peer}")
}

/// GUID-suffixed cursor name (zrepl's scheme): a new cursor is created
/// under a fresh name *before* stale ones are destroyed, so a crash in
/// between leaves at least one cursor alive.
fn cursor_bookmark_name(dataset: &str, job_name: &str, peer: &str, guid: u64) -> String {
    format!("{dataset}#arctern_cursor_G_{guid:x}_J_{job_name}_P_{peer}")
}

/// Matches any cursor bookmark leaf for this (job, peer), regardless of
/// GUID. Used to find stale cursors after advancing.
fn is_cursor_bookmark_leaf(leaf: &str, job_name: &str, peer: &str) -> bool {
    leaf.starts_with("arctern_cursor_G_") && leaf.ends_with(&format!("_J_{job_name}_P_{peer}"))
}

/// Plan one filesystem cycle against the receiver. Pure planner glue
/// over zfskit + the control channel. Returns the plan plus the
/// filtered sender snapshot list (the executor's hold sweep reuses it).
async fn plan_one_filesystem(
    runner: &dyn CommandRunner,
    peer: &PeerLink,
    sender_dataset: &str,
    target_dataset: &str,
    filter: &CompiledFilter,
) -> Result<(SnapshotPlan, Vec<SnapshotEntry>), String> {
    let sender = list_sender_snaps(runner, sender_dataset, filter)
        .await
        .map_err(|e| format!("{e}"))?;
    if sender.is_empty() {
        return Ok((SnapshotPlan::Nothing, sender));
    }
    // Deliberately UNFILTERED: the planner intersects by GUID, and a
    // common snapshot (or bookmark-fallback base) may carry a different
    // prefix than this job's filter — zrepl_* history after a prefix
    // switch, a manual snapshot that travelled in a send stream. The
    // sender-side list stays filtered, so the filter still decides what
    // gets SENT; the receiver list only decides what counts as a
    // common base. Filtering here forced a full resend in exactly the
    // migration scenarios the bookmark fallback exists for.
    let reply = peer
        .list_receiver_guids(target_dataset.to_string(), None)
        .await
        .map_err(|e| format!("list_receiver_guids: {e}"))?;
    let (guids, token) = (reply.guids, reply.receive_resume_token);
    // The planner intersects on GUID only (see pick_plan); the receiver's
    // snapshot names and createtxg are unused, so carry each GUID in an
    // otherwise-empty SnapshotEntry to keep the pure planner signature.
    let receiver: Vec<SnapshotEntry> = guids
        .into_iter()
        .map(|guid| SnapshotEntry {
            name: String::new(),
            guid,
            createtxg: 0,
        })
        .collect();
    let decoded = match token.as_deref() {
        Some(t) => match zfskit::resume_token::decode(runner, t).await {
            Ok(d) => Some(d),
            Err(e) => {
                tracing::info!(
                    target = %target_dataset,
                    error = %e,
                    "push: receiver token failed to decode, treating as stale"
                );
                let plan = pick_plan_with_discard(&sender, &receiver, true);
                let plan =
                    maybe_bookmark_fallback(runner, sender_dataset, plan, &sender, &receiver).await;
                return Ok((plan, sender));
            }
        },
        None => None,
    };
    // Bookmarks participate in resume validation; list them once here
    // (only when a token is in play — the common no-token path pays
    // nothing extra; the fallback path lists lazily as before).
    let bookmarks = if decoded.is_some() {
        list_sender_bookmarks(runner, sender_dataset)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let plan = pick_plan_with_token(
        &sender,
        &receiver,
        token.as_deref(),
        decoded.as_ref(),
        &bookmarks,
    );
    let plan = maybe_bookmark_fallback(runner, sender_dataset, plan, &sender, &receiver).await;
    Ok((plan, sender))
}

/// Wrap `apply_bookmark_fallback` with the bookmark listing, skipping
/// the extra `zfs list` entirely when the plan can't benefit. A listing
/// failure degrades to the original plan with a warning — a full resend
/// (or a refused incremental retried next cycle) is correct, just
/// expensive.
async fn maybe_bookmark_fallback(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    plan: SnapshotPlan,
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
) -> SnapshotPlan {
    if !matches!(
        plan,
        SnapshotPlan::Full { .. } | SnapshotPlan::Incremental { .. }
    ) || receiver.is_empty()
    {
        return plan;
    }
    match list_sender_bookmarks(runner, sender_dataset).await {
        Ok(bookmarks) => {
            let plan = apply_bookmark_fallback(plan, sender, receiver, &bookmarks);
            if let SnapshotPlan::IncrementalFromBookmark { from, .. } = &plan {
                tracing::info!(
                    dataset = %sender_dataset,
                    bookmark = %from.name,
                    "push: incremental base is the cursor bookmark (sender's copy of the \
                     receiver's newest snapshot already pruned — expected between syncs)"
                );
            }
            plan
        }
        Err(e) => {
            warn!(dataset = %sender_dataset, error = %e, "push: bookmark listing failed; keeping the snapshot-based plan");
            plan
        }
    }
}

/// Shared token bucket for one job's outgoing bandwidth. Debt-based:
/// each stream may overshoot by one chunk and then sleeps off the
/// debt, so N parallel sends still sum to `rate`. Burst credit is
/// capped at half a second of rate — absorbs scheduler jitter without
/// letting an idle gap turn into an unthrottled surge.
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    inner: Mutex<(tokio::time::Instant, f64)>,
}

impl RateLimiter {
    pub fn new(rate: u64) -> Self {
        let rate = rate as f64;
        let burst = (rate * 0.5).max(256.0 * 1024.0);
        Self {
            rate,
            burst,
            inner: Mutex::new((tokio::time::Instant::now(), burst)),
        }
    }

    /// Account `n` sent bytes; sleeps whatever is needed to keep the
    /// aggregate under the configured rate.
    pub async fn throttle(&self, n: u64) {
        let wait = {
            let mut g = self.inner.lock().unwrap();
            let now = tokio::time::Instant::now();
            let dt = now.duration_since(g.0).as_secs_f64();
            g.0 = now;
            g.1 = (g.1 + dt * self.rate).min(self.burst);
            g.1 -= n as f64;
            if g.1 < 0.0 {
                StdDuration::from_secs_f64(-g.1 / self.rate)
            } else {
                StdDuration::ZERO
            }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// Open a recv channel for one plan, spawn `zfs send` locally, copy
/// stdout into the channel, await the receiver's terminal Response.
/// Cancellation: the `cancel` token races against the bulk copy loop;
/// on cancel we drop the recv channel (closing the SSH child's stdin)
/// and `start_kill` the local send child.
#[allow(clippy::too_many_arguments)]
async fn execute_one_plan(
    runner: &dyn CommandRunner,
    peer: &PeerLink,
    job_name: &str,
    plan: &SnapshotPlan,
    target_dataset: &str,
    sender_dataset: &str,
    flags: &SendFlagsConfig,
    limiter: Option<&RateLimiter>,
    cancel: &CancellationToken,
    transfers: &Mutex<HashMap<String, TransferInfo>>,
    transfer_key: &str,
) -> Result<(), String> {
    let Some(send_header) = build_send_header(plan, flags) else {
        return Err("build_send_header returned None for non-Nothing plan".into());
    };
    let Some(args) = build_send_args(plan, sender_dataset, flags) else {
        return Err("build_send_args returned None for non-Nothing plan".into());
    };

    let header = RecvHeader {
        version: PROTOCOL_VERSION,
        target_dataset: target_dataset.to_string(),
        send: send_header,
    };
    let mut channel = peer
        .open_recv(job_name, &header)
        .await
        .map_err(|e| format!("open_recv: {e}"))?;

    let mut child = zfs_send(runner, &args)
        .await
        .map_err(|e| format!("spawn zfs send: {e}"))?;
    let mut child_stdout = child
        .take_stdout()
        .ok_or_else(|| "no stdout on send child".to_string())?;

    let mut channel_stdin = channel
        .stdin
        .take()
        .ok_or_else(|| "no stdin on recv channel".to_string())?;
    // Manual copy loop instead of tokio::io::copy: publishes progress
    // into `transfer` for the live job-status stream and races
    // the job/cycle CancellationToken. On cancel the recv channel's
    // stdin closes (SIGPIPE to the remote zfs recv, which keeps its
    // resumable partial state) and the local send child is killed.
    let copy_res: std::io::Result<u64> = async {
        let mut buf = vec![0u8; 256 * 1024];
        let mut copied: u64 = 0;
        let mut last_published: u64 = 0;
        let mut last_publish_at = tokio::time::Instant::now();
        loop {
            // A stalled read means zfs send is still producing the next
            // record. Keep the same read future alive so no data is lost.
            let (n, read_waited) = {
                let read = child_stdout.read(&mut buf);
                tokio::pin!(read);
                let slow = sleep(StdDuration::from_secs(2));
                tokio::pin!(slow);
                let mut waiting = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            return Err(std::io::Error::other("cancelled"));
                        }
                        r = &mut read => break (r?, waiting),
                        _ = &mut slow, if !waiting => {
                            waiting = true;
                            set_transfer_phase(transfers, transfer_key, "waiting_sender");
                        }
                    }
                }
            };
            if n == 0 {
                break;
            }
            if read_waited {
                set_transfer_phase(transfers, transfer_key, "sending");
            }

            // A stalled write means the SSH channel has applied
            // backpressure: network or (most commonly) receiver zfs recv /
            // storage. write_all is not cancellation-safe, so retain and
            // continue polling this exact future after publishing the phase.
            let write_waited = {
                let write = channel_stdin.write_all(&buf[..n]);
                tokio::pin!(write);
                let slow = sleep(StdDuration::from_secs(2));
                tokio::pin!(slow);
                let mut waiting = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            return Err(std::io::Error::other("cancelled"));
                        }
                        r = &mut write => {
                            r?;
                            break waiting;
                        }
                        _ = &mut slow, if !waiting => {
                            waiting = true;
                            set_transfer_phase(transfers, transfer_key, "waiting_receiver");
                        }
                    }
                }
            };
            copied += n as u64;
            if write_waited {
                set_transfer_phase(transfers, transfer_key, "sending");
            }
            // Publish accepted bytes before any configured rate-limit sleep;
            // otherwise deliberate throttling looks like missing progress.
            if copied - last_published >= 8 * 1024 * 1024
                || last_publish_at.elapsed() >= StdDuration::from_millis(250)
            {
                last_published = copied;
                last_publish_at = tokio::time::Instant::now();
                if let Some(t) = transfers.lock().unwrap().get_mut(transfer_key) {
                    t.bytes_sent = copied;
                }
            }
            // Chunk-grained (256 KiB) throttling, plenty smooth at
            // network scales. The bucket is shared job-wide so
            // parallel sends stay under the aggregate limit.
            if let Some(l) = limiter {
                l.throttle(n as u64).await;
            }
        }
        if let Some(t) = transfers.lock().unwrap().get_mut(transfer_key) {
            t.bytes_sent = copied;
        }
        Ok(copied)
    }
    .await;
    if let Err(error) = copy_res {
        let _ = channel_stdin.shutdown().await;
        let _ = child.cancel().await;
        if cancel.is_cancelled() {
            return Err("cancelled".into());
        }
        return Err(format!("stream copy: {error}"));
    }
    set_transfer_phase(transfers, transfer_key, "finalizing");
    let _ = channel_stdin.shutdown().await;
    drop(channel_stdin);
    child
        .finish()
        .await
        .map_err(|e| format!("zfs send failed: {e}"))?;
    let resp = channel
        .finish()
        .await
        .map_err(|e| format!("read recv response: {e}"))?;
    match resp {
        Response::Ok => Ok(()),
        Response::Error { message, .. } => Err(format!("receiver: {message}")),
    }
}

fn set_transfer_phase(
    transfers: &Mutex<HashMap<String, TransferInfo>>,
    transfer_key: &str,
    phase: &str,
) {
    if let Some(transfer) = transfers.lock().unwrap().get_mut(transfer_key)
        && transfer.phase != phase
    {
        transfer.phase = phase.to_string();
        transfer.phase_since = OffsetDateTime::now_utc().unix_timestamp();
    }
}

/// Step hold + cursor bookmark choreography around a successful execute.
/// Holds are placed BEFORE the send so a concurrent prune cannot kill
/// the snapshot mid-stream. On success the step-hold tag is swept from
/// every filtered snapshot (current `to` plus stale holds from earlier
/// failed cycles); on failure holds stay so a retry can find the
/// snapshot. The cursor bookmark is GUID-named: the new one is created
/// first, stale same-(job, peer) cursors destroyed after — crash-safe.
#[allow(clippy::too_many_arguments)]
async fn run_one_filesystem(
    runner: &dyn CommandRunner,
    peer: &PeerLink,
    job_name: &str,
    peer_name: &str,
    sender_dataset: &str,
    target_dataset: &str,
    plan: &SnapshotPlan,
    sender_snaps: &[SnapshotEntry],
    flags: &SendFlagsConfig,
    limiter: Option<&RateLimiter>,
    cancel: &CancellationToken,
    transfers: &Mutex<HashMap<String, TransferInfo>>,
    transfer_key: &str,
) -> Result<(), String> {
    let to_hold_target: Option<(String, u64)> = match plan {
        SnapshotPlan::Full { to, .. }
        | SnapshotPlan::Incremental { to, .. }
        | SnapshotPlan::IncrementalFromBookmark { to, .. } => {
            Some((format!("{sender_dataset}@{}", to.name), to.guid))
        }
        // The token's toname is the full sender-side dataset@snap.
        SnapshotPlan::Resume { decoded, .. } => Some((decoded.to_name.clone(), decoded.to_guid)),
        SnapshotPlan::Nothing => None,
    };
    // The `from` base needs the same protection for the duration of the
    // step (zrepl holds both ends): losing it mid-send or between a
    // failed step and its retry breaks incrementality / resumability.
    // Bookmark bases can't be held — snapshot prune can't destroy a
    // bookmark, so they're safe without one.
    let from_hold_target: Option<String> = match plan {
        SnapshotPlan::Incremental { from, .. } => Some(format!("{sender_dataset}@{}", from.name)),
        _ => None,
    };
    let tag = step_hold_tag(job_name, peer_name);
    for snap in from_hold_target
        .iter()
        .chain(to_hold_target.iter().map(|(s, _)| s))
    {
        // hold is idempotent at the zfskit layer (no-op when the
        // tag already exists for that snapshot).
        if let Err(e) = zfskit::hold::hold(runner, snap, &tag).await {
            return Err(format!("step hold failed for {snap} with tag {tag}: {e}"));
        }
    }

    // Leave the step hold in place on failure — it protects the snapshot
    // for the next cycle's retry. Hence `?` propagates without a release.
    execute_one_plan(
        runner,
        peer,
        job_name,
        plan,
        target_dataset,
        sender_dataset,
        flags,
        limiter,
        cancel,
        transfers,
        transfer_key,
    )
    .await?;

    if let Some((snap, guid)) = &to_hold_target {
        set_transfer_phase(transfers, transfer_key, "committing");
        advance_cursor(runner, sender_dataset, job_name, peer_name, snap, *guid).await;
        sweep_step_holds(runner, sender_dataset, sender_snaps, snap, &tag).await;
    }
    Ok(())
}

/// Create the new GUID-named cursor bookmark, then destroy stale
/// cursors for the same (job, peer). Failures degrade to warnings —
/// the send already succeeded and the receiver has the data.
async fn advance_cursor(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    job_name: &str,
    peer_name: &str,
    to_snap: &str,
    to_guid: u64,
) {
    let cursor = cursor_bookmark_name(sender_dataset, job_name, peer_name, to_guid);
    if let Err(e) = zfskit::bookmark::create(runner, to_snap, &cursor).await {
        warn!(snapshot = %to_snap, bookmark = %cursor, error = %e, "create cursor bookmark");
        // Keep the old cursor rather than risk destroying the only one.
        return;
    }
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Bookmark],
        roots: vec![sender_dataset.to_string()],
        ..ListOptions::default()
    };
    let bookmarks = match zfskit::dataset::list(runner, &opts).await {
        Ok(v) => v,
        Err(e) => {
            warn!(dataset = %sender_dataset, error = %e, "list bookmarks for cursor sweep");
            return;
        }
    };
    for b in &bookmarks {
        let Some((_, leaf)) = b.name.split_once('#') else {
            continue;
        };
        if b.name != cursor
            && is_cursor_bookmark_leaf(leaf, job_name, peer_name)
            && let Err(e) = zfskit::bookmark::destroy(runner, &b.name).await
        {
            warn!(bookmark = %b.name, error = %e, "destroy stale cursor bookmark");
        }
    }
}

/// Release this (job, peer)'s step-hold tag from every filtered sender
/// snapshot — the current `to` plus any stale holds left by earlier
/// failed cycles (a failed cycle keeps its hold; the next cycle usually
/// targets a newer snapshot, so without the sweep the old hold would
/// pin its snapshot against prune forever). One `zfs holds` invocation
/// for the whole set, then one release per actual holder.
async fn sweep_step_holds(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    sender_snaps: &[SnapshotEntry],
    to_snap_full: &str,
    tag: &str,
) {
    let mut names: Vec<String> = sender_snaps
        .iter()
        .map(|s| format!("{sender_dataset}@{}", s.name))
        .collect();
    if !names.iter().any(|n| n == to_snap_full) {
        names.push(to_snap_full.to_string());
    }
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let holds = match zfskit::hold::list_holds_many(runner, &refs).await {
        Ok(h) => h,
        Err(e) => {
            warn!(dataset = %sender_dataset, error = %e, "step-hold sweep holds query failed");
            return;
        }
    };
    for h in holds.iter().filter(|h| h.tag == tag) {
        if let Err(e) = zfskit::hold::release(runner, &h.dataset, tag).await {
            warn!(snapshot = %h.dataset, tag = %tag, error = %e, "release step hold");
        }
    }
}

pub const KIND: &str = arctern_api::JOB_KIND_PUSH;

/// Per-peer last outcome: (last successful sync unix seconds, last error).
type PeerOutcomes = HashMap<String, (Option<i64>, Option<String>)>;

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
    /// Bumped by the reconnect tasks on any peer state change so the
    /// scheduler re-evaluates due-ness the moment a link appears.
    peers_changed: Option<tokio::sync::watch::Receiver<u64>>,
    /// `[[peers]]` entries for this job's targets (mode, auto_interval).
    peer_configs: Vec<PeerConfig>,
    filter: CompiledFilter,
    status: Mutex<JobStatusInner>,
    wakeup: Arc<tokio::sync::Notify>,
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
    /// Last known per-peer outcome: (last_success unix, last_error).
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
            peers,
            transfers: Arc::new(Mutex::new(HashMap::new())),
            paused: AtomicBool::new(false),
            manual_requests: Mutex::new(BTreeSet::new()),
            cycle_cancel: Mutex::new(None),
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
                let ok = r.status == "ok";
                o.insert(
                    r.peer,
                    (ok.then_some(r.finished_at), if ok { None } else { r.error }),
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
        errors: &mut Vec<String>,
    ) -> Vec<(String, Arc<PeerLink>, &'static str)> {
        let manual: BTreeSet<String> = std::mem::take(&mut *self.manual_requests.lock().unwrap());
        let mut selected = Vec::new();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        for name in &self.config.targets {
            let link = self.link_for(name).await;
            if manual.contains(name) {
                match link {
                    Some((l, route, _)) => {
                        tracing::info!(peer = %name, route = %route, "manual push queued");
                        selected.push((name.clone(), l, "manual"));
                    }
                    None => errors.push(format!("manual push to {name:?}: peer not connected")),
                }
                continue;
            }
            if self.peer_mode(name) != PeerMode::Auto {
                continue;
            }
            let last_success = self
                .peer_outcomes
                .lock()
                .unwrap()
                .get(name)
                .and_then(|o| o.0);
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
                    let due = match (self.peer_auto_interval(name), last_success) {
                        (None, _) => true,
                        (Some(_), None) => true,
                        (Some(iv), Some(ts)) => now - ts >= iv.as_secs() as i64,
                    };
                    if due {
                        selected.push((name.clone(), l, "auto"));
                    }
                }
                None => {
                    if let Some(ts) = last_success
                        && now - ts > cadence.saturating_mul(3)
                    {
                        errors.push(format!(
                            "auto target {name:?} has no auto-eligible route and last successful sync is {}h old",
                            (now - ts) / 3600
                        ));
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
        mut errors: Vec<String>,
    ) -> (u64, Result<(), String>) {
        let mut total_bytes: u64 = 0;
        for (peer_name, link, reason) in selected {
            if cancel.is_cancelled() {
                break;
            }
            tracing::info!(peer = %peer_name, reason, "push: replicating to target");
            let mut peer_errors: Vec<String> = Vec::new();
            let bytes = self
                .run_for_peer(ctx, cancel, &peer_name, &link, &mut peer_errors)
                .await;
            total_bytes += bytes;
            let finished = OffsetDateTime::now_utc().unix_timestamp();
            let (status, err_text) = if peer_errors.is_empty() {
                ("ok", None)
            } else {
                ("error", Some(peer_errors.join("; ")))
            };
            if let Some(pool) = ctx.state.as_ref() {
                let _ = crate::state::push_syncs::record(
                    pool,
                    &self.config.name,
                    &peer_name,
                    finished,
                    status,
                    err_text.as_deref(),
                )
                .await;
            }
            {
                let mut o = self.peer_outcomes.lock().unwrap();
                let entry = o.entry(peer_name.clone()).or_insert((None, None));
                if peer_errors.is_empty() {
                    *entry = (Some(finished), None);
                } else {
                    entry.1 = err_text.clone();
                }
            }
            errors.extend(peer_errors);
        }
        let result = if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
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
        errors: &mut Vec<String>,
    ) -> u64 {
        let runner = ctx.zfs.command_runner();
        let sender_paths = match self.expand_filesystems(runner).await {
            Ok(p) => p,
            Err(e) => {
                errors.push(e);
                return 0;
            }
        };
        // Up to `parallel` filesystems replicate concurrently, each on
        // its own recv channel. The futures run on this task (no
        // spawn), so borrowing &self is fine; the shared RateLimiter
        // keeps the aggregate under bandwidth_limit.
        let errs = tokio::sync::Mutex::new(Vec::new());
        let cycle_bytes = std::sync::atomic::AtomicU64::new(0);
        futures_util::StreamExt::for_each_concurrent(
            futures_util::stream::iter(sender_paths.iter()),
            self.parallel,
            |sender_path| {
                let errs = &errs;
                let cycle_bytes = &cycle_bytes;
                async move {
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
        errors.extend(errs.into_inner());
        cycle_bytes.into_inner()
    }

    /// Plan + execute one filesystem against one peer. Returns bytes
    /// actually sent and at most one error message.
    async fn replicate_one(
        &self,
        ctx: &JobContext,
        cancel: &CancellationToken,
        peer_name: &str,
        peer: &Arc<PeerLink>,
        sender_path: &str,
    ) -> (u64, Option<String>) {
        let runner = ctx.zfs.command_runner();
        // FR-005: literal concat — target = root_fs/sender_path.
        let target = format!("{}/{}", self.config.target.root_fs, sender_path);
        tracing::info!(sender = %sender_path, target = %target, "push: planning");
        let (plan, sender_snaps) =
            match plan_one_filesystem(runner, peer.as_ref(), sender_path, &target, &self.filter)
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    let msg = format!("plan {sender_path}: {e}");
                    warn!(error = %msg);
                    return (0, Some(msg));
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
            } | SnapshotPlan::IncrementalFromBookmark {
                discard_partial_recv: true,
                ..
            }
        );
        if needs_discard {
            if self.config.dry_run {
                tracing::info!(target = %target, "push: dry-run would discard partial receive state");
            } else if let Err(e) = peer.discard_partial_recv(target.clone()).await {
                warn!(target = %target, error = %e, "discard_partial_recv RPC failed");
            }
        }
        match &plan {
            SnapshotPlan::Nothing => {
                tracing::info!(sender = %sender_path, "push: nothing to do");
                return (0, None);
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
            return (0, None);
        }
        // Publish transfer info for the UI. Total is a dry-run
        // estimate; resume streams have no cheap estimate.
        let kind = match &plan {
            SnapshotPlan::Full { .. } => "full",
            SnapshotPlan::Incremental { .. } | SnapshotPlan::IncrementalFromBookmark { .. } => {
                "incremental"
            }
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
        let res = run_one_filesystem(
            runner,
            peer.as_ref(),
            &self.config.name,
            peer_name,
            sender_path,
            &target,
            &plan,
            &sender_snaps,
            &self.config.send,
            self.limiter.as_deref(),
            cancel,
            &self.transfers,
            &key,
        )
        .await;
        let bytes = self
            .transfers
            .lock()
            .unwrap()
            .remove(&key)
            .map(|t| t.bytes_sent)
            .unwrap_or(0);
        match res {
            Ok(()) => (bytes, None),
            Err(e) => {
                let msg = format!("execute {sender_path}: {e}");
                warn!(error = %msg);
                (bytes, Some(msg))
            }
        }
    }
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
        s.transfers = {
            let g = self.transfers.lock().unwrap();
            let mut v: Vec<TransferInfo> = g.values().cloned().collect();
            v.sort_by(|a, b| (a.started_at, &a.dataset).cmp(&(b.started_at, &b.dataset)));
            v
        };
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
        let outcomes = self.peer_outcomes.lock().unwrap();
        s.targets = self
            .config
            .targets
            .iter()
            .map(|name| {
                let (last_success, last_error) =
                    outcomes.get(name).cloned().unwrap_or((None, None));
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
                    auto_interval_secs: self.peer_auto_interval(name).map(|d| d.as_secs()),
                    last_success,
                    last_error,
                }
            })
            .collect();
        s
    }
    fn wakeup(&self) {
        self.wakeup.notify_one();
    }
    fn cancel_current(&self) -> bool {
        if let Some(tok) = self.cycle_cancel.lock().unwrap().as_ref() {
            tok.cancel();
        }
        true
    }
    fn pause(&self) -> bool {
        self.paused.store(true, Ordering::Relaxed);
        if let Some(tok) = self.cycle_cancel.lock().unwrap().as_ref() {
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
            let due_at = match (
                self.peer_auto_interval(name),
                outcomes.get(name).and_then(|o| o.0),
            ) {
                (Some(iv), Some(ts)) => ts + iv.as_secs() as i64,
                // No interval or no history: due immediately.
                _ => now,
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
        let mut errors: Vec<String> = Vec::new();
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
        if let Some(pool) = ctx.state.as_ref() {
            let _ = crate::state::job_runs::record_start(pool, job_name, started_at).await;
        }
        let (bytes, outcome) = self.run_cycle(ctx, &cycle_token, selected, errors).await;
        *self.cycle_cancel.lock().unwrap() = None;
        let finished_at = OffsetDateTime::now_utc().unix_timestamp();
        let (status, err_msg) = match &outcome {
            Ok(()) => (crate::state::job_runs::STATUS_OK, None),
            Err(_) if cycle_token.is_cancelled() && !cancel.is_cancelled() => {
                (crate::state::job_runs::STATUS_CANCELLED, Some("cancelled"))
            }
            Err(e) => (crate::state::job_runs::STATUS_ERROR, Some(e.as_str())),
        };
        if let Some(pool) = ctx.state.as_ref() {
            let _ = crate::state::job_runs::record_finish(
                pool,
                job_name,
                started_at,
                finished_at,
                status,
                err_msg,
                Some(bytes as i64),
            )
            .await;
        }
        self.record_cycle(
            match status {
                "error" => outcome.err(),
                _ => None,
            },
            interval,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(name: &str, guid: u64) -> SnapshotEntry {
        SnapshotEntry {
            name: name.into(),
            guid,
            createtxg: guid,
        }
    }
    fn r(name: &str, guid: u64) -> SnapshotRef {
        SnapshotRef {
            name: name.into(),
            guid,
        }
    }
    fn e(name: &str, guid: u64, createtxg: u64) -> SnapshotEntry {
        SnapshotEntry {
            name: name.into(),
            guid,
            createtxg,
        }
    }

    #[test]
    fn empty_sender_means_nothing() {
        assert_eq!(pick_plan(&[], &[]), SnapshotPlan::Nothing);
        assert_eq!(pick_plan(&[], &[e("a", 1, 1)]), SnapshotPlan::Nothing);
    }

    #[test]
    fn empty_receiver_means_full_send_of_latest() {
        let sender = vec![s("a", 1), s("b", 2)];
        assert_eq!(
            pick_plan(&sender, &[]),
            SnapshotPlan::Full {
                to: r("b", 2),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn sender_already_at_receiver_latest_means_nothing() {
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("a", 1, 1), e("b", 2, 2)];
        assert_eq!(pick_plan(&sender, &receiver), SnapshotPlan::Nothing);
    }

    #[test]
    fn sender_ahead_by_one_means_incremental() {
        let sender = vec![s("a", 1), s("b", 2), s("c", 3)];
        let receiver = vec![e("a", 1, 1), e("b", 2, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Incremental {
                from: r("b", 2),
                to: r("c", 3),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn intersects_correctly_at_u64_above_i64_max() {
        let sender = vec![
            s("zrepl_001", 11587258101628135412),
            s("zrepl_002", 1711743136468914064),
            s("manual_001", 14719774020884296672),
        ];
        let receiver = vec![e("zrepl_001", 11587258101628135412, 8)];
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Incremental {
                from: r("zrepl_001", 11587258101628135412),
                to: r("manual_001", 14719774020884296672),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn compiled_filter_prefix_matches() {
        let cfg = SnapshotFilterConfig {
            prefix: Some("zrepl_".into()),
            regex: None,
        };
        let f = CompiledFilter::from_config(&cfg).unwrap();
        assert!(f.matches("zrepl_001"));
        assert!(!f.matches("manual_001"));
    }

    #[test]
    fn build_send_args_full_with_all_flags() {
        let plan = SnapshotPlan::Full {
            to: r("snap1", 1),
            discard_partial_recv: false,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(v, vec!["send", "-w", "-c", "-L", "-e", "tank/data@snap1"]);
    }

    #[test]
    fn build_send_args_resume_uses_dash_t() {
        let decoded = zfskit::resume_token::ResumeToken {
            token: "1-abc".into(),
            to_name: "tank/data@snap1".into(),
            to_guid: 42,
            from_guid: None,
            bytes_received: 1024,
        };
        let plan = SnapshotPlan::Resume {
            token: "1-abc".into(),
            decoded,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(v, vec!["send", "-t", "1-abc"]);
    }

    #[test]
    fn step_hold_tag_includes_peer_for_multi_target_isolation() {
        assert_eq!(
            step_hold_tag("backup", "home"),
            "arctern_step_J_backup_P_home"
        );
    }

    #[test]
    fn cursor_bookmark_name_includes_guid_job_and_peer() {
        let name = cursor_bookmark_name("tank/data", "backup", "home", 0x2a);
        assert_eq!(name, "tank/data#arctern_cursor_G_2a_J_backup_P_home");
        let leaf = name.split_once('#').unwrap().1;
        assert!(is_cursor_bookmark_leaf(leaf, "backup", "home"));
        assert!(!is_cursor_bookmark_leaf(leaf, "backup", "other"));
        assert!(!is_cursor_bookmark_leaf(leaf, "other", "home"));
    }

    #[test]
    fn resume_token_with_bookmark_base_stays_resume() {
        // Interrupted cursor-based send: the token's from_guid is the
        // BOOKMARK guid, absent from the snapshot list. The plan must
        // stay Resume instead of discarding the partial receive.
        let decoded = zfskit::resume_token::ResumeToken {
            token: "1-abc".into(),
            to_name: "tank/data@zrepl_new".into(),
            to_guid: 42,
            from_guid: Some(777),
            bytes_received: 4096,
        };
        let sender = vec![s("zrepl_new", 42)];
        let receiver = vec![e("old", 1, 1)];
        let bookmarks = vec![BookmarkRef {
            leaf: "arctern_cursor_G_309_J_push_P_mira".into(),
            guid: 777,
            createtxg: 10,
        }];
        let got = pick_plan_with_token(
            &sender,
            &receiver,
            Some("1-abc"),
            Some(&decoded),
            &bookmarks,
        );
        assert!(matches!(got, SnapshotPlan::Resume { .. }), "got {got:?}");
    }

    #[test]
    fn resume_token_with_vanished_base_discards() {
        // Neither a snapshot nor a bookmark carries the token's
        // from_guid — the partial is genuinely unresumable.
        let decoded = zfskit::resume_token::ResumeToken {
            token: "1-abc".into(),
            to_name: "tank/data@zrepl_new".into(),
            to_guid: 42,
            from_guid: Some(777),
            bytes_received: 4096,
        };
        let sender = vec![s("zrepl_new", 42)];
        let receiver = vec![e("zrepl_new", 42, 9)];
        let got = pick_plan_with_token(&sender, &receiver, Some("1-abc"), Some(&decoded), &[]);
        assert!(
            !matches!(got, SnapshotPlan::Resume { .. }),
            "must not resume, got {got:?}"
        );
    }

    #[test]
    fn bookmark_fallback_downgrades_full_to_incremental() {
        // Mirrors the zrepl-migration shape: receiver's newest snapshot
        // was pruned on the sender, but the cursor bookmark survives.
        let plan = SnapshotPlan::Full {
            to: r("zrepl_new", 42),
            discard_partial_recv: false,
        };
        let receiver = vec![e("zrepl_old", 13681249742552200977, 100)];
        let bookmarks = vec![
            BookmarkRef {
                leaf: "zrepl_CURSOR_G_bddd90278c3a7711_J_push_to_local".into(),
                guid: 13681249742552200977,
                createtxg: 100,
            },
            BookmarkRef {
                leaf: "unrelated".into(),
                guid: 7,
                createtxg: 999,
            },
        ];
        let got = apply_bookmark_fallback(plan, &[], &receiver, &bookmarks);
        assert_eq!(
            got,
            SnapshotPlan::IncrementalFromBookmark {
                from: SnapshotRef {
                    name: "zrepl_CURSOR_G_bddd90278c3a7711_J_push_to_local".into(),
                    guid: 13681249742552200977,
                },
                to: r("zrepl_new", 42),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn bookmark_fallback_picks_youngest_matching_base() {
        let plan = SnapshotPlan::Full {
            to: r("new", 42),
            discard_partial_recv: true,
        };
        let receiver = vec![e("a", 1, 10), e("b", 2, 20)];
        let bookmarks = vec![
            BookmarkRef {
                leaf: "old_cursor".into(),
                guid: 1,
                createtxg: 10,
            },
            BookmarkRef {
                leaf: "newer_cursor".into(),
                guid: 2,
                createtxg: 20,
            },
        ];
        let got = apply_bookmark_fallback(plan, &[], &receiver, &bookmarks);
        let SnapshotPlan::IncrementalFromBookmark {
            from,
            discard_partial_recv,
            ..
        } = got
        else {
            panic!("expected IncrementalFromBookmark, got {got:?}");
        };
        assert_eq!(from.name, "newer_cursor");
        assert_eq!(from.guid, 2);
        // The discard directive survives the downgrade.
        assert!(discard_partial_recv);
    }

    #[test]
    fn bookmark_fallback_keeps_full_when_no_guid_matches() {
        let plan = SnapshotPlan::Full {
            to: r("new", 42),
            discard_partial_recv: false,
        };
        let receiver = vec![e("a", 1, 10)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor".into(),
            guid: 999,
            createtxg: 10,
        }];
        let got = apply_bookmark_fallback(plan.clone(), &[], &receiver, &bookmarks);
        assert_eq!(got, plan);
    }

    #[test]
    fn bookmark_fallback_ignores_first_replication_and_non_full_plans() {
        // Empty receiver: Full is the correct first-replication plan.
        let full = SnapshotPlan::Full {
            to: r("new", 42),
            discard_partial_recv: false,
        };
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor".into(),
            guid: 42,
            createtxg: 10,
        }];
        assert_eq!(
            apply_bookmark_fallback(full.clone(), &[], &[], &bookmarks),
            full
        );
        // An Incremental whose bookmarks share no GUID with the receiver
        // passes through untouched.
        let incr = SnapshotPlan::Incremental {
            from: r("a", 1),
            to: r("b", 2),
            discard_partial_recv: false,
        };
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("a", 1, 10)];
        assert_eq!(
            apply_bookmark_fallback(incr.clone(), &sender, &receiver, &bookmarks),
            incr
        );
    }

    /// The 2026-07-09 production incident: retention pruned the sender's
    /// copy of the receiver's newest snapshots, an OLDER snapshot was
    /// still common, and the planner sent an incremental from it — which
    /// the receiver refused ("destination has been modified": its head
    /// was newer than the base). The cursor bookmark carried the
    /// receiver's head GUID the whole time and must win as the base.
    #[test]
    fn bookmark_newer_than_common_snapshot_replaces_incremental_base() {
        // Sender: old common snapshot (txg 10) + brand-new one (txg 40).
        let sender = vec![s("old_common", 10), s("new", 40)];
        // Receiver also has GUID 30 — the pruned-on-sender head.
        let receiver = vec![e("", 10, 0), e("", 30, 0)];
        let bookmarks = vec![BookmarkRef {
            leaf: "arctern_cursor_G_1e_J_push_P_mira".into(),
            guid: 30,
            createtxg: 30,
        }];
        let plan = pick_plan(&sender, &receiver);
        // Baseline picks the (unreceivable) old snapshot base...
        assert_eq!(
            plan,
            SnapshotPlan::Incremental {
                from: r("old_common", 10),
                to: r("new", 40),
                discard_partial_recv: false,
            }
        );
        // ...and the fallback upgrades it to the cursor bookmark.
        assert_eq!(
            apply_bookmark_fallback(plan, &sender, &receiver, &bookmarks),
            SnapshotPlan::IncrementalFromBookmark {
                from: SnapshotRef {
                    name: "arctern_cursor_G_1e_J_push_P_mira".into(),
                    guid: 30,
                },
                to: r("new", 40),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn common_snapshot_at_least_as_new_as_bookmark_keeps_snapshot_base() {
        // Snapshots can carry holds; bookmarks cannot — prefer the
        // snapshot when it is the same replication point.
        let sender = vec![s("common", 30), s("new", 40)];
        let receiver = vec![e("", 30, 0)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor_same_point".into(),
            guid: 30,
            createtxg: 30,
        }];
        let plan = pick_plan(&sender, &receiver);
        assert_eq!(
            apply_bookmark_fallback(plan.clone(), &sender, &receiver, &bookmarks),
            plan
        );
    }

    #[test]
    fn bookmark_of_latest_snapshot_means_nothing_to_do() {
        // The receiver already holds the sender's newest GUID; the only
        // sender-side witness of that is the bookmark. Sending an
        // incremental to the same point would be an empty stream.
        let sender = vec![s("old_common", 10), s("new", 40)];
        let receiver = vec![e("", 10, 0), e("", 40, 0)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor_at_head".into(),
            guid: 40,
            createtxg: 40,
        }];
        let plan = pick_plan(&sender, &receiver);
        assert_eq!(plan, SnapshotPlan::Nothing);
        assert_eq!(
            apply_bookmark_fallback(plan, &sender, &receiver, &bookmarks),
            SnapshotPlan::Nothing
        );
    }

    #[test]
    fn build_send_args_incremental_from_bookmark_uses_hash_base() {
        let plan = SnapshotPlan::IncrementalFromBookmark {
            from: SnapshotRef {
                name: "zrepl_CURSOR_G_bddd_J_push".into(),
                guid: 1,
            },
            to: r("zrepl_new", 2),
            discard_partial_recv: false,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(
            v,
            vec![
                "send",
                "-w",
                "-c",
                "-L",
                "-e",
                "-i",
                "tank/data#zrepl_CURSOR_G_bddd_J_push",
                "tank/data@zrepl_new"
            ]
        );
    }

    #[test]
    fn build_send_header_incremental_from_bookmark_is_wire_incremental() {
        let plan = SnapshotPlan::IncrementalFromBookmark {
            from: SnapshotRef {
                name: "zrepl_CURSOR_G_bddd_J_push".into(),
                guid: 1,
            },
            to: r("zrepl_new", 2),
            discard_partial_recv: false,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Incremental);
        assert_eq!(
            h.from_snap.as_ref().map(|f| f.name.as_str()),
            Some("zrepl_CURSOR_G_bddd_J_push")
        );
        assert_eq!(h.to_snap.name, "zrepl_new");
    }

    #[test]
    fn build_send_header_resume_does_not_set_discard_and_uses_leaf_name() {
        let decoded = zfskit::resume_token::ResumeToken {
            token: "1-abc".into(),
            to_name: "tank/data@snap1".into(),
            to_guid: 42,
            from_guid: None,
            bytes_received: 1024,
        };
        let plan = SnapshotPlan::Resume {
            token: "1-abc".into(),
            decoded,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Resume);
        assert!(!h.discard_partial_recv);
        // The receiver validates to_snap.name as a snapshot leaf (no
        // '/' or '@') and uses it to name target_dataset@<leaf> — the
        // full sender-side toname would be rejected.
        assert_eq!(h.to_snap.name, "snap1");
        assert_eq!(h.to_snap.guid, 42);
    }
}
