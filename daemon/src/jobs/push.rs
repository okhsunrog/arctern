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
    PROTOCOL_VERSION, RecvHeader, Request, Response, SendFlagsWire, SendHeader, SendKind,
    SnapshotEntry, SnapshotRef, compile_prefix_regex, regex,
};
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::CommandRunner;
use palimpsest::send::{SendArgs, send as zfs_send};
use thiserror::Error;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};

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
        decoded: palimpsest::resume_token::ResumeToken,
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
        source: palimpsest::ZfsError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSnap {
    name: String,
    guid: u64,
    createtxg: u64,
}

pub async fn list_sender_snaps(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    filter: &CompiledFilter,
) -> Result<Vec<SnapshotRef>, PlanError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![sender_dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = palimpsest::dataset::list(runner, &opts)
        .await
        .map_err(|source| PlanError::SenderList {
            dataset: sender_dataset.to_string(),
            source,
        })?;
    let mut snaps: Vec<LocalSnap> = entries
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
            Some(LocalSnap {
                name: snap_name,
                guid,
                createtxg,
            })
        })
        .collect();
    snaps.sort_by_key(|s| s.createtxg);
    Ok(snaps
        .into_iter()
        .map(|s| SnapshotRef {
            name: s.name,
            guid: s.guid,
        })
        .collect())
}

pub fn pick_plan(sender: &[SnapshotRef], receiver: &[SnapshotEntry]) -> SnapshotPlan {
    pick_plan_with_discard(sender, receiver, false)
}

fn pick_plan_with_discard(
    sender: &[SnapshotRef],
    receiver: &[SnapshotEntry],
    discard_partial_recv: bool,
) -> SnapshotPlan {
    let Some(latest) = sender.last() else {
        return SnapshotPlan::Nothing;
    };
    if receiver.is_empty() {
        return SnapshotPlan::Full {
            to: latest.clone(),
            discard_partial_recv,
        };
    }
    use std::collections::BTreeSet;
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    let mut from: Option<&SnapshotRef> = None;
    for s in sender.iter().rev() {
        if recv_guids.contains(&s.guid) {
            from = Some(s);
            break;
        }
    }
    match from {
        None => SnapshotPlan::Full {
            to: latest.clone(),
            discard_partial_recv,
        },
        Some(f) if f.guid == latest.guid => SnapshotPlan::Nothing,
        Some(f) => SnapshotPlan::Incremental {
            from: f.clone(),
            to: latest.clone(),
            discard_partial_recv,
        },
    }
}

/// Fallback for the "no common snapshot" case: if the base plan is a
/// Full send but some sender bookmark's GUID is present on the
/// receiver, downgrade to an incremental from that bookmark (the
/// youngest matching one). Any bookmark qualifies — arctern's own
/// cursors, zrepl's `#zrepl_CURSOR_*` left over from a migration, or a
/// hand-made `zfs bookmark`. Plans other than Full pass through.
pub fn apply_bookmark_fallback(
    plan: SnapshotPlan,
    receiver: &[SnapshotEntry],
    bookmarks: &[BookmarkRef],
) -> SnapshotPlan {
    let SnapshotPlan::Full {
        to,
        discard_partial_recv,
    } = plan
    else {
        return plan;
    };
    if receiver.is_empty() {
        // First replication — Full is correct, not a degraded case.
        return SnapshotPlan::Full {
            to,
            discard_partial_recv,
        };
    }
    use std::collections::BTreeSet;
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    let base = bookmarks
        .iter()
        .filter(|b| recv_guids.contains(&b.guid))
        .max_by_key(|b| b.createtxg);
    match base {
        Some(b) => SnapshotPlan::IncrementalFromBookmark {
            from: SnapshotRef {
                name: b.leaf.clone(),
                guid: b.guid,
            },
            to,
            discard_partial_recv,
        },
        None => SnapshotPlan::Full {
            to,
            discard_partial_recv,
        },
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
    let entries = palimpsest::dataset::list(runner, &opts)
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
    sender: &[SnapshotRef],
    receiver: &[SnapshotEntry],
    token: Option<&str>,
    decoded: Option<&palimpsest::resume_token::ResumeToken>,
) -> SnapshotPlan {
    let (Some(token), Some(decoded)) = (token, decoded) else {
        return pick_plan(sender, receiver);
    };
    use std::collections::BTreeSet;
    let sender_guids: BTreeSet<u64> = sender.iter().map(|s| s.guid).collect();
    let to_live = sender_guids.contains(&decoded.to_guid);
    let from_live = decoded
        .from_guid
        .map(|g| sender_guids.contains(&g))
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
        let mut args = SendArgs::new("ignored").resume_token(token);
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
        return Some(args);
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
/// over palimpsest + the control channel. Returns the plan plus the
/// filtered sender snapshot list (the executor's hold sweep reuses it).
async fn plan_one_filesystem(
    runner: &dyn CommandRunner,
    peer: &PeerLink,
    sender_dataset: &str,
    target_dataset: &str,
    filter: &CompiledFilter,
) -> Result<(SnapshotPlan, Vec<SnapshotRef>), String> {
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
    let resp = peer
        .rpc(Request::ListReceiverGuids {
            dataset: target_dataset.to_string(),
            prefix_regex: None,
        })
        .await
        .map_err(|e| format!("ListReceiverGuids: {e}"))?;
    let (guids, token) = match resp {
        Response::ListReceiverGuidsOk {
            guids,
            receive_resume_token,
        } => (guids, receive_resume_token),
        Response::Error { message, .. } => {
            return Err(format!("ListReceiverGuids receiver error: {message}"));
        }
        other => return Err(format!("unexpected ListReceiverGuids response: {other:?}")),
    };
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
        Some(t) => match palimpsest::resume_token::decode(runner, t).await {
            Ok(d) => Some(d),
            Err(e) => {
                tracing::info!(
                    target = %target_dataset,
                    error = %e,
                    "push: receiver token failed to decode, treating as stale"
                );
                let plan = pick_plan_with_discard(&sender, &receiver, true);
                let plan = maybe_bookmark_fallback(runner, sender_dataset, plan, &receiver).await;
                return Ok((plan, sender));
            }
        },
        None => None,
    };
    let plan = pick_plan_with_token(&sender, &receiver, token.as_deref(), decoded.as_ref());
    let plan = maybe_bookmark_fallback(runner, sender_dataset, plan, &receiver).await;
    Ok((plan, sender))
}

/// Wrap `apply_bookmark_fallback` with the bookmark listing, skipping
/// the extra `zfs list` entirely when the plan can't benefit. A listing
/// failure degrades to the original Full plan with a warning — a full
/// resend is correct, just expensive.
async fn maybe_bookmark_fallback(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    plan: SnapshotPlan,
    receiver: &[SnapshotEntry],
) -> SnapshotPlan {
    if !matches!(plan, SnapshotPlan::Full { .. }) || receiver.is_empty() {
        return plan;
    }
    match list_sender_bookmarks(runner, sender_dataset).await {
        Ok(bookmarks) => {
            let plan = apply_bookmark_fallback(plan, receiver, &bookmarks);
            if let SnapshotPlan::IncrementalFromBookmark { from, .. } = &plan {
                tracing::info!(
                    dataset = %sender_dataset,
                    bookmark = %from.name,
                    "push: no common snapshot; falling back to incremental from bookmark"
                );
            }
            plan
        }
        Err(e) => {
            warn!(dataset = %sender_dataset, error = %e, "push: bookmark listing failed; keeping full-send plan");
            plan
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
    bandwidth_limit: Option<u64>,
    cancel: &CancellationToken,
    transfer: &Mutex<Option<TransferInfo>>,
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
        .stdout
        .take()
        .ok_or_else(|| "no stdout on send child".to_string())?;
    let mut child_stderr = child
        .stderr
        .take()
        .ok_or_else(|| "no stderr on send child".to_string())?;
    let stderr_drain = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = child_stderr.read_to_end(&mut buf).await;
        buf
    });

    let mut channel_stdin = channel
        .stdin
        .take()
        .ok_or_else(|| "no stdin on recv channel".to_string())?;
    // Manual copy loop instead of tokio::io::copy: publishes progress
    // into `transfer` (the UI derives speed from poll deltas) and races
    // the job/cycle CancellationToken. On cancel the recv channel's
    // stdin closes (SIGPIPE to the remote zfs recv, which keeps its
    // resumable partial state) and the local send child is killed.
    let copy_res: std::io::Result<u64> = async {
        let mut buf = vec![0u8; 256 * 1024];
        let mut copied: u64 = 0;
        let mut last_published: u64 = 0;
        let throttle_start = tokio::time::Instant::now();
        loop {
            let step = async {
                let n = child_stdout.read(&mut buf).await?;
                if n > 0 {
                    channel_stdin.write_all(&buf[..n]).await?;
                }
                std::io::Result::Ok(n)
            };
            let n = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err(std::io::Error::other("cancelled"));
                }
                r = step => r?,
            };
            if n == 0 {
                break;
            }
            copied += n as u64;
            // Token-bucket-by-arithmetic: if we are ahead of the
            // configured rate, sleep off the surplus. Chunk-grained
            // (256 KiB) which is plenty smooth at network scales.
            if let Some(rate) = bandwidth_limit {
                let expected = StdDuration::from_secs_f64(copied as f64 / rate as f64);
                let elapsed = throttle_start.elapsed();
                if expected > elapsed {
                    tokio::time::sleep(expected - elapsed).await;
                }
            }
            if copied - last_published >= 8 * 1024 * 1024 {
                last_published = copied;
                if let Some(t) = transfer.lock().unwrap().as_mut() {
                    t.bytes_sent = copied;
                }
            }
        }
        if let Some(t) = transfer.lock().unwrap().as_mut() {
            t.bytes_sent = copied;
        }
        Ok(copied)
    }
    .await;
    if copy_res.is_err() {
        let _ = channel_stdin.shutdown().await;
        let _ = child.start_kill();
        if cancel.is_cancelled() {
            let _ = child.wait().await;
            return Err("cancelled".into());
        }
    }
    let _ = channel_stdin.shutdown().await;
    drop(channel_stdin);
    let stderr_bytes = stderr_drain.await.unwrap_or_default();
    let exit = child.wait().await.map_err(|e| format!("send wait: {e}"))?;
    if let Err(e) = copy_res {
        return Err(format!(
            "stream copy: {e}; send stderr: {}",
            String::from_utf8_lossy(&stderr_bytes).trim()
        ));
    }
    if !exit.success() {
        return Err(format!(
            "zfs send failed (exit {:?}): {}",
            exit.code(),
            String::from_utf8_lossy(&stderr_bytes).trim()
        ));
    }
    let resp = channel
        .finish()
        .await
        .map_err(|e| format!("read recv response: {e}"))?;
    match resp {
        Response::Ok => Ok(()),
        Response::Error { message, .. } => Err(format!("receiver: {message}")),
        other => Err(format!("unexpected recv response: {other:?}")),
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
    sender_snaps: &[SnapshotRef],
    flags: &SendFlagsConfig,
    bandwidth_limit: Option<u64>,
    cancel: &CancellationToken,
    transfer: &Mutex<Option<TransferInfo>>,
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
        // hold is idempotent at the palimpsest layer (no-op when the
        // tag already exists for that snapshot).
        if let Err(e) = palimpsest::hold::hold(runner, snap, &tag).await {
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
        bandwidth_limit,
        cancel,
        transfer,
    )
    .await?;

    if let Some((snap, guid)) = &to_hold_target {
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
    if let Err(e) = palimpsest::bookmark::create(runner, to_snap, &cursor).await {
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
    let bookmarks = match palimpsest::dataset::list(runner, &opts).await {
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
            && let Err(e) = palimpsest::bookmark::destroy(runner, &b.name).await
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
    sender_snaps: &[SnapshotRef],
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
    let holds = match palimpsest::hold::list_holds_many(runner, &refs).await {
        Ok(h) => h,
        Err(e) => {
            warn!(dataset = %sender_dataset, error = %e, "step-hold sweep holds query failed");
            return;
        }
    };
    for h in holds.iter().filter(|h| h.tag == tag) {
        if let Err(e) = palimpsest::hold::release(runner, &h.dataset, tag).await {
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
    /// Parsed `bandwidth_limit`, bytes per second.
    bandwidth_limit: Option<u64>,
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
    /// In-flight transfer progress, mirrored into `status()`.
    transfer: Arc<Mutex<Option<TransferInfo>>>,
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
        let bandwidth_limit = config
            .bandwidth_limit
            .as_deref()
            .and_then(|s| arctern_config::parse_bytes_per_sec(s).ok());
        let peer_configs = all_peer_configs
            .iter()
            .filter(|p| config.targets.contains(&p.name))
            .cloned()
            .collect();
        Ok(Self {
            config,
            bandwidth_limit,
            peers_changed,
            peer_configs,
            filter,
            status: Mutex::new(JobStatusInner::default()),
            wakeup: Arc::new(tokio::sync::Notify::new()),
            peers,
            transfer: Arc::new(Mutex::new(None)),
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
        let entries = palimpsest::dataset::list(runner, &opts)
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
        let runner = ctx.runner.as_ref();
        let mut cycle_bytes: u64 = 0;
        let sender_paths = match self.expand_filesystems(runner).await {
            Ok(p) => p,
            Err(e) => {
                errors.push(e);
                return 0;
            }
        };
        for sender_path in &sender_paths {
            if cancel.is_cancelled() {
                break;
            }
            // FR-005: literal concat — target = root_fs/sender_path.
            let target = format!("{}/{}", self.config.target.root_fs, sender_path);
            tracing::info!(sender = %sender_path, target = %target, "push: planning");
            let (plan, sender_snaps) = match plan_one_filesystem(
                runner,
                peer.as_ref(),
                sender_path,
                &target,
                &self.filter,
            )
            .await
            {
                Ok(p) => p,
                Err(e) => {
                    let msg = format!("plan {sender_path}: {e}");
                    warn!(error = %msg);
                    errors.push(msg);
                    continue;
                }
            };
            // If the planner picked discard, send the explicit Request
            // before opening the recv channel — it's idempotent and
            // makes the recv channel's first action a fresh, clean recv.
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
                } else if let Err(e) = peer
                    .rpc(Request::DiscardPartialRecv {
                        dataset: target.clone(),
                    })
                    .await
                {
                    warn!(target = %target, error = %e, "DiscardPartialRecv RPC failed");
                }
            }
            match &plan {
                SnapshotPlan::Nothing => {
                    tracing::info!(sender = %sender_path, "push: nothing to do");
                    continue;
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
                continue;
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
                Some(args) if kind != "resume" => palimpsest::send::dry_run(runner, &args)
                    .await
                    .ok()
                    .map(|d| d.total_bytes),
                _ => None,
            };
            *self.transfer.lock().unwrap() = Some(TransferInfo {
                dataset: sender_path.clone(),
                peer: peer_name.to_string(),
                kind: kind.to_string(),
                bytes_sent: 0,
                total_bytes: total,
                started_at: OffsetDateTime::now_utc().unix_timestamp(),
            });
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
                self.bandwidth_limit,
                cancel,
                &self.transfer,
            )
            .await;
            if let Some(t) = self.transfer.lock().unwrap().take() {
                cycle_bytes += t.bytes_sent;
            }
            if let Err(e) = res {
                let msg = format!("execute {sender_path}: {e}");
                warn!(error = %msg);
                errors.push(msg);
            }
        }
        cycle_bytes
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
        s.transfer = self.transfer.lock().unwrap().clone();
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

    fn s(name: &str, guid: u64) -> SnapshotRef {
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
                to: s("b", 2),
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
                from: s("b", 2),
                to: s("c", 3),
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
                from: s("zrepl_001", 11587258101628135412),
                to: s("manual_001", 14719774020884296672),
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
            to: s("snap1", 1),
            discard_partial_recv: false,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(v, vec!["send", "-w", "-c", "-L", "-e", "tank/data@snap1"]);
    }

    #[test]
    fn build_send_args_resume_uses_dash_t() {
        let decoded = palimpsest::resume_token::ResumeToken {
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
        assert_eq!(v, vec!["send", "-w", "-c", "-L", "-e", "-t", "1-abc"]);
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
    fn bookmark_fallback_downgrades_full_to_incremental() {
        // Mirrors the zrepl-migration shape: receiver's newest snapshot
        // was pruned on the sender, but the cursor bookmark survives.
        let plan = SnapshotPlan::Full {
            to: s("zrepl_new", 42),
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
        let got = apply_bookmark_fallback(plan, &receiver, &bookmarks);
        assert_eq!(
            got,
            SnapshotPlan::IncrementalFromBookmark {
                from: SnapshotRef {
                    name: "zrepl_CURSOR_G_bddd90278c3a7711_J_push_to_local".into(),
                    guid: 13681249742552200977,
                },
                to: s("zrepl_new", 42),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn bookmark_fallback_picks_youngest_matching_base() {
        let plan = SnapshotPlan::Full {
            to: s("new", 42),
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
        let got = apply_bookmark_fallback(plan, &receiver, &bookmarks);
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
            to: s("new", 42),
            discard_partial_recv: false,
        };
        let receiver = vec![e("a", 1, 10)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor".into(),
            guid: 999,
            createtxg: 10,
        }];
        let got = apply_bookmark_fallback(plan.clone(), &receiver, &bookmarks);
        assert_eq!(got, plan);
    }

    #[test]
    fn bookmark_fallback_ignores_first_replication_and_non_full_plans() {
        // Empty receiver: Full is the correct first-replication plan.
        let full = SnapshotPlan::Full {
            to: s("new", 42),
            discard_partial_recv: false,
        };
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor".into(),
            guid: 42,
            createtxg: 10,
        }];
        assert_eq!(apply_bookmark_fallback(full.clone(), &[], &bookmarks), full);
        // Non-Full plans pass through untouched.
        let incr = SnapshotPlan::Incremental {
            from: s("a", 1),
            to: s("b", 2),
            discard_partial_recv: false,
        };
        let receiver = vec![e("a", 1, 10)];
        assert_eq!(
            apply_bookmark_fallback(incr.clone(), &receiver, &bookmarks),
            incr
        );
    }

    #[test]
    fn build_send_args_incremental_from_bookmark_uses_hash_base() {
        let plan = SnapshotPlan::IncrementalFromBookmark {
            from: SnapshotRef {
                name: "zrepl_CURSOR_G_bddd_J_push".into(),
                guid: 1,
            },
            to: s("zrepl_new", 2),
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
            to: s("zrepl_new", 2),
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
        let decoded = palimpsest::resume_token::ResumeToken {
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
