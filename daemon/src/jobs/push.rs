//! Push job — active sender. Each cycle, for every configured filesystem:
//! list local matching snapshots, ask the receiver via the SSH control
//! channel what it has, intersect by GUID, then open a recv channel and
//! pipe `zfs send`'s stdout into it.
//!
//! The planner (`pick_plan`, `pick_plan_with_token`, `build_send_header`,
//! `build_send_args`, `CompiledFilter`) is pure and unchanged from the
//! QUIC days. Only the executor is rebuilt on top of `peer::PeerLink`.
//!
//! Holds and cursor bookmarks (ARCHITECTURE.md "Holds and replication
//! cursor"):
//!
//!   - Step hold tag `arctern_step_J_<jobname>` is placed on the `to`
//!     snapshot before the send begins. Released on success; left in
//!     place on failure so a retry can find the snapshot regardless of
//!     intervening prune.
//!   - Cursor bookmark `<dataset>#arctern_cursor_J_<jobname>` is created
//!     from the new `to` snapshot on success; the previous cursor (same
//!     name, GUID-anchored) is destroyed after the new one lands so the
//!     transition is crash-safe.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use arctern_config::{PushJobConfig, SendFlagsConfig, SnapshotFilterConfig};
use arctern_transport::{
    PROTOCOL_VERSION, ProtocolError, RecvHeader, Request, Response, SendFlagsWire, SendHeader,
    SendKind, SnapshotEntry, SnapshotRef, compile_prefix_regex, regex,
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
    Resume {
        token: String,
        decoded: palimpsest::resume_token::ResumeToken,
    },
}

#[derive(Debug, Clone)]
pub struct CompiledFilter {
    re: Option<regex::Regex>,
    wire: Option<String>,
}

impl CompiledFilter {
    pub fn from_config(cfg: &SnapshotFilterConfig) -> Result<Self, regex::Error> {
        let wire = cfg.as_regex_str();
        let re = compile_prefix_regex(wire.as_deref())?;
        Ok(Self { re, wire })
    }

    pub fn matches(&self, snap_name: &str) -> bool {
        match &self.re {
            None => true,
            Some(r) => r.is_match(snap_name),
        }
    }

    #[allow(dead_code)]
    pub fn wire_regex(&self) -> Option<&str> {
        self.wire.as_deref()
    }
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum PlanError {
    #[error("list sender snapshots on {dataset}: {source}")]
    SenderList {
        dataset: String,
        #[source]
        source: palimpsest::ZfsError,
    },
    #[error("wire: {0}")]
    Wire(#[from] ProtocolError),
    #[error("LIST receiver error: {message}")]
    Receiver { message: String },
    #[error("decode resume token: {0}")]
    ResumeTokenDecode(#[from] palimpsest::resume_token::ResumeTokenError),
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
    let entries =
        palimpsest::dataset::list(runner, &opts)
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
            let guid = e.properties.get("guid").and_then(|p| p.value.parse::<u64>().ok())?;
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
                name: decoded.to_name.clone(),
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
        SnapshotPlan::Full { to, .. } => format!("{sender_dataset}@{}", to.name),
        SnapshotPlan::Incremental { to, .. } => format!("{sender_dataset}@{}", to.name),
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
    if let SnapshotPlan::Incremental { from, .. } = plan {
        args = args.incremental(format!("{sender_dataset}@{}", from.name));
    }
    Some(args)
}

/// Naming conventions pinned in ARCHITECTURE.md.
fn step_hold_tag(job_name: &str) -> String {
    format!("arctern_step_J_{job_name}")
}

fn cursor_bookmark_name(dataset: &str, job_name: &str) -> String {
    format!("{dataset}#arctern_cursor_J_{job_name}")
}

/// Plan one filesystem cycle against the receiver. Pure planner glue
/// over palimpsest + the control channel.
async fn plan_one_filesystem(
    runner: &dyn CommandRunner,
    peer: &PeerLink,
    sender_dataset: &str,
    target_dataset: &str,
    filter: &CompiledFilter,
) -> Result<SnapshotPlan, String> {
    let sender = list_sender_snaps(runner, sender_dataset, filter)
        .await
        .map_err(|e| format!("{e}"))?;
    if sender.is_empty() {
        return Ok(SnapshotPlan::Nothing);
    }
    let resp = peer
        .rpc(Request::ListSnapshots {
            dataset: target_dataset.to_string(),
            prefix_regex: filter.wire_regex().map(String::from),
        })
        .await
        .map_err(|e| format!("ListSnapshots: {e}"))?;
    let (receiver, token) = match resp {
        Response::ListSnapshotsOk {
            snapshots,
            receive_resume_token,
        } => (snapshots, receive_resume_token),
        Response::Error { message, .. } => {
            return Err(format!("ListSnapshots receiver error: {message}"));
        }
        other => return Err(format!("unexpected ListSnapshots response: {other:?}")),
    };
    let decoded = match token.as_deref() {
        Some(t) => match palimpsest::resume_token::decode(runner, t).await {
            Ok(d) => Some(d),
            Err(e) => {
                tracing::info!(
                    target = %target_dataset,
                    error = %e,
                    "push: receiver token failed to decode, treating as stale"
                );
                return Ok(pick_plan_with_discard(&sender, &receiver, true));
            }
        },
        None => None,
    };
    Ok(pick_plan_with_token(
        &sender,
        &receiver,
        token.as_deref(),
        decoded.as_ref(),
    ))
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
    cancel: &CancellationToken,
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
    let copy_res = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            // Drop both halves: the recv channel's stdin closes the SSH
            // child's stdin, propagating SIGPIPE to the remote zfs recv;
            // start_kill on the local send child terminates it.
            let _ = channel_stdin.shutdown().await;
            let _ = child.start_kill();
            return Err("cancelled".into());
        }
        r = tokio::io::copy(&mut child_stdout, &mut channel_stdin) => r,
    };
    let _ = channel_stdin.shutdown().await;
    drop(channel_stdin);
    let stderr_bytes = stderr_drain.await.unwrap_or_default();
    let exit = child
        .wait()
        .await
        .map_err(|e| format!("send wait: {e}"))?;
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
/// the snapshot mid-stream; released only on success. Cursor bookmark
/// is created from the new `to` snapshot after the receiver has
/// acknowledged; the previous cursor (same name, GUID-anchored) is
/// destroyed afterwards so the transition is crash-safe.
#[allow(clippy::too_many_arguments)]
async fn run_one_filesystem(
    runner: &dyn CommandRunner,
    peer: &PeerLink,
    job_name: &str,
    sender_dataset: &str,
    target_dataset: &str,
    plan: &SnapshotPlan,
    flags: &SendFlagsConfig,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let to_snap_name: Option<String> = match plan {
        SnapshotPlan::Full { to, .. } | SnapshotPlan::Incremental { to, .. } => {
            Some(format!("{sender_dataset}@{}", to.name))
        }
        SnapshotPlan::Resume { decoded, .. } => Some(decoded.to_name.clone()),
        SnapshotPlan::Nothing => None,
    };
    let tag = step_hold_tag(job_name);
    if let Some(snap) = &to_snap_name {
        // hold is idempotent at the palimpsest layer (no-op when the
        // tag already exists for that snapshot).
        if let Err(e) = palimpsest::hold::hold(runner, snap, &tag).await {
            warn!(snapshot = %snap, error = %e, "step hold failed; sending anyway");
        }
    }

    // Leave the step hold in place on failure — it protects the snapshot
    // for the next cycle's retry. Hence `?` propagates without a release.
    execute_one_plan(
        runner, peer, job_name, plan, target_dataset, sender_dataset, flags, cancel,
    )
    .await?;

    if let Some(snap) = &to_snap_name {
        let cursor = cursor_bookmark_name(sender_dataset, job_name);
        if let Err(e) = palimpsest::bookmark::create(runner, snap, &cursor).await {
            warn!(snapshot = %snap, bookmark = %cursor, error = %e, "create cursor bookmark");
        }
        if let Err(e) = palimpsest::hold::release(runner, snap, &tag).await {
            warn!(snapshot = %snap, tag = %tag, error = %e, "release step hold");
        }
    }
    Ok(())
}

pub const KIND: &str = arctern_api::JOB_KIND_PUSH;

pub struct PushJob {
    config: PushJobConfig,
    filter: CompiledFilter,
    status: Mutex<JobStatusInner>,
    wakeup: Arc<tokio::sync::Notify>,
    /// Shared peers state. Each cycle looks up the configured peer name
    /// here so that a reconnect performed by the background task takes
    /// effect on the next cycle without restarting the job.
    peers: Option<PeersState>,
}

impl PushJob {
    pub fn new(config: PushJobConfig, peers: Option<PeersState>) -> Result<Self, regex::Error> {
        let filter = CompiledFilter::from_config(&config.snapshot_filter)?;
        Ok(Self {
            config,
            filter,
            status: Mutex::new(JobStatusInner::default()),
            wakeup: Arc::new(tokio::sync::Notify::new()),
            peers,
        })
    }

    async fn current_link(&self) -> Option<Arc<PeerLink>> {
        let peers = self.peers.as_ref()?;
        let name = self.config.peer.as_deref()?;
        let g = peers.read().await;
        g.get(name).and_then(|e| e.link.clone())
    }

    fn record_cycle(&self, last_error: Option<String>, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.last_run = Some(now);
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.last_error = last_error;
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
        Ok(arctern_config::filter::resolve_all(&self.config.filesystems, &names)
            .into_iter()
            .map(str::to_string)
            .collect())
    }

    async fn run_cycle(&self, ctx: &JobContext, cancel: &CancellationToken) -> Result<(), String> {
        let Some(peer) = self.current_link().await else {
            return Err(format!(
                "push job {:?}: peer {:?} not currently connected",
                self.config.name,
                self.config.peer.as_deref().unwrap_or("<unset>")
            ));
        };
        let runner = ctx.runner.as_ref();
        let mut errors: Vec<String> = Vec::new();
        let sender_paths = self.expand_filesystems(runner).await?;
        for sender_path in &sender_paths {
            if cancel.is_cancelled() {
                break;
            }
            // FR-005: literal concat — target = root_fs/sender_path.
            let target = format!("{}/{}", self.config.target.root_fs, sender_path);
            tracing::info!(sender = %sender_path, target = %target, "push: planning");
            let plan = match plan_one_filesystem(
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
                }
            );
            if needs_discard
                && let Err(e) = peer
                    .rpc(Request::DiscardPartialRecv {
                        dataset: target.clone(),
                    })
                    .await
            {
                warn!(target = %target, error = %e, "DiscardPartialRecv RPC failed");
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
                SnapshotPlan::Resume { decoded, .. } => {
                    tracing::info!(
                        sender = %sender_path,
                        to = %decoded.to_name,
                        bytes = decoded.bytes_received,
                        "push: resuming from token"
                    );
                }
            }
            if let Err(e) = run_one_filesystem(
                runner,
                peer.as_ref(),
                &self.config.name,
                sender_path,
                &target,
                &plan,
                &self.config.send,
                cancel,
            )
            .await
            {
                let msg = format!("execute {sender_path}: {e}");
                warn!(error = %msg);
                errors.push(msg);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
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
        self.status.lock().unwrap().clone()
    }
    fn wakeup(&self) {
        self.wakeup.notify_one();
    }
    fn run(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let span = info_span!("push_job", name = %self.config.name);
        Box::pin(
            async move {
                let interval = self.config.interval;
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = sleep(interval) => {}
                        _ = self.wakeup.notified() => {}
                    }
                    let outcome = self.run_cycle(&ctx, &cancel).await;
                    self.record_cycle(outcome.err(), interval);
                }
            }
            .instrument(span),
        )
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
        assert_eq!(f.wire_regex(), Some("^zrepl_"));
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
    fn step_hold_tag_format_matches_architecture_doc() {
        assert_eq!(step_hold_tag("backup"), "arctern_step_J_backup");
    }

    #[test]
    fn cursor_bookmark_name_format_matches_architecture_doc() {
        assert_eq!(
            cursor_bookmark_name("tank/data", "backup"),
            "tank/data#arctern_cursor_J_backup"
        );
    }

    #[test]
    fn build_send_header_resume_does_not_set_discard() {
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
    }
}
