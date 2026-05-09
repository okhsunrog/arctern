//! Push job — active sender. Each cycle, for every configured
//! filesystem: list the sender's matching snapshots, ask the receiver
//! over QUIC LIST what it already has, intersect by GUID to choose
//! Full vs Incremental, then open a SEND stream and pipe
//! `palimpsest::send::send`'s stdout into it.
//!
//! This file holds the planner (T004), executor (T005), and PushJob
//! cycle loop (T006). Splitting into submodules would cost more in
//! re-exports than it saves; the surface stays inside one file
//! together with its tests.
//!
//! T004 landed the planner; T005 the executor; T006 wires both into
//! a PushJob with cycle loop + wakeup.

use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use arctern_config::PushJobConfig;
use arctern_transport::client_config_accept_any;
use time::OffsetDateTime;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};

use super::{Job, JobContext, JobStatusInner};

use arctern_config::{SendFlagsConfig, SnapshotFilterConfig};
use arctern_transport::{
    Op, ProtocolError, ReceiveHeader, ReceiveResponse, SendFlagsWire, SendHeader, SendKind,
    SnapshotEntry, SnapshotRef, compile_prefix_regex, read_response, regex, write_header,
};
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::CommandRunner;
use palimpsest::send::{SendArgs, send as zfs_send};
use thiserror::Error;
use tokio::io::AsyncReadExt;

/// What the planner decided to do for one filesystem this cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPlan {
    /// Sender has no matching snapshots, OR the latest sender snapshot
    /// is already on the receiver. No QUIC SEND stream this cycle.
    Nothing,
    /// First-replication path. Send the sender's latest matching
    /// snapshot in full. `discard_partial_recv` is set when the
    /// receiver advertised a stale resume token that must be cleared
    /// (`zfs recv -A`) before this fresh stream can land — D6
    /// confirmed in the VM that ZFS rejects a fresh full into a
    /// dataset with `receive_resume_token` set, even with -F.
    Full {
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    /// Send the delta from `from` (highest-createtxg common GUID with
    /// the receiver) to `to` (sender's latest matching snapshot).
    /// Same `discard_partial_recv` semantics as `Full`.
    Incremental {
        from: SnapshotRef,
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    /// Continue an in-flight partial recv on the receiver via
    /// `zfs send -t <token>`. The token encodes the destination's
    /// expected `(from_guid, to_guid)`; the planner verifies both
    /// remain on the sender before emitting this variant. Falls
    /// through to Full/Incremental with `discard_partial_recv = true`
    /// when validation fails.
    Resume {
        token: String,
        decoded: palimpsest::resume_token::ResumeToken,
    },
}

/// Compiled snapshot filter. Owns the optional regex (so the planner
/// doesn't recompile each cycle) and the original wire string (so the
/// LIST request can carry the same regex the planner is using).
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

    pub fn wire_regex(&self) -> Option<&str> {
        self.wire.as_deref()
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
    #[error("LIST request: {0}")]
    Wire(#[from] ProtocolError),
    #[error("LIST receiver error: {message}")]
    Receiver { message: String },
    #[error("quinn: {0}")]
    Quinn(String),
    /// Slice 006: receiver advertised a `receive_resume_token` but the
    /// sender failed to decode it via `zfs send -nvt`. Per spec edge
    /// case "Token decode itself fails", we do NOT fall through to a
    /// fresh send — that would fail at recv with the partial-state
    /// error and not get cleaned up. Operator-driven recovery (manual
    /// `zfs recv -A`).
    #[error("decode resume token: {0}")]
    ResumeTokenDecode(#[from] palimpsest::resume_token::ResumeTokenError),
}

/// Sender-side snapshot pulled out of palimpsest into the planner's
/// internal representation. Kept private; the planner emits
/// `SnapshotRef`s on `SnapshotPlan` for the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSnap {
    name: String,
    guid: u64,
    createtxg: u64,
}

/// List the sender's snapshots under `sender_dataset`, filter by
/// `filter`, sort by `createtxg` ascending. Public for testability.
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

/// Pure planner core: given sender + receiver snapshot lists (sender
/// sorted by createtxg ascending, receiver in any order), return the
/// `SnapshotPlan`. Split out so the GUID-intersection algorithm is
/// trivially testable without ZFS or QUIC.
pub fn pick_plan(sender: &[SnapshotRef], receiver: &[SnapshotEntry]) -> SnapshotPlan {
    pick_plan_with_discard(sender, receiver, false)
}

/// Same algorithm as `pick_plan` but stamps the `discard_partial_recv`
/// flag on the returned `Full` / `Incremental` (slice 006). Split out
/// so `pick_plan` keeps its slice-005 signature and call sites that
/// don't care about the flag stay terse.
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
    // Walk sender from highest createtxg down; first GUID-hit is the from.
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

/// Slice 006 planner core: choose between Resume, Full+discard,
/// Incremental+discard, and the slice-005 plain Full / Incremental /
/// Nothing. Pure (no I/O) so the GUID-validation logic is testable
/// without QUIC or ZFS.
///
/// The token validation is "are BOTH endpoints encoded in the token
/// still on the sender?" — to_guid must always match; from_guid must
/// match when the token is for an incremental resume (full-send
/// tokens have `from_guid == None`, in which case only to_guid matters).
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
        // Token is stale. Fall back to slice-005 algorithm but mark the
        // plan so the sink runs `zfs recv -A` first.
        pick_plan_with_discard(sender, receiver, true)
    }
}

/// Open a fresh QUIC bi stream, send a LIST request, read the
/// receiver's snapshot list AND the receiver's `receive_resume_token`
/// (slice 006). Returns `(snapshots, token)`. An empty snapshot Vec
/// means the dataset doesn't exist yet (sink maps DatasetNotFound to
/// an empty Ok per slice 005 D16). `token = None` means there is no
/// partial recv in flight on the receiver — the most common case.
pub async fn fetch_receiver_snaps(
    connection: &quinn::Connection,
    target_dataset: &str,
    filter: &CompiledFilter,
) -> Result<(Vec<SnapshotEntry>, Option<String>), PlanError> {
    use arctern_transport::{ListResponse, read_list_response};

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|e| PlanError::Quinn(format!("open_bi (list): {e}")))?;
    let header = ReceiveHeader {
        version: arctern_transport::PROTOCOL_VERSION,
        op: Op::List,
        target_dataset: target_dataset.to_string(),
        prefix_regex: filter.wire_regex().map(String::from),
        send: None,
    };
    write_header(&mut send, &header).await?;
    send.finish()
        .map_err(|e| PlanError::Quinn(format!("finish (list): {e}")))?;
    let resp = read_list_response(&mut recv).await?;
    match resp {
        ListResponse::Ok {
            snapshots,
            receive_resume_token,
        } => Ok((snapshots, receive_resume_token)),
        ListResponse::Error { message } => Err(PlanError::Receiver { message }),
    }
}

/// Plan one filesystem cycle: list sender, ask receiver via LIST,
/// decide via `pick_plan`. The QUIC connection is borrowed; the
/// caller manages its lifecycle (opens it once per cycle and closes
/// it after every filesystem is processed).
pub async fn plan_one_filesystem(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    target_dataset: &str,
    filter: &CompiledFilter,
    connection: &quinn::Connection,
) -> Result<SnapshotPlan, PlanError> {
    let sender = list_sender_snaps(runner, sender_dataset, filter).await?;
    if sender.is_empty() {
        return Ok(SnapshotPlan::Nothing);
    }
    let (receiver, token) = fetch_receiver_snaps(connection, target_dataset, filter).await?;
    let decoded = match token.as_deref() {
        Some(t) => Some(palimpsest::resume_token::decode(runner, t).await?),
        None => None,
    };
    Ok(pick_plan_with_token(
        &sender,
        &receiver,
        token.as_deref(),
        decoded.as_ref(),
    ))
}

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("nothing to do")]
    Nothing,
    #[error("spawn zfs send: {0}")]
    SpawnSend(palimpsest::ZfsError),
    #[error("zfs send failed (exit {exit:?}): {stderr}")]
    SendFailed { exit: Option<i32>, stderr: String },
    #[error("io copy stream -> quic: {source}; send stderr: {stderr}")]
    StreamCopy {
        #[source]
        source: std::io::Error,
        stderr: String,
    },
    #[error("wire: {0}")]
    Wire(#[from] ProtocolError),
    #[error("quinn: {0}")]
    Quinn(String),
    #[error("receiver: {message}")]
    Receiver { message: String },
}

/// Build the wire-side SendHeader from a plan + the operator's flag
/// config. Pure helper, easy to test.
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
            // D18: Resume MUST NOT discard the partial — that IS the
            // partial we are continuing.
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

/// Build a `palimpsest::send::SendArgs` for the chosen plan against
/// the sender's dataset path.
pub fn build_send_args(
    plan: &SnapshotPlan,
    sender_dataset: &str,
    flags: &SendFlagsConfig,
) -> Option<SendArgs> {
    // Resume builds differently: the snapshot positional is unused
    // (palimpsest's SendArgs ignores it when from = ResumeToken) and
    // the `-i` form does not apply.
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

/// Open a SEND stream for one plan, spawn `zfs send` via palimpsest,
/// pipe its stdout to QUIC, await the receiver's response. Returns
/// `Ok(())` only if both the send process exits cleanly AND the
/// receiver replies Ok.
pub async fn execute_one_plan(
    runner: &dyn CommandRunner,
    plan: &SnapshotPlan,
    target_dataset: &str,
    sender_dataset: &str,
    flags: &SendFlagsConfig,
    connection: &quinn::Connection,
) -> Result<(), ExecError> {
    let Some(send_header) = build_send_header(plan, flags) else {
        return Err(ExecError::Nothing);
    };
    let Some(args) = build_send_args(plan, sender_dataset, flags) else {
        return Err(ExecError::Nothing);
    };

    let (mut quic_send, mut quic_recv) = connection
        .open_bi()
        .await
        .map_err(|e| ExecError::Quinn(format!("open_bi (send): {e}")))?;
    let header = ReceiveHeader {
        version: arctern_transport::PROTOCOL_VERSION,
        op: Op::Send,
        target_dataset: target_dataset.to_string(),
        prefix_regex: None,
        send: Some(send_header),
    };
    write_header(&mut quic_send, &header).await?;

    let mut child = zfs_send(runner, &args).await.map_err(ExecError::SpawnSend)?;
    let mut child_stdout = child
        .stdout
        .take()
        .ok_or_else(|| ExecError::Quinn("no stdout on send child".into()))?;
    let mut child_stderr = child
        .stderr
        .take()
        .ok_or_else(|| ExecError::Quinn("no stderr on send child".into()))?;
    let stderr_drain = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = child_stderr.read_to_end(&mut buf).await;
        buf
    });
    let copy_res = tokio::io::copy(&mut child_stdout, &mut quic_send).await;
    quic_send
        .finish()
        .map_err(|e| ExecError::Quinn(format!("finish (send): {e}")))?;
    let stderr_bytes = stderr_drain.await.unwrap_or_default();
    let exit = child
        .wait()
        .await
        .map_err(|e| ExecError::Quinn(format!("send wait: {e}")))?;
    if let Err(source) = copy_res {
        return Err(ExecError::StreamCopy {
            source,
            stderr: String::from_utf8_lossy(&stderr_bytes).trim().to_string(),
        });
    }
    if !exit.success() {
        return Err(ExecError::SendFailed {
            exit: exit.code(),
            stderr: String::from_utf8_lossy(&stderr_bytes).trim().to_string(),
        });
    }

    // The receiver finishes its send half after writing the response;
    // read_response consumes until EOF.
    let resp = read_response(&mut quic_recv).await?;
    match resp {
        ReceiveResponse::Ok => Ok(()),
        ReceiveResponse::Error { message } => Err(ExecError::Receiver { message }),
    }
}

pub const KIND: &str = arctern_api::JOB_KIND_PUSH;

pub struct PushJob {
    config: PushJobConfig,
    filter: CompiledFilter,
    status: Mutex<JobStatusInner>,
    wakeup: Arc<tokio::sync::Notify>,
}

impl PushJob {
    pub fn new(config: PushJobConfig) -> Result<Self, regex::Error> {
        let filter = CompiledFilter::from_config(&config.snapshot_filter)?;
        Ok(Self {
            config,
            filter,
            status: Mutex::new(JobStatusInner::default()),
            wakeup: Arc::new(tokio::sync::Notify::new()),
        })
    }

    fn record_cycle(&self, last_error: Option<String>, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.last_run = Some(now);
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.last_error = last_error;
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
                    let outcome = self.run_cycle(&ctx).await;
                    self.record_cycle(outcome.err(), interval);
                }
            }
            .instrument(span),
        )
    }
}

impl PushJob {
    async fn run_cycle(&self, ctx: &JobContext) -> Result<(), String> {
        // Open one QUIC connection per cycle, used across every
        // configured filesystem in declared order. Closing at the end
        // of the cycle keeps connection state from leaking across
        // cycle boundaries (next cycle starts with a fresh handshake).
        let mut endpoint = quinn::Endpoint::client((Ipv4Addr::UNSPECIFIED, 0).into())
            .map_err(|e| format!("client endpoint: {e}"))?;
        let client_cfg = client_config_accept_any()
            .map_err(|e| format!("client TLS config: {e}"))?;
        endpoint.set_default_client_config(client_cfg);
        let connection = endpoint
            .connect(self.config.connect, &self.config.server_name)
            .map_err(|e| format!("connect {} ({}): {e}", self.config.connect, self.config.server_name))?
            .await
            .map_err(|e| format!("handshake {}: {e}", self.config.connect))?;

        let runner = ctx.runner.as_ref();
        let mut errors: Vec<String> = Vec::new();
        let sender_paths = self.expand_filesystems(runner).await.map_err(|e| {
            format!("expand filesystems: {e}")
        })?;
        for sender_path in &sender_paths {
            // D5/FR-005: literal concat — target = root_fs/sender_path.
            let target = format!("{}/{}", self.config.target.root_fs, sender_path);
            tracing::info!(sender = %sender_path, target = %target, "push: planning");
            let plan = match plan_one_filesystem(
                runner,
                sender_path,
                &target,
                &self.filter,
                &connection,
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
            match &plan {
                SnapshotPlan::Nothing => {
                    tracing::info!(sender = %sender_path, "push: nothing to do");
                    continue;
                }
                SnapshotPlan::Full {
                    to,
                    discard_partial_recv,
                } => {
                    if *discard_partial_recv {
                        tracing::info!(
                            sender = %sender_path,
                            to = %to.name,
                            "push: full send (discarding stale partial)"
                        );
                    } else {
                        tracing::info!(sender = %sender_path, to = %to.name, "push: full send");
                    }
                }
                SnapshotPlan::Incremental {
                    from,
                    to,
                    discard_partial_recv,
                } => {
                    if *discard_partial_recv {
                        tracing::info!(
                            sender = %sender_path, from = %from.name, to = %to.name,
                            "push: incremental send (discarding stale partial)"
                        );
                    } else {
                        tracing::info!(
                            sender = %sender_path, from = %from.name, to = %to.name,
                            "push: incremental send"
                        );
                    }
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
            if let Err(e) = execute_one_plan(
                runner,
                &plan,
                &target,
                sender_path,
                &self.config.send,
                &connection,
            )
            .await
            {
                let msg = format!("execute {sender_path}: {e}");
                warn!(error = %msg);
                errors.push(msg);
            }
        }

        connection.close(0u32.into(), b"cycle done");
        endpoint.wait_idle().await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// Resolve the configured `[[jobs.filesystems]]` entries into
    /// concrete sender dataset paths. Reuses the same
    /// recursive+exclude semantics as snap by querying palimpsest's
    /// dataset list per declared filter.
    async fn expand_filesystems(
        &self,
        runner: &dyn CommandRunner,
    ) -> Result<Vec<String>, String> {
        let mut pools: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
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
        assert_eq!(
            pick_plan(&[], &[e("a", 1, 1)]),
            SnapshotPlan::Nothing
        );
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
    fn sender_ahead_by_many_picks_highest_common_guid() {
        // Sender: a, b, c, d, e  GUIDs 1..5
        // Receiver has b + d (out of order), neither is the latest
        let sender = vec![s("a", 1), s("b", 2), s("c", 3), s("d", 4), s("e", 5)];
        let receiver = vec![e("d", 4, 4), e("b", 2, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Incremental {
                from: s("d", 4),
                to: s("e", 5),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn disjoint_guids_means_full_send() {
        // Operator rolled back receiver by hand; new GUIDs everywhere.
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("x", 100, 1), e("y", 200, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Full {
                to: s("b", 2),
                discard_partial_recv: false,
            }
        );
    }

    /// Real ZFS GUIDs from the palimpsest test VM (all > i64::MAX).
    /// The intersection map keys on u64 so these are correct.
    #[test]
    fn intersects_correctly_at_u64_above_i64_max() {
        let sender = vec![
            s("zrepl_001", 11587258101628135412),
            s("zrepl_002", 1711743136468914064),
            s("manual_001", 14719774020884296672),
        ];
        let receiver = vec![e("zrepl_001", 11587258101628135412, 8)];
        // The latest sender snap is manual_001; receiver has only zrepl_001
        // which is the OLDEST sender snap. Incremental from zrepl_001 to
        // manual_001 (the createtxg-ascending sort places manual_001 last
        // because it has the highest createtxg in the test fixture).
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
        // The wire form escapes regex metacharacters in the prefix.
        assert_eq!(f.wire_regex(), Some("^zrepl_"));
    }

    #[test]
    fn build_send_header_full_uses_all_default_flags() {
        let plan = SnapshotPlan::Full {
            to: s("snap1", 42),
            discard_partial_recv: false,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Full);
        assert!(h.from_snap.is_none());
        assert_eq!(h.to_snap.guid, 42);
        assert!(!h.discard_partial_recv);
        assert_eq!(
            h.flags,
            SendFlagsWire {
                raw: true,
                embedded: true,
                compressed: true,
                large_blocks: true
            }
        );
    }

    #[test]
    fn build_send_header_incremental_carries_from_and_to() {
        let plan = SnapshotPlan::Incremental {
            from: s("snap1", 1),
            to: s("snap2", 2),
            discard_partial_recv: false,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Incremental);
        assert_eq!(h.from_snap.unwrap().guid, 1);
        assert_eq!(h.to_snap.guid, 2);
        assert!(!h.discard_partial_recv);
    }

    #[test]
    fn build_send_header_nothing_yields_none() {
        assert!(build_send_header(&SnapshotPlan::Nothing, &SendFlagsConfig::default()).is_none());
    }

    #[test]
    fn build_send_args_full_with_all_flags() {
        let plan = SnapshotPlan::Full {
            to: s("snap1", 1),
            discard_partial_recv: false,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        // Order matches palimpsest::send::SendArgs::build_args.
        assert_eq!(v, vec!["send", "-w", "-c", "-L", "-e", "tank/data@snap1"]);
    }

    #[test]
    fn build_send_args_incremental_uses_dash_i() {
        let plan = SnapshotPlan::Incremental {
            from: s("snap1", 1),
            to: s("snap2", 2),
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
                "tank/data@snap1",
                "tank/data@snap2"
            ]
        );
    }

    #[test]
    fn compiled_filter_regex_passthrough() {
        let cfg = SnapshotFilterConfig {
            prefix: None,
            regex: Some("^auto_[0-9]+$".into()),
        };
        let f = CompiledFilter::from_config(&cfg).unwrap();
        assert!(f.matches("auto_42"));
        assert!(!f.matches("auto_x"));
        assert_eq!(f.wire_regex(), Some("^auto_[0-9]+$"));
    }

    // ─── Slice 006 ─────────────────────────────────────────────────

    fn rt(to_guid: u64, from_guid: Option<u64>) -> palimpsest::resume_token::ResumeToken {
        palimpsest::resume_token::ResumeToken {
            token: "1-deadbeef".into(),
            to_name: "tank/data@snap_x".into(),
            to_guid,
            from_guid,
            bytes_received: 1024,
        }
    }

    #[test]
    fn pick_plan_with_token_none_falls_through_to_full() {
        // Slice 005 behaviour preserved when no token is present.
        let sender = vec![s("a", 1), s("b", 2)];
        let p = pick_plan_with_token(&sender, &[], None, None);
        assert_eq!(
            p,
            SnapshotPlan::Full {
                to: s("b", 2),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn pick_plan_with_token_live_full_emits_resume() {
        // Token's to_guid is on the sender; from_guid is None (full-send
        // resume). Plan = Resume.
        let sender = vec![s("a", 1), s("b", 11587258101628135412)];
        let decoded = rt(11587258101628135412, None);
        let p = pick_plan_with_token(&sender, &[], Some("1-deadbeef"), Some(&decoded));
        match p {
            SnapshotPlan::Resume { token, decoded: d } => {
                assert_eq!(token, "1-deadbeef");
                assert_eq!(d.to_guid, 11587258101628135412);
            }
            other => panic!("expected Resume, got {other:?}"),
        }
    }

    #[test]
    fn pick_plan_with_token_live_incremental_emits_resume() {
        // Both endpoints of the incremental-resume token are on the sender.
        let sender = vec![s("a", 1), s("b", 2)];
        let decoded = rt(2, Some(1));
        let p = pick_plan_with_token(&sender, &[], Some("1-deadbeef"), Some(&decoded));
        assert!(matches!(p, SnapshotPlan::Resume { .. }));
    }

    #[test]
    fn pick_plan_with_token_to_guid_dead_emits_full_with_discard() {
        // Token's to_guid is no longer on the sender. Receiver is empty
        // so the only option is Full + discard.
        let sender = vec![s("a", 1), s("b", 2)];
        let decoded = rt(99999, None);
        let p = pick_plan_with_token(&sender, &[], Some("1-deadbeef"), Some(&decoded));
        assert_eq!(
            p,
            SnapshotPlan::Full {
                to: s("b", 2),
                discard_partial_recv: true,
            }
        );
    }

    #[test]
    fn pick_plan_with_token_from_guid_dead_emits_with_discard() {
        // Incremental token, sender has to_guid but not from_guid.
        let sender = vec![s("a", 1), s("b", 2)];
        let decoded = rt(2, Some(99999));
        let p = pick_plan_with_token(&sender, &[], Some("1-deadbeef"), Some(&decoded));
        assert_eq!(
            p,
            SnapshotPlan::Full {
                to: s("b", 2),
                discard_partial_recv: true,
            }
        );
    }

    #[test]
    fn pick_plan_with_token_dead_but_common_snap_exists_emits_incremental_with_discard() {
        // Sender + receiver share a different GUID; token is stale →
        // Incremental + discard, NOT Full + discard.
        let sender = vec![s("a", 1), s("b", 2), s("c", 3)];
        let receiver = vec![e("b", 2, 5)];
        let decoded = rt(99999, None);
        let p = pick_plan_with_token(&sender, &receiver, Some("1-deadbeef"), Some(&decoded));
        assert_eq!(
            p,
            SnapshotPlan::Incremental {
                from: s("b", 2),
                to: s("c", 3),
                discard_partial_recv: true,
            }
        );
    }

    #[test]
    fn build_send_header_full_with_discard_sets_flag() {
        let plan = SnapshotPlan::Full {
            to: s("snap1", 42),
            discard_partial_recv: true,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert!(h.discard_partial_recv);
        assert_eq!(h.send_kind, SendKind::Full);
    }

    #[test]
    fn build_send_header_resume_uses_decoded_to_snap() {
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
        assert!(h.from_snap.is_none());
        assert_eq!(h.to_snap.name, "tank/data@snap1");
        assert_eq!(h.to_snap.guid, 42);
        // D18 — Resume MUST NOT set discard_partial_recv.
        assert!(!h.discard_partial_recv);
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
        // The flags from SendFlagsConfig::default() apply to the resumed
        // stream just like a fresh send. No snapshot positional and no -i.
        assert_eq!(v, vec!["send", "-w", "-c", "-L", "-e", "-t", "1-abc"]);
    }
}
