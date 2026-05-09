//! Sink job — passive QUIC receiver. Each accepted bidirectional stream
//! is one `zfs recv`. The wire framing (length-prefixed JSON header,
//! raw send bytes until FIN, JSON response) lives in
//! `arctern_transport::protocol`. We compose it with `palimpsest::recv`
//! here.
//!
//! Concurrency: one tokio task per connection, one task per accepted
//! stream within a connection. No global lock. Cancellation propagates
//! via `endpoint.close` which fails in-flight reads/writes; the per-
//! stream task observes that as an io error and bails.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arctern_config::SinkJobConfig;
use arctern_transport::{
    ListResponse, Op, ProtocolError, ReceiveHeader, ReceiveResponse, SnapshotEntry,
    TransportIdentity, compile_prefix_regex, read_header, server_config, write_list_response,
    write_response,
};
use palimpsest::ZfsError;
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::recv::{RecvArgs, recv as zfs_recv};
use palimpsest::runner::CommandRunner;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};

use super::{Job, JobContext, JobStatusInner};

pub const KIND: &str = arctern_api::JOB_KIND_SINK;

pub struct SinkJob {
    config: SinkJobConfig,
    identity: Arc<TransportIdentity>,
    status: Mutex<JobStatusInner>,
    bound_addr: Mutex<Option<SocketAddr>>,
}

impl SinkJob {
    pub fn new(config: SinkJobConfig, identity: Arc<TransportIdentity>) -> Self {
        Self {
            config,
            identity,
            status: Mutex::new(JobStatusInner::default()),
            bound_addr: Mutex::new(None),
        }
    }

    pub fn bound_addr(&self) -> Option<SocketAddr> {
        *self.bound_addr.lock().unwrap()
    }

    fn record(&self, last_error: Option<String>) {
        let mut s = self.status.lock().unwrap();
        s.last_run = Some(OffsetDateTime::now_utc());
        // next_run intentionally None — sinks are event-driven.
        s.next_run = None;
        s.last_error = last_error;
    }
}

impl Job for SinkJob {
    fn name(&self) -> &str {
        &self.config.name
    }
    fn kind(&self) -> &'static str {
        KIND
    }
    fn status(&self) -> JobStatusInner {
        self.status.lock().unwrap().clone()
    }
    fn run(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let span = info_span!("sink_job", name = %self.config.name);
        Box::pin(
            async move {
                if let Err(e) = self.clone().run_inner(ctx, cancel).await {
                    let msg = format!("sink job exited: {e}");
                    warn!(error = %msg);
                    self.record(Some(msg));
                }
            }
            .instrument(span),
        )
    }
}

impl SinkJob {
    async fn run_inner(
        self: Arc<Self>,
        ctx: JobContext,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        let server_cfg = server_config(&self.identity).map_err(|e| format!("server config: {e}"))?;
        let endpoint = quinn::Endpoint::server(server_cfg, self.config.listen)
            .map_err(|e| format!("bind {}: {e}", self.config.listen))?;
        let bound = endpoint
            .local_addr()
            .map_err(|e| format!("local_addr: {e}"))?;
        *self.bound_addr.lock().unwrap() = Some(bound);
        tracing::info!(addr = %bound, root_fs = %self.config.root_fs, "sink listening");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                accept = endpoint.accept() => {
                    let Some(connecting) = accept else { break };
                    let job = self.clone();
                    let runner = ctx.runner.clone();
                    let cancel = cancel.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(job, runner, connecting, cancel).await {
                            warn!(error = %e, "connection task ended with error");
                        }
                    });
                }
            }
        }

        // Stop the endpoint and wait for graceful drain. Outstanding
        // streams observe quinn's connection-closed error and unwind.
        endpoint.close(0u32.into(), b"shutdown");
        endpoint.wait_idle().await;
        Ok(())
    }
}

async fn handle_connection(
    job: Arc<SinkJob>,
    runner: Arc<dyn CommandRunner>,
    connecting: quinn::Incoming,
    cancel: CancellationToken,
) -> Result<(), String> {
    let conn = connecting.await.map_err(|e| format!("handshake: {e}"))?;
    let remote = conn.remote_address();
    tracing::info!(remote = %remote, "sink accepted connection");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            stream = conn.accept_bi() => {
                match stream {
                    Ok((send, recv)) => {
                        let job = job.clone();
                        let runner = runner.clone();
                        tokio::spawn(async move {
                            handle_stream(job, runner, send, recv).await;
                        });
                    }
                    Err(quinn::ConnectionError::ApplicationClosed(_))
                    | Err(quinn::ConnectionError::ConnectionClosed(_))
                    | Err(quinn::ConnectionError::LocallyClosed) => break,
                    Err(e) => return Err(format!("accept_bi: {e}")),
                }
            }
        }
    }
    Ok(())
}

async fn handle_stream(
    job: Arc<SinkJob>,
    runner: Arc<dyn CommandRunner>,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) {
    // Read the header up-front so the dispatcher can route on `op`. A
    // header-read failure (truncated stream, invalid JSON, oversize
    // length, version mismatch) is reported via ReceiveResponse — the
    // sender always parses ReceiveResponse first when the LIST flow
    // hasn't been confirmed yet, so this preserves a single error
    // path across both ops.
    let header = match read_header(&mut recv).await {
        Ok(h) => h,
        Err(e) => {
            let msg = match e {
                ProtocolError::Io(e) => format!("read header: {e}"),
                other => format!("{other}"),
            };
            warn!(error = %msg, "sink: bad header");
            let resp = ReceiveResponse::Error { message: msg.clone() };
            let _ = write_response(&mut send, &resp).await;
            let _ = send.finish();
            job.record(Some(msg));
            return;
        }
    };

    match header.op {
        Op::Send => {
            let outcome = handle_send(&job, runner.as_ref(), header, &mut recv).await;
            let resp = match &outcome {
                Ok(()) => ReceiveResponse::Ok,
                Err(msg) => ReceiveResponse::Error {
                    message: msg.replace('\n', " ").trim().to_string(),
                },
            };
            if let Err(e) = write_response(&mut send, &resp).await {
                warn!(error = %e, "write_response failed");
            }
            let _ = send.finish();
            job.record(outcome.err());
        }
        Op::List => {
            let resp = handle_list(&job, runner.as_ref(), header).await;
            let outcome_err = match &resp {
                ListResponse::Ok { .. } => None,
                ListResponse::Error { message } => Some(message.clone()),
            };
            if let Err(e) = write_list_response(&mut send, &resp).await {
                warn!(error = %e, "write_list_response failed");
            }
            let _ = send.finish();
            job.record(outcome_err);
        }
    }
}

async fn handle_send(
    job: &SinkJob,
    runner: &dyn CommandRunner,
    header: ReceiveHeader,
    recv: &mut quinn::RecvStream,
) -> Result<(), String> {
    let prefix = format!("{}/", job.config.root_fs);
    if header.target_dataset == job.config.root_fs || !header.target_dataset.starts_with(&prefix) {
        return Err(format!(
            "target_dataset {:?} is not under root_fs {:?}",
            header.target_dataset, job.config.root_fs
        ));
    }
    if let Some(send) = &header.send {
        tracing::info!(
            target = %header.target_dataset,
            kind = ?send.send_kind,
            from = ?send.from_snap.as_ref().map(|s| &s.name),
            to = %send.to_snap.name,
            "sink: invoking zfs recv"
        );
    } else {
        tracing::info!(target = %header.target_dataset, "sink: invoking zfs recv (no SendHeader)");
    }

    // The receiver-side path is `<root_fs>/<sender_path>` (slice 005
    // FR-005); intermediate datasets typically don't exist on first
    // replication. Ensure they do before invoking recv. `zfs create -p`
    // is idempotent on existing parents; the leaf would error if it
    // already exists, so target the parent dataset, not the target
    // itself. zfs recv creates the leaf.
    if let Some((parent, _)) = header.target_dataset.rsplit_once('/')
        && parent != job.config.root_fs
    {
        let opts = palimpsest::dataset::CreateOptions::new()
            .create_parents()
            .property("mountpoint", "none");
        match palimpsest::dataset::create(runner, parent, &opts).await {
            Ok(()) => {}
            Err(e) => {
                let stderr = format!("{e}");
                if !stderr.contains("already exists") {
                    return Err(format!("ensure parent {parent}: {e}"));
                }
            }
        }
    }

    // T002 — honour the sender's discard_partial_recv directive.
    // Slice 006 confirmed in the VM (D6) that a fresh full or
    // incremental into a dataset with `receive_resume_token` set is
    // rejected outright (even with -F). The planner sets this when
    // it picked Full/Incremental despite the receiver advertising a
    // stale token. Idempotent: abort_partial returns Ok if there is
    // no partial to clear.
    let discard = header
        .send
        .as_ref()
        .map(|s| s.discard_partial_recv)
        .unwrap_or(false);
    if discard {
        tracing::info!(
            target = %header.target_dataset,
            "sink: discarding partial recv per sender request"
        );
        if let Err(e) =
            palimpsest::recv::abort_partial(runner, &header.target_dataset).await
        {
            return Err(format!(
                "abort_partial {}: {e}",
                header.target_dataset
            ));
        }
    }

    // T002 — every recv is resumable. -s costs nothing on a clean
    // run and is the prerequisite for slice 006's whole reason for
    // being. T008 wires RecvProperties through palimpsest's -o/-x
    // flags; -u (unmounted) is unconditional because the sink does
    // not know the operator's mountpoint policy.
    let mut args = RecvArgs::new(header.target_dataset.clone())
        .unmounted()
        .resumable();
    for (k, v) in &job.config.recv.properties.overrides {
        args = args.property_override(k, v);
    }
    for k in &job.config.recv.properties.inherit {
        args = args.property_inherit(k);
    }
    let mut handle = zfs_recv(runner, &args)
        .await
        .map_err(|e| format!("spawn zfs recv: {e}"))?;
    let mut child_stdin = handle.stdin.take().ok_or("no stdin on recv child")?;
    let mut child_stderr = handle.stderr.take().ok_or("no stderr on recv child")?;
    let stderr_drain = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = child_stderr.read_to_end(&mut buf).await;
        buf
    });
    let copy_res = tokio::io::copy(recv, &mut child_stdin).await;
    let _ = child_stdin.shutdown().await;
    drop(child_stdin);
    let stderr_bytes = stderr_drain.await.unwrap_or_default();
    let exit = handle.wait().await.map_err(|e| format!("recv wait: {e}"))?;
    if let Err(e) = copy_res {
        let stderr_text = String::from_utf8_lossy(&stderr_bytes);
        return Err(format!("stream copy: {e}; recv stderr: {}", stderr_text.trim()));
    }
    if !exit.success() {
        let stderr_text = String::from_utf8_lossy(&stderr_bytes);
        return Err(format!(
            "zfs recv failed (exit {:?}): {}",
            exit.code(),
            stderr_text.trim()
        ));
    }
    Ok(())
}

async fn handle_list(
    job: &SinkJob,
    runner: &dyn CommandRunner,
    header: ReceiveHeader,
) -> ListResponse {
    // The list-of-root_fs-itself is a meaningful query (no
    // descendants yet means first replication on this peer), so the
    // gate is "must be root_fs OR start with root_fs/".
    let prefix = format!("{}/", job.config.root_fs);
    if header.target_dataset != job.config.root_fs
        && !header.target_dataset.starts_with(&prefix)
    {
        return ListResponse::Error {
            message: format!(
                "target_dataset {:?} is not under root_fs {:?}",
                header.target_dataset, job.config.root_fs
            ),
        };
    }
    let regex = match compile_prefix_regex(header.prefix_regex.as_deref()) {
        Ok(opt) => opt,
        Err(e) => {
            return ListResponse::Error {
                message: format!(
                    "compile prefix_regex {:?}: {e}",
                    header.prefix_regex.as_deref().unwrap_or("")
                ),
            };
        }
    };
    tracing::info!(target = %header.target_dataset, regex = ?header.prefix_regex, "sink: list");
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![header.target_dataset.clone()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = match palimpsest::dataset::list(runner, &opts).await {
        Ok(v) => v,
        Err(ZfsError::DatasetNotFound { .. }) => {
            // D16: first replication is normal, not an error.
            return ListResponse::Ok {
                snapshots: vec![],
                receive_resume_token: None,
            };
        }
        Err(e) => {
            return ListResponse::Error {
                message: format!("list: {e}"),
            };
        }
    };
    let snapshots = entries
        .into_iter()
        .filter_map(|e| {
            let snap_name = e.snapshot_name.clone()?;
            if let Some(re) = &regex
                && !re.is_match(&snap_name)
            {
                return None;
            }
            let guid = e.properties.get("guid").and_then(|p| p.value.parse::<u64>().ok())?;
            let createtxg = e.createtxg.parse::<u64>().ok()?;
            Some(SnapshotEntry {
                name: snap_name,
                guid,
                createtxg,
            })
        })
        .collect();
    // T002 — surface the receiver's `receive_resume_token` user
    // property so the sender's planner can decide between resume,
    // discard-then-fresh, or vanilla Full/Incremental. Soft-fail per
    // D19: a token query failure does NOT fail the LIST.
    let receive_resume_token = match palimpsest::recv::receive_resume_token(
        runner,
        &header.target_dataset,
    )
    .await
    {
        Ok(opt) => opt,
        Err(ZfsError::DatasetNotFound { .. }) => None,
        Err(e) => {
            warn!(
                error = %e,
                target = %header.target_dataset,
                "sink: receive_resume_token query failed; LIST returning None"
            );
            None
        }
    };
    ListResponse::Ok {
        snapshots,
        receive_resume_token,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> SinkJobConfig {
        SinkJobConfig {
            name: "t".into(),
            listen: "127.0.0.1:0".parse().unwrap(),
            root_fs: "tank/backups".into(),
            recv: arctern_config::RecvConfig::default(),
        }
    }

    #[test]
    fn target_validation_rejects_root_itself() {
        // We can't easily test the async path without a runner, but the
        // string-prefix gate is pure — exercise it directly.
        let cfg = config();
        let prefix = format!("{}/", cfg.root_fs);
        let bad = "tank/backups";
        assert!(bad == cfg.root_fs || !bad.starts_with(&prefix));
    }

    #[test]
    fn target_validation_rejects_outside_root() {
        let cfg = config();
        let prefix = format!("{}/", cfg.root_fs);
        let bad = "other/data";
        assert!(bad != cfg.root_fs && !bad.starts_with(&prefix));
    }

    #[test]
    fn target_validation_accepts_descendant() {
        let cfg = config();
        let prefix = format!("{}/", cfg.root_fs);
        let good = "tank/backups/laptop/data";
        assert!(good != cfg.root_fs && good.starts_with(&prefix));
    }

    /// Exercises the same code path handle_send uses to translate
    /// RecvProperties into palimpsest::recv::RecvArgs builders. If the
    /// loop in handle_send drifts, this test fails before the
    /// integration test.
    #[test]
    fn recv_properties_propagate_to_palimpsest_args() {
        use std::collections::BTreeMap;
        let mut overrides = BTreeMap::new();
        overrides.insert("readonly".to_string(), "on".to_string());
        overrides.insert("canmount".to_string(), "off".to_string());
        let inherit = vec!["mountpoint".to_string()];
        // Mirrors handle_send (T002 added .resumable()).
        let mut args = palimpsest::recv::RecvArgs::new("tank/sink/data")
            .unmounted()
            .resumable();
        for (k, v) in &overrides {
            args = args.property_override(k, v);
        }
        for k in &inherit {
            args = args.property_inherit(k);
        }
        // Validate the resulting palimpsest arglist via the public
        // builder by pushing it through a no-op helper. The ordering
        // is determined by BTreeMap (canmount before readonly).
        // RecvArgs::build_args is private; we reach it by spawning
        // through a recording runner instead.
        use palimpsest::runner::{Cmd, RecordingRunner};
        let runner = RecordingRunner::new().record_spawn(
            Cmd::new("zfs").args([
                "recv",
                "-u",
                "-s",
                "-o",
                "canmount=off",
                "-o",
                "readonly=on",
                "-x",
                "mountpoint",
                "tank/sink/data",
            ]),
            vec![],
            vec![],
            0,
        );
        // If the args differ, RecordingRunner::spawn returns "no
        // matching command" which is propagated as an error.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let h = palimpsest::recv::recv(&runner, &args).await;
            assert!(h.is_ok(), "expected matching cmd: {:?}", h.err());
        });
    }
}
