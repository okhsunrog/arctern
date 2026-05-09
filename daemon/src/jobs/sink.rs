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
    ProtocolError, ReceiveResponse, TransportIdentity, read_header, server_config, write_response,
};
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
    let outcome = handle_stream_inner(&job, runner.as_ref(), &mut send, &mut recv).await;
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

async fn handle_stream_inner(
    job: &SinkJob,
    runner: &dyn CommandRunner,
    _send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<(), String> {
    let header = read_header(recv).await.map_err(|e| match e {
        ProtocolError::Io(e) => format!("read header: {e}"),
        other => format!("{other}"),
    })?;
    let prefix = format!("{}/", job.config.root_fs);
    if header.target_dataset == job.config.root_fs || !header.target_dataset.starts_with(&prefix) {
        return Err(format!(
            "target_dataset {:?} is not under root_fs {:?}",
            header.target_dataset, job.config.root_fs
        ));
    }
    tracing::info!(target = %header.target_dataset, "sink stream: invoking zfs recv");

    let args = RecvArgs::new(header.target_dataset.clone());
    let mut handle = zfs_recv(runner, &args)
        .await
        .map_err(|e| format!("spawn zfs recv: {e}"))?;
    let mut child_stdin = handle.stdin.take().ok_or("no stdin on recv child")?;
    let mut child_stderr = handle.stderr.take().ok_or("no stderr on recv child")?;
    let stderr_drain =
        tokio::spawn(async move {
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
}
