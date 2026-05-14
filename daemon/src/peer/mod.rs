//! Outbound SSH peer link. The laptop's daemon owns one `PeerLink` per
//! `[[peers]]` entry. Each link multiplexes:
//!
//! - one **control** channel: a long-lived `arctern stdinserver-dispatch
//!   <id>` child carrying length-delimited Request/Response/Event frames.
//!   Owned by `ControlClient`'s background task.
//! - zero or more **recv** channels: short-lived children spawned per
//!   replication step via `PeerLink::open_recv`.
//!
//! A single `openssh::Session` (with ControlMaster) backs both. Recv
//! channels are returned to the caller (the push executor) which owns
//! their lifetime; the control channel runs forever inside the link.
//!
//! Reconnect lives in `reconnect.rs` and runs eagerly in a background
//! task per peer per ARCHITECTURE.md: 1s, 2s, 4s, ... capped at 60s.

pub mod control;
pub mod reconnect;
pub mod state;

use std::sync::Arc;

use arctern_transport::{
    RecvHeader, Request, Response, ResponseFrame, read_response, write_header,
};
use openssh::{KnownHosts, Session, SessionBuilder, Stdio};
use thiserror::Error;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

pub use control::{ControlClient, RpcError};

/// Outbound link to one peer. Cheaply cloneable — handlers and the
/// scheduler share the same Arc.
#[derive(Clone)]
pub struct PeerLink {
    #[allow(dead_code)]
    pub(crate) name: String,
    pub(crate) session: Arc<openssh::Session>,
    pub(crate) control: ControlClient,
}

impl PeerLink {
    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Open a fresh SSH session to `ssh_target`, spawn the remote control
    /// channel, hand its stdio to a `ControlClient`, return the link.
    /// `identity` matches the receiver-side `[[allowed_clients]].identity`
    /// referenced by `command="arctern stdinserver-dispatch <id>"` in
    /// authorized_keys; `job` is the value passed to the receiver as
    /// `<job>` inside `arctern stdinserver <job> <op>`.
    pub async fn connect(name: String, ssh_target: &str, job: &str) -> Result<Self, PeerError> {
        let session = SessionBuilder::default()
            .known_hosts_check(KnownHosts::Strict)
            .connect_mux(ssh_target)
            .await?;
        let session = Arc::new(session);
        let mut cmd = Arc::clone(&session).arc_command("arctern");
        cmd.arg("stdinserver").arg(job).arg("control");
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let mut child = cmd.spawn().await?;
        let stdin = child
            .stdin()
            .take()
            .ok_or_else(|| PeerError::Internal("control child has no stdin".into()))?;
        let stdout = child
            .stdout()
            .take()
            .ok_or_else(|| PeerError::Internal("control child has no stdout".into()))?;
        // Drain stderr to the daemon's tracing layer.
        if let Some(mut stderr) = child.stderr().take() {
            let peer_name = name.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut lines = BufReader::new(&mut stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::warn!(peer = %peer_name, "stdinserver stderr: {line}");
                }
            });
        }
        // The control child's lifetime is tied to the openssh::Session
        // (Arc'd into the link); leak the Child so its drop doesn't
        // prematurely kill the channel — the session shutdown reaps it.
        std::mem::forget(child);
        let (control, _task) = ControlClient::spawn(stdout, stdin);
        Ok(PeerLink {
            name,
            session,
            control,
        })
    }

    /// Send a Request on the control channel and await its matching
    /// Response. Returns immediately with `RpcError::ChannelClosed`
    /// when the background task has died (e.g. session lost; reconnect
    /// is in progress) — handlers should map that to HTTP 503 with
    /// Retry-After per ARCHITECTURE.md "UI federation".
    pub async fn rpc(&self, req: Request) -> Result<Response, RpcError> {
        self.control.send(req).await
    }

    /// Subscribe to server-pushed Event frames on this peer's control
    /// channel. New subscribers see events that arrive after they
    /// subscribe; backlog replay is the SSE bridge's responsibility.
    pub fn subscribe_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<arctern_transport::EventWire> {
        self.control.subscribe_events()
    }

    /// Open a fresh recv channel to the receiver, write the RecvHeader,
    /// and return the channel (caller pipes the `zfs send` byte stream
    /// into `stdin`, then awaits `finish` to get the receiver's final
    /// Response).
    pub async fn open_recv(
        &self,
        job: &str,
        header: &RecvHeader,
    ) -> Result<RecvChannel, PeerError> {
        let mut cmd = Arc::clone(&self.session).arc_command("arctern");
        cmd.arg("stdinserver").arg(job).arg("recv");
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let mut child = cmd.spawn().await?;
        let mut stdin = child
            .stdin()
            .take()
            .ok_or_else(|| PeerError::Internal("recv child has no stdin".into()))?;
        let stdout = child
            .stdout()
            .take()
            .ok_or_else(|| PeerError::Internal("recv child has no stdout".into()))?;
        let stderr = child.stderr().take();
        write_header(&mut stdin, header)
            .await
            .map_err(|e| PeerError::Internal(format!("write RecvHeader: {e}")))?;
        Ok(RecvChannel {
            child,
            stdin: Some(stdin),
            stdout,
            stderr,
        })
    }
}

/// One open recv channel. Caller writes the `zfs send` byte stream into
/// `stdin`, calls `shutdown_stdin()` on completion, then `finish()` to
/// drain stderr + read the final Response.
///
/// Wraps openssh's child + stdio handles. The Arc<Session> in the type
/// parameter binds the channel's lifetime to the underlying SSH session
/// so the Drop order is correct on early termination.
#[allow(dead_code)]
pub struct RecvChannel {
    child: openssh::Child<Arc<Session>>,
    pub stdin: Option<openssh::ChildStdin>,
    pub stdout: openssh::ChildStdout,
    pub stderr: Option<openssh::ChildStderr>,
}

#[allow(dead_code)]
impl RecvChannel {
    /// Close the local stdin half so the remote `zfs recv` sees EOF and
    /// finalises. Idempotent.
    pub async fn shutdown_stdin(&mut self) -> std::io::Result<()> {
        if let Some(mut s) = self.stdin.take() {
            s.shutdown().await
        } else {
            Ok(())
        }
    }

    /// Read the receiver's final Response, drain stderr, wait on the
    /// child. The caller must have already shut down stdin (or be sure
    /// the bulk send is finished) before calling this.
    pub async fn finish(mut self) -> Result<Response, PeerError> {
        let resp_frame: ResponseFrame = read_response(&mut self.stdout)
            .await
            .map_err(|e| PeerError::Internal(format!("read Response: {e}")))?;
        if let Some(mut stderr) = self.stderr.take() {
            use tokio::io::AsyncReadExt;
            let mut buf = Vec::new();
            let _ = stderr.read_to_end(&mut buf).await;
            if !buf.is_empty() {
                tracing::warn!(
                    "recv channel stderr: {}",
                    String::from_utf8_lossy(&buf).trim()
                );
            }
        }
        let _ = self.child.wait().await;
        Ok(resp_frame.body)
    }
}

#[derive(Debug, Error)]
#[allow(dead_code)] // Constructed by handlers/peers (step 10) and push executor (step 9).
pub enum PeerError {
    #[error("ssh session: {0}")]
    Session(#[from] openssh::Error),
    #[error("control channel: {0}")]
    Control(#[from] RpcError),
    #[error("internal: {0}")]
    Internal(String),
}

/// One in-flight RPC. The control task holds the `tx` half until the
/// matching ResponseFrame arrives; the caller awaits `rx`.
pub(crate) type Pending = oneshot::Sender<Result<Response, RpcError>>;
