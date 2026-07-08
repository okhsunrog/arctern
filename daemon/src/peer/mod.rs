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
use tokio::io::BufReader;
use tokio::sync::oneshot;

pub use control::{ControlClient, RpcError};

/// Strip ANSI escape sequences (CSI colour codes and friends) from a
/// remote process's stderr line. The dispatcher disables colours when
/// stderr is a pipe, but a foreign or older binary may not — and raw
/// escapes would otherwise land verbatim in the event log and the UI.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'[') {
            chars.next();
            // CSI: parameter/intermediate bytes end at a final byte in
            // the 0x40..=0x7e range.
            for c2 in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&c2) {
                    break;
                }
            }
        }
        // Bare ESC (or other escape kinds): drop the ESC itself.
    }
    out
}

/// Outbound link to one peer. Cheaply cloneable — handlers and the
/// scheduler share the same Arc.
#[derive(Clone)]
pub struct PeerLink {
    pub(crate) name: String,
    pub(crate) session: Arc<openssh::Session>,
    pub(crate) control: ControlClient,
    /// Keeps the remote control process alive for the lifetime of the
    /// link: openssh's `Child` tears down the mux channel on drop (and
    /// `disconnect()` does so immediately — the remote dispatcher sees
    /// stdin EOF and exits). Holding it here means the channel dies
    /// exactly when the last clone of this link is dropped, i.e. on
    /// reconnect — no leak, no premature teardown.
    _control_child: Arc<openssh::Child<Arc<Session>>>,
    /// Guards the one-time `SubscribeEvents` RPC. The receiver spawns a
    /// pusher task per SubscribeEvents it accepts; sending it once per
    /// link keeps that at one pusher per control channel no matter how
    /// many local SSE clients subscribe to the broadcast.
    events_subscribed: Arc<tokio::sync::OnceCell<()>>,
}

impl PeerLink {
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
                    let line = strip_ansi(&line);
                    tracing::warn!(peer = %peer_name, "stdinserver stderr: {line}");
                }
            });
        }
        let (control, _task) = ControlClient::spawn(stdout, stdin);
        Ok(PeerLink {
            name,
            session,
            control,
            _control_child: Arc::new(child),
            events_subscribed: Arc::new(tokio::sync::OnceCell::new()),
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
    /// channel. The first subscriber triggers the one-time
    /// `SubscribeEvents` RPC that makes the receiver start pushing;
    /// later subscribers reuse the same server-side pusher via the
    /// local broadcast. New subscribers see events that arrive after
    /// they subscribe; backlog replay is the SSE bridge's
    /// responsibility.
    pub async fn subscribe_events(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<arctern_transport::EventWire>, RpcError> {
        // Grab the receiver before the RPC so frames pushed between the
        // request landing and our return are not missed.
        let rx = self.control.subscribe_events();
        self.events_subscribed
            .get_or_try_init(|| async {
                match self
                    .control
                    .send(Request::SubscribeEvents { since: None })
                    .await?
                {
                    Response::Ok => Ok(()),
                    Response::Error { message, .. } => Err(RpcError::Server(message)),
                    other => Err(RpcError::Server(format!(
                        "unexpected SubscribeEvents response: {other:?}"
                    ))),
                }
            })
            .await?;
        Ok(rx)
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
        tracing::debug!(peer = %self.name, job, "opening recv channel");
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

/// One open recv channel. Caller takes `stdin`, writes the `zfs send`
/// byte stream into it, shuts it down, then calls `finish()` to drain
/// stderr + read the final Response.
///
/// Wraps openssh's child + stdio handles. The Arc<Session> in the type
/// parameter binds the channel's lifetime to the underlying SSH session
/// so the Drop order is correct on early termination.
pub struct RecvChannel {
    child: openssh::Child<Arc<Session>>,
    pub stdin: Option<openssh::ChildStdin>,
    pub stdout: openssh::ChildStdout,
    pub stderr: Option<openssh::ChildStderr>,
}

impl RecvChannel {
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
                    strip_ansi(String::from_utf8_lossy(&buf).trim())
                );
            }
        }
        let _ = self.child.wait().await;
        Ok(resp_frame.body)
    }
}

#[derive(Debug, Error)]
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

#[cfg(test)]
mod tests {
    use super::strip_ansi;

    #[test]
    fn strip_ansi_removes_colour_codes() {
        let colored = "\u{1b}[2m2026-07-07T20:38:17Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m opening channel \u{1b}[3midentity\u{1b}[0m\u{1b}[2m=\u{1b}[0m\"laptop_nova\"";
        assert_eq!(
            strip_ansi(colored),
            "2026-07-07T20:38:17Z  INFO opening channel identity=\"laptop_nova\""
        );
    }

    #[test]
    fn strip_ansi_passes_plain_text_through() {
        let plain = "zsh:1: permission denied: /usr/local/bin/arctern";
        assert_eq!(strip_ansi(plain), plain);
    }
}
