//! Outbound SSH peer link. The laptop's daemon owns one `PeerLink` per
//! `[[peers]]` entry. Each link multiplexes:
//!
//! - one **control** channel: a long-lived `arctern stdinserver-dispatch
//!   <id>` child carrying tarpc `ArcternControl` RPC over
//!   length-delimited JSON frames. tarpc's dispatch task owns the demux.
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
    ArcternControlClient, GuidsReply, ProxyReply, RecvHeader, Response, ResponseFrame,
    read_response, write_header,
};
use openssh::{KnownHosts, Session, SessionBuilder, Stdio};
use thiserror::Error;
use tokio::io::BufReader;

pub use control::RpcError;

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

/// How long a single `ssh` connect attempt may take before the route
/// is declared unreachable. Route probing runs while ranking — an
/// unreachable LAN address away from home must fail in seconds, not
/// hang until the kernel's SYN timeout (~2 min).
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Re-emit one line of a remote stdinserver's stderr at the severity
/// the remote's own tracing-fmt layer assigned it (`<RFC3339>  LEVEL
/// target: message`). Unparseable lines stay WARN — unexpected output
/// (shell errors, panics) deserves attention.
fn emit_remote_stderr(peer: &str, channel: &'static str, raw: &str) {
    let line = strip_ansi(raw);
    let mut parts = line.splitn(3, char::is_whitespace);
    let _ts = parts.next().unwrap_or("");
    let level = parts.next().unwrap_or("");
    // Drop the remote fmt layer's own `timestamp LEVEL target:` prefix —
    // re-logging it verbatim reads as line noise in the event feed. The
    // remainder after "target: " is the actual message.
    let rest = parts.next().unwrap_or("").trim_start();
    let msg = rest.split_once(": ").map(|(_, m)| m).unwrap_or(rest);
    match level {
        "TRACE" => tracing::trace!(peer = %peer, "{peer} {channel}: {msg}"),
        "DEBUG" => tracing::debug!(peer = %peer, "{peer} {channel}: {msg}"),
        "INFO" => tracing::info!(peer = %peer, "{peer} {channel}: {msg}"),
        "WARN" => tracing::warn!(peer = %peer, "{peer} {channel}: {msg}"),
        "ERROR" => tracing::error!(peer = %peer, "{peer} {channel}: {msg}"),
        // Not a tracing-formatted line (shell error, panic) — keep it
        // whole and loud.
        _ => tracing::warn!(peer = %peer, "{peer} {channel}: {line}"),
    }
}

/// Outbound link to one peer. Cheaply cloneable — handlers and the
/// scheduler share the same Arc.
#[derive(Clone)]
pub struct PeerLink {
    pub(crate) name: String,
    pub(crate) session: Arc<openssh::Session>,
    pub(crate) control: ArcternControlClient,
    /// Number of recv channels currently streaming over this link.
    /// The reconnect loop skips its liveness probe and route re-rank
    /// while this is non-zero: a bulk send legitimately starves the
    /// control channel, and swapping routes mid-transfer is pointless.
    active_recvs: Arc<std::sync::atomic::AtomicUsize>,
    /// Keeps the remote control process alive for the lifetime of the
    /// link: openssh's `Child` tears down the mux channel on drop (and
    /// `disconnect()` does so immediately — the remote dispatcher sees
    /// stdin EOF and exits). Holding it here means the channel dies
    /// exactly when the last clone of this link is dropped, i.e. on
    /// reconnect — no leak, no premature teardown.
    _control_child: Arc<openssh::Child<Arc<Session>>>,
    /// Fan-out of the peer's event stream. Filled by a reader task
    /// over the dedicated `events` channel, spawned once per link on
    /// first subscription; every SSE client shares it.
    events: tokio::sync::broadcast::Sender<arctern_transport::EventWire>,
    events_started: Arc<tokio::sync::OnceCell<()>>,
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
            .connect_timeout(CONNECT_TIMEOUT)
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
        // Drain stderr to the daemon's tracing layer, preserving the
        // remote event's own severity.
        if let Some(mut stderr) = child.stderr().take() {
            let peer_name = name.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut lines = BufReader::new(&mut stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    emit_remote_stderr(&peer_name, "stdinserver", &line);
                }
            });
        }
        let control = control::spawn(stdout, stdin);
        Ok(PeerLink {
            name,
            session,
            control,
            active_recvs: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            _control_child: Arc::new(child),
            events: tokio::sync::broadcast::channel(256).0,
            events_started: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    /// Recv channels currently streaming over this link.
    pub fn active_recvs(&self) -> usize {
        self.active_recvs.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Receiver snapshot GUIDs + resume token for the planner. Fails
    /// fast with `RpcError::ChannelClosed` when tarpc's dispatch task
    /// has died (session lost; reconnect in progress) — handlers map
    /// that to HTTP 503 per ARCHITECTURE.md "UI federation".
    pub async fn list_receiver_guids(
        &self,
        dataset: String,
        prefix_regex: Option<String>,
    ) -> Result<GuidsReply, RpcError> {
        Ok(self
            .control
            .list_receiver_guids(control::ctx(), dataset, prefix_regex)
            .await??)
    }

    /// `zfs recv -A` on the receiver: drop stale partial-receive state.
    pub async fn discard_partial_recv(&self, dataset: String) -> Result<(), RpcError> {
        Ok(self
            .control
            .discard_partial_recv(control::ctx(), dataset)
            .await??)
    }

    /// Cheap liveness probe; also the receiver's event-log cursor.
    pub async fn log_cursor(&self) -> Result<u64, RpcError> {
        Ok(self.control.log_cursor(control::ctx()).await?)
    }

    /// Generic passthrough into the receiver's local daemon HTTP API.
    pub async fn proxy(
        &self,
        method: String,
        path: String,
        body: Option<String>,
    ) -> Result<ProxyReply, RpcError> {
        Ok(self
            .control
            .proxy(control::ctx(), method, path, body)
            .await??)
    }

    /// Subscribe to the peer's event stream. Events ride a dedicated
    /// one-way channel (`stdinserver <id> events`, NDJSON lines) — not
    /// the control channel — opened once per link by the first
    /// subscriber; every SSE client shares the broadcast. Backlog
    /// replay is the HTTP bridge's job (proxy `/events/recent`).
    pub async fn subscribe_events(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<arctern_transport::EventWire>, RpcError> {
        let rx = self.events.subscribe();
        self.events_started
            .get_or_try_init(|| async {
                let mut cmd = Arc::clone(&self.session).arc_command("arctern");
                cmd.arg("stdinserver").arg("control").arg("events");
                cmd.stdin(Stdio::null());
                cmd.stdout(Stdio::piped());
                cmd.stderr(Stdio::piped());
                let mut child = cmd
                    .spawn()
                    .await
                    .map_err(|e| RpcError::Transport(format!("spawn events channel: {e}")))?;
                let stdout = child
                    .stdout()
                    .take()
                    .ok_or_else(|| RpcError::Transport("events child has no stdout".into()))?;
                if let Some(mut stderr) = child.stderr().take() {
                    let peer_name = self.name.clone();
                    tokio::spawn(async move {
                        use tokio::io::AsyncBufReadExt;
                        let mut lines = BufReader::new(&mut stderr).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            emit_remote_stderr(&peer_name, "events channel", &line);
                        }
                    });
                }
                let events = self.events.clone();
                let peer_name = self.name.clone();
                tokio::spawn(async move {
                    use tokio::io::AsyncBufReadExt;
                    // The child rides along so the remote process lives
                    // exactly as long as this reader.
                    let child = child;
                    let mut lines = BufReader::new(stdout).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        match serde_json::from_str::<arctern_transport::EventWire>(&line) {
                            Ok(ev) => {
                                let _ = events.send(ev);
                            }
                            Err(e) => {
                                tracing::debug!(peer = %peer_name, error = %e, "events channel: bad line");
                            }
                        }
                    }
                    let _ = child.wait().await;
                });
                Ok::<(), RpcError>(())
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
        self.active_recvs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(RecvChannel {
            peer: self.name.clone(),
            child,
            stdin: Some(stdin),
            stdout,
            stderr,
            _guard: RecvGuard(self.active_recvs.clone()),
        })
    }
}

/// Decrements the owning link's recv counter when the channel is
/// dropped — whether via `finish()` or an early error/cancel drop.
struct RecvGuard(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for RecvGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
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
    peer: String,
    child: openssh::Child<Arc<Session>>,
    pub stdin: Option<openssh::ChildStdin>,
    pub stdout: openssh::ChildStdout,
    pub stderr: Option<openssh::ChildStderr>,
    _guard: RecvGuard,
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
            let text = String::from_utf8_lossy(&buf);
            for line in text.lines().filter(|l| !l.trim().is_empty()) {
                emit_remote_stderr(&self.peer, "recv channel", line);
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
