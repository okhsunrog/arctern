//! Reconnect backoff for `PeerLink`. Exponential 1s, 2s, 4s, ... capped
//! at 60s per ARCHITECTURE.md "UI federation". The background loop
//! `run_for_peer` is spawned once per `[[peers]]` entry at startup; it
//! owns the entry's PeerStatus and bridges PeerLink lifecycle into the
//! shared peers state map.

use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use super::PeerLink;
use super::state::{PeerEntry, PeerStatus, PeersState};

/// Stateless next-delay calculator. `attempt = 0` is the first retry
/// after a fresh disconnect. Caps at `MAX_BACKOFF`.
pub fn next_delay(attempt: u32) -> Duration {
    const MAX: Duration = Duration::from_secs(60);
    let shift = attempt.min(6);
    let secs: u64 = 1u64 << shift;
    Duration::from_secs(secs).min(MAX)
}

/// Reconnect loop for one peer. Runs until `cancel` fires. On
/// successful connect, parks waiting for the link to die (RPC failure)
/// then re-enters the connect cycle from attempt 0. Each transition
/// updates `state` so handlers see the current reachability.
pub async fn run_for_peer(
    state: PeersState,
    peer_name: String,
    ssh_target: String,
    cancel: CancellationToken,
) {
    let mut attempt: u32 = 0;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        // The control channel is per-peer, not per-job, so the `<job>`
        // token is the literal `control`; the dispatcher does not require
        // it to be a configured job (see stdinserver::dispatch::decide).
        match PeerLink::connect(peer_name.clone(), &ssh_target, "control").await {
            Ok(link) => {
                let link = Arc::new(link);
                {
                    let mut g = state.write().await;
                    g.insert(
                        peer_name.clone(),
                        PeerEntry {
                            name: peer_name.clone(),
                            ssh_target: ssh_target.clone(),
                            status: PeerStatus::Connected,
                            link: Some(link.clone()),
                        },
                    );
                }
                attempt = 0;
                // Park until the link breaks. Detection: poll the
                // control client by sending a cheap RPC (ListJobs)
                // periodically; on RpcError::ChannelClosed/Transport
                // the link is dead and we restart the loop. A future
                // refinement could expose the JoinHandle from
                // ControlClient::spawn through PeerLink instead.
                let probe_interval = Duration::from_secs(15);
                // Bound the probe itself: on a half-open connection the RPC
                // would otherwise hang until TCP keepalive (~2h), defeating
                // reconnect entirely. `rpc` also enforces its own ceiling,
                // but a tighter bound here speeds up dead-peer detection.
                let probe_timeout = Duration::from_secs(20);
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(probe_interval) => {
                            let probe = tokio::time::timeout(
                                probe_timeout,
                                link.rpc(arctern_transport::Request::ListJobs),
                            )
                            .await
                            .unwrap_or(Err(super::RpcError::Timeout));
                            if let Err(e) = probe {
                                tracing::warn!(
                                    peer = %peer_name,
                                    error = %e,
                                    "peer link probe failed; reconnecting"
                                );
                                let mut g = state.write().await;
                                g.insert(
                                    peer_name.clone(),
                                    PeerEntry {
                                        name: peer_name.clone(),
                                        ssh_target: ssh_target.clone(),
                                        status: PeerStatus::Reconnecting {
                                            since: OffsetDateTime::now_utc(),
                                        },
                                        link: None,
                                    },
                                );
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let now = OffsetDateTime::now_utc();
                {
                    let mut g = state.write().await;
                    g.insert(
                        peer_name.clone(),
                        PeerEntry {
                            name: peer_name.clone(),
                            ssh_target: ssh_target.clone(),
                            status: PeerStatus::Failed {
                                since: now,
                                last_error: format!("{e}"),
                            },
                            link: None,
                        },
                    );
                }
                let delay = next_delay(attempt);
                tracing::warn!(
                    peer = %peer_name,
                    error = %e,
                    delay_ms = delay.as_millis() as u64,
                    "peer connect failed; retrying"
                );
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_attempt_is_1s() {
        assert_eq!(next_delay(0), Duration::from_secs(1));
    }

    #[test]
    fn doubles_until_cap() {
        assert_eq!(next_delay(1), Duration::from_secs(2));
        assert_eq!(next_delay(2), Duration::from_secs(4));
        assert_eq!(next_delay(3), Duration::from_secs(8));
        assert_eq!(next_delay(4), Duration::from_secs(16));
        assert_eq!(next_delay(5), Duration::from_secs(32));
    }

    #[test]
    fn caps_at_60s() {
        assert_eq!(next_delay(6), Duration::from_secs(60));
        assert_eq!(next_delay(20), Duration::from_secs(60));
        assert_eq!(next_delay(u32::MAX), Duration::from_secs(60));
    }
}
