//! Reconnect + route ranking for `PeerLink`. One background task per
//! `[[peers]]` entry owns that peer's lifecycle:
//!
//! - **Connect**: try routes in config (priority) order, first success
//!   wins and becomes the active route. Every attempt is bounded by
//!   `peer::CONNECT_TIMEOUT` so an unreachable LAN address away from
//!   home fails in seconds.
//! - **Park**: probe the live link every 15s with a cheap RPC; a probe
//!   failure tears the entry down and re-enters connect with
//!   exponential backoff (1s, 2s, 4s, ... capped at 60s). The probe is
//!   skipped while recv channels are streaming — a bulk send
//!   legitimately starves the control channel and must not be
//!   mistaken for a dead link.
//! - **Re-rank**: while connected over a non-top route, periodically
//!   try the higher-priority routes; on success the link swaps over
//!   (the old session stays alive for any in-flight work via its Arc).
//!   Skipped while recvs are active — never preempt a running send.

use std::sync::Arc;
use std::time::Duration;

use arctern_config::RouteConfig;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use super::PeerLink;
use super::state::{PeerEntry, PeerStatus, PeersState, RouteHealth, RouteState};

/// Stateless next-delay calculator. `attempt = 0` is the first retry
/// after a fresh disconnect. Caps at 60s.
pub fn next_delay(attempt: u32) -> Duration {
    const MAX: Duration = Duration::from_secs(60);
    let shift = attempt.min(6);
    let secs: u64 = 1u64 << shift;
    Duration::from_secs(secs).min(MAX)
}

fn route_states(routes: &[RouteConfig]) -> Vec<RouteState> {
    routes
        .iter()
        .map(|r| RouteState {
            name: r.name.clone(),
            ssh_target: r.ssh_target.clone(),
            auto: r.auto,
            health: RouteHealth::Unknown,
            last_checked: None,
        })
        .collect()
}

/// Try routes in priority order; return the first that connects,
/// updating `states` health/last_checked along the way.
async fn connect_ranked(
    peer_name: &str,
    routes: &[RouteConfig],
    states: &mut [RouteState],
    cancel: &CancellationToken,
) -> Option<(usize, PeerLink)> {
    for (idx, route) in routes.iter().enumerate() {
        if cancel.is_cancelled() {
            return None;
        }
        let now = OffsetDateTime::now_utc();
        match PeerLink::connect(peer_name.to_string(), &route.ssh_target, "control").await {
            Ok(link) => {
                states[idx].health = RouteHealth::Connected;
                states[idx].last_checked = Some(now);
                return Some((idx, link));
            }
            Err(e) => {
                tracing::debug!(
                    peer = %peer_name,
                    route = %route.name,
                    error = %e,
                    "route connect failed"
                );
                states[idx].health = RouteHealth::Failed {
                    last_error: format!("{e}"),
                };
                states[idx].last_checked = Some(now);
            }
        }
    }
    None
}

async fn publish(state: &PeersState, entry: PeerEntry) {
    let mut g = state.write().await;
    g.insert(entry.name.clone(), entry);
}

/// Reconnect loop for one peer. Runs until `cancel` fires.
pub async fn run_for_peer(
    state: PeersState,
    peer_name: String,
    routes: Vec<RouteConfig>,
    cancel: CancellationToken,
) {
    let mut attempt: u32 = 0;
    let mut states = route_states(&routes);
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match connect_ranked(&peer_name, &routes, &mut states, &cancel).await {
            Some((active_idx, link)) => {
                let mut link = Arc::new(link);
                let mut active_idx = active_idx;
                tracing::info!(
                    peer = %peer_name,
                    route = %routes[active_idx].name,
                    "peer connected"
                );
                publish(
                    &state,
                    PeerEntry {
                        name: peer_name.clone(),
                        status: PeerStatus::Connected,
                        active_route: Some(routes[active_idx].name.clone()),
                        routes: states.clone(),
                        link: Some(link.clone()),
                    },
                )
                .await;
                attempt = 0;
                let probe_interval = Duration::from_secs(15);
                // Bound the probe itself: on a half-open connection the RPC
                // would otherwise hang until TCP keepalive (~2h), defeating
                // reconnect entirely.
                let probe_timeout = Duration::from_secs(20);
                // Re-rank cadence while parked on a lower-priority route.
                const RERANK_EVERY_TICKS: u32 = 4; // 4 × 15s = 1 min
                let mut ticks: u32 = 0;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(probe_interval) => {}
                    }
                    ticks = ticks.wrapping_add(1);
                    // A streaming send starves the control channel; a
                    // timed-out probe here would be a false positive and
                    // tearing the entry down would hide the peer from
                    // select_targets mid-transfer.
                    if link.active_recvs() > 0 {
                        continue;
                    }
                    // GetLogCursor, not ListJobs: the probe checks the
                    // CHANNEL, and ListJobs now proxies into the
                    // receiver's local daemon — a stopped daemon there
                    // must not read as a dead link.
                    let probe = tokio::time::timeout(
                        probe_timeout,
                        link.rpc(arctern_transport::Request::GetLogCursor),
                    )
                    .await
                    .unwrap_or(Err(super::RpcError::Timeout));
                    if let Err(e) = probe {
                        tracing::warn!(
                            peer = %peer_name,
                            route = %routes[active_idx].name,
                            error = %e,
                            "peer link probe failed; reconnecting"
                        );
                        states[active_idx].health = RouteHealth::Failed {
                            last_error: format!("probe: {e}"),
                        };
                        states[active_idx].last_checked = Some(OffsetDateTime::now_utc());
                        publish(
                            &state,
                            PeerEntry {
                                name: peer_name.clone(),
                                status: PeerStatus::Reconnecting {
                                    since: OffsetDateTime::now_utc(),
                                },
                                active_route: None,
                                routes: states.clone(),
                                link: None,
                            },
                        )
                        .await;
                        break;
                    }
                    // Re-rank: prefer a higher-priority route once it is
                    // reachable again (e.g. back on the home LAN).
                    if active_idx > 0
                        && ticks.is_multiple_of(RERANK_EVERY_TICKS)
                        && let Some((better_idx, better_link)) = connect_ranked(
                            &peer_name,
                            &routes[..active_idx],
                            &mut states[..active_idx],
                            &cancel,
                        )
                        .await
                    {
                        tracing::info!(
                            peer = %peer_name,
                            from = %routes[active_idx].name,
                            to = %routes[better_idx].name,
                            "switching to higher-priority route"
                        );
                        // The old session's Arc stays alive inside any
                        // handler that grabbed the link; new work goes
                        // over the better route.
                        states[active_idx].health = RouteHealth::Unknown;
                        link = Arc::new(better_link);
                        active_idx = better_idx;
                        publish(
                            &state,
                            PeerEntry {
                                name: peer_name.clone(),
                                status: PeerStatus::Connected,
                                active_route: Some(routes[active_idx].name.clone()),
                                routes: states.clone(),
                                link: Some(link.clone()),
                            },
                        )
                        .await;
                    }
                }
            }
            None => {
                if cancel.is_cancelled() {
                    return;
                }
                let now = OffsetDateTime::now_utc();
                let last_error = states
                    .iter()
                    .rev()
                    .find_map(|s| match &s.health {
                        RouteHealth::Failed { last_error } => Some(last_error.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| "no routes attempted".into());
                publish(
                    &state,
                    PeerEntry {
                        name: peer_name.clone(),
                        status: PeerStatus::Failed {
                            since: now,
                            last_error: last_error.clone(),
                        },
                        active_route: None,
                        routes: states.clone(),
                        link: None,
                    },
                )
                .await;
                let delay = next_delay(attempt);
                tracing::warn!(
                    peer = %peer_name,
                    error = %last_error,
                    delay_ms = delay.as_millis() as u64,
                    "peer connect failed on every route; retrying"
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

    #[test]
    fn route_states_mirror_config_order_and_auto() {
        let routes = vec![
            RouteConfig {
                name: "lan".into(),
                ssh_target: "a".into(),
                auto: true,
            },
            RouteConfig {
                name: "wg".into(),
                ssh_target: "b".into(),
                auto: false,
            },
        ];
        let states = route_states(&routes);
        assert_eq!(states.len(), 2);
        assert_eq!(states[0].name, "lan");
        assert!(states[0].auto);
        assert!(matches!(states[0].health, RouteHealth::Unknown));
        assert_eq!(states[1].name, "wg");
        assert!(!states[1].auto);
    }
}
