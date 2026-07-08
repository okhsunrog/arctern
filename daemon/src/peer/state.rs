//! Shared per-peer state. The daemon's reconnect background task is
//! the sole writer; HTTP handlers read through `RwLock` to render
//! `GET /api/v1/peers` and to grab the `PeerLink` for proxied calls.

use std::collections::HashMap;
use std::sync::Arc;

use time::OffsetDateTime;
use tokio::sync::RwLock;

use super::PeerLink;

#[derive(Debug, Clone)]
pub enum PeerStatus {
    Connected,
    /// Between reconnect attempts. The reconnect loop sleeps for
    /// `next_delay(attempt)` then re-enters connect.
    Reconnecting {
        since: OffsetDateTime,
    },
    /// Last connect attempt failed on every route. The loop will
    /// sleep, increment attempt, and try again.
    Failed {
        since: OffsetDateTime,
        last_error: String,
    },
}

/// Last known result of connecting over one route. `Unknown` means the
/// route hasn't been attempted since a higher-priority one succeeded
/// first — the link connects only the active route (no idle SSH
/// session per route).
#[derive(Debug, Clone)]
pub enum RouteHealth {
    Unknown,
    Connected,
    Failed { last_error: String },
}

#[derive(Debug, Clone)]
pub struct RouteState {
    pub name: String,
    pub ssh_target: String,
    /// Whether scheduled (auto) replication may run while this route
    /// is active. Mirrors `RouteConfig::auto`.
    pub auto: bool,
    pub health: RouteHealth,
    pub last_checked: Option<OffsetDateTime>,
}

#[derive(Clone)]
pub struct PeerEntry {
    pub name: String,
    pub status: PeerStatus,
    /// Name of the route the live link runs over; None while down.
    pub active_route: Option<String>,
    /// Per-route snapshot, in priority order (config order).
    pub routes: Vec<RouteState>,
    /// Some only when status is Connected; None otherwise.
    pub link: Option<Arc<PeerLink>>,
}

impl PeerEntry {
    pub fn active_route(&self) -> Option<&RouteState> {
        let name = self.active_route.as_deref()?;
        self.routes.iter().find(|r| r.name == name)
    }
}

pub type PeersState = Arc<RwLock<HashMap<String, PeerEntry>>>;

pub fn new_state() -> PeersState {
    Arc::new(RwLock::new(HashMap::new()))
}
