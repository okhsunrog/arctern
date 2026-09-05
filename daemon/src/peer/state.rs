//! Shared per-peer state. The reconnect background task is the sole
//! writer; push jobs and HTTP handlers read snapshots. Readers never
//! hold the lock across an await, and writes are rare (a connectivity
//! change), so a plain `std::sync::RwLock` serves both sync and async
//! callers without a `try_read` compromise.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use time::OffsetDateTime;
use tokio::sync::watch;

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

/// The peers map plus the edge signal push schedulers sleep on.
#[derive(Clone)]
pub struct PeersState {
    inner: Arc<Inner>,
}

struct Inner {
    entries: RwLock<HashMap<String, PeerEntry>>,
    changed: watch::Sender<u64>,
}

impl Default for PeersState {
    fn default() -> Self {
        Self::new()
    }
}

impl PeersState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                entries: RwLock::new(HashMap::new()),
                changed: watch::channel(0).0,
            }),
        }
    }

    pub fn get(&self, name: &str) -> Option<PeerEntry> {
        self.inner.entries.read().unwrap().get(name).cloned()
    }

    /// Every peer, sorted by name.
    pub fn all(&self) -> Vec<PeerEntry> {
        let mut entries: Vec<PeerEntry> = self
            .inner
            .entries
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        entries
    }

    /// The live link to `name`, if connected.
    pub fn link(&self, name: &str) -> Option<Arc<PeerLink>> {
        self.inner.entries.read().unwrap().get(name)?.link.clone()
    }

    /// Replace one peer's entry and wake every subscriber.
    pub fn publish(&self, entry: PeerEntry) {
        self.inner
            .entries
            .write()
            .unwrap()
            .insert(entry.name.clone(), entry);
        self.inner.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// Fires on every `publish`. Push jobs re-evaluate due-ness on it
    /// instead of waiting out their nap.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.changed.subscribe()
    }
}
