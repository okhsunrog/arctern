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
    Reconnecting { since: OffsetDateTime },
    /// Last connect attempt failed. The loop will sleep, increment
    /// attempt, and try again.
    Failed {
        since: OffsetDateTime,
        last_error: String,
    },
}

#[derive(Clone)]
pub struct PeerEntry {
    pub name: String,
    pub ssh_target: String,
    pub status: PeerStatus,
    /// Some only when status is Connected; None otherwise.
    pub link: Option<Arc<PeerLink>>,
}

pub type PeersState = Arc<RwLock<HashMap<String, PeerEntry>>>;

pub fn new_state() -> PeersState {
    Arc::new(RwLock::new(HashMap::new()))
}
