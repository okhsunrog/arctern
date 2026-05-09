//! Shared HTTP handler state. Carries the JobManager (for local job
//! endpoints) and the peers map (for proxied / peers-list endpoints).

use std::sync::Arc;

use crate::jobs::JobManager;
use crate::peer::state::PeersState;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<JobManager>,
    pub peers: PeersState,
}
