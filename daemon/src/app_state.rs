//! Shared HTTP handler state. Carries the JobManager (for local job
//! endpoints), the peers map (for proxied / peers-list endpoints) and
//! the broadcast channel of locally-generated log events (for SSE).

use std::sync::Arc;

use arctern_api::LogEvent;
use sqlx::SqlitePool;
use tokio::sync::broadcast;

use crate::jobs::JobManager;
use crate::peer::state::PeersState;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<JobManager>,
    pub peers: PeersState,
    /// Broadcast of locally-generated log events. The poller spawned in
    /// `state::log_events::spawn_poller` is the sole producer; SSE
    /// handlers subscribe per-request.
    pub events: broadcast::Sender<LogEvent>,
    /// SQLite pool for `job_runs` / `log_events`. Handlers that read
    /// historical state (e.g. /api/v1/jobs/{name}/runs) query through this.
    pub state: Arc<SqlitePool>,
}
