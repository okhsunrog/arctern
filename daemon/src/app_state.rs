//! Shared HTTP handler state. Carries the JobManager (for local job
//! endpoints), the peers map (for proxied / peers-list endpoints) and
//! the broadcast channel of locally-generated log events (for SSE).

use std::path::PathBuf;
use std::sync::Arc;

use arctern_api::LogEvent;
use sqlx::SqlitePool;
use tokio::sync::broadcast;

use crate::auth::AdminAuth;
use crate::jobs::JobManager;
use crate::peer::state::PeersState;

#[derive(Clone)]
pub struct AppState {
    /// Browser authentication for the loopback TCP listener. The UNIX
    /// listener deliberately ignores it and authenticates via SO_PEERCRED.
    pub auth: AdminAuth,
    pub manager: Arc<JobManager>,
    pub peers: PeersState,
    /// Broadcast of locally-generated log events. The poller spawned in
    /// `state::log_events::spawn_poller` is the sole producer; SSE
    /// handlers subscribe per-request.
    pub events: broadcast::Sender<LogEvent>,
    /// SQLite pool for `job_runs` / `log_events`. Handlers that read
    /// historical state (e.g. /api/v1/jobs/{name}/runs) query through this.
    pub state: Arc<SqlitePool>,
    /// Shared typed ZFS facade — RealRunner in production, or an SSH-backed
    /// facade when ZFSKIT_SSH_TARGET is set for development/integration tests.
    pub zfs: zfskit::Zfs,
    /// Absolute path the daemon was started with (`--config <path>`),
    /// surfaced by `GET /api/v1/config` so the UI can show "you're
    /// editing the file at …".
    pub config_path: PathBuf,
    /// Fired on SIGTERM/SIGINT. Long-lived response streams (SSE) must
    /// end when this fires — axum's graceful shutdown waits for every
    /// connection to drain, and an open EventSource would otherwise
    /// stall it until systemd's SIGKILL.
    pub shutdown: tokio_util::sync::CancellationToken,
}
