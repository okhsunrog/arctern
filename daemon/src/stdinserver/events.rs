//! Server-side events channel: a one-way stream of newline-delimited
//! JSON `EventWire` lines on stdout. Events are a stream, not RPC —
//! this channel carries no framing and accepts no input; it lives
//! until the client hangs up (stdout write fails).
//!
//! Two backends, picked at startup:
//! - **Daemon bridge** (preferred): subscribe to the local daemon's
//!   SSE endpoint over its UNIX socket and forward each event — the
//!   in-process bus latency end to end.
//! - **SQLite poll** (daemon-less receiver): tail `log_events` the way
//!   the old control-channel pusher did. Only stdinserver processes
//!   write events on such a host anyway.

use std::sync::Arc;
use std::time::Duration;

use arctern_config::Config;
use arctern_transport::EventWire;
use sqlx::SqlitePool;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub async fn run<W>(
    config: Arc<Config>,
    pool: Option<Arc<SqlitePool>>,
    mut writer: W,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let socket = config
        .socket
        .clone()
        .unwrap_or_else(crate::default_socket_path);
    match arctern_client::stream_sse_data(&socket, "/api/v1/events").await {
        Ok(mut rx) => {
            tracing::info!("events channel: bridging daemon SSE");
            while let Some(payload) = rx.recv().await {
                // The daemon's SSE payload is already a LogEvent JSON
                // object with the EventWire field set — pass through.
                writer.write_all(payload.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
            }
            Ok(())
        }
        Err(e) => {
            let Some(pool) = pool else {
                tracing::warn!(error = %e, "events channel: no daemon and no SQLite; closing");
                return Ok(());
            };
            tracing::info!(error = %e, "events channel: daemon unreachable; polling SQLite");
            poll_sqlite(pool, &mut writer).await
        }
    }
}

async fn poll_sqlite<W>(pool: Arc<SqlitePool>, writer: &mut W) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut since = crate::state::log_events::cursor(&pool)
        .await
        .unwrap_or(0)
        .saturating_sub(100);
    loop {
        let rows = match crate::state::log_events::since(&pool, since, 256).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "events channel: log_events poll failed");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        for row in rows {
            since = row.id;
            let ev = EventWire {
                id: row.id as u64,
                timestamp: row.timestamp,
                level: row.level,
                job_name: row.job_name,
                message: row.message,
            };
            let line = serde_json::to_string(&ev).unwrap_or_else(|_| "{}".into());
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }
        writer.flush().await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
