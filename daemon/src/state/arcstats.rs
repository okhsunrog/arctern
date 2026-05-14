//! `arcstats_history` queries + the 60-second background sweep.
//!
//! Retention policy: keep 24h at 1-minute resolution (~1440 rows).
//! Drop older rows on every sweep. Cheap; if we ever want capacity
//! forecasting (roadmap #10) we'll add tiered downsampling.

use std::sync::Arc;
use std::time::Duration;

use arctern_api::ArcHistoryPoint;
use sqlx::{Row, SqlitePool};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use super::StateError;

pub const RETENTION_SECONDS: i64 = 24 * 60 * 60;
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Record one sample. Idempotent on `(timestamp)` PK conflict so a
/// daemon restart inside the same wall-clock minute doesn't error.
pub async fn record(
    pool: &SqlitePool,
    timestamp: i64,
    size: u64,
    c: u64,
    hits: u64,
    misses: u64,
) -> Result<(), StateError> {
    sqlx::query(
        "INSERT INTO arcstats_history (timestamp, size, c, hits, misses)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(timestamp) DO NOTHING",
    )
    .bind(timestamp)
    .bind(size as i64)
    .bind(c as i64)
    .bind(hits as i64)
    .bind(misses as i64)
    .execute(pool)
    .await?;
    Ok(())
}

/// Return points newest-first. Caller typically reverses for chart x-axis.
pub async fn list_recent(
    pool: &SqlitePool,
    since_unix_seconds: Option<i64>,
    limit: i64,
) -> Result<Vec<ArcHistoryPoint>, StateError> {
    let rows = sqlx::query(
        "SELECT timestamp, size, c, hits, misses
           FROM arcstats_history
          WHERE (? IS NULL OR timestamp >= ?)
          ORDER BY timestamp DESC
          LIMIT ?",
    )
    .bind(since_unix_seconds)
    .bind(since_unix_seconds)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| ArcHistoryPoint {
            timestamp: r.get::<i64, _>("timestamp"),
            size: r.get::<i64, _>("size") as u64,
            c: r.get::<i64, _>("c") as u64,
            hits: r.get::<i64, _>("hits") as u64,
            misses: r.get::<i64, _>("misses") as u64,
        })
        .collect())
}

pub async fn trim_older_than(
    pool: &SqlitePool,
    cutoff_unix_seconds: i64,
) -> Result<u64, StateError> {
    let res = sqlx::query("DELETE FROM arcstats_history WHERE timestamp < ?")
        .bind(cutoff_unix_seconds)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

/// Background task: every `SWEEP_INTERVAL` reads
/// `/proc/spl/kstat/zfs/arcstats`, records a row, prunes old rows.
/// Errors are logged at WARN and do not abort the loop — a missing
/// kstat file (non-ZFS host, mock environment) is survivable.
pub fn spawn_sweeper(
    pool: Arc<SqlitePool>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = interval(SWEEP_INTERVAL);
        // The first tick fires immediately; we want it that way so the
        // dashboard has a sample within seconds of startup.
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {}
            }
            match palimpsest::system::arc_stats() {
                Ok(s) => {
                    let now = time::OffsetDateTime::now_utc().unix_timestamp();
                    if let Err(e) = record(&pool, now, s.size, s.c, s.hits, s.misses).await {
                        tracing::warn!(error = %e, "arcstats record failed");
                    }
                    let cutoff = now - RETENTION_SECONDS;
                    if let Err(e) = trim_older_than(&pool, cutoff).await {
                        tracing::warn!(error = %e, "arcstats trim failed");
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "arcstats read failed");
                }
            }
        }
    })
}
