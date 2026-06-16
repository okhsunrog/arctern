//! `log_events` table queries + the `tracing-subscriber` Layer that
//! writes INFO+ events into it.
//!
//! Filtering MUST be applied as a per-layer filter on this layer alone
//! (`SqliteLogLayer::filter()` / `with_filter`), NOT via `Layer::enabled`.
//! `enabled` is AND-combined and short-circuiting across the whole
//! `Layered` stack, so gating DEBUG/TRACE there would also suppress them
//! on the stderr/journald fmt layer — breaking `RUST_LOG=debug`. The
//! per-layer filter affects only this layer, keeping verbose events on
//! stderr while still keeping them out of SQLite (kHz-rate debug events
//! from tokio internals would otherwise explode the DB). The same filter
//! drops the `sqlx` target so a slow/failed insert can't recursively
//! generate more inserts.

// Query helpers below are wired into the SSE bridge in step 11 and the
// scheduler trim sweep in step 9.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::SqlitePool;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use super::StateError;

/// Insert a single event row. `timestamp` is unix seconds.
pub async fn insert(
    pool: &SqlitePool,
    timestamp: i64,
    level: &str,
    job_name: Option<&str>,
    message: &str,
) -> Result<i64, StateError> {
    let res = sqlx::query(
        "INSERT INTO log_events (timestamp, level, job_name, message)
         VALUES (?, ?, ?, ?)",
    )
    .bind(timestamp)
    .bind(level)
    .bind(job_name)
    .bind(message)
    .execute(pool)
    .await?;
    Ok(res.last_insert_rowid())
}

/// Read events strictly newer than `since_id`, oldest first, capped at
/// `limit`. Used by the SSE bridge in step 11 to replay backlog before
/// switching to the live broadcast.
pub async fn since(
    pool: &SqlitePool,
    since_id: i64,
    limit: i64,
) -> Result<Vec<LogRow>, StateError> {
    let rows: Vec<(i64, i64, String, Option<String>, String)> = sqlx::query_as(
        "SELECT id, timestamp, level, job_name, message
           FROM log_events
          WHERE id > ?
          ORDER BY id ASC
          LIMIT ?",
    )
    .bind(since_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, timestamp, level, job_name, message)| LogRow {
            id,
            timestamp,
            level,
            job_name,
            message,
        })
        .collect())
}

/// Current max(id), or 0 if the table is empty. Used by
/// `Request::GetLogCursor`.
pub async fn cursor(pool: &SqlitePool) -> Result<i64, StateError> {
    let v: Option<i64> = sqlx::query_scalar("SELECT MAX(id) FROM log_events")
        .fetch_one(pool)
        .await?;
    Ok(v.unwrap_or(0))
}

/// Spawn a background task that polls `log_events` every 500ms and
/// broadcasts each new row as `arctern_api::LogEvent` to subscribers.
/// Stops when `cancel` fires.
pub fn spawn_poller(
    pool: Arc<SqlitePool>,
    sender: tokio::sync::broadcast::Sender<arctern_api::LogEvent>,
    cancel: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    use std::time::Duration;
    tokio::spawn(async move {
        // Start from the current cursor — the broadcast is for live
        // events; backlog replay is the SSE handler's job (it queries
        // `since` directly before subscribing).
        let mut cursor = cursor(&pool).await.unwrap_or(0);
        let interval = Duration::from_millis(500);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }
            let rows = match since(&pool, cursor, 256).await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("log_events poller: {e}");
                    continue;
                }
            };
            for row in rows {
                cursor = row.id;
                let ev = arctern_api::LogEvent {
                    id: row.id as u64,
                    timestamp: row.timestamp,
                    level: row.level,
                    job_name: row.job_name,
                    message: row.message,
                };
                // Best-effort send; if there are no subscribers the
                // broadcast::Sender swallows the value harmlessly.
                let _ = sender.send(ev);
            }
        }
    })
}

/// Trim rows older than `cutoff_unix_seconds` (typically `now - 24h`).
pub async fn trim_older_than(
    pool: &SqlitePool,
    cutoff_unix_seconds: i64,
) -> Result<u64, StateError> {
    let res = sqlx::query("DELETE FROM log_events WHERE timestamp < ?")
        .bind(cutoff_unix_seconds)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRow {
    pub id: i64,
    pub timestamp: i64,
    pub level: String,
    pub job_name: Option<String>,
    pub message: String,
}

/// `tracing` Layer that mirrors INFO+ events into the SQLite log table.
/// Writes happen via `tokio::spawn(async move { ... })` so the calling
/// task is never blocked on the DB; failures are logged to stderr (not
/// recursively traced — that would be a feedback loop).
pub struct SqliteLogLayer {
    pool: Arc<SqlitePool>,
}

impl SqliteLogLayer {
    pub fn new(pool: Arc<SqlitePool>) -> Self {
        Self { pool }
    }

    /// Per-layer filter that gates this layer alone: INFO and above, and
    /// never the `sqlx` target (whose slow/failed-statement WARN/ERROR
    /// events would otherwise feed back into the insert path). Apply via
    /// `layer.with_filter(SqliteLogLayer::filter())`.
    pub fn filter() -> tracing_subscriber::filter::FilterFn {
        tracing_subscriber::filter::filter_fn(|metadata: &Metadata<'_>| {
            *metadata.level() <= Level::INFO && !metadata.target().starts_with("sqlx")
        })
    }
}

impl<S> Layer<S> for SqliteLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let message = visitor.message.unwrap_or_else(|| {
            // No `message` field — fall back to the event target so the
            // row is still useful for forensic queries.
            event.metadata().target().to_string()
        });
        let job_name = visitor.job_name;
        let level = event.metadata().level().to_string();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let pool = self.pool.clone();
        tokio::spawn(async move {
            if let Err(e) = insert(
                pool.as_ref(),
                timestamp,
                &level,
                job_name.as_deref(),
                &message,
            )
            .await
            {
                // Bypass tracing to avoid a feedback loop on DB failure.
                eprintln!("SqliteLogLayer insert failed: {e}");
            }
        });
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
    job_name: Option<String>,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else if field.name() == "job_name" || field.name() == "name" {
            self.job_name = Some(value.to_string());
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        } else if field.name() == "job_name" || field.name() == "name" {
            // Strip surrounding quotes that Debug formatting adds for &str.
            let s = format!("{value:?}");
            let stripped = s.trim_matches('"').to_string();
            self.job_name = Some(stripped);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::open_in_memory;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[tokio::test]
    async fn since_returns_strictly_newer() {
        let pool = open_in_memory().await.unwrap();
        let id1 = insert(&pool, 100, "INFO", Some("backup"), "first")
            .await
            .unwrap();
        let _id2 = insert(&pool, 200, "INFO", None, "second").await.unwrap();
        let rows = since(&pool, id1, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message, "second");
    }

    #[tokio::test]
    async fn cursor_reflects_max_id() {
        let pool = open_in_memory().await.unwrap();
        assert_eq!(cursor(&pool).await.unwrap(), 0);
        let id = insert(&pool, 100, "INFO", None, "x").await.unwrap();
        assert_eq!(cursor(&pool).await.unwrap(), id);
    }

    #[tokio::test]
    async fn layer_writes_info_events_and_skips_debug() {
        use tracing_subscriber::Layer as _;
        let pool = Arc::new(open_in_memory().await.unwrap());
        let layer = SqliteLogLayer::new(pool.clone()).with_filter(SqliteLogLayer::filter());
        let subscriber = Registry::default().with(layer);
        let _guard = subscriber.set_default();

        tracing::info!(job_name = "backup", "cycle ok");
        tracing::debug!("noisy detail");

        // The layer fires writes via tokio::spawn; let them drain.
        for _ in 0..50 {
            let n: i64 = sqlx::query_scalar("SELECT count(*) FROM log_events")
                .fetch_one(pool.as_ref())
                .await
                .unwrap();
            if n >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let rows = since(pool.as_ref(), 0, 100).await.unwrap();
        assert_eq!(rows.len(), 1, "INFO row landed, DEBUG dropped");
        assert_eq!(rows[0].message, "cycle ok");
        assert_eq!(rows[0].level, "INFO");
        assert_eq!(rows[0].job_name.as_deref(), Some("backup"));
    }
}
