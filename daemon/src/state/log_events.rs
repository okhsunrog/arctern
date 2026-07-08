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

/// The most recent `limit` events, oldest first. Used by the SSE
/// handler to replay backlog before switching to the live broadcast.
pub async fn recent(pool: &SqlitePool, limit: i64) -> Result<Vec<LogRow>, StateError> {
    let mut rows: Vec<(i64, i64, String, Option<String>, String)> = sqlx::query_as(
        "SELECT id, timestamp, level, job_name, message
           FROM log_events
          ORDER BY id DESC
          LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.reverse();
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

/// One event captured by the tracing layer, before it has an id.
#[derive(Debug)]
struct PendingEvent {
    timestamp: i64,
    level: String,
    job_name: Option<String>,
    message: String,
}

/// `tracing` Layer that mirrors INFO+ events into the event pipeline:
/// layer → mpsc → single writer task → SQLite insert (assigns the id)
/// → in-process broadcast. The broadcast is the PRIMARY live channel
/// (SSE and the peer event stream read it); SQLite is a subscriber
/// that provides durability and backlog replay — never a bus to poll.
pub struct SqliteLogLayer {
    tx: tokio::sync::mpsc::Sender<PendingEvent>,
}

impl SqliteLogLayer {
    /// Build the layer plus its writer task. Events flow to `events_tx`
    /// with their SQLite rowid as `id`, microseconds after emission —
    /// the 500ms poll latency this replaces is gone. `events_tx` may
    /// have zero subscribers (stdinserver processes); sends are
    /// best-effort.
    pub fn with_writer(
        pool: Arc<SqlitePool>,
        events_tx: tokio::sync::broadcast::Sender<arctern_api::LogEvent>,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PendingEvent>(1024);
        let writer = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                match insert(
                    pool.as_ref(),
                    ev.timestamp,
                    &ev.level,
                    ev.job_name.as_deref(),
                    &ev.message,
                )
                .await
                {
                    Ok(id) => {
                        let _ = events_tx.send(arctern_api::LogEvent {
                            id: id as u64,
                            timestamp: ev.timestamp,
                            level: ev.level,
                            job_name: ev.job_name,
                            message: ev.message,
                        });
                    }
                    // Bypass tracing to avoid a feedback loop on DB failure.
                    Err(e) => eprintln!("SqliteLogLayer insert failed: {e}"),
                }
            }
        });
        (Self { tx }, writer)
    }

    /// Per-layer filter that gates this layer alone: INFO and above,
    /// never the `sqlx` target (whose slow/failed-statement WARN/ERROR
    /// events would otherwise feed back into the insert path), and
    /// never `memory_serve` (one INFO row per static asset served would
    /// drown the event log in UI-chrome noise on every page load).
    /// Apply via `layer.with_filter(SqliteLogLayer::filter())`.
    pub fn filter() -> tracing_subscriber::filter::FilterFn {
        tracing_subscriber::filter::filter_fn(|metadata: &Metadata<'_>| {
            *metadata.level() <= Level::INFO
                && !metadata.target().starts_with("sqlx")
                && !metadata.target().starts_with("memory_serve")
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
        let (job_name, mut message) = visitor.render();
        if message.is_empty() {
            // No fields at all — fall back to the event target so the
            // row is still useful for forensic queries.
            message = event.metadata().target().to_string();
        }
        let level = event.metadata().level().to_string();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        // try_send: the layer must never block the emitting task; a
        // full queue (1024 pending inserts) means SQLite is wedged and
        // dropping the mirror copy is the right failure mode — stderr
        // still carries the event via the fmt layer.
        if let Err(e) = self.tx.try_send(PendingEvent {
            timestamp,
            level,
            job_name,
            message,
        }) {
            eprintln!("SqliteLogLayer: event queue full/closed; dropping: {e}");
        }
    }
}

/// Captures `message`, the job name, AND every other structured field
/// as `key=value` — "creating snapshot" with the dataset/snapshot
/// fields dropped told the operator nothing.
#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
    job_name: Option<String>,
    fields: Vec<(String, String)>,
}

impl MessageVisitor {
    fn record_any(&mut self, field: &Field, value: String) {
        match field.name() {
            "message" => self.message = Some(value),
            "job_name" | "name" => self.job_name = Some(value),
            other => self.fields.push((other.to_string(), value)),
        }
    }

    /// `message key=value key=value`, fields in emission order.
    fn render(self) -> (Option<String>, String) {
        let mut out = self.message.unwrap_or_default();
        for (k, v) in &self.fields {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(k);
            out.push('=');
            out.push_str(v);
        }
        (self.job_name, out)
    }
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_any(field, value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Strip surrounding quotes that Debug formatting adds for &str.
        let s = format!("{value:?}");
        self.record_any(field, s.trim_matches('"').to_string());
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
        let (events_tx, mut events_rx) = tokio::sync::broadcast::channel(16);
        let (layer, _writer) = SqliteLogLayer::with_writer(pool.clone(), events_tx);
        let layer = layer.with_filter(SqliteLogLayer::filter());
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

        // The broadcast half saw the same event, with the SQLite id.
        let live = events_rx.try_recv().expect("event on the bus");
        assert_eq!(live.id as i64, rows[0].id);
        assert_eq!(live.message, "cycle ok");
    }
}
