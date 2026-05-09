//! Per-daemon SQLite state. Observability only — replication state
//! itself lives in ZFS (holds, bookmarks, `receive_resume_token`).
//!
//! Schema and trim policy follow `ARCHITECTURE.md` ("State storage"):
//! WAL + NORMAL, two tables (`job_runs`, `log_events`), 30 days of
//! `job_runs`, 24 h of `log_events`. The `tracing-subscriber` Layer in
//! `log_events::SqliteLogLayer` writes INFO+ events here; DEBUG/TRACE
//! never reach this DB (kHz event rates from tokio internals would
//! explode it).

use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;
use thiserror::Error;

pub mod job_runs;
pub mod log_events;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("sqlite open {path}: {source}")]
    Open {
        path: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("sqlite migrate: {0}")]
    Migrate(#[source] sqlx::Error),
    #[error("sqlite query: {0}")]
    Query(#[from] sqlx::Error),
}

/// Open (creating if necessary) the daemon's SQLite at
/// `<state_dir>/state.db`, configure WAL + NORMAL, run schema migrations.
/// Returns a connection pool sized for the daemon's expected concurrency
/// (a handful of jobs + the tracing layer + occasional HTTP handlers).
pub async fn open(state_dir: &Path) -> Result<SqlitePool, StateError> {
    let path = state_dir.join("state.db");
    let display = path.display().to_string();
    let opts = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal);
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await
        .map_err(|source| StateError::Open {
            path: display,
            source,
        })?;
    migrate(&pool).await?;
    Ok(pool)
}

async fn migrate(pool: &SqlitePool) -> Result<(), StateError> {
    // Single inline migration for now. When the schema gains a second
    // version, switch to sqlx::migrate! against a migrations dir.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS job_runs (
            job_name      TEXT NOT NULL,
            started_at    INTEGER NOT NULL,
            finished_at   INTEGER,
            status        TEXT NOT NULL,
            error_message TEXT,
            bytes_sent    INTEGER,
            PRIMARY KEY (job_name, started_at)
        )",
    )
    .execute(pool)
    .await
    .map_err(StateError::Migrate)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS log_events (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            level     TEXT NOT NULL,
            job_name  TEXT,
            message   TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(StateError::Migrate)?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_log_recent ON log_events(timestamp DESC)")
        .execute(pool)
        .await
        .map_err(StateError::Migrate)?;
    Ok(())
}

#[cfg(test)]
pub(crate) async fn open_in_memory() -> Result<SqlitePool, StateError> {
    let opts = SqliteConnectOptions::new()
        .filename(":memory:")
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .map_err(|source| StateError::Open {
            path: ":memory:".into(),
            source,
        })?;
    migrate(&pool).await?;
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_in_memory_runs_migrations() {
        let pool = open_in_memory().await.unwrap();
        // Both tables must be queryable post-migration.
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM job_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 0);
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM log_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }
}
