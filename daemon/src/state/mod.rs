//! Per-daemon SQLite state. Replication state itself lives in ZFS (holds,
//! bookmarks, `receive_resume_token`); SQLite holds observability data and
//! persistent browser sessions.
//!
//! Schema and trim policy follow `ARCHITECTURE.md` ("State storage"):
//! WAL + NORMAL, two tables (`job_runs`, `log_events`), 30 days of
//! `job_runs`, 24 h of `log_events`. The `tracing-subscriber` Layer in
//! `log_events::SqliteLogLayer` writes INFO+ events here; DEBUG/TRACE
//! never reach this DB (kHz event rates from tokio internals would
//! explode it).

use std::path::Path;
use std::sync::Arc;

use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use thiserror::Error;

pub mod arcstats;
pub mod job_runs;
pub mod log_events;
pub mod push_syncs;
pub mod recv_transfers;

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
    // Any row left `running` predates this process: its writer died
    // before recording a terminal state. Reconcile before the UI can
    // read stale "in progress" history.
    let orphaned = job_runs::reconcile_orphaned(&pool).await?;
    if orphaned > 0 {
        tracing::warn!(
            count = orphaned,
            "reconciled orphaned job_runs from a prior crash"
        );
    }
    Ok(pool)
}

async fn migrate(pool: &SqlitePool) -> Result<(), StateError> {
    // Single inline migration for now. When the schema gains a second
    // version, switch to sqlx::migrate! against a migrations dir.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS job_runs (
            run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name      TEXT NOT NULL,
            started_at    INTEGER NOT NULL,
            finished_at   INTEGER,
            status        TEXT NOT NULL,
            error_message TEXT,
            bytes_sent    INTEGER
        )",
    )
    .execute(pool)
    .await
    .map_err(StateError::Migrate)?;
    let has_run_id: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('job_runs') WHERE name = 'run_id'",
    )
    .fetch_one(pool)
    .await
    .map_err(StateError::Migrate)?;
    if has_run_id == 0 {
        let mut tx = pool.begin().await.map_err(StateError::Migrate)?;
        sqlx::query("ALTER TABLE job_runs RENAME TO job_runs_legacy")
            .execute(&mut *tx)
            .await
            .map_err(StateError::Migrate)?;
        sqlx::query(
            "CREATE TABLE job_runs (
                run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name      TEXT NOT NULL,
                started_at    INTEGER NOT NULL,
                finished_at   INTEGER,
                status        TEXT NOT NULL,
                error_message TEXT,
                bytes_sent    INTEGER
            )",
        )
        .execute(&mut *tx)
        .await
        .map_err(StateError::Migrate)?;
        sqlx::query(
            "INSERT INTO job_runs
               (job_name, started_at, finished_at, status, error_message, bytes_sent)
             SELECT job_name, started_at, finished_at, status, error_message, bytes_sent
               FROM job_runs_legacy",
        )
        .execute(&mut *tx)
        .await
        .map_err(StateError::Migrate)?;
        sqlx::query("DROP TABLE job_runs_legacy")
            .execute(&mut *tx)
            .await
            .map_err(StateError::Migrate)?;
        tx.commit().await.map_err(StateError::Migrate)?;
    }
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
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS push_syncs (
            job_name    TEXT NOT NULL,
            peer        TEXT NOT NULL,
            finished_at INTEGER NOT NULL,
            status      TEXT NOT NULL,
            error       TEXT,
            last_success_at INTEGER,
            PRIMARY KEY (job_name, peer)
        )",
    )
    .execute(pool)
    .await
    .map_err(StateError::Migrate)?;
    // v2: an unsuccessful attempt must not erase the scheduling anchor.
    // Keep the existing attempt columns so upgrades are additive and old
    // databases remain readable during a rolling binary replacement.
    let has_last_success: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('push_syncs') WHERE name = 'last_success_at'",
    )
    .fetch_one(pool)
    .await
    .map_err(StateError::Migrate)?;
    if has_last_success == 0 {
        sqlx::query("ALTER TABLE push_syncs ADD COLUMN last_success_at INTEGER")
            .execute(pool)
            .await
            .map_err(StateError::Migrate)?;
        sqlx::query("UPDATE push_syncs SET last_success_at = finished_at WHERE status = 'ok'")
            .execute(pool)
            .await
            .map_err(StateError::Migrate)?;
    }
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS recv_transfers (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            completed_at  INTEGER NOT NULL,
            job           TEXT NOT NULL,
            identity      TEXT NOT NULL,
            dataset       TEXT NOT NULL,
            to_snapshot   TEXT NOT NULL,
            from_snapshot TEXT,
            bytes         INTEGER NOT NULL,
            duration_ms   INTEGER NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(StateError::Migrate)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS arcstats_history (
            timestamp INTEGER PRIMARY KEY,
            size      INTEGER NOT NULL,
            c         INTEGER NOT NULL,
            hits      INTEGER NOT NULL,
            misses    INTEGER NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(StateError::Migrate)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS browser_sessions (
            session_hash BLOB PRIMARY KEY NOT NULL,
            namespace    TEXT NOT NULL,
            expires_at   INTEGER NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(StateError::Migrate)?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_browser_sessions_expiry
         ON browser_sessions(namespace, expires_at)",
    )
    .execute(pool)
    .await
    .map_err(StateError::Migrate)?;
    Ok(())
}

/// Background task that enforces the retention policy from
/// `ARCHITECTURE.md` ("State storage"): every 6 hours, drop `job_runs`
/// older than 30 days and `log_events` older than 24 hours. Without this
/// the observability tables grow without bound — the INFO+ filter caps
/// the rate, not the total. Errors are logged and do not abort the loop.
pub fn spawn_trim_sweeper(
    pool: Arc<SqlitePool>,
    cancel: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    use std::time::Duration;
    const SWEEP_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
    const JOB_RUNS_RETENTION_SECONDS: i64 = 30 * 24 * 60 * 60;
    const LOG_EVENTS_RETENTION_SECONDS: i64 = 24 * 60 * 60;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {}
            }
            let now = time::OffsetDateTime::now_utc().unix_timestamp();
            if let Err(e) = job_runs::trim_older_than(&pool, now - JOB_RUNS_RETENTION_SECONDS).await
            {
                tracing::warn!(error = %e, "job_runs trim failed");
            }
            if let Err(e) =
                log_events::trim_older_than(&pool, now - LOG_EVENTS_RETENTION_SECONDS).await
            {
                tracing::warn!(error = %e, "log_events trim failed");
            }
            if let Err(e) =
                recv_transfers::trim_older_than(&pool, now - JOB_RUNS_RETENTION_SECONDS).await
            {
                tracing::warn!(error = %e, "recv_transfers trim failed");
            }
        }
    })
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

    #[tokio::test]
    async fn migrates_legacy_job_runs_without_losing_history() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(":memory:")
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE job_runs (
                job_name TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                finished_at INTEGER,
                status TEXT NOT NULL,
                error_message TEXT,
                bytes_sent INTEGER,
                PRIMARY KEY (job_name, started_at)
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_runs
               (job_name, started_at, finished_at, status, bytes_sent)
             VALUES ('push', 10, 20, 'ok', 42)",
        )
        .execute(&pool)
        .await
        .unwrap();

        migrate(&pool).await.unwrap();

        let row: (i64, String, i64, Option<i64>) =
            sqlx::query_as("SELECT run_id, job_name, started_at, bytes_sent FROM job_runs")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(row.0 > 0);
        assert_eq!(row.1, "push");
        assert_eq!(row.2, 10);
        assert_eq!(row.3, Some(42));
    }
}
