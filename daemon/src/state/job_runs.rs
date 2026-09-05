//! `job_runs` table queries. The scheduler writes one row per cycle
//! attempt; HTTP handlers read recent rows for the UI's "history" pane
//! (added in step 10). Trim policy: drop rows older than 30 days at
//! every sweep call (driven by the daemon's scheduler every 6 hours).

use arctern_api::RunStatus;
use sqlx::SqlitePool;

use super::StateError;

/// Insert a `running` row for a freshly started cycle and return its
/// unique database id. Timestamps are intentionally not identifiers:
/// two operator actions can start within the same second.
pub async fn record_start(
    pool: &SqlitePool,
    job_name: &str,
    started_at: i64,
) -> Result<i64, StateError> {
    let status = RunStatus::Running.as_str();
    let result = sqlx::query!(
        "INSERT INTO job_runs (job_name, started_at, status) VALUES (?, ?, ?)",
        job_name,
        started_at,
        status,
    )
    .execute(pool)
    .await?;
    Ok(result.last_insert_rowid())
}

/// Update an in-flight row to its terminal state.
pub async fn record_finish(
    pool: &SqlitePool,
    run_id: i64,
    finished_at: i64,
    status: RunStatus,
    error_message: Option<&str>,
    bytes_sent: Option<i64>,
) -> Result<(), StateError> {
    let status = status.as_str();
    sqlx::query!(
        "UPDATE job_runs
            SET finished_at = ?, status = ?, error_message = ?, bytes_sent = ?
          WHERE run_id = ?",
        finished_at,
        status,
        error_message,
        bytes_sent,
        run_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Rewrite any row still marked `running` to `interrupted`, stamping
/// `finished_at` with the current time. A `running` row can only survive
/// a restart if the process died between `record_start` and
/// `record_finish` (SIGKILL, panic, power loss, or a shutdown-deadline
/// abort), so at startup every such row is by definition an orphan.
/// Returns the number of rows reconciled.
pub async fn reconcile_orphaned(pool: &SqlitePool) -> Result<u64, StateError> {
    let interrupted = RunStatus::Interrupted.as_str();
    let running = RunStatus::Running.as_str();
    let res = sqlx::query!(
        "UPDATE job_runs
            SET status = ?, finished_at = unixepoch()
          WHERE status = ? AND finished_at IS NULL",
        interrupted,
        running,
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Recent runs for `job_name`, newest first. `since_unix_seconds`
/// filters out rows older than the cutoff when `Some`; `limit` caps
/// the result set. A row whose status this build does not know (a
/// newer daemon wrote it) is skipped rather than failing the listing.
pub async fn list_recent(
    pool: &SqlitePool,
    job_name: &str,
    since_unix_seconds: Option<i64>,
    limit: i64,
) -> Result<Vec<arctern_api::JobRun>, StateError> {
    let rows = sqlx::query!(
        r#"SELECT started_at, finished_at, status, error_message, bytes_sent
             FROM job_runs
            WHERE job_name = ?
              AND (? IS NULL OR started_at >= ?)
            ORDER BY started_at DESC, run_id DESC
            LIMIT ?"#,
        job_name,
        since_unix_seconds,
        since_unix_seconds,
        limit,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|r| {
            Some(arctern_api::JobRun {
                started_at: r.started_at,
                finished_at: r.finished_at,
                status: r.status.parse().ok()?,
                error_message: r.error_message,
                bytes_sent: r.bytes_sent,
            })
        })
        .collect())
}

/// Trim rows older than `cutoff_unix_seconds` (typically `now - 30d`).
/// Returns the number of rows removed.
pub async fn trim_older_than(
    pool: &SqlitePool,
    cutoff_unix_seconds: i64,
) -> Result<u64, StateError> {
    let res = sqlx::query!(
        "DELETE FROM job_runs WHERE started_at < ?",
        cutoff_unix_seconds
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::open_in_memory;

    #[tokio::test]
    async fn record_start_then_finish() {
        let pool = open_in_memory().await.unwrap();
        let run_id = record_start(&pool, "backup", 100).await.unwrap();
        record_finish(&pool, run_id, 200, RunStatus::Ok, None, Some(2048))
            .await
            .unwrap();
        let row: (
            String,
            i64,
            Option<i64>,
            String,
            Option<String>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT job_name, started_at, finished_at, status, error_message, bytes_sent
               FROM job_runs WHERE job_name = ? AND started_at = ?",
        )
        .bind("backup")
        .bind(100i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.3, "ok");
        assert_eq!(row.5, Some(2048));
    }

    #[tokio::test]
    async fn trim_older_than_drops_old_rows() {
        let pool = open_in_memory().await.unwrap();
        record_start(&pool, "j", 100).await.unwrap();
        record_start(&pool, "j", 500).await.unwrap();
        let removed = trim_older_than(&pool, 200).await.unwrap();
        assert_eq!(removed, 1);
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM job_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn reconcile_rewrites_only_orphaned_running_rows() {
        let pool = open_in_memory().await.unwrap();
        let orphan = record_start(&pool, "push", 100).await.unwrap();
        let done = record_start(&pool, "push", 100).await.unwrap();
        record_finish(&pool, done, 200, RunStatus::Ok, None, None)
            .await
            .unwrap();

        let reconciled = reconcile_orphaned(&pool).await.unwrap();
        assert_eq!(reconciled, 1);

        let (status, finished): (String, Option<i64>) =
            sqlx::query_as("SELECT status, finished_at FROM job_runs WHERE run_id = ?")
                .bind(orphan)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, RunStatus::Interrupted.as_str());
        assert!(finished.is_some());

        // The already-finished row is untouched.
        let (status, _): (String, Option<i64>) =
            sqlx::query_as("SELECT status, finished_at FROM job_runs WHERE run_id = ?")
                .bind(done)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, RunStatus::Ok.as_str());

        // Idempotent: a second pass finds nothing left to reconcile.
        assert_eq!(reconcile_orphaned(&pool).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn starts_in_the_same_second_are_distinct() {
        let pool = open_in_memory().await.unwrap();
        let first = record_start(&pool, "push", 100).await.unwrap();
        let second = record_start(&pool, "push", 100).await.unwrap();
        assert_ne!(first, second);

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM job_runs WHERE job_name = 'push' AND started_at = 100",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 2);
    }
}
