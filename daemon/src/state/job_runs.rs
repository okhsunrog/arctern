//! `job_runs` table queries. The scheduler writes one row per cycle
//! attempt; HTTP handlers read recent rows for the UI's "history" pane
//! (added in step 10). Trim policy: drop rows older than 30 days at
//! every sweep call (driven by the daemon's scheduler every 6 hours).

use sqlx::{Row, SqlitePool};

use super::StateError;

/// Lifecycle status string written to `job_runs.status`. Wire-typed as
/// a free-form `&str` rather than an enum so adding a new status (e.g.
/// `"skipped"`) is non-breaking.
pub const STATUS_OK: &str = "ok";
pub const STATUS_ERROR: &str = "error";
pub const STATUS_RUNNING: &str = "running";
#[allow(dead_code)]
pub const STATUS_CANCELLED: &str = "cancelled";
/// A push cycle in `dry_run` mode that completed without errors. Kept
/// apart from `ok` because `ok` is what `push_syncs` treats as a real
/// sync (`last_success_at`), and a dry run has replicated nothing.
pub const STATUS_DRY_RUN: &str = "dry_run";
/// A `running` row that outlived the process that wrote it: the daemon
/// was hard-killed or hit its shutdown deadline before `record_finish`
/// ran. Reconciled at startup so history never shows a perpetual
/// "in progress".
pub const STATUS_INTERRUPTED: &str = "interrupted";

/// Insert a `running` row for a freshly started cycle and return its
/// unique database id. Timestamps are intentionally not identifiers:
/// two operator actions can start within the same second.
pub async fn record_start(
    pool: &SqlitePool,
    job_name: &str,
    started_at: i64,
) -> Result<i64, StateError> {
    let result = sqlx::query(
        "INSERT INTO job_runs (job_name, started_at, status)
         VALUES (?, ?, ?)",
    )
    .bind(job_name)
    .bind(started_at)
    .bind(STATUS_RUNNING)
    .execute(pool)
    .await?;
    Ok(result.last_insert_rowid())
}

/// Update an in-flight row to its terminal state.
pub async fn record_finish(
    pool: &SqlitePool,
    run_id: i64,
    finished_at: i64,
    status: &str,
    error_message: Option<&str>,
    bytes_sent: Option<i64>,
) -> Result<(), StateError> {
    sqlx::query(
        "UPDATE job_runs
            SET finished_at = ?, status = ?, error_message = ?, bytes_sent = ?
          WHERE run_id = ?",
    )
    .bind(finished_at)
    .bind(status)
    .bind(error_message)
    .bind(bytes_sent)
    .bind(run_id)
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
    let res = sqlx::query(
        "UPDATE job_runs
            SET status = ?, finished_at = unixepoch()
          WHERE status = ? AND finished_at IS NULL",
    )
    .bind(STATUS_INTERRUPTED)
    .bind(STATUS_RUNNING)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Recent runs for `job_name`, newest first. `since_unix_seconds`
/// filters out rows older than the cutoff when `Some`; `limit` caps
/// the result set.
pub async fn list_recent(
    pool: &SqlitePool,
    job_name: &str,
    since_unix_seconds: Option<i64>,
    limit: i64,
) -> Result<Vec<arctern_api::JobRun>, StateError> {
    let rows = sqlx::query(
        "SELECT started_at, finished_at, status, error_message, bytes_sent
           FROM job_runs
          WHERE job_name = ?
            AND (? IS NULL OR started_at >= ?)
          ORDER BY started_at DESC, run_id DESC
          LIMIT ?",
    )
    .bind(job_name)
    .bind(since_unix_seconds)
    .bind(since_unix_seconds)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| arctern_api::JobRun {
            started_at: r.get::<i64, _>("started_at"),
            finished_at: r.get::<Option<i64>, _>("finished_at"),
            status: r.get::<String, _>("status"),
            error_message: r.get::<Option<String>, _>("error_message"),
            bytes_sent: r.get::<Option<i64>, _>("bytes_sent"),
        })
        .collect())
}

/// Trim rows older than `cutoff_unix_seconds` (typically `now - 30d`).
/// Returns the number of rows removed.
pub async fn trim_older_than(
    pool: &SqlitePool,
    cutoff_unix_seconds: i64,
) -> Result<u64, StateError> {
    let res = sqlx::query("DELETE FROM job_runs WHERE started_at < ?")
        .bind(cutoff_unix_seconds)
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
        record_finish(&pool, run_id, 200, STATUS_OK, None, Some(2048))
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
        record_finish(&pool, done, 200, STATUS_OK, None, None)
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
        assert_eq!(status, STATUS_INTERRUPTED);
        assert!(finished.is_some());

        // The already-finished row is untouched.
        let (status, _): (String, Option<i64>) =
            sqlx::query_as("SELECT status, finished_at FROM job_runs WHERE run_id = ?")
                .bind(done)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, STATUS_OK);

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
