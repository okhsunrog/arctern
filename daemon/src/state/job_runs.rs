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

/// Insert a `running` row for a freshly started cycle. The
/// `(job_name, started_at)` pair is the primary key; callers are
/// expected to use the cycle's UNIX-second start as `started_at` and
/// to call `finish` later with the same value.
pub async fn record_start(
    pool: &SqlitePool,
    job_name: &str,
    started_at: i64,
) -> Result<(), StateError> {
    sqlx::query(
        "INSERT INTO job_runs (job_name, started_at, status)
         VALUES (?, ?, ?)
         ON CONFLICT(job_name, started_at) DO NOTHING",
    )
    .bind(job_name)
    .bind(started_at)
    .bind(STATUS_RUNNING)
    .execute(pool)
    .await?;
    Ok(())
}

/// Update an in-flight row to its terminal state.
pub async fn record_finish(
    pool: &SqlitePool,
    job_name: &str,
    started_at: i64,
    finished_at: i64,
    status: &str,
    error_message: Option<&str>,
    bytes_sent: Option<i64>,
) -> Result<(), StateError> {
    sqlx::query(
        "UPDATE job_runs
            SET finished_at = ?, status = ?, error_message = ?, bytes_sent = ?
          WHERE job_name = ? AND started_at = ?",
    )
    .bind(finished_at)
    .bind(status)
    .bind(error_message)
    .bind(bytes_sent)
    .bind(job_name)
    .bind(started_at)
    .execute(pool)
    .await?;
    Ok(())
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
          ORDER BY started_at DESC
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
#[allow(dead_code)]
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
        record_start(&pool, "backup", 100).await.unwrap();
        record_finish(&pool, "backup", 100, 200, STATUS_OK, None, Some(2048))
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
}
