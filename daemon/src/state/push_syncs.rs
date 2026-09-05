//! Latest per-(job, peer) push outcome. Drives the `auto_interval`
//! policy ("don't auto-sync this peer more often than X") and the UI's
//! per-target status. One row per pair — history stays in `job_runs`.
//!
//! Losing this table is harmless: a missing row reads as "never
//! synced", which makes the peer due — one redundant (cheap, GUID-
//! deduplicated) sync, not data loss. Replication state proper lives
//! in ZFS per ARCHITECTURE.md.

use arctern_api::RunStatus;
use sqlx::SqlitePool;

use super::StateError;

pub async fn record(
    pool: &SqlitePool,
    job_name: &str,
    peer: &str,
    finished_at: i64,
    status: RunStatus,
    error: Option<&str>,
) -> Result<(), StateError> {
    let status = status.as_str();
    sqlx::query!(
        "INSERT INTO push_syncs
           (job_name, peer, finished_at, status, error, last_success_at)
         VALUES (?, ?, ?, ?, ?, CASE WHEN ? = 'ok' THEN ? ELSE NULL END)
         ON CONFLICT(job_name, peer) DO UPDATE SET
           finished_at = excluded.finished_at,
           status = excluded.status,
           error = excluded.error,
           last_success_at = CASE
             WHEN excluded.status = 'ok' THEN excluded.finished_at
             ELSE push_syncs.last_success_at
           END",
        job_name,
        peer,
        finished_at,
        status,
        error,
        status,
        finished_at,
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PeerSync {
    pub peer: String,
    pub finished_at: i64,
    /// None when a newer daemon wrote a status this build does not know.
    pub status: Option<RunStatus>,
    pub error: Option<String>,
    pub last_success_at: Option<i64>,
}

/// All recorded outcomes for a job, keyed by peer.
pub async fn for_job(pool: &SqlitePool, job_name: &str) -> Result<Vec<PeerSync>, StateError> {
    let rows = sqlx::query!(
        "SELECT peer, finished_at, status, error, last_success_at
           FROM push_syncs WHERE job_name = ?",
        job_name
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| PeerSync {
            peer: r.peer,
            finished_at: r.finished_at,
            status: r.status.parse().ok(),
            error: r.error,
            last_success_at: r.last_success_at,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::open_in_memory;

    #[tokio::test]
    async fn cancelled_attempt_preserves_last_success() {
        let pool = open_in_memory().await.unwrap();
        record(&pool, "push", "peer", 100, RunStatus::Ok, None)
            .await
            .unwrap();
        record(&pool, "push", "peer", 200, RunStatus::Cancelled, None)
            .await
            .unwrap();

        let rows = for_job(&pool, "push").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].finished_at, 200);
        assert_eq!(rows[0].status, Some(RunStatus::Cancelled));
        assert_eq!(rows[0].last_success_at, Some(100));
        assert_eq!(rows[0].error, None);
    }

    #[tokio::test]
    async fn error_attempt_preserves_last_success_and_message() {
        let pool = open_in_memory().await.unwrap();
        record(&pool, "push", "peer", 100, RunStatus::Ok, None)
            .await
            .unwrap();
        record(
            &pool,
            "push",
            "peer",
            300,
            RunStatus::Error,
            Some("broken pipe"),
        )
        .await
        .unwrap();

        let rows = for_job(&pool, "push").await.unwrap();
        assert_eq!(rows[0].last_success_at, Some(100));
        assert_eq!(rows[0].error.as_deref(), Some("broken pipe"));
    }
}
