//! Latest per-(job, peer) push outcome. Drives the `auto_interval`
//! policy ("don't auto-sync this peer more often than X") and the UI's
//! per-target status. One row per pair — history stays in `job_runs`.
//!
//! Losing this table is harmless: a missing row reads as "never
//! synced", which makes the peer due — one redundant (cheap, GUID-
//! deduplicated) sync, not data loss. Replication state proper lives
//! in ZFS per ARCHITECTURE.md.

use sqlx::{Row, SqlitePool};

use super::StateError;

pub async fn record(
    pool: &SqlitePool,
    job_name: &str,
    peer: &str,
    finished_at: i64,
    status: &str,
    error: Option<&str>,
) -> Result<(), StateError> {
    sqlx::query(
        "INSERT INTO push_syncs (job_name, peer, finished_at, status, error)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(job_name, peer) DO UPDATE SET
           finished_at = excluded.finished_at,
           status = excluded.status,
           error = excluded.error",
    )
    .bind(job_name)
    .bind(peer)
    .bind(finished_at)
    .bind(status)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PeerSync {
    pub peer: String,
    pub finished_at: i64,
    pub status: String,
    pub error: Option<String>,
}

/// All recorded outcomes for a job, keyed by peer.
pub async fn for_job(pool: &SqlitePool, job_name: &str) -> Result<Vec<PeerSync>, StateError> {
    let rows =
        sqlx::query("SELECT peer, finished_at, status, error FROM push_syncs WHERE job_name = ?")
            .bind(job_name)
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|r| PeerSync {
            peer: r.get("peer"),
            finished_at: r.get("finished_at"),
            status: r.get("status"),
            error: r.get("error"),
        })
        .collect())
}
