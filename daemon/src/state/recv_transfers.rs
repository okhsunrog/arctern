//! Completed inbound transfers, recorded by the recv channel handler.
//! Receiver-side visibility only — the sender already tracks its view
//! in `job_runs`/`push_syncs`. Replication state proper lives in ZFS;
//! losing this table loses nothing but the "Incoming" history panel.

use sqlx::{Row, SqlitePool};

use super::StateError;

#[allow(clippy::too_many_arguments)]
pub async fn record(
    pool: &SqlitePool,
    completed_at: i64,
    job: &str,
    identity: &str,
    dataset: &str,
    to_snapshot: &str,
    from_snapshot: Option<&str>,
    bytes: i64,
    duration_ms: i64,
) -> Result<(), StateError> {
    sqlx::query(
        "INSERT INTO recv_transfers
           (completed_at, job, identity, dataset, to_snapshot, from_snapshot, bytes, duration_ms)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(completed_at)
    .bind(job)
    .bind(identity)
    .bind(dataset)
    .bind(to_snapshot)
    .bind(from_snapshot)
    .bind(bytes)
    .bind(duration_ms)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct RecvTransferRow {
    pub id: i64,
    pub completed_at: i64,
    pub job: String,
    pub identity: String,
    pub dataset: String,
    pub to_snapshot: String,
    pub from_snapshot: Option<String>,
    pub bytes: i64,
    pub duration_ms: i64,
}

/// Most recent completed transfers, newest first.
pub async fn recent(pool: &SqlitePool, limit: i64) -> Result<Vec<RecvTransferRow>, StateError> {
    let rows = sqlx::query(
        "SELECT id, completed_at, job, identity, dataset, to_snapshot, from_snapshot,
                bytes, duration_ms
         FROM recv_transfers ORDER BY id DESC LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| RecvTransferRow {
            id: r.get("id"),
            completed_at: r.get("completed_at"),
            job: r.get("job"),
            identity: r.get("identity"),
            dataset: r.get("dataset"),
            to_snapshot: r.get("to_snapshot"),
            from_snapshot: r.get("from_snapshot"),
            bytes: r.get("bytes"),
            duration_ms: r.get("duration_ms"),
        })
        .collect())
}

pub async fn trim_older_than(pool: &SqlitePool, cutoff: i64) -> Result<(), StateError> {
    sqlx::query("DELETE FROM recv_transfers WHERE completed_at < ?")
        .bind(cutoff)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn record_and_recent_roundtrip() {
        let pool = crate::state::open_in_memory().await.unwrap();
        record(
            &pool,
            1000,
            "push_test",
            "laptop_nova",
            "tank/backups/x",
            "arctern_1",
            None,
            42,
            150,
        )
        .await
        .unwrap();
        record(
            &pool,
            2000,
            "push_test",
            "laptop_nova",
            "tank/backups/x",
            "arctern_2",
            Some("arctern_1"),
            7,
            30,
        )
        .await
        .unwrap();
        let rows = recent(&pool, 10).await.unwrap();
        assert_eq!(rows.len(), 2);
        // Newest first.
        assert_eq!(rows[0].to_snapshot, "arctern_2");
        assert_eq!(rows[0].from_snapshot.as_deref(), Some("arctern_1"));
        assert_eq!(rows[0].bytes, 7);
        assert_eq!(rows[1].to_snapshot, "arctern_1");
        assert_eq!(rows[1].from_snapshot, None);
    }

    #[tokio::test]
    async fn trim_drops_old_rows() {
        let pool = crate::state::open_in_memory().await.unwrap();
        for (at, snap) in [(1000, "a"), (2000, "b")] {
            record(&pool, at, "j", "i", "d", snap, None, 1, 1)
                .await
                .unwrap();
        }
        trim_older_than(&pool, 1500).await.unwrap();
        let rows = recent(&pool, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].to_snapshot, "b");
    }
}
