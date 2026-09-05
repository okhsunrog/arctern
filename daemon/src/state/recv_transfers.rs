//! Completed inbound transfers, recorded by the recv channel handler.
//! Receiver-side visibility only — the sender already tracks its view
//! in `job_runs`/`push_syncs`. Replication state proper lives in ZFS;
//! losing this table loses nothing but the "Incoming" history panel.

use arctern_api::RecvTransfer;
use sqlx::SqlitePool;

use super::StateError;

/// One completed transfer, as the recv channel hands it over. The wire
/// row adds only the database id.
#[derive(Debug, Clone)]
pub struct NewTransfer<'a> {
    pub completed_at: i64,
    pub job: &'a str,
    pub identity: &'a str,
    pub dataset: &'a str,
    pub to_snapshot: &'a str,
    pub from_snapshot: Option<&'a str>,
    pub bytes: i64,
    pub duration_ms: i64,
}

pub async fn record(pool: &SqlitePool, t: NewTransfer<'_>) -> Result<(), StateError> {
    sqlx::query!(
        "INSERT INTO recv_transfers
           (completed_at, job, identity, dataset, to_snapshot, from_snapshot, bytes, duration_ms)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        t.completed_at,
        t.job,
        t.identity,
        t.dataset,
        t.to_snapshot,
        t.from_snapshot,
        t.bytes,
        t.duration_ms,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Most recent completed transfers, newest first.
pub async fn recent(pool: &SqlitePool, limit: i64) -> Result<Vec<RecvTransfer>, StateError> {
    let rows = sqlx::query!(
        r#"SELECT id, completed_at, job, identity, dataset, to_snapshot, from_snapshot,
                  bytes, duration_ms
             FROM recv_transfers ORDER BY id DESC LIMIT ?"#,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| RecvTransfer {
            id: r.id,
            completed_at: r.completed_at,
            job: r.job,
            identity: r.identity,
            dataset: r.dataset,
            to_snapshot: r.to_snapshot,
            from_snapshot: r.from_snapshot,
            bytes: r.bytes,
            duration_ms: r.duration_ms,
        })
        .collect())
}

pub async fn trim_older_than(pool: &SqlitePool, cutoff: i64) -> Result<(), StateError> {
    sqlx::query!("DELETE FROM recv_transfers WHERE completed_at < ?", cutoff)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer<'a>(
        completed_at: i64,
        to_snapshot: &'a str,
        from_snapshot: Option<&'a str>,
        bytes: i64,
    ) -> NewTransfer<'a> {
        NewTransfer {
            completed_at,
            job: "push_test",
            identity: "laptop_nova",
            dataset: "tank/backups/x",
            to_snapshot,
            from_snapshot,
            bytes,
            duration_ms: 30,
        }
    }

    #[tokio::test]
    async fn record_and_recent_roundtrip() {
        let pool = crate::state::open_in_memory().await.unwrap();
        record(&pool, transfer(1000, "arctern_1", None, 42))
            .await
            .unwrap();
        record(&pool, transfer(2000, "arctern_2", Some("arctern_1"), 7))
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
            record(&pool, transfer(at, snap, None, 1)).await.unwrap();
        }
        trim_older_than(&pool, 1500).await.unwrap();
        let rows = recent(&pool, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].to_snapshot, "b");
    }
}
