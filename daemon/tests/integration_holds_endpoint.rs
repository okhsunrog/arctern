//! End-to-end test for `GET /api/v1/datasets/{name}/holds`.
//!
//! The per-snapshot holds endpoint costs one `zfs holds` per row, which
//! turned a snapshot-browser refresh into hundreds of process spawns.
//! The batch endpoint answers for the whole dataset in one (chunked)
//! invocation — and chunking is the part that cannot be exercised
//! without a real dataset carrying more snapshots than the chunk size.

#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use std::collections::HashMap;

use arctern_api::SnapshotHold;

use common::{LoopbackPool, run_remote_shell, spawn_daemon_uds, ssh_runner_from_env};

/// Mirrors `HOLDS_ARGV_CHUNK` in `daemon/src/handlers/snapshots.rs`.
const CHUNK: usize = 500;

async fn fetch_holds(socket: &std::path::Path, pool: &str) -> HashMap<String, Vec<SnapshotHold>> {
    let path = format!("/api/v1/datasets/{pool}/holds");
    let (status, body) = arctern_client::raw(socket, "GET", &path, None)
        .await
        .expect("batch holds request");
    assert_eq!(status, 200, "body: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).expect("holds map parses")
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_holds_endpoint_covers_the_whole_dataset() {
    let runner = ssh_runner_from_env();
    let pool = LoopbackPool::create(runner).await.expect("create pool");
    let name = pool.name().to_string();

    // More snapshots than one `zfs holds` argv chunk. ZFS refuses several
    // snapshots of the same filesystem in one `zfs snapshot`, so this
    // loops remotely — one ssh round trip, ~8s inside the VM.
    let total = CHUNK + 100;
    run_remote_shell(&format!(
        "for i in $(seq 0 {}); do zfs snapshot {name}@s$(printf '%04d' $i); done",
        total - 1
    ));

    let (mut child, socket) = spawn_daemon_uds(None);

    // No holds anywhere: the response is an empty map, not an error, and
    // absence of a key is the answer "this snapshot is destroy-eligible".
    let empty = fetch_holds(&socket, &name).await;
    assert!(empty.is_empty(), "expected no holds, got {empty:?}");

    // One hold in the first argv chunk and one in the last, so a chunk
    // whose rows were dropped or overwritten cannot pass.
    let first = "s0000";
    let last = format!("s{:04}", total - 1);
    run_remote_shell(&format!("zfs hold keep-first {name}@{first}"));
    run_remote_shell(&format!("zfs hold keep-last {name}@{last}"));

    let holds = fetch_holds(&socket, &name).await;

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&socket);

    assert_eq!(
        holds.len(),
        2,
        "only the held snapshots belong in the map, got {:?}",
        holds.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        holds
            .get(first)
            .map(|h| h.iter().map(|x| x.tag.as_str()).collect::<Vec<_>>()),
        Some(vec!["keep-first"]),
    );
    assert_eq!(
        holds
            .get(&last)
            .map(|h| h.iter().map(|x| x.tag.as_str()).collect::<Vec<_>>()),
        Some(vec!["keep-last"]),
        "a hold past the chunk boundary must survive chunking",
    );

    pool.destroy().await.expect("destroy pool");
}
