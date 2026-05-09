//! End-to-end test: boot a fresh loopback pool inside the VM, spawn the
//! arctern daemon over a per-test UNIX socket, list datasets via
//! `arctern-client`, assert the test pool appears.
//!
//! Gated behind the `integration` cargo feature. Requires
//! PALIMPSEST_SSH_TARGET + (optional) PALIMPSEST_SSH_PASSWORD pointing at
//! a ZFS-capable VM — `just vm-up` from either palimpsest or arctern's
//! repo brings one up on port 2226.

#![cfg(feature = "integration")]
// Tests panic-out from `.expect(...)` between spawn and kill+wait. Acceptable
// here: the test process exits immediately after, so the OS reaps any
// orphaned children. Production callers should use a Drop-based guard.
#![allow(clippy::zombie_processes)]

mod common;

use common::{LoopbackPool, spawn_daemon_uds, ssh_runner_from_env};

#[tokio::test(flavor = "multi_thread")]
async fn get_datasets_returns_test_pool() {
    let runner = ssh_runner_from_env();
    let pool = LoopbackPool::create(runner).await.expect("create pool");

    let (mut child, socket) = spawn_daemon_uds(None);

    let datasets = arctern_client::list_datasets(&socket)
        .await
        .expect("list_datasets over UDS");

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&socket);

    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert!(
        names.contains(&pool.name()),
        "expected pool {} in response, got {names:?}",
        pool.name()
    );
    let pool_entry = datasets
        .iter()
        .find(|d| d.name == pool.name())
        .expect("pool entry");
    assert_eq!(pool_entry.dataset_type, "filesystem");

    pool.destroy().await.expect("destroy pool");
}
