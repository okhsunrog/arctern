//! End-to-end test: boot a fresh loopback pool inside the VM, spawn the
//! arctern daemon pointing at the VM, hit GET /api/v1/datasets, assert
//! the test pool appears in the response.
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

use arctern_api::DatasetSummary;

use common::{LoopbackPool, spawn_daemon, ssh_runner_from_env};

#[tokio::test(flavor = "multi_thread")]
async fn get_datasets_returns_test_pool() {
    let runner = ssh_runner_from_env();
    let pool = LoopbackPool::create(runner).await.expect("create pool");

    let (mut child, base) = spawn_daemon();

    let url = format!("{base}/api/v1/datasets");
    let datasets: Vec<DatasetSummary> = reqwest::get(&url)
        .await
        .expect("HTTP request to daemon")
        .error_for_status()
        .expect("2xx response")
        .json()
        .await
        .expect("decode JSON body");

    let _ = child.kill();
    let _ = child.wait();

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

#[tokio::test(flavor = "multi_thread")]
async fn openapi_doc_lists_datasets_path_and_schemas() {
    let (mut child, base) = spawn_daemon();

    let doc: serde_json::Value = reqwest::get(&format!("{base}/api-docs/openapi.json"))
        .await
        .expect("openapi request")
        .json()
        .await
        .expect("decode openapi");

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        doc.pointer("/paths/~1api~1v1~1datasets/get").is_some(),
        "openapi missing GET /api/v1/datasets: {doc:#}"
    );
    let schemas = doc
        .pointer("/components/schemas")
        .and_then(|v| v.as_object())
        .expect("openapi components.schemas object");
    assert!(schemas.contains_key("DatasetSummary"));
    assert!(schemas.contains_key("ApiErrorBody"));
}
