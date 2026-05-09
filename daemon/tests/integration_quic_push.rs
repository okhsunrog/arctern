//! End-to-end test for the slice-005 push job.
//!
//! Two LoopbackPools (sender + receiver) inside the same VM. Two
//! arctern daemons on the host: one with a sink job over the receiver
//! pool, one with a push job pointing at the first daemon's QUIC
//! port. The test pre-creates the sender dataset, manually snapshots
//! it, hits the wakeup endpoint, asserts the receiver gained the
//! expected snapshot — then snapshots again, wakes up again, asserts
//! the second snapshot landed via the incremental path with matching
//! GUIDs.
//!
//! Slice-005 success looks like: with both daemons running, snapshots
//! created on the sender appear on the receiver under root_fs. This
//! test exercises full + incremental in sequence, which is the
//! shortest path through the planner's Full → Incremental transition.

#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::{Cmd, CommandRunner};

use common::{
    LoopbackPool, spawn_daemon_uds_with_config, spawn_daemon_uds_with_quic,
    ssh_runner_from_env, unique_suffix,
};

#[tokio::test(flavor = "multi_thread")]
async fn quic_push_full_then_incremental() {
    let sender_pool = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("create sender pool");
    let receiver_pool = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("create receiver pool");
    let runner = ssh_runner_from_env();

    let sink_root = format!("{}/sink", receiver_pool.name());
    let source = format!("{}/data", sender_pool.name());
    for ds in [&sink_root, &source] {
        let out = runner
            .run(Cmd::new("zfs").args(["create", "-o", "mountpoint=none", ds]))
            .await
            .expect("zfs create runs");
        assert!(out.status.success(), "zfs create {ds} failed: {out:?}");
    }

    // ─── Sink daemon ───────────────────────────────────────────────
    let sink_state = format!("/tmp/arctern_sink_state_{}", unique_suffix());
    let sink_cfg_path =
        PathBuf::from(format!("/tmp/arctern_sink_{}.toml", unique_suffix()));
    let sink_sock =
        PathBuf::from(format!("/tmp/arctern_sink_{}.sock", unique_suffix()));
    let sink_cfg = format!(
        r#"
state_dir = "{sink_state}"
[[jobs]]
type = "sink"
name = "sink"
listen = "127.0.0.1:0"
root_fs = "{sink_root}"
"#
    );
    std::fs::write(&sink_cfg_path, sink_cfg).expect("write sink config");

    let (mut sink_child, sink_sock_actual, quic_addrs) =
        spawn_daemon_uds_with_quic(Some(sink_sock.clone()), Some(sink_cfg_path.clone()), 1);
    assert_eq!(quic_addrs.len(), 1);
    let sink_addr = quic_addrs[0];

    // ─── Sender daemon ─────────────────────────────────────────────
    // Long interval — we drive cycles via wakeup, not the timer.
    let sender_state = format!("/tmp/arctern_sender_state_{}", unique_suffix());
    let sender_cfg_path =
        PathBuf::from(format!("/tmp/arctern_sender_{}.toml", unique_suffix()));
    let sender_sock =
        PathBuf::from(format!("/tmp/arctern_sender_{}.sock", unique_suffix()));
    let sender_cfg = format!(
        r#"
state_dir = "{sender_state}"
[[jobs]]
type = "push"
name = "push"
connect = "{sink_addr}"
interval = "1h"
[[jobs.filesystems]]
path = "{source}"
[jobs.target]
root_fs = "{sink_root}"
[jobs.snapshot_filter]
prefix = "test_"
"#
    );
    std::fs::write(&sender_cfg_path, sender_cfg).expect("write sender config");

    let (mut sender_child, sender_sock_actual) =
        spawn_daemon_uds_with_config(Some(sender_sock.clone()), Some(sender_cfg_path.clone()));

    // Tear-down helper used on every assertion failure path.
    let teardown =
        |sink_child: &mut std::process::Child, sender_child: &mut std::process::Child| {
            let _ = sender_child.kill();
            let _ = sender_child.wait();
            let _ = sink_child.kill();
            let _ = sink_child.wait();
        };

    // ─── Phase 1: full send ────────────────────────────────────────
    let snap1 = format!("{source}@test_001");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap1]))
        .await
        .expect("zfs snapshot test_001");
    assert!(out.status.success(), "zfs snapshot {snap1} failed: {out:?}");

    wakeup_via_uds(&sender_sock_actual, "push").await;
    let target_dataset = format!("{sink_root}/{source}");
    if let Err(e) = wait_for_snapshot_count(&runner, &target_dataset, 1, Duration::from_secs(20))
        .await
    {
        teardown(&mut sink_child, &mut sender_child);
        cleanup_files(&[&sink_cfg_path, &sender_cfg_path, &sink_sock_actual, &sender_sock_actual]);
        let _ = std::fs::remove_dir_all(&sink_state);
        let _ = std::fs::remove_dir_all(&sender_state);
        let _ = receiver_pool.destroy().await;
        let _ = sender_pool.destroy().await;
        panic!("phase 1 (full) failed: {e}");
    }
    let snaps_after_full =
        list_target_snapshots(&runner, &target_dataset).await.expect("list after full");
    assert_eq!(snaps_after_full.len(), 1, "expected one snapshot after full send");
    assert_eq!(snaps_after_full[0].snapshot_name.as_deref(), Some("test_001"));

    // ─── Phase 2: incremental send ─────────────────────────────────
    let snap2 = format!("{source}@test_002");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap2]))
        .await
        .expect("zfs snapshot test_002");
    assert!(out.status.success(), "zfs snapshot {snap2} failed: {out:?}");

    wakeup_via_uds(&sender_sock_actual, "push").await;
    let phase2_res =
        wait_for_snapshot_count(&runner, &target_dataset, 2, Duration::from_secs(20)).await;
    let snaps_after_inc = if phase2_res.is_ok() {
        list_target_snapshots(&runner, &target_dataset).await.ok()
    } else {
        None
    };

    // ─── Tear down ─────────────────────────────────────────────────
    teardown(&mut sink_child, &mut sender_child);
    cleanup_files(&[&sink_cfg_path, &sender_cfg_path, &sink_sock_actual, &sender_sock_actual]);
    let _ = std::fs::remove_dir_all(&sink_state);
    let _ = std::fs::remove_dir_all(&sender_state);

    // Compare GUIDs sender-vs-receiver before destroying pools so the
    // assertion message can be specific.
    let sender_snaps = list_target_snapshots(&runner, &source)
        .await
        .expect("list sender snaps");

    let _ = receiver_pool.destroy().await;
    let _ = sender_pool.destroy().await;

    if let Err(e) = phase2_res {
        panic!("phase 2 (incremental) failed: {e}");
    }
    let snaps_after_inc = snaps_after_inc.expect("should have snapshot list after Ok");
    assert_eq!(snaps_after_inc.len(), 2, "expected two snapshots after incremental");
    let recv_names: Vec<&str> = snaps_after_inc
        .iter()
        .map(|e| e.snapshot_name.as_deref().unwrap_or(""))
        .collect();
    assert_eq!(recv_names, vec!["test_001", "test_002"]);

    // GUIDs must match per-snapshot — proves the planner picked the
    // right from-snap and the executor sent the right delta.
    let sender_by_name: std::collections::BTreeMap<String, String> = sender_snaps
        .iter()
        .filter_map(|e| {
            let name = e.snapshot_name.clone()?;
            let guid = e.properties.get("guid")?.value.clone();
            Some((name, guid))
        })
        .collect();
    for r in &snaps_after_inc {
        let name = r.snapshot_name.clone().unwrap();
        let recv_guid = r
            .properties
            .get("guid")
            .map(|p| p.value.clone())
            .unwrap_or_default();
        let send_guid = sender_by_name.get(&name).cloned().unwrap_or_default();
        assert_eq!(
            recv_guid, send_guid,
            "GUID mismatch for {name}: sender={send_guid}, receiver={recv_guid}"
        );
    }
}

/// Hit POST /api/v1/jobs/<name>/wakeup over the daemon's UDS using a
/// hand-rolled HTTP/1.1 request. Avoids pulling reqwest + hyper-uds
/// into dev-deps just for one POST.
async fn wakeup_via_uds(sock: &std::path::Path, job_name: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::UnixStream::connect(sock)
        .await
        .expect("connect uds");
    let req = format!(
        "POST /api/v1/jobs/{job_name}/wakeup HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.expect("write req");
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf).await;
    let resp = String::from_utf8_lossy(&buf);
    assert!(
        resp.starts_with("HTTP/1.1 204"),
        "wakeup expected 204, got: {}",
        resp.lines().next().unwrap_or("")
    );
}

async fn list_target_snapshots(
    runner: &dyn CommandRunner,
    dataset: &str,
) -> Result<Vec<palimpsest::dataset::ZfsListEntry>, palimpsest::ZfsError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    palimpsest::dataset::list(runner, &opts).await
}

async fn wait_for_snapshot_count(
    runner: &dyn CommandRunner,
    dataset: &str,
    expected: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last_seen = 0usize;
    while Instant::now() < deadline {
        match list_target_snapshots(runner, dataset).await {
            Ok(v) => {
                last_seen = v.len();
                if v.len() >= expected {
                    return Ok(());
                }
            }
            Err(palimpsest::ZfsError::DatasetNotFound { .. }) => {
                // First-replication path: dataset doesn't exist yet.
            }
            Err(e) => {
                return Err(format!("list {dataset}: {e}"));
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!(
        "timeout waiting for {expected} snapshots on {dataset} (last seen: {last_seen})"
    ))
}

fn cleanup_files(paths: &[&std::path::Path]) {
    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}
