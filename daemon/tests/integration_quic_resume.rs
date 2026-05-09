//! End-to-end test for slice 006 — resume tokens.
//!
//! Two LoopbackPools (sender + receiver) inside the VM. Two arctern
//! daemons on the host. Both phases of the slice 006 happy path are
//! exercised in two separate `#[tokio::test]`s in this file:
//!
//! - `quic_resume_after_interrupt` — Strategy C from spec D7 (the
//!   simplest one that actually exercises the resume path): start a
//!   push, sleep ~50 ms so the daemon is mid-stream, SIGKILL the
//!   sender daemon, assert the receiver advertises a token, restart
//!   the sender, assert the second cycle resumes and lands the
//!   snapshot with matching GUID. Strategy A (test reaches into the
//!   daemon to close the QUIC connection) is impossible across the
//!   subprocess boundary; Strategy B (kill the recv child specifically
//!   in the VM) is fragile across timing — Strategy C is brutal but
//!   observable.
//!
//! - `quic_stale_token_triggers_discard` — the partial recv on the
//!   receiver gets stranded because the sender's source snapshot is
//!   destroyed and a new one takes its place. The planner sees a
//!   token whose to_guid is no longer on the sender, falls through
//!   to a fresh full with `discard_partial_recv = true`, the sink
//!   runs `zfs recv -A` then accepts the new stream.
//!
//! Both tests build on slice 005's spawn_daemon_uds_with_quic /
//! spawn_daemon_uds_with_config harness; the only new operation is
//! `palimpsest::recv::receive_resume_token` against the receiver pool.

#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::{Cmd, CommandRunner};

use common::{
    LoopbackPool, spawn_daemon_uds_with_config, spawn_daemon_uds_with_quic,
    ssh_runner_from_env, unique_suffix,
};

#[tokio::test(flavor = "multi_thread")]
async fn quic_resume_after_interrupt() {
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

    // Make the source big enough that a 50 ms truncation produces a
    // partial mid-stream rather than racing past it. 16 MiB of
    // urandom compresses poorly so the QUIC pipe actually carries
    // meaningful bytes.
    let mountpoint = format!("/{source}");
    let blob = format!("{mountpoint}/blob");
    let out = runner
        .run(Cmd::new("zfs").args(["set", &format!("mountpoint={mountpoint}"), &source]))
        .await
        .expect("zfs set mountpoint");
    assert!(out.status.success(), "zfs set mountpoint failed: {out:?}");
    let out = runner
        .run(Cmd::new("dd").args([
            "if=/dev/urandom",
            &format!("of={blob}"),
            "bs=1M",
            "count=16",
            "status=none",
        ]))
        .await
        .expect("dd urandom blob");
    assert!(out.status.success(), "dd urandom failed: {out:?}");
    let snap1 = format!("{source}@test_001");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap1]))
        .await
        .expect("zfs snapshot test_001");
    assert!(out.status.success(), "snapshot {snap1} failed: {out:?}");

    let target_dataset = format!("{sink_root}/{source}");

    // ─── Sink daemon ──────────────────────────────────────────────
    let sink_state = format!("/tmp/arctern_resume_sink_state_{}", unique_suffix());
    let sink_cfg_path =
        PathBuf::from(format!("/tmp/arctern_resume_sink_{}.toml", unique_suffix()));
    let sink_sock =
        PathBuf::from(format!("/tmp/arctern_resume_sink_{}.sock", unique_suffix()));
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

    // ─── Sender daemon (initial) ──────────────────────────────────
    let sender_state = format!("/tmp/arctern_resume_sender_state_{}", unique_suffix());
    let sender_cfg_path = PathBuf::from(format!(
        "/tmp/arctern_resume_sender_{}.toml",
        unique_suffix()
    ));
    let sender_sock =
        PathBuf::from(format!("/tmp/arctern_resume_sender_{}.sock", unique_suffix()));
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

    // Tear-down helper for every assertion failure path.
    let teardown_files = |paths: &[&Path]| {
        for p in paths {
            let _ = std::fs::remove_file(p);
        }
    };

    // ─── Phase 1: induce a partial via SIGKILL mid-stream ─────────
    //
    // The wakeup endpoint returns 204 immediately; the daemon then opens
    // QUIC, runs LIST, opens SEND, spawns zfs send, and starts copying.
    // SIGKILL after a small delay gives us a partial. If the first
    // attempt is too fast (no token produced) we retry with a longer
    // delay — see plan D21.
    let mut token_first: Option<String> = None;
    let mut last_attempt_delay_ms = 0u64;
    let mut attempts: Vec<u64> = Vec::new();
    for delay_ms in [50u64, 150, 400] {
        let (mut sender_child, sender_sock_actual) = spawn_daemon_uds_with_config(
            Some(sender_sock.clone()),
            Some(sender_cfg_path.clone()),
        );
        wakeup_via_uds(&sender_sock_actual, "push").await;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        // SIGKILL — Child::kill on Unix sends SIGKILL.
        let _ = sender_child.kill();
        let _ = sender_child.wait();
        let _ = std::fs::remove_file(&sender_sock_actual);
        attempts.push(delay_ms);
        last_attempt_delay_ms = delay_ms;

        // Allow the sink's stderr pipe + recv to flush.
        tokio::time::sleep(Duration::from_millis(500)).await;
        match palimpsest::recv::receive_resume_token(&runner, &target_dataset).await {
            Ok(Some(t)) => {
                token_first = Some(t);
                break;
            }
            Ok(None) => continue,
            Err(palimpsest::ZfsError::DatasetNotFound { .. }) => continue,
            Err(e) => {
                eprintln!(
                    "(attempt delay={delay_ms}ms) receive_resume_token error: {e}"
                );
                continue;
            }
        }
    }

    let token = match token_first {
        Some(t) => t,
        None => {
            // Tear down then fail with a debuggable message.
            let _ = sink_child.kill();
            let _ = sink_child.wait();
            teardown_files(&[
                &sink_cfg_path,
                &sender_cfg_path,
                &sink_sock_actual,
                &sender_sock,
            ]);
            let _ = std::fs::remove_dir_all(&sink_state);
            let _ = std::fs::remove_dir_all(&sender_state);
            let _ = receiver_pool.destroy().await;
            let _ = sender_pool.destroy().await;
            panic!(
                "phase 1: no resume token produced after {} SIGKILL attempts (delays={:?}); \
                 the SEND path may be too fast or the sink may have rejected the stream",
                attempts.len(),
                attempts
            );
        }
    };
    eprintln!(
        "phase 1: got resume token (delay={last_attempt_delay_ms}ms): {}",
        &token[..token.len().min(48)]
    );

    // ─── Phase 2: respawn sender; cycle resumes from the token ────
    let (mut sender_child, sender_sock_actual) = spawn_daemon_uds_with_config(
        Some(sender_sock.clone()),
        Some(sender_cfg_path.clone()),
    );
    wakeup_via_uds(&sender_sock_actual, "push").await;
    let phase2 = wait_for_snapshot_count(&runner, &target_dataset, 1, Duration::from_secs(30)).await;

    // Capture the receiver's snapshots BEFORE teardown so we can
    // assert GUIDs survive the round-trip.
    let recv_after = if phase2.is_ok() {
        list_target_snapshots(&runner, &target_dataset).await.ok()
    } else {
        None
    };
    let recv_token_after = palimpsest::recv::receive_resume_token(&runner, &target_dataset)
        .await
        .ok()
        .flatten();
    let send_snaps = list_target_snapshots(&runner, &source).await.ok();

    // Teardown.
    let _ = sender_child.kill();
    let _ = sender_child.wait();
    let _ = sink_child.kill();
    let _ = sink_child.wait();
    teardown_files(&[
        &sink_cfg_path,
        &sender_cfg_path,
        &sink_sock_actual,
        &sender_sock_actual,
    ]);
    let _ = std::fs::remove_dir_all(&sink_state);
    let _ = std::fs::remove_dir_all(&sender_state);
    let _ = receiver_pool.destroy().await;
    let _ = sender_pool.destroy().await;

    if let Err(e) = phase2 {
        panic!("phase 2 (resume): {e}");
    }
    let recv_after = recv_after.expect("expected receiver snapshots");
    assert_eq!(
        recv_after.len(),
        1,
        "expected exactly one snapshot on receiver after resume"
    );
    assert_eq!(
        recv_after[0].snapshot_name.as_deref(),
        Some("test_001"),
        "receiver snapshot name mismatch"
    );
    assert_eq!(
        recv_token_after, None,
        "receive_resume_token should be cleared once the resume completes"
    );

    // GUID round-trip — proves the resumed stream landed identical bits.
    let send_snaps = send_snaps.expect("expected sender snapshots");
    let send_guid = send_snaps
        .iter()
        .find(|e| e.snapshot_name.as_deref() == Some("test_001"))
        .and_then(|e| e.properties.get("guid").map(|p| p.value.clone()))
        .expect("sender test_001 GUID");
    let recv_guid = recv_after[0]
        .properties
        .get("guid")
        .map(|p| p.value.clone())
        .expect("receiver test_001 GUID");
    assert_eq!(
        send_guid, recv_guid,
        "GUID mismatch test_001 sender={send_guid} receiver={recv_guid}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn quic_stale_token_triggers_discard() {
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
    let mountpoint = format!("/{source}");
    let blob = format!("{mountpoint}/blob");
    let out = runner
        .run(Cmd::new("zfs").args(["set", &format!("mountpoint={mountpoint}"), &source]))
        .await
        .expect("zfs set mountpoint");
    assert!(out.status.success());
    let out = runner
        .run(Cmd::new("dd").args([
            "if=/dev/urandom",
            &format!("of={blob}"),
            "bs=1M",
            "count=16",
            "status=none",
        ]))
        .await
        .expect("dd urandom");
    assert!(out.status.success());
    let snap_a = format!("{source}@test_001");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap_a]))
        .await
        .expect("zfs snapshot test_001");
    assert!(out.status.success());

    let target_dataset = format!("{sink_root}/{source}");

    // ─── Sink + initial sender ────────────────────────────────────
    let sink_state = format!("/tmp/arctern_stale_sink_state_{}", unique_suffix());
    let sink_cfg_path =
        PathBuf::from(format!("/tmp/arctern_stale_sink_{}.toml", unique_suffix()));
    let sink_sock =
        PathBuf::from(format!("/tmp/arctern_stale_sink_{}.sock", unique_suffix()));
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
    let sink_addr = quic_addrs[0];

    let sender_state = format!("/tmp/arctern_stale_sender_state_{}", unique_suffix());
    let sender_cfg_path = PathBuf::from(format!(
        "/tmp/arctern_stale_sender_{}.toml",
        unique_suffix()
    ));
    let sender_sock =
        PathBuf::from(format!("/tmp/arctern_stale_sender_{}.sock", unique_suffix()));
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

    let teardown_files = |paths: &[&Path]| {
        for p in paths {
            let _ = std::fs::remove_file(p);
        }
    };

    // Phase 1 — induce a partial (same as the resume test).
    let mut token_present = false;
    for delay_ms in [50u64, 150, 400] {
        let (mut sender_child, sender_sock_actual) = spawn_daemon_uds_with_config(
            Some(sender_sock.clone()),
            Some(sender_cfg_path.clone()),
        );
        wakeup_via_uds(&sender_sock_actual, "push").await;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let _ = sender_child.kill();
        let _ = sender_child.wait();
        let _ = std::fs::remove_file(&sender_sock_actual);
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(Some(_)) = palimpsest::recv::receive_resume_token(&runner, &target_dataset).await
        {
            token_present = true;
            break;
        }
    }
    if !token_present {
        let _ = sink_child.kill();
        let _ = sink_child.wait();
        teardown_files(&[
            &sink_cfg_path,
            &sender_cfg_path,
            &sink_sock_actual,
            &sender_sock,
        ]);
        let _ = std::fs::remove_dir_all(&sink_state);
        let _ = std::fs::remove_dir_all(&sender_state);
        let _ = receiver_pool.destroy().await;
        let _ = sender_pool.destroy().await;
        panic!("setup phase: no resume token produced; cannot exercise stale-token discard");
    }

    // ─── Phase 2 — invalidate the token ───────────────────────────
    // Destroy the original snapshot and create a new one. The
    // partial recv on the receiver is now tied to a GUID that no
    // longer exists on the sender → planner picks Full + discard.
    let out = runner
        .run(Cmd::new("zfs").args(["destroy", &snap_a]))
        .await
        .expect("destroy test_001");
    assert!(out.status.success(), "destroy snap_a failed: {out:?}");
    let snap_b = format!("{source}@test_002");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap_b]))
        .await
        .expect("snapshot test_002");
    assert!(out.status.success(), "snapshot snap_b failed: {out:?}");

    // ─── Phase 3 — fresh send must succeed via discard ────────────
    let (mut sender_child, sender_sock_actual) = spawn_daemon_uds_with_config(
        Some(sender_sock.clone()),
        Some(sender_cfg_path.clone()),
    );
    wakeup_via_uds(&sender_sock_actual, "push").await;
    let phase3 = wait_for_snapshot_count(&runner, &target_dataset, 1, Duration::from_secs(30)).await;
    let recv_after = if phase3.is_ok() {
        list_target_snapshots(&runner, &target_dataset).await.ok()
    } else {
        None
    };
    let recv_token_after = palimpsest::recv::receive_resume_token(&runner, &target_dataset)
        .await
        .ok()
        .flatten();
    let send_snaps = list_target_snapshots(&runner, &source).await.ok();

    let _ = sender_child.kill();
    let _ = sender_child.wait();
    let _ = sink_child.kill();
    let _ = sink_child.wait();
    teardown_files(&[
        &sink_cfg_path,
        &sender_cfg_path,
        &sink_sock_actual,
        &sender_sock_actual,
    ]);
    let _ = std::fs::remove_dir_all(&sink_state);
    let _ = std::fs::remove_dir_all(&sender_state);
    let _ = receiver_pool.destroy().await;
    let _ = sender_pool.destroy().await;

    if let Err(e) = phase3 {
        panic!("phase 3 (stale-token discard): {e}");
    }
    let recv_after = recv_after.expect("expected receiver snapshots");
    let names: Vec<&str> = recv_after
        .iter()
        .map(|e| e.snapshot_name.as_deref().unwrap_or(""))
        .collect();
    assert_eq!(
        names,
        vec!["test_002"],
        "receiver should have only test_002 after stale-token discard; got {names:?}"
    );
    assert_eq!(
        recv_token_after, None,
        "receive_resume_token should be cleared after discard+fresh recv"
    );
    let send_snaps = send_snaps.expect("expected sender snapshots");
    let send_guid = send_snaps
        .iter()
        .find(|e| e.snapshot_name.as_deref() == Some("test_002"))
        .and_then(|e| e.properties.get("guid").map(|p| p.value.clone()))
        .expect("sender test_002 GUID");
    let recv_guid = recv_after[0]
        .properties
        .get("guid")
        .map(|p| p.value.clone())
        .expect("receiver test_002 GUID");
    assert_eq!(
        send_guid, recv_guid,
        "GUID mismatch test_002 sender={send_guid} receiver={recv_guid}"
    );
}

// ─── Helpers ──────────────────────────────────────────────────────

async fn wakeup_via_uds(sock: &Path, job_name: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::UnixStream::connect(sock)
        .await
        .expect("connect uds");
    let req = format!(
        "POST /api/v1/jobs/{job_name}/wakeup HTTP/1.1\r\nHost: localhost\r\n\
         Content-Length: 0\r\nConnection: close\r\n\r\n"
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
            Err(palimpsest::ZfsError::DatasetNotFound { .. }) => {}
            Err(e) => return Err(format!("list {dataset}: {e}")),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!(
        "timeout waiting for {expected} snapshots on {dataset} (last seen: {last_seen})"
    ))
}
