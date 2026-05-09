//! End-to-end test for slice 006 — resume tokens.
//!
//! Two LoopbackPools (sender + receiver) inside the VM. Two arctern
//! daemons on the host. Both phases of the slice 006 happy path are
//! exercised in two separate `#[tokio::test]`s in this file.
//!
//! Strategy choice (spec D7): we use a deterministic "pre-staged
//! partial" approach instead of the original Strategy C (SIGKILL the
//! sender mid-stream). Reason: the SSH-mediated palimpsest test
//! runner makes Strategy C non-deterministic — depending on host
//! load and ssh latency, the SIGKILL window either lands during
//! daemon startup (no partial), during a flushed send (no partial),
//! or after the recv has already completed (no partial). We
//! validated on the VM that even with a 64 MiB blob and a 4 s delay
//! the timing is unreliable.
//!
//! The pre-staged-partial approach: bypass the daemon for the
//! initial partial recv. Use the SSH runner to pipe `zfs send | head
//! -c <small> | zfs recv -s` directly inside the VM, producing a
//! genuine partial recv with `receive_resume_token` set. Then start
//! the arctern daemons and verify:
//!
//! - quic_resume_picks_up_existing_partial: planner sees the token,
//!   validates it (to_guid matches a sender snapshot), emits Resume,
//!   executor sends `zfs send -t <token>`, recv completes, snapshot
//!   GUID matches.
//!
//! - quic_stale_token_triggers_discard: same partial setup but the
//!   sender's source snapshot is destroyed and a new snapshot takes
//!   its place. Planner sees a token whose to_guid is no longer on
//!   the sender, falls through to a Full plan with
//!   `discard_partial_recv = true`. Sink runs `zfs recv -A` and
//!   accepts the new full stream.
//!
//! Both tests exercise the wire/planner/sink machinery. The drop-on-
//! cancel path that Strategy C would have validated is covered by
//! palimpsest's own runner tests (kill_on_drop) plus the slice-005
//! push test's SIGTERM flow (FR-013).

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
async fn quic_resume_picks_up_existing_partial() {
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
            .expect("zfs create");
        assert!(out.status.success(), "zfs create {ds} failed: {out:?}");
    }

    // Mount source under altroot so dd can write a urandom blob.
    let altroot_mount = format!("/tmp/{}_root/data", sender_pool.name());
    let blob = format!("{altroot_mount}/blob");
    let out = runner
        .run(Cmd::new("zfs").args(["set", "mountpoint=/data", &source]))
        .await
        .expect("zfs set mountpoint");
    assert!(out.status.success(), "zfs set mountpoint failed: {out:?}");
    let out = runner
        .run(Cmd::new("dd").args([
            "if=/dev/urandom",
            &format!("of={blob}"),
            "bs=1M",
            "count=8",
            "status=none",
        ]))
        .await
        .expect("dd urandom");
    assert!(out.status.success(), "dd urandom failed: {out:?}");
    let snap1 = format!("{source}@test_001");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap1]))
        .await
        .expect("zfs snapshot test_001");
    assert!(out.status.success(), "snapshot {snap1} failed: {out:?}");

    let target_dataset = format!("{sink_root}/{source}");

    // ─── Pre-stage a partial recv (the source of truth for slice 006) ─
    //
    // bash -c forces the shell pipeline to live inside ONE remote SSH
    // command so head -c '|'-tees the byte stream and zfs recv -s
    // sees EOF mid-stream. recv only creates the leaf dataset; we
    // pre-create the parent path with mountpoint=none so the partial
    // can land at <sink_root>/<sender_pool>/data.
    let parent = target_dataset
        .rsplit_once('/')
        .map(|(p, _)| p.to_string())
        .expect("target has a parent");
    let stage = format!(
        "zfs create -p -o mountpoint=none {parent} && \
         zfs send {snap1} | head -c 524288 | zfs recv -s {target_dataset}; \
         zfs get -H -o value receive_resume_token {target_dataset}"
    );
    let out = runner
        .run(Cmd::new("bash").args(["-c", &stage]))
        .await
        .expect("stage partial recv");
    let stdout_text = String::from_utf8_lossy(&out.stdout);
    let token_line = stdout_text.lines().last().unwrap_or("").trim();
    assert!(
        !token_line.is_empty() && token_line != "-",
        "expected partial recv to advertise a token; got stdout={:?} stderr={:?}",
        stdout_text,
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!(
        "pre-stage: receiver advertises token: {}",
        &token_line[..token_line.len().min(48)]
    );

    // ─── Sink + sender daemons (slice 005 spawn shape) ───────────
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
    let sink_addr = quic_addrs[0];

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
    let (mut sender_child, sender_sock_actual) = spawn_daemon_uds_with_config(
        Some(sender_sock.clone()),
        Some(sender_cfg_path.clone()),
    );

    // ─── Trigger the cycle ────────────────────────────────────────
    wakeup_via_uds(&sender_sock_actual, "push").await;
    let cycle_res =
        wait_for_snapshot_count(&runner, &target_dataset, 1, Duration::from_secs(120)).await;

    // Capture state BEFORE teardown so failure messages are debuggable.
    let recv_after = if cycle_res.is_ok() {
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
    cleanup_files(&[
        &sink_cfg_path,
        &sender_cfg_path,
        &sink_sock_actual,
        &sender_sock_actual,
    ]);
    let _ = std::fs::remove_dir_all(&sink_state);
    let _ = std::fs::remove_dir_all(&sender_state);
    let _ = receiver_pool.destroy().await;
    let _ = sender_pool.destroy().await;

    if let Err(e) = cycle_res {
        panic!("resume cycle: {e}");
    }
    let recv_after = recv_after.expect("expected receiver snapshots");
    assert_eq!(recv_after.len(), 1, "expected one snapshot after resume");
    assert_eq!(
        recv_after[0].snapshot_name.as_deref(),
        Some("test_001"),
        "receiver snapshot name mismatch"
    );
    assert_eq!(
        recv_token_after, None,
        "receive_resume_token should be cleared once the resume completes"
    );
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
            .expect("zfs create");
        assert!(out.status.success(), "zfs create {ds} failed: {out:?}");
    }
    let altroot_mount = format!("/tmp/{}_root/data", sender_pool.name());
    let blob = format!("{altroot_mount}/blob");
    let out = runner
        .run(Cmd::new("zfs").args(["set", "mountpoint=/data", &source]))
        .await
        .expect("zfs set mountpoint");
    assert!(out.status.success(), "zfs set mountpoint failed: {out:?}");
    let out = runner
        .run(Cmd::new("dd").args([
            "if=/dev/urandom",
            &format!("of={blob}"),
            "bs=1M",
            "count=8",
            "status=none",
        ]))
        .await
        .expect("dd urandom");
    assert!(out.status.success(), "dd urandom failed: {out:?}");
    let snap_a = format!("{source}@test_001");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap_a]))
        .await
        .expect("snapshot test_001");
    assert!(out.status.success(), "snapshot snap_a failed: {out:?}");

    let target_dataset = format!("{sink_root}/{source}");

    // Pre-stage a partial recv tied to test_001's GUID.
    let parent = target_dataset
        .rsplit_once('/')
        .map(|(p, _)| p.to_string())
        .expect("target has a parent");
    let stage = format!(
        "zfs create -p -o mountpoint=none {parent} && \
         zfs send {snap_a} | head -c 524288 | zfs recv -s {target_dataset}; \
         zfs get -H -o value receive_resume_token {target_dataset}"
    );
    let out = runner
        .run(Cmd::new("bash").args(["-c", &stage]))
        .await
        .expect("stage partial");
    let stdout_text = String::from_utf8_lossy(&out.stdout);
    let token_line = stdout_text.lines().last().unwrap_or("").trim();
    assert!(
        !token_line.is_empty() && token_line != "-",
        "expected partial recv to advertise a token"
    );

    // Invalidate the token: destroy snap_a, snapshot a fresh test_002.
    // The pre-staged partial's to_guid is now gone from the sender →
    // planner picks Full + discard.
    let out = runner
        .run(Cmd::new("zfs").args(["destroy", &snap_a]))
        .await
        .expect("destroy snap_a");
    assert!(out.status.success(), "destroy snap_a failed: {out:?}");
    let snap_b = format!("{source}@test_002");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap_b]))
        .await
        .expect("snapshot test_002");
    assert!(out.status.success(), "snapshot snap_b failed: {out:?}");

    // ─── Daemons ──────────────────────────────────────────────────
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
    let (mut sender_child, sender_sock_actual) = spawn_daemon_uds_with_config(
        Some(sender_sock.clone()),
        Some(sender_cfg_path.clone()),
    );

    wakeup_via_uds(&sender_sock_actual, "push").await;
    let cycle_res =
        wait_for_snapshot_count(&runner, &target_dataset, 1, Duration::from_secs(120)).await;
    let recv_after = if cycle_res.is_ok() {
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
    cleanup_files(&[
        &sink_cfg_path,
        &sender_cfg_path,
        &sink_sock_actual,
        &sender_sock_actual,
    ]);
    let _ = std::fs::remove_dir_all(&sink_state);
    let _ = std::fs::remove_dir_all(&sender_state);
    let _ = receiver_pool.destroy().await;
    let _ = sender_pool.destroy().await;

    if let Err(e) = cycle_res {
        panic!("stale-token cycle: {e}");
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

fn cleanup_files(paths: &[&Path]) {
    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}
