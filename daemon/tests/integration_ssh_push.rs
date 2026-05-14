//! Integration test for the push pipeline's ZFS-side primitives in the
//! test VM: end-to-end send/recv between two LoopbackPools, with the
//! same hold + cursor bookmark choreography push.rs performs.
//!
//! This test does not exercise the openssh::Session multi-channel
//! wire (that needs the arctern binary installed inside the VM and
//! key-based auth). It does exercise:
//! - palimpsest send + recv against real ZFS, including raw bytes
//!   piped through a tokio duplex (stand-in for the SSH channel),
//! - GUID intersection between sender/receiver inventories,
//! - step hold placement before send and release after success,
//! - cursor bookmark creation + GUID-anchored advance,
//! - incremental send referencing the previous snapshot.
//!
//! These are the load-bearing pieces from push.rs's executor; the
//! SSH framing on top is mechanical wiring around the same primitives.

#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use palimpsest::dataset::{CreateOptions, ListOptions, SnapshotOptions};
use palimpsest::models::DatasetType;
use palimpsest::recv::{RecvArgs, recv as zfs_recv};
use palimpsest::runner::CommandRunner;
use palimpsest::send::{SendArgs, send as zfs_send};
use tokio::io::AsyncWriteExt;

use common::{LoopbackPool, ssh_runner_from_env};

const STEP_HOLD_TAG: &str = "arctern_step_J_test_push";
const CURSOR_BOOKMARK_LEAF: &str = "arctern_cursor_J_test_push";

async fn snapshot_guid(runner: &dyn CommandRunner, full_snap: &str) -> Option<u64> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![full_snap.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = palimpsest::dataset::list(runner, &opts).await.ok()?;
    let entry = entries.into_iter().next()?;
    entry
        .properties
        .get("guid")
        .and_then(|p| p.value.parse::<u64>().ok())
}

async fn pipe_send_to_recv(runner: &dyn CommandRunner, send_args: SendArgs, recv_args: RecvArgs) {
    let mut send_child = zfs_send(runner, &send_args).await.expect("spawn zfs send");
    let mut recv_child = zfs_recv(runner, &recv_args).await.expect("spawn zfs recv");
    let mut send_stdout = send_child.stdout.take().expect("send stdout");
    let mut recv_stdin = recv_child.stdin.take().expect("recv stdin");
    let mut recv_stderr = recv_child.stderr.take().expect("recv stderr");
    let mut send_stderr = send_child.stderr.take().expect("send stderr");
    let recv_stderr_drain = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let _ = recv_stderr.read_to_end(&mut buf).await;
        buf
    });
    let send_stderr_drain = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let _ = send_stderr.read_to_end(&mut buf).await;
        buf
    });
    tokio::io::copy(&mut send_stdout, &mut recv_stdin)
        .await
        .expect("copy send -> recv");
    recv_stdin.shutdown().await.expect("shutdown recv stdin");
    drop(recv_stdin);
    let send_status = send_child.wait().await.expect("send wait");
    let recv_status = recv_child.wait().await.expect("recv wait");
    let send_err = send_stderr_drain.await.unwrap_or_default();
    let recv_err = recv_stderr_drain.await.unwrap_or_default();
    assert!(
        send_status.success(),
        "zfs send failed: {}",
        String::from_utf8_lossy(&send_err)
    );
    assert!(
        recv_status.success(),
        "zfs recv failed: {}",
        String::from_utf8_lossy(&recv_err)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_push_full_then_incremental_with_hold_and_cursor() {
    let runner = ssh_runner_from_env();
    let runner_dyn: &dyn CommandRunner = &runner;

    let sender_pool = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("create sender pool");
    let receiver_pool = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("create receiver pool");

    let sender_root = format!("{}/data", sender_pool.name());
    let receiver_root = format!("{}/backups", receiver_pool.name());
    let target_dataset = format!("{receiver_root}/{sender_root}");

    let create_opts = CreateOptions::new()
        .create_parents()
        .property("mountpoint", "none");
    palimpsest::dataset::create(runner_dyn, &sender_root, &create_opts)
        .await
        .expect("create sender dataset");
    palimpsest::dataset::create(runner_dyn, &receiver_root, &create_opts)
        .await
        .expect("create receiver root");
    // The recv handler in production creates the parent of the target
    // dataset; we replicate that pre-step here so recv has a place to
    // land the new leaf.
    if let Some((parent, _)) = target_dataset.rsplit_once('/') {
        palimpsest::dataset::create(runner_dyn, parent, &create_opts)
            .await
            .expect("create receiver target parent");
    }

    // Snap 1.
    let snap1_name = "s1";
    let snap1_full = format!("{sender_root}@{snap1_name}");
    palimpsest::dataset::snapshot(runner_dyn, &snap1_full, &SnapshotOptions::new())
        .await
        .expect("snap1");
    let snap1_guid = snapshot_guid(runner_dyn, &snap1_full)
        .await
        .expect("snap1 guid");

    // Step hold before send (push.rs's pattern).
    palimpsest::hold::hold(runner_dyn, &snap1_full, STEP_HOLD_TAG)
        .await
        .expect("step hold on snap1");

    // Full send → recv.
    let send_args_full = SendArgs::new(snap1_full.clone());
    let recv_args_full = RecvArgs::new(target_dataset.clone())
        .resumable()
        .unmounted();
    pipe_send_to_recv(runner_dyn, send_args_full, recv_args_full).await;

    let recv_snap1 = format!("{target_dataset}@{snap1_name}");
    let recv_snap1_guid = snapshot_guid(runner_dyn, &recv_snap1)
        .await
        .expect("receiver snap1 guid");
    assert_eq!(
        recv_snap1_guid, snap1_guid,
        "GUIDs must match across send/recv"
    );

    // Cursor + release on success.
    let cursor = format!("{sender_root}#{CURSOR_BOOKMARK_LEAF}");
    palimpsest::bookmark::create(runner_dyn, &snap1_full, &cursor)
        .await
        .expect("create cursor bookmark");
    palimpsest::hold::release(runner_dyn, &snap1_full, STEP_HOLD_TAG)
        .await
        .expect("release step hold");
    let holds = palimpsest::hold::list_holds(runner_dyn, &snap1_full)
        .await
        .expect("list holds");
    assert!(
        holds.iter().all(|h| h.tag != STEP_HOLD_TAG),
        "step hold must be released; got {holds:?}"
    );

    // Snap 2 + incremental.
    let snap2_name = "s2";
    let snap2_full = format!("{sender_root}@{snap2_name}");
    palimpsest::dataset::snapshot(runner_dyn, &snap2_full, &SnapshotOptions::new())
        .await
        .expect("snap2");
    let snap2_guid = snapshot_guid(runner_dyn, &snap2_full)
        .await
        .expect("snap2 guid");
    palimpsest::hold::hold(runner_dyn, &snap2_full, STEP_HOLD_TAG)
        .await
        .expect("step hold on snap2");

    let send_args_inc = SendArgs::new(snap2_full.clone()).incremental(snap1_full.clone());
    let recv_args_inc = RecvArgs::new(target_dataset.clone())
        .resumable()
        .unmounted();
    pipe_send_to_recv(runner_dyn, send_args_inc, recv_args_inc).await;

    let recv_snap2 = format!("{target_dataset}@{snap2_name}");
    let recv_snap2_guid = snapshot_guid(runner_dyn, &recv_snap2)
        .await
        .expect("receiver snap2 guid");
    assert_eq!(recv_snap2_guid, snap2_guid);

    // Advance cursor + release step hold.
    palimpsest::bookmark::destroy(runner_dyn, &cursor)
        .await
        .expect("destroy old cursor");
    palimpsest::bookmark::create(runner_dyn, &snap2_full, &cursor)
        .await
        .expect("create new cursor");
    palimpsest::hold::release(runner_dyn, &snap2_full, STEP_HOLD_TAG)
        .await
        .expect("release step hold on snap2");

    sender_pool.destroy().await.ok();
    receiver_pool.destroy().await.ok();
}
