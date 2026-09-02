//! The two recovery paths the planner depends on, against real ZFS.
//!
//! `integration_ssh_push` covers the happy line: full, then incremental
//! from the previous snapshot. The paths that actually run in a
//! long-lived deployment are the other two, and neither was exercised:
//!
//!   - **bookmark fallback** — retention prunes the sender's copy of the
//!     snapshot the receiver holds, so the incremental base has to come
//!     from the cursor bookmark instead. This is the ordinary case
//!     between syncs, not an edge case.
//!   - **resume** — a receive interrupted partway leaves a
//!     `receive_resume_token`, and the next attempt must continue from it
//!     rather than start over.
//!
//! These assert what ZFS does, which is what the planner's unit tests
//! assume: that a bookmark is a usable incremental base after its
//! snapshot is gone, and that a resumed stream lands the same snapshot.

#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zfskit::dataset::{CreateOptions, ListOptions, SnapshotOptions};
use zfskit::models::DatasetType;
use zfskit::recv::{RecvArgs, recv as zfs_recv};
use zfskit::runner::CommandRunner;
use zfskit::send::{SendArgs, send as zfs_send};

use common::{LoopbackPool, ssh_runner_from_env};

async fn snapshot_guid(runner: &dyn CommandRunner, full_snap: &str) -> Option<u64> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![full_snap.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(runner, &opts).await.ok()?;
    entries
        .into_iter()
        .next()?
        .properties
        .get("guid")
        .and_then(|p| p.value.parse::<u64>().ok())
}

async fn pipe(runner: &dyn CommandRunner, send_args: SendArgs, recv_args: RecvArgs) {
    let mut send_child = zfs_send(runner, &send_args).await.expect("spawn zfs send");
    let mut recv_child = zfs_recv(runner, &recv_args).await.expect("spawn zfs recv");
    let mut out = send_child.take_stdout().expect("send stdout");
    let mut input = recv_child.take_stdin().expect("recv stdin");
    tokio::io::copy(&mut out, &mut input)
        .await
        .expect("copy send -> recv");
    input.shutdown().await.expect("shutdown recv stdin");
    drop(input);
    send_child.finish().await.expect("send finish");
    recv_child.finish().await.expect("recv finish");
}

/// A sender dataset holding roughly `mib` of incompressible data, so a
/// send stream is large enough to interrupt partway.
///
/// The test pool is created with an `altroot`, so the dataset's
/// `mountpoint` property is not where it actually lands — ask the system
/// instead of computing it.
async fn fill(runner: &dyn CommandRunner, dataset: &str, mib: usize) {
    zfskit::dataset::set_property(runner, dataset, "mountpoint", "/blobs")
        .await
        .expect("set mountpoint");
    zfskit::dataset::mount(runner, dataset, &Default::default())
        .await
        .expect("mount");

    let found = runner
        .run(zfskit::runner::Cmd::new("findmnt").args(["-n", "-o", "TARGET", "-S", dataset]))
        .await
        .expect("findmnt");
    assert!(found.status.success(), "findmnt could not locate {dataset}");
    let mountpoint = String::from_utf8_lossy(&found.stdout).trim().to_string();
    assert!(!mountpoint.is_empty(), "{dataset} is not mounted");

    let out = runner
        .run(zfskit::runner::Cmd::new("dd").args([
            "if=/dev/urandom",
            &format!("of={mountpoint}/blob"),
            "bs=1M",
            &format!("count={mib}"),
        ]))
        .await
        .expect("dd");
    assert!(
        out.status.success(),
        "dd into {mountpoint} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _ = runner.run(zfskit::runner::Cmd::new("sync")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_send_works_from_a_bookmark_after_its_snapshot_is_pruned() {
    let runner = ssh_runner_from_env();
    let r: &dyn CommandRunner = &runner;
    let sender = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("sender pool");
    let receiver = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("receiver pool");

    let src = format!("{}/data", sender.name());
    let dst_root = format!("{}/backups", receiver.name());
    let dst = format!("{dst_root}/data");
    let opts = CreateOptions::new()
        .create_parents()
        .property("mountpoint", "none");
    zfskit::dataset::create(r, &src, &opts).await.expect("src");
    zfskit::dataset::create(r, &dst_root, &opts)
        .await
        .expect("dst root");

    // s1: full send, then a cursor bookmark, as a successful step leaves it.
    let s1 = format!("{src}@s1");
    zfskit::dataset::snapshot(r, &s1, &SnapshotOptions::new())
        .await
        .expect("s1");
    pipe(
        r,
        SendArgs::new(s1.clone()),
        RecvArgs::new(dst.clone()).resumable().unmounted(),
    )
    .await;
    let cursor = format!("{src}#arctern_cursor_test");
    zfskit::bookmark::create(r, &s1, &cursor)
        .await
        .expect("cursor bookmark");

    // s2 arrives, then retention destroys the sender's copy of s1 — the
    // snapshot the receiver still holds. Only the bookmark remains.
    let s2 = format!("{src}@s2");
    zfskit::dataset::snapshot(r, &s2, &SnapshotOptions::new())
        .await
        .expect("s2");
    let s2_guid = snapshot_guid(r, &s2).await.expect("s2 guid");
    zfskit::dataset::destroy(r, &s1, &Default::default())
        .await
        .expect("prune s1 on the sender");
    assert!(
        snapshot_guid(r, &s1).await.is_none(),
        "s1 must be gone from the sender"
    );

    // The bookmark carries the base. This is the plan `apply_bookmark_fallback`
    // produces, and the assertion is that ZFS accepts it.
    pipe(
        r,
        SendArgs::new(s2.clone()).incremental(cursor.clone()),
        RecvArgs::new(dst.clone()).resumable().unmounted(),
    )
    .await;

    let landed = snapshot_guid(r, &format!("{dst}@s2"))
        .await
        .expect("s2 must land on the receiver");
    assert_eq!(landed, s2_guid, "GUID must survive a bookmark-based send");

    sender.destroy().await.ok();
    receiver.destroy().await.ok();
}

/// The planner refuses a full send at a receiver that already holds
/// snapshots, rather than emitting it and letting `zfs recv` fail every
/// cycle. That refusal is only correct if ZFS really does refuse — this
/// pins the assumption, so the planner is not declining something that
/// would have worked.
#[tokio::test(flavor = "multi_thread")]
async fn zfs_refuses_a_full_send_onto_a_receiver_that_has_snapshots() {
    let runner = ssh_runner_from_env();
    let r: &dyn CommandRunner = &runner;
    let sender = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("sender pool");
    let receiver = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("receiver pool");

    let src = format!("{}/data", sender.name());
    let dst_root = format!("{}/backups", receiver.name());
    let dst = format!("{dst_root}/data");
    let opts = CreateOptions::new()
        .create_parents()
        .property("mountpoint", "none");
    zfskit::dataset::create(r, &src, &opts).await.expect("src");
    zfskit::dataset::create(r, &dst_root, &opts)
        .await
        .expect("dst root");

    // Give the receiver a history of its own, sharing no GUID with the
    // sender — two independently created datasets, as after a restore
    // from elsewhere or a destroyed cursor.
    zfskit::dataset::create(r, &dst, &opts).await.expect("dst");
    zfskit::dataset::snapshot(r, &format!("{dst}@theirs"), &SnapshotOptions::new())
        .await
        .expect("receiver snapshot");

    let s1 = format!("{src}@s1");
    zfskit::dataset::snapshot(r, &s1, &SnapshotOptions::new())
        .await
        .expect("s1");

    // No `-F`, matching what arctern emits.
    let mut send_child = zfs_send(r, &SendArgs::new(s1.clone()))
        .await
        .expect("spawn send");
    let mut recv_child = zfs_recv(r, &RecvArgs::new(dst.clone()).resumable().unmounted())
        .await
        .expect("spawn recv");
    let mut out = send_child.take_stdout().expect("send stdout");
    let mut input = recv_child.take_stdin().expect("recv stdin");
    let _ = tokio::io::copy(&mut out, &mut input).await;
    let _ = input.shutdown().await;
    drop(input);
    let _ = send_child.cancel().await;
    let recv_result = recv_child.finish().await;

    assert!(
        recv_result.is_err(),
        "zfs recv accepted a full stream onto a populated dataset — the planner's refusal would \
         be blocking something that works"
    );
    assert!(
        snapshot_guid(r, &format!("{dst}@s1")).await.is_none(),
        "nothing from the refused stream may land"
    );

    sender.destroy().await.ok();
    receiver.destroy().await.ok();
}

/// A rejecting `zfs recv` exits and closes its stdin, so the sender's
/// copy dies with EPIPE. The reason lives in the child's stderr, and it
/// is worth confirming ZFS actually puts something usable there — the
/// recv handler now reaps the child for that message instead of killing
/// it and reporting "Broken pipe".
#[tokio::test(flavor = "multi_thread")]
async fn a_rejecting_receiver_explains_itself_on_stderr() {
    let runner = ssh_runner_from_env();
    let r: &dyn CommandRunner = &runner;
    let sender = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("sender pool");
    let receiver = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("receiver pool");

    let src = format!("{}/data", sender.name());
    let dst_root = format!("{}/backups", receiver.name());
    let dst = format!("{dst_root}/data");
    let opts = CreateOptions::new()
        .create_parents()
        .property("mountpoint", "none");
    zfskit::dataset::create(r, &src, &opts).await.expect("src");
    zfskit::dataset::create(r, &dst_root, &opts)
        .await
        .expect("dst root");
    zfskit::dataset::create(r, &dst, &opts).await.expect("dst");
    zfskit::dataset::snapshot(r, &format!("{dst}@theirs"), &SnapshotOptions::new())
        .await
        .expect("receiver snapshot");

    let s1 = format!("{src}@s1");
    zfskit::dataset::snapshot(r, &s1, &SnapshotOptions::new())
        .await
        .expect("s1");

    let mut send_child = zfs_send(r, &SendArgs::new(s1.clone()))
        .await
        .expect("spawn send");
    let mut recv_child = zfs_recv(r, &RecvArgs::new(dst.clone()).resumable().unmounted())
        .await
        .expect("spawn recv");
    let mut out = send_child.take_stdout().expect("send stdout");
    let mut input = recv_child.take_stdin().expect("recv stdin");
    let copy = tokio::io::copy(&mut out, &mut input).await;
    let _ = input.shutdown().await;
    drop(input);
    let _ = send_child.cancel().await;

    // Reaping is what surfaces the reason; killing here is what used to
    // discard it.
    let err = recv_child
        .finish()
        .await
        .expect_err("the receiver must refuse this stream");
    let message = err.to_string();
    assert!(
        message.len() > "Broken pipe".len() && !message.contains("Broken pipe"),
        "stderr carried nothing better than the pipe error: {message:?} (copy: {copy:?})"
    );

    sender.destroy().await.ok();
    receiver.destroy().await.ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_receive_resumes_from_its_token() {
    let runner = ssh_runner_from_env();
    let r: &dyn CommandRunner = &runner;
    let sender = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("sender pool");
    let receiver = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("receiver pool");

    let src = format!("{}/data", sender.name());
    let dst_root = format!("{}/backups", receiver.name());
    let dst = format!("{dst_root}/data");
    let opts = CreateOptions::new()
        .create_parents()
        .property("mountpoint", "none");
    zfskit::dataset::create(r, &src, &opts).await.expect("src");
    zfskit::dataset::create(r, &dst_root, &opts)
        .await
        .expect("dst root");

    fill(r, &src, 48).await;
    let s1 = format!("{src}@s1");
    zfskit::dataset::snapshot(r, &s1, &SnapshotOptions::new())
        .await
        .expect("s1");
    let s1_guid = snapshot_guid(r, &s1).await.expect("s1 guid");

    // Feed the receive a prefix of the stream and then close the pipe,
    // which is what a dropped link looks like from the receiver's side.
    {
        let mut send_child = zfs_send(r, &SendArgs::new(s1.clone()))
            .await
            .expect("spawn send");
        let mut recv_child = zfs_recv(r, &RecvArgs::new(dst.clone()).resumable().unmounted())
            .await
            .expect("spawn recv");
        let mut out = send_child.take_stdout().expect("send stdout");
        let mut input = recv_child.take_stdin().expect("recv stdin");
        let mut buf = vec![0u8; 1 << 20];
        let mut copied = 0usize;
        while copied < 8 << 20 {
            let n = out.read(&mut buf).await.expect("read send stdout");
            if n == 0 {
                break;
            }
            input.write_all(&buf[..n]).await.expect("write recv stdin");
            copied += n;
        }
        drop(input);
        let _ = send_child.cancel().await;
        // A truncated stream is an error for the receiver; `-s` is what
        // turns that into resumable state rather than nothing.
        let _ = recv_child.finish().await;
    }

    let token = zfskit::recv::receive_resume_token(r, &dst)
        .await
        .expect("query resume token")
        .expect("an interrupted resumable receive must leave a token");

    assert!(
        snapshot_guid(r, &format!("{dst}@s1")).await.is_none(),
        "the snapshot must not exist before the resume completes"
    );

    // Exactly what the planner emits for SnapshotPlan::Resume.
    pipe(
        r,
        SendArgs::resume(token),
        RecvArgs::new(dst.clone()).resumable().unmounted(),
    )
    .await;

    let landed = snapshot_guid(r, &format!("{dst}@s1"))
        .await
        .expect("s1 must land after the resume");
    assert_eq!(landed, s1_guid, "a resumed stream must land the same GUID");
    assert!(
        zfskit::recv::receive_resume_token(r, &dst)
            .await
            .expect("query resume token")
            .is_none(),
        "the token must be cleared once the receive completes"
    );

    sender.destroy().await.ok();
    receiver.destroy().await.ok();
}
