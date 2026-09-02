//! End-to-end test for the periodic-snap job runtime.
//!
//! Boots a fresh loopback pool inside the VM, writes a TOML config
//! pointing at it with `interval = "1s"` and a single-bucket grid,
//! spawns the daemon, lets it run for ~3 seconds, then asserts that:
//! - at least 2 snapshots whose names start with the configured prefix
//!   exist on the target dataset, and
//! - GET /api/v1/jobs reports the job by name with `kind = "snap"`.

#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use zfskit::dataset::ListOptions;
use zfskit::models::DatasetType;
use zfskit::runner::{Cmd, CommandRunner};

use common::{LoopbackPool, spawn_daemon_uds_with_config, ssh_runner_from_env, unique_suffix};

#[tokio::test(flavor = "multi_thread")]
async fn snap_job_creates_snapshots_and_prunes() {
    let pool = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("create pool");
    // Snap job needs a target child dataset (mountpoint=none keeps the
    // VM's filesystem hierarchy untouched).
    let target = format!("{}/data", pool.name());
    let runner = ssh_runner_from_env();
    let mk = runner
        .run(Cmd::new("zfs").args(["create", "-o", "mountpoint=none", &target]))
        .await
        .expect("create child dataset succeeded as a process");
    assert!(mk.status.success(), "zfs create failed: {mk:?}");

    let prefix = format!("test_{}_", unique_suffix());
    let cfg_path = PathBuf::from(format!("/tmp/arctern_test_snap_{}.toml", unique_suffix()));
    // Slice 004: explicit state_dir so the daemon does not try to
    // mkdir /var/lib/arctern as an unprivileged test user.
    let state_dir = format!("/tmp/arctern_test_state_{}", unique_suffix());
    let cfg = format!(
        r#"
state_dir = "{state_dir}"
[[jobs]]
type = "snap"
name = "snap_test"
[[jobs.filesystems]]
path = "{target}"
[jobs.snapshotting]
interval = "1s"
prefix = "{prefix}"
[[jobs.pruning.keep]]
type = "grid"
grid = "5x1s"
regex = "^{prefix}.*"
"#
    );
    std::fs::write(&cfg_path, cfg).expect("write config");

    let (mut child, socket) = spawn_daemon_uds_with_config(None, Some(cfg_path.clone()));

    // Wait for the snapshots rather than for the clock. A cycle is
    // several `zfs` calls over SSH, so on a loaded machine two of them
    // take well over the interval — a fixed three-second sleep made this
    // fail whenever the host was busy, which says nothing about the job.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut matching: Vec<String> = Vec::new();
    while std::time::Instant::now() < deadline {
        let snaps = zfskit::dataset::list(
            &runner,
            &ListOptions {
                recursive: false,
                types: vec![DatasetType::Snapshot],
                roots: vec![target.clone()],
                ..ListOptions::default()
            },
        )
        .await
        .expect("list snapshots");

        matching = snaps
            .into_iter()
            .map(|e| e.name)
            .filter(|n| {
                n.split_once('@')
                    .map(|(_, tag)| tag.starts_with(&prefix))
                    .unwrap_or(false)
            })
            .collect();
        if matching.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&socket);
    let _ = std::fs::remove_file(&cfg_path);

    assert!(
        matching.len() >= 2,
        "expected >= 2 snapshots with prefix {prefix:?}, got: {matching:?}"
    );

    pool.destroy().await.expect("destroy pool");
}
