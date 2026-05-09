//! End-to-end test for the QUIC sink job.
//!
//! Boots a fresh loopback pool inside the VM as the receiver, creates a
//! sibling source dataset + snapshot in the same VM, captures the
//! corresponding `zfs send` byte stream via SSH, then spawns the daemon
//! with a sink-job config and drives one receive over QUIC using a raw
//! `quinn` client. Asserts the response is `Ok` and the receiver pool
//! gained the named dataset + snapshot.
//!
//! No `arctern client` CLI verb yet; the slice-005 push job will land
//! that. This test exercises the framing + identity + sink runtime.

#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use arctern_transport::{
    PROTOCOL_VERSION, ReceiveHeader, ReceiveResponse, client_config_accept_any, read_response,
    write_header,
};
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::{Cmd, CommandRunner};

use common::{LoopbackPool, spawn_daemon_uds_with_quic, ssh_runner_from_env, unique_suffix};

#[tokio::test(flavor = "multi_thread")]
async fn quic_sink_receives_send_stream() {
    let pool = LoopbackPool::create(ssh_runner_from_env())
        .await
        .expect("create pool");
    let runner = ssh_runner_from_env();

    // Sink root and source dataset live as siblings in the same pool;
    // both are mountpoint=none so the VM filesystem stays untouched.
    let sink_root = format!("{}/sink_root", pool.name());
    let source = format!("{}/source_data", pool.name());
    for ds in [&sink_root, &source] {
        let out = runner
            .run(Cmd::new("zfs").args(["create", "-o", "mountpoint=none", ds]))
            .await
            .expect("zfs create runs");
        assert!(out.status.success(), "zfs create {ds} failed: {out:?}");
    }
    let snap = format!("{source}@s1");
    let out = runner
        .run(Cmd::new("zfs").args(["snapshot", &snap]))
        .await
        .expect("zfs snapshot runs");
    assert!(out.status.success(), "zfs snapshot failed: {out:?}");

    // Capture `zfs send <source>@s1` bytes. Empty-dataset snapshot is
    // a few hundred bytes; the wire path is what matters.
    let out = runner
        .run(Cmd::new("zfs").args(["send", &snap]))
        .await
        .expect("zfs send runs");
    assert!(out.status.success(), "zfs send failed: {out:?}");
    let send_bytes = out.stdout;
    assert!(
        !send_bytes.is_empty(),
        "captured send stream should not be empty"
    );

    // Build a sink-job config pointing at the receiver and binding to
    // a random port (we'll discover it from the LISTEN_QUIC handshake).
    let state_dir = format!("/tmp/arctern_test_state_{}", unique_suffix());
    let cfg_path = PathBuf::from(format!("/tmp/arctern_test_sink_{}.toml", unique_suffix()));
    let cfg = format!(
        r#"
state_dir = "{state_dir}"
[[jobs]]
type = "sink"
name = "sink_test"
listen = "127.0.0.1:0"
root_fs = "{sink_root}"
"#
    );
    std::fs::write(&cfg_path, cfg).expect("write config");

    let (mut child, sock, quic_addrs) =
        spawn_daemon_uds_with_quic(None, Some(cfg_path.clone()), 1);
    assert_eq!(quic_addrs.len(), 1, "expected one LISTEN_QUIC line");
    let server_addr = quic_addrs[0];

    // Build a quinn client with the accept-any verifier and dial.
    let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 0).into();
    let mut endpoint = quinn::Endpoint::client(client_addr).expect("client endpoint");
    endpoint.set_default_client_config(client_config_accept_any().expect("client cfg"));

    let target_dataset = format!("{sink_root}/recv_target");
    let recv_outcome = drive_one_stream(
        &endpoint,
        server_addr,
        ReceiveHeader {
            version: PROTOCOL_VERSION,
            target_dataset: target_dataset.clone(),
            send_flags: None,
        },
        send_bytes,
    )
    .await;

    // Tear down before assertions so a failure leaves no zombie daemon.
    endpoint.close(0u32.into(), b"test done");
    endpoint.wait_idle().await;

    let response = recv_outcome.expect("stream completed");
    assert!(
        matches!(response, ReceiveResponse::Ok),
        "expected Ok, got: {response:?}"
    );

    // Give the receiver a brief moment after FIN before listing —
    // wait() is synchronous in the sink, so by the time we read Ok,
    // the dataset has been created. Still allow 250 ms slack.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let snaps = palimpsest::dataset::list(
        &runner,
        &ListOptions {
            recursive: false,
            types: vec![DatasetType::Snapshot],
            roots: vec![target_dataset.clone()],
            ..ListOptions::default()
        },
    )
    .await
    .expect("list snapshots on receiver");

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&sock);
    let _ = std::fs::remove_file(&cfg_path);
    let _ = std::fs::remove_dir_all(&state_dir);

    assert!(
        !snaps.is_empty(),
        "expected at least one snapshot on {target_dataset}, got: {snaps:?}"
    );

    pool.destroy().await.expect("destroy pool");
}

async fn drive_one_stream(
    endpoint: &quinn::Endpoint,
    server_addr: SocketAddr,
    header: ReceiveHeader,
    payload: Vec<u8>,
) -> Result<ReceiveResponse, String> {
    let connecting = endpoint
        .connect(server_addr, "arctern")
        .map_err(|e| format!("connect: {e}"))?;
    let conn = connecting.await.map_err(|e| format!("handshake: {e}"))?;
    let (mut send, mut recv) = conn.open_bi().await.map_err(|e| format!("open_bi: {e}"))?;
    write_header(&mut send, &header)
        .await
        .map_err(|e| format!("write_header: {e}"))?;
    send.write_all(&payload)
        .await
        .map_err(|e| format!("write payload: {e}"))?;
    send.finish().map_err(|e| format!("finish: {e}"))?;
    let resp = read_response(&mut recv)
        .await
        .map_err(|e| format!("read_response: {e}"))?;
    Ok(resp)
}
