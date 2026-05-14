#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use std::path::PathBuf;
use std::process::Command;

use arctern_transport::{Request, RequestFrame, Response, read_response, write_request};
use openssh::{KnownHosts, SessionBuilder, Stdio};
use tokio::io::AsyncWriteExt;

use common::{
    LoopbackPool, openssh_integration_enabled, run_local_command, run_remote_shell, scp_to_remote,
    ssh_target_from_env, unique_suffix,
};

fn shell_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn forced_command_control_channel_enforces_acl_and_fingerprint() {
    if !openssh_integration_enabled() {
        eprintln!("skipping: set ARCTERN_OPENSSH_INTEGRATION=1 to run real OpenSSH test");
        return;
    }

    let suffix = unique_suffix();
    let remote_bin = format!("/tmp/arctern-openssh-{suffix}");
    let remote_cfg = format!("/tmp/arctern-openssh-{suffix}.toml");
    let remote_home = format!("/tmp/arctern-openssh-home-{suffix}");
    let remote_key = format!("/tmp/arctern-openssh-key-{suffix}");
    let remote_known_hosts = format!("/tmp/arctern-openssh-known-hosts-{suffix}");
    let local_key = PathBuf::from(format!("/tmp/arctern-openssh-key-{suffix}"));
    let local_pub = local_key.with_extension("pub");
    let local_known_hosts = PathBuf::from(format!("/tmp/arctern-openssh-known-hosts-{suffix}"));

    let pool = LoopbackPool::create(common::ssh_runner_from_env())
        .await
        .expect("create receiver pool");
    let receiver_root = format!("{}/backups", pool.name());
    let state_dir = format!("/tmp/arctern-openssh-state-{suffix}");

    let arctern_bin = PathBuf::from(env!("CARGO_BIN_EXE_arctern"));
    scp_to_remote(&arctern_bin, &remote_bin);
    run_remote_shell(&format!("chmod 0755 {}", shell_quote(&remote_bin)));

    let mut keygen = Command::new("ssh-keygen");
    keygen.args([
        "-q",
        "-t",
        "ed25519",
        "-N",
        "",
        "-C",
        &format!("arctern-test-{suffix}"),
        "-f",
    ]);
    keygen.arg(&local_key);
    run_local_command(keygen);

    let fingerprint_output = run_local_command({
        let mut cmd = Command::new("ssh-keygen");
        cmd.args(["-l", "-f"]);
        cmd.arg(&local_pub);
        cmd
    });
    let fingerprint_stdout =
        String::from_utf8(fingerprint_output.stdout).expect("utf8 fingerprint");
    let fingerprint = fingerprint_stdout
        .split_whitespace()
        .nth(1)
        .expect("ssh-keygen -l prints fingerprint")
        .to_string();
    let public_key = std::fs::read_to_string(&local_pub).expect("read public key");

    let cfg = format!(
        r#"state_dir = {state_dir:?}

[[allowed_clients]]
identity = "laptop_test"
fingerprint = {fingerprint:?}
jobs = ["push_test"]
operations = ["control", "recv"]
root_fs = {receiver_root:?}
"#
    );
    let remote_cfg_write = format!("cat > {} <<'EOF'\n{}EOF\n", shell_quote(&remote_cfg), cfg);
    run_remote_shell(&remote_cfg_write);

    let forced = format!(
        "command=\"{} stdinserver-dispatch laptop_test --config {}\",restrict {}",
        remote_bin,
        remote_cfg,
        public_key.trim()
    );
    let setup = format!(
        "set -e; \
         mkdir -p {home}/.ssh {state_dir} {receiver_root}; \
         chmod 700 {home}/.ssh; \
         printf '%s\n' {forced} > {home}/.ssh/authorized_keys; \
         chmod 600 {home}/.ssh/authorized_keys",
        home = shell_quote(&remote_home),
        state_dir = shell_quote(&state_dir),
        receiver_root = shell_quote(&receiver_root),
        forced = shell_quote(&forced),
    );
    run_remote_shell(&setup);

    let target = ssh_target_from_env();
    let mut scan = Command::new("ssh-keyscan");
    scan.args(["-p", &target.port.to_string(), &target.host]);
    let scan_output = run_local_command(scan);
    std::fs::write(&local_known_hosts, scan_output.stdout).expect("write known_hosts");
    scp_to_remote(&local_key, &remote_key);
    scp_to_remote(&local_pub, &format!("{remote_key}.pub"));
    scp_to_remote(&local_known_hosts, &remote_known_hosts);
    run_remote_shell(&format!(
        "chmod 0600 {} {}",
        shell_quote(&remote_key),
        shell_quote(&remote_known_hosts)
    ));

    let session = SessionBuilder::default()
        .user(target.user.clone())
        .port(target.port)
        .keyfile(&local_key)
        .user_known_hosts_file(&local_known_hosts)
        .known_hosts_check(KnownHosts::Strict)
        .connect_mux(&target.host)
        .await
        .expect("connect OpenSSH session with test key");

    let mut child = session
        .command(&remote_bin)
        .arg("stdinserver")
        .arg("push_test")
        .arg("control")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .await
        .expect("spawn forced-command control channel");
    let mut stdin = child.stdin().take().expect("control stdin");
    let mut stdout = child.stdout().take().expect("control stdout");

    write_request(
        &mut stdin,
        &RequestFrame {
            id: 1,
            body: Request::ListSnapshots {
                dataset: receiver_root.clone(),
                prefix_regex: None,
            },
        },
    )
    .await
    .expect("write ListSnapshots request");
    stdin.shutdown().await.expect("shutdown control stdin");

    let response = read_response(&mut stdout)
        .await
        .expect("read ListSnapshots response")
        .body;
    match response {
        Response::ListSnapshotsOk { snapshots, .. } => assert!(snapshots.is_empty()),
        other => panic!("unexpected response: {other:?}"),
    }

    let _ = child.wait().await;

    let bad_cfg = cfg.replace(
        &fingerprint,
        "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    );
    let bad_cfg_write = format!(
        "cat > {} <<'EOF'\n{}EOF\n",
        shell_quote(&remote_cfg),
        bad_cfg
    );
    run_remote_shell(&bad_cfg_write);

    let bad_spawn = session
        .command(&remote_bin)
        .arg("stdinserver")
        .arg("push_test")
        .arg("control")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .await;
    if let Ok(child) = bad_spawn {
        let status = child.wait().await.expect("wait bad fingerprint child");
        assert!(
            !status.success(),
            "control channel should fail with mismatched fingerprint"
        );
    }

    run_remote_shell(&format!(
        "rm -rf {} {} {} {} {} {} {}",
        shell_quote(&remote_bin),
        shell_quote(&remote_cfg),
        shell_quote(&remote_home),
        shell_quote(&remote_key),
        shell_quote(&format!("{remote_key}.pub")),
        shell_quote(&remote_known_hosts),
        shell_quote(&state_dir)
    ));
    let _ = std::fs::remove_file(&local_key);
    let _ = std::fs::remove_file(&local_pub);
    let _ = std::fs::remove_file(&local_known_hosts);
    pool.destroy().await.expect("destroy receiver pool");
}
