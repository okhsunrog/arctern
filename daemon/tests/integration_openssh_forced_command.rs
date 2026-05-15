#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use std::path::PathBuf;
use std::process::Command;

use arctern_transport::{Request, RequestFrame, Response, read_response, write_request};
use openssh::{KnownHosts, SessionBuilder, Stdio};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Duration, timeout};

use common::{
    openssh_integration_enabled, run_local_command, run_remote_shell, scp_to_remote,
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
    eprintln!("openssh test: suffix={suffix}");
    let remote_bin = format!("/tmp/arctern-openssh-{suffix}");
    let remote_cfg = format!("/tmp/arctern-openssh-{suffix}.toml");
    let remote_user = format!(
        "arctern{}",
        suffix
            .split('_')
            .next()
            .expect("suffix has timestamp component")
            .chars()
            .take(12)
            .collect::<String>()
    );
    let remote_home = format!("/home/{remote_user}");
    let remote_auth_keys = format!("{remote_home}/.ssh/authorized_keys");
    let ssh_config = PathBuf::from(format!("/tmp/arctern-openssh-ssh-config-{suffix}"));
    let local_key = PathBuf::from(format!("/tmp/arctern-openssh-key-{suffix}"));
    let local_pub = local_key.with_extension("pub");
    let local_known_hosts = PathBuf::from(format!("/tmp/arctern-openssh-known-hosts-{suffix}"));

    let receiver_root = format!("tank/arctern-openssh-{suffix}");
    let state_dir = format!("/tmp/arctern-openssh-state-{suffix}");

    eprintln!("openssh test: copying arctern binary");
    let arctern_bin = PathBuf::from(env!("CARGO_BIN_EXE_arctern"));
    scp_to_remote(&arctern_bin, &remote_bin);
    run_remote_shell(&format!("chmod 0755 {}", shell_quote(&remote_bin)));

    eprintln!("openssh test: generating key");
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

    eprintln!("openssh test: writing remote config and authorized_keys");
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
         printf '%s\n' 'ExposeAuthInfo yes' > /etc/ssh/sshd_config.d/99-arctern-test.conf; \
         systemctl reload sshd; \
         useradd -m -s /bin/sh {remote_user}; \
         mkdir -p {remote_home}/.ssh {state_dir}; \
         chmod 700 {remote_home}/.ssh; \
         printf '%s\n' {forced} > {auth_keys}; \
         chown -R {remote_user}:{remote_user} {remote_home}/.ssh {state_dir}; \
         chmod 600 {auth_keys}",
        remote_user = shell_quote(&remote_user),
        remote_home = shell_quote(&remote_home),
        state_dir = shell_quote(&state_dir),
        auth_keys = shell_quote(&remote_auth_keys),
        forced = shell_quote(&forced),
    );
    run_remote_shell(&setup);

    let target = ssh_target_from_env();
    let mut scan = Command::new("ssh-keyscan");
    scan.args(["-p", &target.port.to_string(), &target.host]);
    let scan_output = run_local_command(scan);
    std::fs::write(&local_known_hosts, scan_output.stdout).expect("write known_hosts");
    let ssh_config_text = format!(
        "Host arctern-openssh-test-{suffix}\n  HostName {}\n  Port {}\n  User {}\n  IdentityFile {}\n  IdentitiesOnly yes\n  PreferredAuthentications publickey\n  PasswordAuthentication no\n  KbdInteractiveAuthentication no\n  BatchMode yes\n  UserKnownHostsFile {}\n  StrictHostKeyChecking yes\n  LogLevel ERROR\n",
        target.host,
        target.port,
        remote_user,
        local_key.display(),
        local_known_hosts.display()
    );
    std::fs::write(&ssh_config, ssh_config_text).expect("write ssh config");

    eprintln!("openssh test: connecting openssh session");
    let session = SessionBuilder::default()
        .config_file(&ssh_config)
        .known_hosts_check(KnownHosts::Strict)
        .keyfile(&local_key)
        .user(remote_user.clone())
        .port(target.port)
        .connect_mux(format!("arctern-openssh-test-{suffix}"))
        .await
        .expect("connect OpenSSH session with test key");

    eprintln!("openssh test: spawning control channel");
    let mut child = session
        .raw_command("arctern")
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
    let mut stderr = child.stderr().take().expect("control stderr");

    eprintln!("openssh test: sending GetLogCursor");
    write_request(
        &mut stdin,
        &RequestFrame {
            id: 1,
            body: Request::GetLogCursor,
        },
    )
    .await
    .expect("write GetLogCursor request");

    let response_frame = timeout(Duration::from_secs(10), read_response(&mut stdout))
        .await
        .expect("timed out waiting for GetLogCursor response");
    if let Err(e) = &response_frame {
        let mut stderr_text = String::new();
        let _ = timeout(
            Duration::from_secs(2),
            stderr.read_to_string(&mut stderr_text),
        )
        .await;
        panic!("read GetLogCursor response: {e}; stderr:\n{stderr_text}");
    }
    eprintln!("openssh test: got GetLogCursor response");
    let response = response_frame.expect("checked response frame").body;
    match response {
        Response::GetLogCursorOk { .. } => {}
        other => panic!("unexpected response: {other:?}"),
    }

    eprintln!("openssh test: sending Shutdown");
    write_request(
        &mut stdin,
        &RequestFrame {
            id: 2,
            body: Request::Shutdown,
        },
    )
    .await
    .expect("write Shutdown request");
    let _ = timeout(Duration::from_secs(10), read_response(&mut stdout))
        .await
        .expect("timed out waiting for Shutdown response")
        .expect("read Shutdown response");
    stdin.shutdown().await.expect("shutdown control stdin");

    let _ = timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("timed out waiting for control channel exit");

    eprintln!("openssh test: checking bad fingerprint");
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
        .raw_command("arctern")
        .arg("stdinserver")
        .arg("push_test")
        .arg("control")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .await;
    if let Ok(child) = bad_spawn {
        let status = timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("timed out waiting for bad fingerprint child")
            .expect("wait bad fingerprint child");
        assert!(
            !status.success(),
            "control channel should fail with mismatched fingerprint"
        );
    }

    run_remote_shell(&format!(
        "rm -rf {} {} {}; userdel -r {} >/dev/null 2>&1 || true",
        shell_quote(&remote_bin),
        shell_quote(&remote_cfg),
        shell_quote(&state_dir),
        shell_quote(&remote_user)
    ));
    let _ = std::fs::remove_file(&local_key);
    let _ = std::fs::remove_file(&local_pub);
    let _ = std::fs::remove_file(&local_known_hosts);
    let _ = std::fs::remove_file(&ssh_config);
}
