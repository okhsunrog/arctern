#![cfg(feature = "integration")]
#![allow(clippy::zombie_processes)]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use arctern_transport::{
    ErrorCode, RecvHeader, Request, RequestFrame, Response, SendFlagsWire, SendHeader, SendKind,
    SnapshotRef, read_response, write_header, write_request,
};
use openssh::{KnownHosts, Session, SessionBuilder, Stdio};
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

fn run_remote_shell_best_effort(remote_cmd: &str) {
    let Ok(raw_target) = std::env::var("PALIMPSEST_SSH_TARGET") else {
        return;
    };
    let Ok(target) = palimpsest::runner::SshTarget::parse(&raw_target) else {
        return;
    };
    let password = std::env::var("PALIMPSEST_SSH_PASSWORD").ok();
    let mut cmd = match password.as_deref() {
        Some(pw) => {
            let mut c = Command::new("sshpass");
            c.args(["-p", pw, "ssh"]);
            c
        }
        None => Command::new("ssh"),
    };
    cmd.args([
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=5",
        "-o",
        "ServerAliveCountMax=1",
        "-p",
        &target.port.to_string(),
        &format!("{}@{}", target.user, target.host),
        "--",
        remote_cmd,
    ]);
    let _ = cmd.status();
}

struct OpenSshTestCleanup {
    local_paths: Vec<PathBuf>,
    remote_cmd: String,
}

impl OpenSshTestCleanup {
    fn new(local_paths: Vec<PathBuf>, remote_cmd: String) -> Self {
        Self {
            local_paths,
            remote_cmd,
        }
    }
}

impl Drop for OpenSshTestCleanup {
    fn drop(&mut self) {
        if !self.remote_cmd.is_empty() {
            run_remote_shell_best_effort(&self.remote_cmd);
        }
        for path in &self.local_paths {
            remove_local_path_best_effort(path);
        }
    }
}

fn remove_local_path_best_effort(path: &Path) {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_dir() => {
            let _ = std::fs::remove_dir_all(path);
        }
        Ok(_) => {
            let _ = std::fs::remove_file(path);
        }
        Err(_) => {}
    }
}

fn remote_ssh_command(target: &palimpsest::runner::SshTarget, args: &[String]) -> Command {
    let password = std::env::var("PALIMPSEST_SSH_PASSWORD").ok();
    let mut cmd = match password.as_deref() {
        Some(pw) => {
            let mut c = Command::new("sshpass");
            c.args(["-p", pw, "ssh"]);
            c
        }
        None => Command::new("ssh"),
    };
    cmd.args([
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
        "-p",
        &target.port.to_string(),
        &format!("{}@{}", target.user, target.host),
        "--",
    ]);
    cmd.args(args);
    cmd
}

fn run_remote_command_capture(target: &palimpsest::runner::SshTarget, args: &[String]) -> String {
    let output = run_local_command(remote_ssh_command(target, args));
    String::from_utf8(output.stdout).expect("remote command stdout is utf8")
}

fn zfs_snapshot_guid(target: &palimpsest::runner::SshTarget, snapshot: &str) -> u64 {
    let stdout = run_remote_command_capture(
        target,
        &[
            "zfs".to_string(),
            "get".to_string(),
            "-H".to_string(),
            "-p".to_string(),
            "-o".to_string(),
            "value".to_string(),
            "guid".to_string(),
            snapshot.to_string(),
        ],
    );
    stdout
        .trim()
        .parse::<u64>()
        .unwrap_or_else(|e| panic!("parse guid for {snapshot}: {e}; stdout={stdout:?}"))
}

fn zfs_snapshot_exists(target: &palimpsest::runner::SshTarget, snapshot: &str) {
    run_local_command(remote_ssh_command(
        target,
        &[
            "zfs".to_string(),
            "list".to_string(),
            "-H".to_string(),
            "-t".to_string(),
            "snapshot".to_string(),
            "-o".to_string(),
            "name".to_string(),
            snapshot.to_string(),
        ],
    ));
}

fn zfs_send_stream(target: &palimpsest::runner::SshTarget, args: &[String]) -> Vec<u8> {
    let mut remote_args = vec!["zfs".to_string(), "send".to_string()];
    remote_args.extend(args.iter().cloned());
    run_local_command(remote_ssh_command(target, &remote_args)).stdout
}

fn recv_header(
    target_dataset: String,
    send_kind: SendKind,
    from_snap: Option<&str>,
    to_snap: &str,
) -> RecvHeader {
    RecvHeader {
        version: arctern_transport::PROTOCOL_VERSION,
        target_dataset,
        send: SendHeader {
            send_kind,
            from_snap: from_snap.map(|name| SnapshotRef {
                name: name.to_string(),
                guid: 1,
            }),
            to_snap: SnapshotRef {
                name: to_snap.to_string(),
                guid: 1,
            },
            flags: SendFlagsWire {
                raw: false,
                embedded: false,
                compressed: false,
                large_blocks: false,
            },
            discard_partial_recv: false,
        },
    }
}

async fn recv_over_forced_command(
    session: &Session,
    header: RecvHeader,
    stream: &[u8],
) -> Response {
    let mut recv_child = session
        .raw_command("arctern")
        .arg("stdinserver")
        .arg("push_test")
        .arg("recv")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .await
        .expect("spawn forced-command recv channel");
    let mut recv_stdin = recv_child.stdin().take().expect("recv stdin");
    let mut recv_stdout = recv_child.stdout().take().expect("recv stdout");
    let mut recv_stderr = recv_child.stderr().take().expect("recv stderr");

    write_header(&mut recv_stdin, &header)
        .await
        .expect("write recv header");
    recv_stdin
        .write_all(stream)
        .await
        .expect("write zfs stream to recv channel");
    recv_stdin.shutdown().await.expect("shutdown recv stdin");
    drop(recv_stdin);

    let recv_response_frame =
        match timeout(Duration::from_secs(30), read_response(&mut recv_stdout)).await {
            Ok(frame) => frame,
            Err(e) => {
                let mut stderr_text = String::new();
                let _ = timeout(
                    Duration::from_secs(2),
                    recv_stderr.read_to_string(&mut stderr_text),
                )
                .await;
                panic!("timed out waiting for recv response: {e}; stderr:\n{stderr_text}");
            }
        };
    if let Err(e) = &recv_response_frame {
        let mut stderr_text = String::new();
        let _ = timeout(
            Duration::from_secs(2),
            recv_stderr.read_to_string(&mut stderr_text),
        )
        .await;
        panic!("read recv response: {e}; stderr:\n{stderr_text}");
    }
    let body = recv_response_frame
        .expect("checked recv response frame")
        .body;
    let recv_status = timeout(Duration::from_secs(10), recv_child.wait())
        .await
        .expect("timed out waiting for recv channel exit")
        .expect("wait recv child");
    assert!(recv_status.success(), "recv child failed: {recv_status}");
    body
}

fn assert_error_code(response: Response, expected: ErrorCode) {
    match response {
        Response::Error { code, .. } => assert_eq!(code, expected),
        other => panic!("expected {expected:?} error response, got {other:?}"),
    }
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

    let pool = common::LoopbackPool::create(common::ssh_runner_from_env())
        .await
        .expect("create OpenSSH recv test pool");
    let source_dataset = format!("{}/source", pool.name());
    let receiver_root = format!("{}/receiver", pool.name());
    let source_mount = format!("/tmp/arctern-openssh-source-{suffix}");
    let state_dir = format!("/tmp/arctern-openssh-state-{suffix}");
    let cleanup = OpenSshTestCleanup::new(
        vec![
            local_key.clone(),
            local_pub.clone(),
            local_known_hosts.clone(),
            ssh_config.clone(),
        ],
        format!(
            "rm -rf {remote_bin} {remote_cfg} {state_dir} {source_mount}; \
             userdel -r {remote_user} >/dev/null 2>&1 || true; \
             rm -f /etc/ssh/sshd_config.d/99-arctern-test.conf; \
             systemctl reload sshd >/dev/null 2>&1 || true",
            remote_bin = shell_quote(&remote_bin),
            remote_cfg = shell_quote(&remote_cfg),
            state_dir = shell_quote(&state_dir),
            source_mount = shell_quote(&source_mount),
            remote_user = shell_quote(&remote_user),
        ),
    );

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

    eprintln!("openssh test: creating ZFS source/receiver datasets");
    let zfs_setup = format!(
        "set -e; \
         zfs create -p {receiver_root}; \
         zfs allow -u {remote_user} create,mount,receive {receiver_root}; \
         zfs create {source_dataset}; \
         mkdir -p {source_mount}; \
         zfs set mountpoint={source_mount} {source_dataset}; \
         printf '%s\n' {payload1} > {source_mount}/payload.txt; \
         zfs snapshot {source_snapshot}",
        receiver_root = shell_quote(&receiver_root),
        remote_user = shell_quote(&remote_user),
        source_dataset = shell_quote(&source_dataset),
        source_mount = shell_quote(&source_mount),
        payload1 = shell_quote(&format!("payload one {suffix}")),
        source_snapshot = shell_quote(&format!("{source_dataset}@snap1")),
    );
    run_remote_shell(&zfs_setup);

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

    eprintln!("openssh test: capturing source zfs send stream");
    let target_dataset = format!("{receiver_root}/copy");
    let source_snap1 = format!("{source_dataset}@snap1");
    let target_snap1 = format!("{target_dataset}@snap1");
    let send_stream = zfs_send_stream(&target, std::slice::from_ref(&source_snap1));

    eprintln!("openssh test: spawning recv channel");
    let recv_response = recv_over_forced_command(
        &session,
        recv_header(target_dataset.clone(), SendKind::Full, None, "snap1"),
        &send_stream,
    )
    .await;
    match recv_response {
        Response::Ok => {}
        other => panic!("unexpected full recv response: {other:?}"),
    }
    zfs_snapshot_exists(&target, &target_snap1);
    let source_guid1 = zfs_snapshot_guid(&target, &source_snap1);
    let target_guid1 = zfs_snapshot_guid(&target, &target_snap1);
    assert_eq!(
        source_guid1, target_guid1,
        "full recv snapshot GUID mismatch"
    );

    eprintln!("openssh test: receiving incremental zfs stream");
    let source_snap2 = format!("{source_dataset}@snap2");
    let target_snap2 = format!("{target_dataset}@snap2");
    run_remote_shell(&format!(
        "set -e; printf '%s\n' {} >> {}; zfs snapshot {}",
        shell_quote(&format!("payload two {suffix}")),
        shell_quote(&format!("{source_mount}/payload.txt")),
        shell_quote(&source_snap2),
    ));
    let incremental_stream = zfs_send_stream(
        &target,
        &["-i".to_string(), source_snap1.clone(), source_snap2.clone()],
    );
    let recv_response = recv_over_forced_command(
        &session,
        recv_header(
            target_dataset.clone(),
            SendKind::Incremental,
            Some("snap1"),
            "snap2",
        ),
        &incremental_stream,
    )
    .await;
    match recv_response {
        Response::Ok => {}
        other => panic!("unexpected incremental recv response: {other:?}"),
    }
    zfs_snapshot_exists(&target, &target_snap2);
    let source_guid2 = zfs_snapshot_guid(&target, &source_snap2);
    let target_guid2 = zfs_snapshot_guid(&target, &target_snap2);
    assert_eq!(
        source_guid2, target_guid2,
        "incremental recv snapshot GUID mismatch"
    );

    eprintln!("openssh test: checking recv target outside root_fs is rejected");
    let outside_root_response = recv_over_forced_command(
        &session,
        recv_header(
            format!("{}/outside-copy", pool.name()),
            SendKind::Full,
            None,
            "outside",
        ),
        &[],
    )
    .await;
    assert_error_code(outside_root_response, ErrorCode::Unauthorized);
    let outside_snapshot = format!("{}/outside-copy@outside", pool.name());
    let outside_check = remote_ssh_command(
        &target,
        &[
            "zfs".to_string(),
            "list".to_string(),
            "-H".to_string(),
            "-t".to_string(),
            "snapshot".to_string(),
            outside_snapshot,
        ],
    )
    .status()
    .expect("run outside snapshot existence check");
    assert!(
        !outside_check.success(),
        "outside-root recv must not create a snapshot"
    );

    eprintln!("openssh test: checking malformed recv header is rejected");
    let malformed_response = recv_over_forced_command(
        &session,
        recv_header(
            target_dataset.clone(),
            SendKind::Full,
            None,
            "bad/snapshot/name",
        ),
        &[],
    )
    .await;
    assert_error_code(malformed_response, ErrorCode::BadRequest);

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

    drop(cleanup);
}
