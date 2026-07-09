//! Shared helpers for arctern integration tests. Compiled only when the
//! `integration` feature is on. Mirrors zfskit's `tests/common/mod.rs`
//! (LoopbackPool inside the VM) and adds a daemon-spawn helper that reads
//! the LISTEN <addr> handshake line from the daemon's stdout.
//!
//! Per slice 001 plan decision D4: this is copied rather than promoted to
//! a `zfskit::test_support` module. Promote when a third consumer
//! appears.

#![cfg(feature = "integration")]
#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use zfskit::pool::{DestroyOptions, ExportOptions, PoolCreateOptions, Vdev};
use zfskit::runner::{Cmd, CommandRunner, SshTarget};
use zfskit::{SshCommandRunner, ZfsError};

pub fn ssh_runner_from_env() -> SshCommandRunner {
    SshCommandRunner::from_env().unwrap_or_else(|e| {
        panic!(
            "integration test requires ZFSKIT_SSH_TARGET=[user@]host[:port]: {e}\n\
             tip: `just vm-up` boots the archzfs test ISO and exports the right env"
        )
    })
}

pub fn unique_suffix() -> String {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}_{n:x}")
}

pub fn openssh_integration_enabled() -> bool {
    std::env::var("ARCTERN_OPENSSH_INTEGRATION")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

pub fn ssh_target_from_env() -> SshTarget {
    let raw = std::env::var("ZFSKIT_SSH_TARGET")
        .expect("ZFSKIT_SSH_TARGET must be set for integration tests");
    SshTarget::parse(&raw).expect("ZFSKIT_SSH_TARGET parses")
}

pub fn run_local_command(mut cmd: Command) -> Output {
    let display = format!("{cmd:?}");
    let output = cmd
        .output()
        .unwrap_or_else(|e| panic!("spawn {display}: {e}"));
    assert!(
        output.status.success(),
        "{display} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

pub fn run_remote_shell(remote_cmd: &str) {
    let target = std::env::var("ZFSKIT_SSH_TARGET")
        .expect("ZFSKIT_SSH_TARGET must be set for integration tests");
    let password = std::env::var("ZFSKIT_SSH_PASSWORD").ok();
    let status =
        sync_ssh(&target, password.as_deref(), remote_cmd).expect("ssh remote shell command");
    assert!(
        status.success(),
        "remote command failed with {status}: {remote_cmd}"
    );
}

pub fn scp_to_remote(local: &Path, remote: &str) {
    let target = ssh_target_from_env();
    let password = std::env::var("ZFSKIT_SSH_PASSWORD").ok();
    let mut cmd = match password.as_deref() {
        Some(pw) => {
            let mut c = Command::new("sshpass");
            c.args(["-p", pw, "scp"]);
            c
        }
        None => Command::new("scp"),
    };
    cmd.args([
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
        "-P",
        &target.port.to_string(),
    ]);
    cmd.arg(local);
    cmd.arg(format!("{}@{}:{remote}", target.user, target.host));
    run_local_command(cmd);
}

pub struct LoopbackPool {
    runner: SshCommandRunner,
    name: String,
    img_path: String,
    altroot: String,
    destroyed: bool,
}

impl LoopbackPool {
    pub async fn create(runner: SshCommandRunner) -> Result<Self, ZfsError> {
        let suffix = unique_suffix();
        let name = format!("zfskit_test_{suffix}");
        let img_path = format!("/tmp/{name}.img");
        let altroot = format!("/tmp/{name}_root");

        run_check(
            &runner,
            Cmd::new("truncate").args(["-s", "256M", &img_path]),
        )
        .await?;
        run_check(&runner, Cmd::new("mkdir").args(["-p", &altroot])).await?;

        let opts = PoolCreateOptions::new(&name)
            .force()
            .pool_property("ashift", "12")
            .fs_property("compression", "lz4")
            .fs_property("atime", "off")
            .mountpoint("none")
            .altroot(&altroot)
            .vdev(Vdev::Stripe(vec![img_path.clone().into()]));
        zfskit::pool::create(&runner, &opts).await?;

        Ok(Self {
            runner,
            name,
            img_path,
            altroot,
            destroyed: false,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn destroy(mut self) -> Result<(), ZfsError> {
        self.destroyed = true;
        let _ = zfskit::pool::export(&self.runner, &self.name, &ExportOptions::default()).await;
        let _ =
            zfskit::pool::destroy(&self.runner, &self.name, &DestroyOptions { force: true }).await;
        let _ = run_check(&self.runner, Cmd::new("rm").args(["-f", &self.img_path])).await;
        let _ = run_check(&self.runner, Cmd::new("rm").args(["-rf", &self.altroot])).await;
        Ok(())
    }
}

impl Drop for LoopbackPool {
    fn drop(&mut self) {
        if self.destroyed {
            return;
        }
        let target = std::env::var("ZFSKIT_SSH_TARGET").ok();
        let Some(target) = target else { return };
        let pw = std::env::var("ZFSKIT_SSH_PASSWORD").ok();
        let cmds = [
            format!("zpool destroy -f {} 2>/dev/null || true", self.name),
            format!("rm -f {} 2>/dev/null || true", self.img_path),
            format!("rm -rf {} 2>/dev/null || true", self.altroot),
        ];
        let _ = sync_ssh(&target, pw.as_deref(), &cmds.join("; "));
    }
}

async fn run_check(runner: &SshCommandRunner, cmd: Cmd) -> Result<(), ZfsError> {
    let display = format!("{cmd}");
    let out = runner.run(cmd).await?;
    if out.status.success() {
        return Ok(());
    }
    Err(ZfsError::Other {
        exit_code: out.status.code(),
        stderr: format!(
            "`{display}` failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ),
    })
}

fn sync_ssh(
    target: &str,
    password: Option<&str>,
    remote_cmd: &str,
) -> std::io::Result<std::process::ExitStatus> {
    let (user, rest) = match target.split_once('@') {
        Some((u, r)) => (u.to_string(), r),
        None => ("root".to_string(), target),
    };
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(22u16)),
        None => (rest.to_string(), 22),
    };
    let mut cmd = match password {
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
    ]);
    if password.is_some() {
        cmd.args([
            "-o",
            "PreferredAuthentications=password",
            "-o",
            "PubkeyAuthentication=no",
        ]);
    } else {
        cmd.args(["-o", "BatchMode=yes"]);
    }
    cmd.args(["-p", &port.to_string()]);
    cmd.arg(format!("{user}@{host}"));
    cmd.arg("--");
    cmd.arg(remote_cmd);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
}

/// Spawn the arctern daemon as a subprocess pointing at the same VM the
/// caller's `SshCommandRunner` targets, listening on a fresh UNIX socket.
/// Reads stdout until the `LISTEN unix:<path>` handshake line arrives;
/// returns `(child, socket_path)`.
///
/// `socket` is optional: if `None`, the helper picks a unique
/// `/tmp/arctern_test_<nanos>_<seq>.sock` path. Pass `Some(path)` to
/// dictate it (e.g., to test the `--socket` flag explicitly).
///
/// Daemon binary path comes from `CARGO_BIN_EXE_arctern`, which cargo
/// sets at compile time for tests in the same package as the bin.
pub fn spawn_daemon_uds(socket: Option<PathBuf>) -> (Child, PathBuf) {
    spawn_daemon_uds_with_config(socket, None)
}

/// Slice 003: the daemon now requires `--config <path>`. If the caller
/// does not supply one, write a minimal zero-jobs TOML so existing
/// slice-001 / slice-002 tests stay valid without knowing about config.
pub fn spawn_daemon_uds_with_config(
    socket: Option<PathBuf>,
    config: Option<PathBuf>,
) -> (Child, PathBuf) {
    let (child, sock, _quic) = spawn_daemon_full(socket, config, 0);
    (child, sock)
}

/// Slice 004: like `spawn_daemon_uds_with_config` but also waits for
/// `expected_quic` `LISTEN_QUIC <addr>` lines on stdout and returns
/// them. Use this when the test config declares sink jobs.
pub fn spawn_daemon_uds_with_quic(
    socket: Option<PathBuf>,
    config: Option<PathBuf>,
    expected_quic: usize,
) -> (Child, PathBuf, Vec<std::net::SocketAddr>) {
    spawn_daemon_full(socket, config, expected_quic)
}

fn spawn_daemon_full(
    socket: Option<PathBuf>,
    config: Option<PathBuf>,
    expected_quic: usize,
) -> (Child, PathBuf, Vec<std::net::SocketAddr>) {
    let socket_path = socket
        .unwrap_or_else(|| PathBuf::from(format!("/tmp/arctern_test_{}.sock", unique_suffix())));
    let _ = std::fs::remove_file(&socket_path);

    let config_path = config.unwrap_or_else(|| {
        let p = PathBuf::from(format!("/tmp/arctern_test_{}.toml", unique_suffix()));
        // Slice 004: state_dir defaults to /var/lib/arctern at the
        // daemon, which is unwritable by an unprivileged test user.
        // Steer it under /tmp so empty-config helpers keep working.
        let state = format!("/tmp/arctern_test_state_{}", unique_suffix());
        std::fs::write(&p, format!("state_dir = {state:?}\n")).expect("write empty config");
        p
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_arctern"))
        .arg("daemon")
        .arg("--socket")
        .arg(&socket_path)
        .arg("--config")
        .arg(&config_path)
        .arg("--http-address")
        .arg("127.0.0.1:0")
        .env(
            "ZFSKIT_SSH_TARGET",
            std::env::var("ZFSKIT_SSH_TARGET").expect("ZFSKIT_SSH_TARGET must be set"),
        )
        .env(
            "ZFSKIT_SSH_PASSWORD",
            std::env::var("ZFSKIT_SSH_PASSWORD").unwrap_or_default(),
        )
        .stdout(Stdio::piped())
        .stderr({
            // Capture stderr to a per-test file so panics inside tests
            // surface daemon-side tracing output. Slice 003 needs this
            // to debug snap-job loops; previously stderr inherited and
            // got hidden by `cargo test`'s stdout capture.
            let p = format!("/tmp/arctern_test_{}.stderr", unique_suffix());
            std::fs::File::create(&p)
                .map(Into::into)
                .unwrap_or_else(|_| Stdio::inherit())
        })
        .spawn()
        .expect("spawn arctern daemon");

    let stdout = child.stdout.take().expect("daemon stdout piped");
    let mut reader = BufReader::new(stdout);
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut line = String::new();
    let mut socket_path: Option<PathBuf> = None;
    let mut quic: Vec<std::net::SocketAddr> = Vec::new();
    loop {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!(
                "daemon did not print LISTEN handshake (got socket={:?}, quic={:?}) within 15s",
                socket_path, quic
            );
        }
        line.clear();
        let n = reader.read_line(&mut line).expect("read daemon stdout");
        if n == 0 {
            let _ = child.kill();
            panic!("daemon stdout closed before LISTEN handshake completed");
        }
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("LISTEN unix:") {
            socket_path = Some(PathBuf::from(rest));
        } else if let Some(rest) = trimmed.strip_prefix("LISTEN_QUIC ") {
            quic.push(rest.parse().expect("LISTEN_QUIC addr parses"));
        }
        if let Some(p) = &socket_path
            && quic.len() >= expected_quic
        {
            return (child, p.clone(), quic);
        }
    }
}
