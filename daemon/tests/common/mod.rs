//! Shared helpers for arctern integration tests. Compiled only when the
//! `integration` feature is on. Mirrors palimpsest's `tests/common/mod.rs`
//! (LoopbackPool inside the VM) and adds a daemon-spawn helper that reads
//! the LISTEN <addr> handshake line from the daemon's stdout.
//!
//! Per slice 001 plan decision D4: this is copied rather than promoted to
//! a `palimpsest::test_support` module. Promote when a third consumer
//! appears.

#![cfg(feature = "integration")]
#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use palimpsest::pool::{DestroyOptions, ExportOptions, PoolCreateOptions, Vdev};
use palimpsest::runner::{Cmd, CommandRunner};
use palimpsest::{SshCommandRunner, ZfsError};

pub fn ssh_runner_from_env() -> SshCommandRunner {
    SshCommandRunner::from_env().unwrap_or_else(|e| {
        panic!(
            "integration test requires PALIMPSEST_SSH_TARGET=[user@]host[:port]: {e}\n\
             tip: `just vm-up` boots the archzfs test ISO and exports the right env"
        )
    })
}

fn unique_suffix() -> String {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}_{n:x}")
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
        let name = format!("palimpsest_test_{suffix}");
        let img_path = format!("/tmp/{name}.img");
        let altroot = format!("/tmp/{name}_root");

        run_check(&runner, Cmd::new("truncate").args(["-s", "256M", &img_path])).await?;
        run_check(&runner, Cmd::new("mkdir").args(["-p", &altroot])).await?;

        let opts = PoolCreateOptions::new(&name)
            .force()
            .pool_property("ashift", "12")
            .fs_property("compression", "lz4")
            .fs_property("atime", "off")
            .mountpoint("none")
            .altroot(&altroot)
            .vdev(Vdev::Stripe(vec![img_path.clone().into()]));
        palimpsest::pool::create(&runner, &opts).await?;

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
        let _ = palimpsest::pool::export(&self.runner, &self.name, &ExportOptions::default()).await;
        let _ =
            palimpsest::pool::destroy(&self.runner, &self.name, &DestroyOptions { force: true })
                .await;
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
        let target = std::env::var("PALIMPSEST_SSH_TARGET").ok();
        let Some(target) = target else { return };
        let pw = std::env::var("PALIMPSEST_SSH_PASSWORD").ok();
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

fn sync_ssh(target: &str, password: Option<&str>, remote_cmd: &str) -> std::io::Result<()> {
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
        .status()?;
    Ok(())
}

/// Spawn the arctern daemon as a subprocess pointing at the same VM the
/// caller's `SshCommandRunner` targets. Reads stdout until the
/// `LISTEN <addr>` handshake line arrives, returns `(child, base_url)`
/// where `base_url` is `http://<addr>`.
///
/// Daemon binary path comes from `CARGO_BIN_EXE_arctern`, which cargo
/// sets at compile time for tests in the same package as the bin.
pub fn spawn_daemon() -> (Child, String) {
    let bin = env!("CARGO_BIN_EXE_arctern");
    let mut child = Command::new(bin)
        .arg("daemon")
        .env(
            "PALIMPSEST_SSH_TARGET",
            std::env::var("PALIMPSEST_SSH_TARGET").expect("PALIMPSEST_SSH_TARGET must be set"),
        )
        .env(
            "PALIMPSEST_SSH_PASSWORD",
            std::env::var("PALIMPSEST_SSH_PASSWORD").unwrap_or_default(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn arctern daemon");

    let stdout = child.stdout.take().expect("daemon stdout piped");
    let mut reader = BufReader::new(stdout);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut line = String::new();
    loop {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("daemon did not print LISTEN <addr> within 10s");
        }
        line.clear();
        let n = reader.read_line(&mut line).expect("read daemon stdout");
        if n == 0 {
            let _ = child.kill();
            panic!("daemon stdout closed before LISTEN line");
        }
        if let Some(addr) = line.trim().strip_prefix("LISTEN ") {
            return (child, format!("http://{addr}"));
        }
    }
}
