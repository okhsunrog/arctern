//! `arctern stdinserver-dispatch <identity>` entry point.
//!
//! sshd invokes this via `authorized_keys` `command="..."`. The dispatcher
//! parses `SSH_ORIGINAL_COMMAND`, looks up the identity in
//! `[[allowed_clients]]`, verifies ACL/fingerprint policy, and dispatches
//! to the `control` or `recv` stdinserver handler.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arctern_config::{AllowedClient, Config};
use palimpsest::runner::{CommandRunner, RealRunner};

/// Outcome of `dispatch::run`. Encoded so step 7/8 can fork on it
/// without re-parsing the command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchAction {
    Control {
        job: String,
    },
    Recv {
        job: String,
    },
    /// Returned for unsupported / not-yet-implemented operations. Caller
    /// should log + exit cleanly with a non-zero code.
    Unsupported {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DispatchError {
    #[error("malformed SSH_ORIGINAL_COMMAND: {0}")]
    MalformedCommand(String),
    #[error("unknown identity: {0:?}")]
    UnknownIdentity(String),
    #[error("identity {identity:?} not allowed for job {job:?}")]
    JobNotAllowed { identity: String, job: String },
    #[error("identity {identity:?} not allowed for operation {op:?}")]
    OpNotAllowed { identity: String, op: String },
    #[error("identity {identity:?} authenticated with unexpected key fingerprint")]
    FingerprintMismatch { identity: String },
    #[error(
        "identity {identity:?} requires SSH key fingerprint verification, but no SSH auth info is available"
    )]
    MissingAuthInfo { identity: String },
}

/// Top-level entry. Loads config, parses argv + env, validates ACL,
/// dispatches. Test/legacy callers without a SQLite pool use this entry.
#[allow(dead_code)]
pub async fn run(identity: &str, config_path: &Path) -> eyre::Result<()> {
    let config =
        arctern_config::load_from_path(config_path).map_err(|e| eyre::eyre!("config load: {e}"))?;
    run_with(identity, config, None).await
}

/// Entry used by main.rs once it has resolved config + opened the
/// optional SQLite pool. Splitting this out keeps the subscriber setup
/// (which needs the pool) and the dispatch logic in one process tree.
pub async fn run_with(
    identity: &str,
    config: Config,
    pool: Option<Arc<sqlx::SqlitePool>>,
) -> eyre::Result<()> {
    let original = std::env::var("SSH_ORIGINAL_COMMAND").unwrap_or_default();
    let auth_info = ssh_auth_info();
    let action = match decide(identity, &original, auth_info.as_deref(), &config) {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(error = %e, identity, "stdinserver-dispatch refused");
            std::process::exit(1);
        }
    };
    let acl = lookup_identity(&config, identity)
        .expect("decide() already validated identity")
        .clone();
    let config = Arc::new(config);
    let runner: Arc<dyn CommandRunner> = Arc::new(RealRunner);
    match action {
        DispatchAction::Control { job } => {
            tracing::info!(identity, job, "stdinserver control: opening channel");
            let stdin = tokio::io::stdin();
            let stdout = tokio::io::stdout();
            super::control::run(runner, config, acl, pool, stdin, stdout)
                .await
                .map_err(|e| eyre::eyre!("control channel: {e}"))?;
            Ok(())
        }
        DispatchAction::Recv { job } => {
            tracing::info!(identity, job, "stdinserver recv: opening channel");
            let stdin = tokio::io::stdin();
            let stdout = tokio::io::stdout();
            super::recv::run(runner, acl, stdin, stdout)
                .await
                .map_err(|e| eyre::eyre!("recv channel: {e}"))?;
            Ok(())
        }
        DispatchAction::Unsupported { reason } => {
            tracing::warn!(identity, reason, "stdinserver-dispatch unsupported op");
            Ok(())
        }
    }
}

/// Pure decision function: parse `SSH_ORIGINAL_COMMAND`, look up the
/// identity, return the action. Split out so it's straightforward to
/// unit-test without env/process plumbing.
pub fn decide(
    identity: &str,
    original_command: &str,
    auth_info: Option<&str>,
    config: &Config,
) -> Result<DispatchAction, DispatchError> {
    let parts: Vec<&str> = original_command.split_whitespace().collect();
    let (job, op) = match parts.as_slice() {
        ["arctern", "stdinserver", job, op, ..] => (*job, *op),
        _ => {
            return Err(DispatchError::MalformedCommand(
                original_command.to_string(),
            ));
        }
    };
    let acl = lookup_identity(config, identity)
        .ok_or_else(|| DispatchError::UnknownIdentity(identity.to_string()))?;
    verify_fingerprint(identity, acl, auth_info)?;
    // The control channel is per-peer, not per-job: one long-lived channel
    // carries RPC for every job the peer serves, and individual control
    // operations are gated separately (the `operations` list here, plus
    // fine-grained `control:*` checks in the control handler). Only
    // replication ops (recv) are bound to a specific `<job>`, so the
    // job-membership check applies to those alone. The active side passes
    // the literal `control` as `<job>` when opening the control channel.
    if op != "control" && !acl.jobs.iter().any(|j| j == job) {
        return Err(DispatchError::JobNotAllowed {
            identity: identity.to_string(),
            job: job.to_string(),
        });
    }
    if !acl.operations.iter().any(|o| o == op) {
        return Err(DispatchError::OpNotAllowed {
            identity: identity.to_string(),
            op: op.to_string(),
        });
    }
    match op {
        "control" => Ok(DispatchAction::Control {
            job: job.to_string(),
        }),
        "recv" => Ok(DispatchAction::Recv {
            job: job.to_string(),
        }),
        other => Ok(DispatchAction::Unsupported {
            reason: format!("operation {other:?} has no handler"),
        }),
    }
}

fn lookup_identity<'a>(config: &'a Config, identity: &str) -> Option<&'a AllowedClient> {
    config
        .allowed_clients
        .iter()
        .find(|c| c.identity == identity)
}

fn ssh_auth_info() -> Option<String> {
    if let Ok(info) = std::env::var("SSH_AUTH_INFO_0")
        && !info.trim().is_empty()
    {
        return Some(info);
    }

    let path = std::env::var("SSH_USER_AUTH").ok()?;
    let text = fs::read_to_string(path).ok()?;
    let text = text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

fn verify_fingerprint(
    identity: &str,
    acl: &AllowedClient,
    auth_info: Option<&str>,
) -> Result<(), DispatchError> {
    let Some(expected) = acl.fingerprint.as_deref() else {
        return Ok(());
    };
    let Some(auth_info) = auth_info else {
        return Err(DispatchError::MissingAuthInfo {
            identity: identity.to_string(),
        });
    };
    if auth_info_matches_fingerprint(auth_info, expected) {
        Ok(())
    } else {
        Err(DispatchError::FingerprintMismatch {
            identity: identity.to_string(),
        })
    }
}

fn auth_info_matches_fingerprint(auth_info: &str, expected: &str) -> bool {
    if auth_info.split_whitespace().any(|part| part == expected) {
        return true;
    }

    for line in auth_info.lines() {
        let mut parts = line.split_whitespace();
        let Some(method) = parts.next() else {
            continue;
        };
        if method != "publickey" {
            continue;
        }
        let Some(key_type) = parts.next() else {
            continue;
        };
        let Some(key_blob) = parts.next() else {
            continue;
        };
        if fingerprint_from_public_key_parts(key_type, key_blob).as_deref() == Some(expected) {
            return true;
        }
    }

    false
}

fn fingerprint_from_public_key_parts(key_type: &str, key_blob: &str) -> Option<String> {
    let mut child = Command::new("ssh-keygen")
        .args(["-l", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;

    {
        use std::io::Write;
        let stdin = child.stdin.as_mut()?;
        writeln!(stdin, "{key_type} {key_blob}").ok()?;
    }

    let output = child.wait_with_output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    stdout.split_whitespace().nth(1).map(str::to_string)
}

#[cfg(test)]
fn fingerprint_from_public_key_line(line: &str) -> Option<String> {
    let mut parts = line.split_whitespace();
    let key_type = parts.next()?;
    let key_blob = parts.next()?;
    fingerprint_from_public_key_parts(key_type, key_blob)
}

#[cfg(test)]
fn ssh_keygen_available() -> bool {
    Command::new("ssh-keygen")
        .arg("-V")
        .output()
        .map(|_| true)
        .unwrap_or(false)
}

#[cfg(test)]
fn temp_path(prefix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

#[cfg(test)]
fn generate_test_public_key() -> Option<(std::path::PathBuf, String, String)> {
    if !ssh_keygen_available() {
        return None;
    }
    let key_path = temp_path("arctern-dispatch-test-key");
    let status = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-f"])
        .arg(&key_path)
        .status()
        .ok()?;
    if !status.success() {
        return None;
    }
    let pub_path = key_path.with_extension("pub");
    let public_key = fs::read_to_string(&pub_path).ok()?;
    let fingerprint = fingerprint_from_public_key_line(&public_key)?;
    let _ = fs::remove_file(&pub_path);
    Some((key_path, public_key, fingerprint))
}

#[cfg(test)]
fn cleanup_test_key(path: &std::path::Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("pub"));
}

#[cfg(test)]
fn with_env_var<T>(key: &str, value: Option<&str>, f: impl FnOnce() -> T) -> T {
    let old = std::env::var_os(key);
    match value {
        Some(v) => unsafe { std::env::set_var(key, v) },
        None => unsafe { std::env::remove_var(key) },
    }
    let result = f();
    match old {
        Some(v) => unsafe { std::env::set_var(key, v) },
        None => unsafe { std::env::remove_var(key) },
    }
    result
}

#[cfg(test)]
fn with_clean_auth_env<T>(f: impl FnOnce() -> T) -> T {
    with_env_var("SSH_AUTH_INFO_0", None, || {
        with_env_var("SSH_USER_AUTH", None, f)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arctern_config::AllowedClient;

    fn cfg(identity: &str, jobs: &[&str], ops: &[&str]) -> Config {
        cfg_with_fingerprint(identity, jobs, ops, None)
    }

    fn cfg_with_fingerprint(
        identity: &str,
        jobs: &[&str],
        ops: &[&str],
        fingerprint: Option<&str>,
    ) -> Config {
        Config {
            allowed_clients: vec![AllowedClient {
                identity: identity.into(),
                fingerprint: fingerprint.map(str::to_string),
                jobs: jobs.iter().map(|s| (*s).to_string()).collect(),
                operations: ops.iter().map(|s| (*s).to_string()).collect(),
                root_fs: None,
                recv: Default::default(),
            }],
            ..Config::default()
        }
    }

    #[test]
    fn happy_path_control() {
        let c = cfg("laptop_nova", &["backup"], &["control", "recv"]);
        let a = decide(
            "laptop_nova",
            "arctern stdinserver backup control",
            None,
            &c,
        )
        .unwrap();
        assert_eq!(
            a,
            DispatchAction::Control {
                job: "backup".into()
            }
        );
    }

    #[test]
    fn happy_path_recv() {
        let c = cfg("laptop_nova", &["backup"], &["control", "recv"]);
        let a = decide(
            "laptop_nova",
            "arctern stdinserver backup recv extra args ignored",
            None,
            &c,
        )
        .unwrap();
        assert_eq!(
            a,
            DispatchAction::Recv {
                job: "backup".into()
            }
        );
    }

    #[test]
    fn malformed_command_rejected() {
        let c = cfg("laptop_nova", &["backup"], &["control"]);
        let err = decide("laptop_nova", "ls -la", None, &c).unwrap_err();
        assert!(matches!(err, DispatchError::MalformedCommand(_)));
    }

    #[test]
    fn unknown_identity_rejected() {
        let c = cfg("laptop_nova", &["backup"], &["control"]);
        let err = decide("intruder", "arctern stdinserver backup control", None, &c).unwrap_err();
        assert!(matches!(err, DispatchError::UnknownIdentity(_)));
    }

    #[test]
    fn job_not_in_acl_rejected() {
        // Job membership is enforced for replication ops (recv), not for
        // the per-peer control channel.
        let c = cfg("laptop_nova", &["backup"], &["control", "recv"]);
        let err = decide(
            "laptop_nova",
            "arctern stdinserver other_job recv",
            None,
            &c,
        )
        .unwrap_err();
        assert!(matches!(err, DispatchError::JobNotAllowed { .. }));
    }

    #[test]
    fn control_channel_is_not_job_scoped() {
        // The active side opens the control channel as `<job> = control`,
        // which need not appear in the identity's configured jobs.
        let c = cfg("laptop_nova", &["backup"], &["control"]);
        let a = decide(
            "laptop_nova",
            "arctern stdinserver control control",
            None,
            &c,
        )
        .unwrap();
        assert_eq!(
            a,
            DispatchAction::Control {
                job: "control".into()
            }
        );
    }

    #[test]
    fn op_not_in_acl_rejected() {
        let c = cfg("laptop_nova", &["backup"], &["control"]);
        let err = decide("laptop_nova", "arctern stdinserver backup recv", None, &c).unwrap_err();
        assert!(matches!(err, DispatchError::OpNotAllowed { .. }));
    }

    #[test]
    fn unsupported_op_with_acl_returns_unsupported() {
        let c = cfg("laptop_nova", &["backup"], &["control", "weird"]);
        let a = decide("laptop_nova", "arctern stdinserver backup weird", None, &c).unwrap();
        assert!(matches!(a, DispatchAction::Unsupported { .. }));
    }

    #[test]
    fn fingerprint_match_allows_dispatch() {
        let c = cfg_with_fingerprint(
            "laptop_nova",
            &["backup"],
            &["control"],
            Some("SHA256:abc123"),
        );
        let a = decide(
            "laptop_nova",
            "arctern stdinserver backup control",
            Some("publickey SHA256:abc123"),
            &c,
        )
        .unwrap();
        assert!(matches!(a, DispatchAction::Control { .. }));
    }

    #[test]
    fn fingerprint_mismatch_rejected() {
        let c = cfg_with_fingerprint(
            "laptop_nova",
            &["backup"],
            &["control"],
            Some("SHA256:abc123"),
        );
        let err = decide(
            "laptop_nova",
            "arctern stdinserver backup control",
            Some("publickey SHA256:def456"),
            &c,
        )
        .unwrap_err();
        assert!(matches!(err, DispatchError::FingerprintMismatch { .. }));
    }

    #[test]
    fn exposes_auth_info_public_key_blob_matches_fingerprint() {
        let Some((key_path, public_key, fingerprint)) = generate_test_public_key() else {
            eprintln!("skipping: ssh-keygen unavailable");
            return;
        };
        let auth_info = format!("publickey {public_key}");
        assert!(auth_info_matches_fingerprint(&auth_info, &fingerprint));
        cleanup_test_key(&key_path);
    }

    #[test]
    fn ssh_user_auth_file_is_used_when_auth_info_env_missing() {
        let Some((key_path, public_key, fingerprint)) = generate_test_public_key() else {
            eprintln!("skipping: ssh-keygen unavailable");
            return;
        };
        let auth_file = temp_path("arctern-dispatch-ssh-user-auth");
        fs::write(&auth_file, format!("publickey {public_key}")).expect("write auth file");

        let got = with_clean_auth_env(|| {
            with_env_var(
                "SSH_USER_AUTH",
                Some(auth_file.to_str().unwrap()),
                ssh_auth_info,
            )
        });
        assert!(auth_info_matches_fingerprint(
            got.as_deref().expect("auth info from SSH_USER_AUTH"),
            &fingerprint
        ));

        let _ = fs::remove_file(&auth_file);
        cleanup_test_key(&key_path);
    }

    #[test]
    fn configured_fingerprint_requires_auth_info() {
        let c = cfg_with_fingerprint(
            "laptop_nova",
            &["backup"],
            &["control"],
            Some("SHA256:abc123"),
        );
        let err = decide(
            "laptop_nova",
            "arctern stdinserver backup control",
            None,
            &c,
        )
        .unwrap_err();
        assert!(matches!(err, DispatchError::MissingAuthInfo { .. }));
    }
}
