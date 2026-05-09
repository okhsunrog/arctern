//! `arctern stdinserver-dispatch <identity>` entry point.
//!
//! sshd invokes this via `authorized_keys` `command="..."`. The dispatcher
//! parses `SSH_ORIGINAL_COMMAND`, looks up the identity in
//! `[[allowed_clients]]`, and (in this commit) logs an unimplemented
//! exit. The `control` and `recv` handlers land in steps 7 and 8.

use std::path::Path;
use std::sync::Arc;

use arctern_config::{AllowedClient, Config};
use palimpsest::runner::{CommandRunner, RealRunner};

/// Outcome of `dispatch::run`. Encoded so step 7/8 can fork on it
/// without re-parsing the command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchAction {
    Control { job: String },
    Recv { job: String },
    /// Returned for unsupported / not-yet-implemented operations. Caller
    /// should log + exit cleanly with a non-zero code.
    Unsupported { reason: String },
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
}

/// Top-level entry. Loads config, parses argv + env, validates ACL,
/// dispatches. Test/legacy callers without a SQLite pool use this entry.
#[allow(dead_code)]
pub async fn run(identity: &str, config_path: &Path) -> eyre::Result<()> {
    let config = arctern_config::load_from_path(config_path)
        .map_err(|e| eyre::eyre!("config load: {e}"))?;
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
    let action = match decide(identity, &original, &config) {
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
    if !acl.jobs.iter().any(|j| j == job) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arctern_config::AllowedClient;

    fn cfg(identity: &str, jobs: &[&str], ops: &[&str]) -> Config {
        Config {
            allowed_clients: vec![AllowedClient {
                identity: identity.into(),
                fingerprint: None,
                jobs: jobs.iter().map(|s| (*s).to_string()).collect(),
                operations: ops.iter().map(|s| (*s).to_string()).collect(),
                root_fs: None,
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
        let err = decide("laptop_nova", "ls -la", &c).unwrap_err();
        assert!(matches!(err, DispatchError::MalformedCommand(_)));
    }

    #[test]
    fn unknown_identity_rejected() {
        let c = cfg("laptop_nova", &["backup"], &["control"]);
        let err =
            decide("intruder", "arctern stdinserver backup control", &c).unwrap_err();
        assert!(matches!(err, DispatchError::UnknownIdentity(_)));
    }

    #[test]
    fn job_not_in_acl_rejected() {
        let c = cfg("laptop_nova", &["backup"], &["control"]);
        let err = decide(
            "laptop_nova",
            "arctern stdinserver other_job control",
            &c,
        )
        .unwrap_err();
        assert!(matches!(err, DispatchError::JobNotAllowed { .. }));
    }

    #[test]
    fn op_not_in_acl_rejected() {
        let c = cfg("laptop_nova", &["backup"], &["control"]);
        let err = decide(
            "laptop_nova",
            "arctern stdinserver backup recv",
            &c,
        )
        .unwrap_err();
        assert!(matches!(err, DispatchError::OpNotAllowed { .. }));
    }

    #[test]
    fn unsupported_op_with_acl_returns_unsupported() {
        let c = cfg("laptop_nova", &["backup"], &["control", "weird"]);
        let a = decide(
            "laptop_nova",
            "arctern stdinserver backup weird",
            &c,
        )
        .unwrap();
        assert!(matches!(a, DispatchAction::Unsupported { .. }));
    }
}
