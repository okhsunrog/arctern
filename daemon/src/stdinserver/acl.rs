//! The `[[allowed_clients]]` checks every receiver-side handler applies:
//! is the operation granted, and is the dataset inside `root_fs`.

use arctern_config::AllowedClient;
use arctern_transport::{ErrorCode, WireError};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AclError {
    #[error("identity {identity:?} is not allowed for control operation {op:?}")]
    OperationNotGranted { identity: String, op: &'static str },
    #[error("{dataset:?} is not under allowed root_fs {root:?}")]
    NotUnderRootFs { dataset: String, root: String },
}

impl From<AclError> for WireError {
    fn from(e: AclError) -> Self {
        WireError::new(ErrorCode::Unauthorized, e.to_string())
    }
}

/// `op` must be granted explicitly. `allow_legacy_control` lets the
/// umbrella `control` grant cover read-only operations; mutating ones
/// (discard, proxy_admin) never accept it.
pub fn check_operation(
    acl: &AllowedClient,
    op: &'static str,
    allow_legacy_control: bool,
) -> Result<(), AclError> {
    let granted = acl.operations.iter().any(|configured| configured == op)
        || (allow_legacy_control
            && acl
                .operations
                .iter()
                .any(|configured| configured == "control"));
    if granted {
        Ok(())
    } else {
        Err(AclError::OperationNotGranted {
            identity: acl.identity.clone(),
            op,
        })
    }
}

/// `dataset` must equal `root_fs` or sit below it. No `root_fs` means
/// no restriction.
pub fn check_root_fs(acl: &AllowedClient, dataset: &str) -> Result<(), AclError> {
    let Some(root) = acl.root_fs.as_deref() else {
        return Ok(());
    };
    if dataset == root || dataset.starts_with(&format!("{root}/")) {
        return Ok(());
    }
    Err(AclError::NotUnderRootFs {
        dataset: dataset.to_string(),
        root: root.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acl(root_fs: Option<&str>, ops: &[&str]) -> AllowedClient {
        AllowedClient {
            identity: "laptop".into(),
            fingerprint: None,
            jobs: vec![],
            operations: ops.iter().map(|s| s.to_string()).collect(),
            root_fs: root_fs.map(str::to_string),
            recv: Default::default(),
        }
    }

    #[test]
    fn root_fs_accepts_the_root_and_its_subtree_only() {
        let a = acl(Some("tank/backups"), &[]);
        assert!(check_root_fs(&a, "tank/backups").is_ok());
        assert!(check_root_fs(&a, "tank/backups/laptop").is_ok());
        // A sibling that merely shares the prefix string is outside.
        assert!(check_root_fs(&a, "tank/backups2").is_err());
        assert!(check_root_fs(&a, "tank").is_err());
        assert!(check_root_fs(&acl(None, &[]), "anything").is_ok());
    }

    #[test]
    fn the_control_umbrella_covers_reads_but_not_mutations() {
        let a = acl(None, &["control"]);
        assert!(check_operation(&a, "control:list_snapshots", true).is_ok());
        assert!(check_operation(&a, "control:discard_partial_recv", false).is_err());
        let fine = acl(None, &["control:discard_partial_recv"]);
        assert!(check_operation(&fine, "control:discard_partial_recv", false).is_ok());
    }
}
