//! arctern configuration loader.
//!
//! Leaf crate: no `tokio`, no `palimpsest`, no `axum`. Both the daemon
//! and `arctern configcheck` consume this; future slices' tooling will
//! too. Per CLAUDE.md / spec NFR-002, this is the only place in arctern
//! source allowed to use `regex::` — config parsing, not ZFS invocation.

use std::path::Path;

use thiserror::Error;

pub mod filter;
pub mod grid;
pub mod prune;
pub mod schema;

pub use grid::{GridParseError, GridSpec, KeepCount, RetentionInterval, SnapshotEntry};
pub use prune::{PruneError, evaluate as evaluate_keep_rules};
pub use schema::{
    AllowedClient, Config, FilesystemFilter, JobConfig, KeepRule, PeerConfig, PruningConfig,
    PushJobConfig, PushTarget, SendFlagsConfig, SnapJobConfig, SnapshotFilterConfig,
    SnapshottingConfig,
};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("read {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("validate {path}: {message}")]
    Validate { path: String, message: String },
}

pub fn load_from_path(path: &Path) -> Result<Config, ConfigError> {
    let display = path.display().to_string();
    let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: display.clone(),
        source,
    })?;
    let cfg: Config = toml::from_str(&raw).map_err(|source| ConfigError::Parse {
        path: display.clone(),
        source,
    })?;
    validate(&cfg).map_err(|message| ConfigError::Validate {
        path: display,
        message,
    })?;
    Ok(cfg)
}

/// Semantic validation that serde cannot express. Returns a string with
/// the field path on failure (`jobs[N].pruning.keep[M].grid` shape).
pub fn validate(cfg: &Config) -> Result<(), String> {
    use std::collections::BTreeSet;
    let mut seen_names: BTreeSet<&str> = BTreeSet::new();
    for (i, job) in cfg.jobs.iter().enumerate() {
        let name = job.name();
        if !seen_names.insert(name) {
            return Err(format!("jobs[{i}]: duplicate name {name:?}"));
        }
        match job {
            JobConfig::Snap(s) => validate_snap(i, s)?,
            JobConfig::Push(s) => validate_push(i, s)?,
        }
    }
    Ok(())
}

fn validate_push(idx: usize, s: &PushJobConfig) -> Result<(), String> {
    if s.name.is_empty() {
        return Err(format!("jobs[{idx}].name: must not be empty"));
    }
    if s.target.root_fs.is_empty() {
        return Err(format!("jobs[{idx}].target.root_fs: must not be empty"));
    }
    if s.target.root_fs.starts_with('/') || s.target.root_fs.ends_with('/') {
        return Err(format!(
            "jobs[{idx}].target.root_fs: {:?} must not start or end with '/'",
            s.target.root_fs
        ));
    }
    // Reuse snap's filesystem-filter validation. The shapes are identical;
    // calling through validate_snap would also try to read pruning, so we
    // walk filesystems explicitly here.
    for (fi, f) in s.filesystems.iter().enumerate() {
        if !f.exclude.is_empty() && !f.recursive {
            return Err(format!(
                "jobs[{idx}].filesystems[{fi}]: exclude requires recursive = true"
            ));
        }
        if f.recursive {
            for (ei, e) in f.exclude.iter().enumerate() {
                let same = e == &f.path;
                let descendant = e.starts_with(&format!("{}/", f.path));
                if !(same || descendant) {
                    return Err(format!(
                        "jobs[{idx}].filesystems[{fi}].exclude[{ei}]: {e:?} is not a descendant of {:?}",
                        f.path
                    ));
                }
            }
        }
    }
    // Snapshot filter: exactly one of prefix xor regex.
    match (&s.snapshot_filter.prefix, &s.snapshot_filter.regex) {
        (None, None) => {
            return Err(format!(
                "jobs[{idx}].snapshot_filter: exactly one of prefix or regex required"
            ));
        }
        (Some(_), Some(_)) => {
            return Err(format!(
                "jobs[{idx}].snapshot_filter: prefix and regex are mutually exclusive"
            ));
        }
        (None, Some(re)) => {
            if let Err(e) = regex::Regex::new(re) {
                return Err(format!(
                    "jobs[{idx}].snapshot_filter.regex: {re:?}: {e}"
                ));
            }
        }
        (Some(_), None) => {}
    }
    Ok(())
}

fn validate_snap(idx: usize, s: &SnapJobConfig) -> Result<(), String> {
    if s.name.is_empty() {
        return Err(format!("jobs[{idx}].name: must not be empty"));
    }
    for (fi, f) in s.filesystems.iter().enumerate() {
        if !f.exclude.is_empty() && !f.recursive {
            return Err(format!(
                "jobs[{idx}].filesystems[{fi}]: exclude requires recursive = true"
            ));
        }
        if f.recursive {
            for (ei, e) in f.exclude.iter().enumerate() {
                let same = e == &f.path;
                let descendant = e.starts_with(&format!("{}/", f.path));
                if !(same || descendant) {
                    return Err(format!(
                        "jobs[{idx}].filesystems[{fi}].exclude[{ei}]: {e:?} is not a descendant of {:?}",
                        f.path
                    ));
                }
            }
        }
    }
    // Compile every regex once to surface bad patterns early.
    for (ki, k) in s.pruning.keep.iter().enumerate() {
        let pat = match k {
            KeepRule::Grid { regex, .. } => regex,
            KeepRule::Regex { regex, .. } => regex,
        };
        if let Err(e) = regex::Regex::new(pat) {
            return Err(format!(
                "jobs[{idx}].pruning.keep[{ki}].regex: {pat:?}: {e}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Config, ConfigError> {
        let cfg: Config =
            toml::from_str(s).map_err(|source| ConfigError::Parse {
                path: "<test>".into(),
                source,
            })?;
        validate(&cfg).map_err(|message| ConfigError::Validate {
            path: "<test>".into(),
            message,
        })?;
        Ok(cfg)
    }

    const MIN_OK: &str = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
type = "periodic"
interval = "1s"
prefix = "x_"
[jobs.pruning]
keep = []
"#;

    #[test]
    fn minimal_valid_parses() {
        let c = parse(MIN_OK).unwrap();
        assert_eq!(c.jobs.len(), 1);
    }

    #[test]
    fn empty_jobs_table_is_ok() {
        let c = parse("").unwrap();
        assert!(c.jobs.is_empty());
    }

    #[test]
    fn duplicate_names_rejected() {
        let s = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
type = "periodic"
interval = "1s"
prefix = "x_"
[jobs.pruning]
keep = []

[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
type = "periodic"
interval = "1s"
prefix = "x_"
[jobs.pruning]
keep = []
"#;
        let err = parse(s).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("duplicate"));
    }

    #[test]
    fn unknown_top_level_key_rejected() {
        let s = "what = 1\n";
        assert!(parse(s).is_err());
    }

    #[test]
    fn unknown_job_type_rejected() {
        let s = r#"
[[jobs]]
type = "push"
"#;
        assert!(parse(s).is_err());
    }

    #[test]
    fn exclude_without_recursive_rejected() {
        let s = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
exclude = ["tank/foo"]
[jobs.snapshotting]
type = "periodic"
interval = "1s"
prefix = "x_"
[jobs.pruning]
keep = []
"#;
        assert!(parse(s).is_err());
    }

    #[test]
    fn exclude_outside_subtree_rejected() {
        let s = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank/data"
recursive = true
exclude = ["other/foo"]
[jobs.snapshotting]
type = "periodic"
interval = "1s"
prefix = "x_"
[jobs.pruning]
keep = []
"#;
        assert!(parse(s).is_err());
    }

    #[test]
    fn bad_grid_rejected_via_serde_path() {
        let s = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
type = "periodic"
interval = "1s"
prefix = "x_"
[[jobs.pruning.keep]]
type = "grid"
grid = "6x4z"
regex = ".*"
"#;
        let err = parse(s).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("4z") || msg.contains("duration"));
    }

    #[test]
    fn bad_regex_rejected_at_validate() {
        let s = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
type = "periodic"
interval = "1s"
prefix = "x_"
[[jobs.pruning.keep]]
type = "regex"
regex = "("
"#;
        assert!(parse(s).is_err());
    }

    #[test]
    fn state_dir_optional() {
        let c = parse("state_dir = \"/var/lib/arctern\"\n").unwrap();
        assert_eq!(
            c.state_dir.as_deref().map(|p| p.display().to_string()),
            Some("/var/lib/arctern".to_string())
        );
    }

    const PUSH_OK: &str = r#"
[[jobs]]
type = "push"
name = "push_to_server"
peer = "home"
interval = "15m"
[[jobs.filesystems]]
path = "okdata/data/home"
[jobs.target]
root_fs = "okdata/backups/laptop"
[jobs.snapshot_filter]
prefix = "zrepl_"
"#;

    #[test]
    fn minimal_push_parses() {
        let c = parse(PUSH_OK).unwrap();
        let JobConfig::Push(p) = &c.jobs[0] else {
            panic!("expected Push")
        };
        assert_eq!(p.name, "push_to_server");
        assert_eq!(p.peer, "home");
        assert_eq!(p.target.root_fs, "okdata/backups/laptop");
        assert_eq!(p.snapshot_filter.prefix.as_deref(), Some("zrepl_"));
    }

    #[test]
    fn push_send_flags_default_true_when_omitted() {
        let c = parse(PUSH_OK).unwrap();
        let JobConfig::Push(p) = &c.jobs[0] else {
            panic!("expected Push")
        };
        assert!(p.send.encrypted);
        assert!(p.send.embedded_data);
        assert!(p.send.compressed);
        assert!(p.send.large_blocks);
    }

    #[test]
    fn push_send_flags_can_be_overridden() {
        let s = r#"
[[jobs]]
type = "push"
name = "p"
peer = "p"
interval = "1s"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "tank/sink"
[jobs.send]
encrypted = false
[jobs.snapshot_filter]
prefix = "x_"
"#;
        let c = parse(s).unwrap();
        let JobConfig::Push(p) = &c.jobs[0] else {
            panic!("expected Push")
        };
        assert!(!p.send.encrypted);
        assert!(p.send.embedded_data);
    }

    #[test]
    fn push_filter_neither_prefix_nor_regex_rejected() {
        let s = r#"
[[jobs]]
type = "push"
name = "p"
peer = "p"
interval = "1s"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "tank/sink"
[jobs.snapshot_filter]
"#;
        let err = parse(s).unwrap_err();
        assert!(format!("{err}").contains("exactly one"));
    }

    #[test]
    fn push_filter_both_prefix_and_regex_rejected() {
        let s = r#"
[[jobs]]
type = "push"
name = "p"
peer = "p"
interval = "1s"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "tank/sink"
[jobs.snapshot_filter]
prefix = "zrepl_"
regex = "^zrepl_.*"
"#;
        let err = parse(s).unwrap_err();
        assert!(format!("{err}").contains("mutually exclusive"));
    }

    #[test]
    fn push_bad_regex_rejected() {
        let s = r#"
[[jobs]]
type = "push"
name = "p"
peer = "p"
interval = "1s"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "tank/sink"
[jobs.snapshot_filter]
regex = "("
"#;
        assert!(parse(s).is_err());
    }

    #[test]
    fn push_bad_root_fs_rejected() {
        for root in &["", "/tank", "tank/"] {
            let s = format!(
                r#"
[[jobs]]
type = "push"
name = "p"
peer = "p"
interval = "1s"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "{root}"
[jobs.snapshot_filter]
prefix = "x_"
"#
            );
            let err = parse(&s).unwrap_err();
            assert!(format!("{err}").contains("root_fs"), "expected root_fs error for {root:?}");
        }
    }

    #[test]
    fn push_missing_peer_rejected() {
        let s = r#"
[[jobs]]
type = "push"
name = "p"
interval = "1s"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "tank/sink"
[jobs.snapshot_filter]
prefix = "x_"
"#;
        assert!(parse(s).is_err());
    }

    #[test]
    fn push_bad_interval_rejected() {
        let s = r#"
[[jobs]]
type = "push"
name = "p"
peer = "p"
interval = "4 fortnights"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "tank/sink"
[jobs.snapshot_filter]
prefix = "x_"
"#;
        assert!(parse(s).is_err());
    }

    #[test]
    fn push_filter_as_regex_str_escapes_prefix() {
        let f = SnapshotFilterConfig {
            prefix: Some("zrepl_".into()),
            regex: None,
        };
        assert_eq!(f.as_regex_str().as_deref(), Some("^zrepl_"));
        // Special regex chars in the prefix must be escaped.
        let f = SnapshotFilterConfig {
            prefix: Some("a.b".into()),
            regex: None,
        };
        assert_eq!(f.as_regex_str().as_deref(), Some("^a\\.b"));
    }

    #[test]
    fn bad_interval_rejected() {
        let s = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
type = "periodic"
interval = "4 fortnights"
prefix = "x_"
[jobs.pruning]
keep = []
"#;
        assert!(parse(s).is_err());
    }
}
