//! arctern configuration loader.
//!
//! Leaf crate: no `tokio`, no `zfskit`, no `axum`. Both the daemon
//! and `arctern configcheck` consume this; future slices' tooling will
//! too. Per CLAUDE.md / spec NFR-002, this is the only place in arctern
//! source allowed to use `regex::` — config parsing, not ZFS invocation.

use std::path::Path;

use thiserror::Error;

use crate::zfs_names::validate_dataset_name;

pub mod filter;
pub mod grid;
pub mod prune;
pub mod schema;
pub mod zfs_names;

pub use grid::{GridParseError, GridSpec, KeepCount, RetentionInterval, SnapshotEntry};
pub use prune::{PruneError, evaluate as evaluate_keep_rules};
pub use schema::{
    AllowedClient, Config, Defaults, FilesystemFilter, JobConfig, KeepRule, PeerConfig, PeerMode,
    PruneJobConfig, PruningConfig, PruningDefaults, PushJobConfig, PushTarget, RecvConfig,
    ReplicateMode, RouteConfig, SendFlagsConfig, SnapJobConfig, SnapshotFilterConfig,
    SnapshottingConfig, SnapshottingDefaults,
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
    load_from_str(&raw, &display)
}

pub fn load_from_str(raw: &str, display: &str) -> Result<Config, ConfigError> {
    let mut cfg: Config =
        toml::from_str(raw).map_err(|source| classify_toml_error(raw, display, source))?;
    resolve_defaults(&mut cfg).map_err(|message| ConfigError::Validate {
        path: display.to_string(),
        message,
    })?;
    validate(&cfg).map_err(|message| ConfigError::Validate {
        path: display.to_string(),
        message,
    })?;
    Ok(cfg)
}

/// `toml::from_str` reports everything as a "TOML parse error" -- syntax
/// errors, but also missing/unknown fields and the `custom` errors our
/// field deserializers raise. Only the first kind is a parse error; the
/// rest are schema violations in a well-formed document and should read
/// as such, with the line kept so the operator still knows where to look.
fn classify_toml_error(raw: &str, display: &str, source: toml::de::Error) -> ConfigError {
    if raw.parse::<toml::Table>().is_err() {
        return ConfigError::Parse {
            path: display.to_string(),
            source,
        };
    }
    let message = match source.span() {
        Some(span) => {
            let line = raw[..span.start.min(raw.len())].matches('\n').count() + 1;
            format!("line {line}: {}", source.message())
        }
        None => source.message().to_string(),
    };
    ConfigError::Validate {
        path: display.to_string(),
        message,
    }
}

/// Fill missing per-job fields from `[defaults]`. Mutates in place;
/// downstream code can rely on `SnapJobConfig::snapshotting()` etc.
/// not panicking. Returns the offending `jobs[N].<field>` path when a
/// required field is missing from both the job and the defaults.
pub fn resolve_defaults(cfg: &mut Config) -> Result<(), String> {
    use schema::{KeepRule, PruningConfig, SnapshotFilterConfig};

    let defaults_prefix = cfg.defaults.prefix.clone();
    let defaults_snapshotting = cfg.defaults.snapshotting.clone();
    let defaults_pruning = cfg.defaults.pruning.clone();

    // Normalise the single-route peer shorthand: `ssh_target = "x"` →
    // `routes = [{ name = "default", ssh_target = "x" }]`. Downstream
    // code can then rely on `routes` being the single source of truth.
    for (i, peer) in cfg.peers.iter_mut().enumerate() {
        match (peer.ssh_target.take(), peer.routes.is_empty()) {
            (Some(target), true) => {
                peer.routes = vec![schema::RouteConfig {
                    name: "default".into(),
                    ssh_target: target,
                    auto: true,
                }];
            }
            (None, false) => {}
            (Some(_), false) => {
                return Err(format!(
                    "peers[{i}]: `ssh_target` and `routes` are mutually exclusive — use one or the other"
                ));
            }
            (None, true) => {
                return Err(format!(
                    "peers[{i}]: missing connection — set `ssh_target = \"...\"` or [[peers.routes]]"
                ));
            }
        }
    }

    // Build pruning keep-rules from `[defaults.pruning]` against the
    // prefix the job actually snapshots with. Returns `None` (without
    // erroring) when defaults aren't available — empty `keep` is a valid
    // "do nothing" prune so we leave it alone rather than rejecting the
    // config.
    //
    // The prefix is a parameter, not `[defaults].prefix`, because the
    // two differ exactly when a job sets its own. Building the rules
    // from the defaults then aimed the grid at another job's snapshots
    // while `protect_non_prefixed` classified this job's own as
    // non-prefixed — protecting them from every rule, forever.
    let build_default_pruning = |prefix: &str| -> Option<PruningConfig> {
        let pd = defaults_pruning.as_ref()?;
        let anchored = format!("^{}.*", regex::escape(prefix));
        let mut keep: Vec<KeepRule> = vec![KeepRule::Grid {
            grid: pd.grid.clone(),
            regex: anchored.clone(),
        }];
        if pd.protect_non_prefixed {
            keep.push(KeepRule::Regex {
                regex: anchored,
                negate: true,
            });
        }
        Some(PruningConfig { keep })
    };

    for (i, job) in cfg.jobs.iter_mut().enumerate() {
        match job {
            JobConfig::Snap(s) => {
                // Fill any field the per-job [jobs.snapshotting] left
                // blank from defaults. Empty fields propagate as
                // errors (a snap job needs both an interval and a
                // prefix to operate).
                if s.snapshotting.interval == std::time::Duration::ZERO {
                    s.snapshotting.interval = defaults_snapshotting
                        .as_ref()
                        .map(|d| d.interval)
                        .ok_or_else(|| {
                            format!(
                                "jobs[{i}].snapshotting.interval: missing — set [jobs.snapshotting].interval or [defaults.snapshotting]"
                            )
                        })?;
                }
                if s.snapshotting.prefix.is_empty() {
                    s.snapshotting.prefix = defaults_prefix.clone().ok_or_else(|| {
                        format!(
                            "jobs[{i}].snapshotting.prefix: missing — set [defaults.prefix] or [jobs.snapshotting].prefix"
                        )
                    })?;
                }
                // Resolved just above, so this is the prefix the job will
                // actually create snapshots with.
                if s.pruning.keep.is_empty()
                    && let Some(pd) = build_default_pruning(&s.snapshotting.prefix)
                {
                    s.pruning = pd;
                }
            }
            JobConfig::Push(p) => {
                // Resolve `peer = "x"` → `targets = ["x"]`. Reject
                // both-given and neither-given as config errors.
                match (p.peer.take(), p.targets.is_empty()) {
                    (Some(single), true) => p.targets = vec![single],
                    (None, false) => {}
                    (Some(_), false) => {
                        return Err(format!(
                            "jobs[{i}]: `peer` and `targets` are mutually exclusive — use one or the other"
                        ));
                    }
                    (None, true) => {
                        return Err(format!(
                            "jobs[{i}]: missing target peer — set `peer = \"...\"` or `targets = [\"...\"]`"
                        ));
                    }
                }
                if p.snapshot_filter.prefix.is_none()
                    && p.snapshot_filter.regex.is_none()
                    && let Some(prefix) = defaults_prefix.clone()
                {
                    p.snapshot_filter = SnapshotFilterConfig {
                        prefix: Some(prefix),
                        regex: None,
                    };
                }
                // Don't error if filter still unset here — let
                // validate_push surface the canonical "exactly one of
                // prefix or regex required" message.
            }
            JobConfig::Prune(p) => {
                // A standalone prune job does not snapshot, so it has no
                // prefix of its own; the defaults' prefix is what it is
                // meant to clean up after.
                if p.pruning.keep.is_empty()
                    && let Some(prefix) = defaults_prefix.as_deref()
                    && let Some(pd) = build_default_pruning(prefix)
                {
                    p.pruning = pd;
                }
            }
        }
    }
    Ok(())
}

/// Semantic validation that serde cannot express. Returns a string with
/// the field path on failure (`jobs[N].pruning.keep[M].grid` shape).
pub fn validate(cfg: &Config) -> Result<(), String> {
    use std::collections::BTreeSet;
    let mut peer_names: BTreeSet<&str> = BTreeSet::new();
    for (i, peer) in cfg.peers.iter().enumerate() {
        validate_identifier(&peer.name).map_err(|e| format!("peers[{i}].name: {e}"))?;
        if !peer_names.insert(peer.name.as_str()) {
            return Err(format!("peers[{i}]: duplicate name {:?}", peer.name));
        }
        // resolve_defaults normalised the shorthand, so routes is the
        // single source of truth here.
        let mut route_names: BTreeSet<&str> = BTreeSet::new();
        for (ri, route) in peer.routes.iter().enumerate() {
            if route.name.is_empty() {
                return Err(format!("peers[{i}].routes[{ri}].name: must not be empty"));
            }
            if route.ssh_target.is_empty() {
                return Err(format!(
                    "peers[{i}].routes[{ri}].ssh_target: must not be empty"
                ));
            }
            if !route_names.insert(route.name.as_str()) {
                return Err(format!(
                    "peers[{i}].routes[{ri}]: duplicate route name {:?}",
                    route.name
                ));
            }
        }
    }
    let mut seen_names: BTreeSet<&str> = BTreeSet::new();
    for (i, job) in cfg.jobs.iter().enumerate() {
        let name = job.name();
        validate_identifier(name).map_err(|e| format!("jobs[{i}].name: {e}"))?;
        if !seen_names.insert(name) {
            return Err(format!("jobs[{i}]: duplicate name {name:?}"));
        }
        match job {
            JobConfig::Snap(s) => validate_snap(i, s)?,
            JobConfig::Push(s) => validate_push(i, s, &peer_names)?,
            JobConfig::Prune(s) => validate_prune(i, s)?,
        }
    }
    // The dispatcher looks an identity up by name and takes the first
    // match, so a duplicate silently shadows the later entry — including
    // one an operator added to widen or narrow that client's grants.
    let mut seen_identities: BTreeSet<&str> = BTreeSet::new();
    for (i, client) in cfg.allowed_clients.iter().enumerate() {
        validate_allowed_client(i, client)?;
        if !seen_identities.insert(client.identity.as_str()) {
            return Err(format!(
                "allowed_clients[{i}]: duplicate identity {:?}",
                client.identity
            ));
        }
    }
    Ok(())
}

/// Longest job or peer name. The replication cursor is
/// `<dataset>#arctern_cursor_G_<16 hex>_J_<job>_P_<peer>` and ZFS caps a
/// full name at 255 bytes, so the two identifiers have to leave room for
/// the dataset. 64 each is far past anything an operator would type and
/// still leaves the budget comfortable.
const MAX_IDENTIFIER_LEN: usize = 64;

/// Job and peer names are not just labels: they are interpolated into
/// the step hold tag, into the cursor bookmark name, and into
/// `SSH_ORIGINAL_COMMAND`, which the dispatcher splits on whitespace.
///
/// `_J_` and `_P_` are refused because those are the markers the cursor
/// scheme parses on. `is_cursor_bookmark_leaf` matches a leaf by
/// `ends_with("_J_<job>_P_<peer>")`, so a job named `x_J_y` replicating
/// to `z` produces a cursor that a job named `y` would also claim — and
/// destroy as a stale one of its own, silently costing incrementality
/// and forcing a full send.
fn validate_identifier(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("must not be empty".into());
    }
    if name.len() > MAX_IDENTIFIER_LEN {
        return Err(format!(
            "must be at most {MAX_IDENTIFIER_LEN} bytes, got {}",
            name.len()
        ));
    }
    zfs_names::validate_name_component(name)?;
    for marker in ["_J_", "_P_"] {
        if name.contains(marker) {
            return Err(format!(
                "must not contain {marker:?}: it separates the job and peer \
                 in replication cursor bookmark names"
            ));
        }
    }
    Ok(())
}

fn validate_allowed_client(idx: usize, client: &AllowedClient) -> Result<(), String> {
    if client.identity.is_empty() {
        return Err(format!(
            "allowed_clients[{idx}].identity: must not be empty"
        ));
    }
    if client.jobs.is_empty() {
        return Err(format!("allowed_clients[{idx}].jobs: must not be empty"));
    }
    if client.operations.is_empty() {
        return Err(format!(
            "allowed_clients[{idx}].operations: must not be empty"
        ));
    }
    // Catch typos early: a misspelled privileged op (e.g. "recieve") would
    // otherwise silently grant nothing and skip the root_fs requirement.
    // Recognised: "recv", "control", and fine-grained "control:*" ops.
    for op in &client.operations {
        let known = op == "recv" || op == "control" || op == "events" || op.starts_with("control:");
        if !known {
            return Err(format!(
                "allowed_clients[{idx}].operations: unknown operation {op:?} (expected \"recv\", \"control\", \"events\", or \"control:*\")"
            ));
        }
    }

    let needs_root_fs = client
        .operations
        .iter()
        .any(|op| op == "recv" || op == "control");
    if needs_root_fs {
        let Some(root_fs) = client.root_fs.as_deref() else {
            return Err(format!(
                "allowed_clients[{idx}].root_fs: required when operations include recv or control"
            ));
        };
        if root_fs.is_empty() {
            return Err(format!("allowed_clients[{idx}].root_fs: must not be empty"));
        }
        if let Err(e) = validate_dataset_name(root_fs) {
            return Err(format!(
                "allowed_clients[{idx}].root_fs: invalid dataset name {root_fs:?}: {e}"
            ));
        }
    }

    Ok(())
}

fn validate_filesystem_filter(idx: usize, fi: usize, f: &FilesystemFilter) -> Result<(), String> {
    if let Err(e) = validate_dataset_name(&f.path) {
        return Err(format!(
            "jobs[{idx}].filesystems[{fi}].path: invalid dataset name {:?}: {e}",
            f.path
        ));
    }
    if !f.exclude.is_empty() && !f.recursive {
        return Err(format!(
            "jobs[{idx}].filesystems[{fi}]: exclude requires recursive = true"
        ));
    }
    if f.recursive {
        for (ei, e) in f.exclude.iter().enumerate() {
            // A trailing `/` marks a subtree exclude (mirrors the
            // include-key syntax); the dataset name is what precedes it.
            let name = e.trim_end_matches('/');
            if let Err(err) = validate_dataset_name(name) {
                return Err(format!(
                    "jobs[{idx}].filesystems[{fi}].exclude[{ei}]: invalid dataset name {e:?}: {err}"
                ));
            }
            let same = name == f.path;
            let descendant = name.starts_with(&format!("{}/", f.path));
            if !(same || descendant) {
                return Err(format!(
                    "jobs[{idx}].filesystems[{fi}].exclude[{ei}]: {e:?} is not a descendant of {:?}",
                    f.path
                ));
            }
        }
    }
    Ok(())
}

fn validate_prune(idx: usize, s: &PruneJobConfig) -> Result<(), String> {
    if s.name.is_empty() {
        return Err(format!("jobs[{idx}].name: must not be empty"));
    }
    if s.filesystems.is_empty() {
        return Err(format!(
            "jobs[{idx}].filesystems: must not be empty (the job would match nothing)"
        ));
    }
    // A prune job whose only purpose is retention, with no rules, keeps
    // nothing and destroys nothing forever. Silently doing nothing is
    // worse than saying so at load.
    if s.pruning().keep.is_empty() {
        return Err(format!(
            "jobs[{idx}].pruning.keep: must not be empty for a prune job"
        ));
    }
    if s.interval.is_zero() {
        return Err(format!(
            "jobs[{idx}].interval: must be longer than zero (a zero interval spins)"
        ));
    }
    for (fi, f) in s.filesystems.iter().enumerate() {
        validate_filesystem_filter(idx, fi, f)?;
    }
    for (ki, k) in s.pruning().keep.iter().enumerate() {
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

/// Parse a human bandwidth figure ("10MiB", "800KiB", "1GiB", plain
/// bytes) into bytes per second.
pub fn parse_bytes_per_sec(s: &str) -> Result<u64, String> {
    let t = s.trim().trim_end_matches("/s").trim();
    let split = t.find(|c: char| !c.is_ascii_digit() && c != '.');
    let (num, unit) = match split {
        Some(i) => t.split_at(i),
        None => (t, ""),
    };
    let n: f64 = num
        .trim()
        .parse()
        .map_err(|e| format!("bad number {num:?}: {e}"))?;
    let mult: u64 = match unit.trim() {
        "" | "B" => 1,
        "KiB" | "K" | "k" => 1 << 10,
        "MiB" | "M" | "m" => 1 << 20,
        "GiB" | "G" | "g" => 1 << 30,
        other => return Err(format!("unknown unit {other:?} (use B/KiB/MiB/GiB)")),
    };
    let v = (n * mult as f64) as u64;
    if v == 0 {
        return Err("bandwidth limit must be positive".into());
    }
    Ok(v)
}

fn validate_push(
    idx: usize,
    s: &PushJobConfig,
    peer_names: &std::collections::BTreeSet<&str>,
) -> Result<(), String> {
    if s.name.is_empty() {
        return Err(format!("jobs[{idx}].name: must not be empty"));
    }
    if let Some(bw) = &s.bandwidth_limit
        && let Err(e) = parse_bytes_per_sec(bw)
    {
        return Err(format!("jobs[{idx}].bandwidth_limit: {e}"));
    }
    if s.target.root_fs.is_empty() {
        return Err(format!("jobs[{idx}].target.root_fs: must not be empty"));
    }
    if let Err(e) = validate_dataset_name(&s.target.root_fs) {
        return Err(format!(
            "jobs[{idx}].target.root_fs: invalid dataset name {:?}: {e}",
            s.target.root_fs
        ));
    }
    // Reuse snap's filesystem-filter validation. The shapes are identical;
    // calling through validate_snap would also try to read pruning, so we
    // walk filesystems explicitly here.
    for (fi, f) in s.filesystems.iter().enumerate() {
        validate_filesystem_filter(idx, fi, f)?;
    }
    // Snapshot filter: exactly one of prefix xor regex.
    let snapshot_filter = s.snapshot_filter();
    match (&snapshot_filter.prefix, &snapshot_filter.regex) {
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
                return Err(format!("jobs[{idx}].snapshot_filter.regex: {re:?}: {e}"));
            }
        }
        (Some(_), None) => {}
    }
    // A typo'd target would otherwise surface only as a perpetual
    // "none of targets connected" at runtime.
    for (ti, t) in s.targets.iter().enumerate() {
        if !peer_names.contains(t.as_str()) {
            return Err(format!(
                "jobs[{idx}].targets[{ti}]: {t:?} does not match any [[peers]] entry"
            ));
        }
    }
    Ok(())
}

fn validate_snap(idx: usize, s: &SnapJobConfig) -> Result<(), String> {
    if s.name.is_empty() {
        return Err(format!("jobs[{idx}].name: must not be empty"));
    }
    for (fi, f) in s.filesystems.iter().enumerate() {
        validate_filesystem_filter(idx, fi, f)?;
    }
    // Compile every regex once to surface bad patterns early.
    for (ki, k) in s.pruning().keep.iter().enumerate() {
        let pat = match k {
            KeepRule::Grid { regex, .. } => regex,
            KeepRule::Regex { regex, .. } => regex,
        };
        if let Err(e) = regex::Regex::new(pat) {
            return Err(format!(
                "jobs[{idx}].pruning.keep[{ki}].regex: {pat:?}: {e}"
            ));
        }
        // The grid keeps the OLDEST snapshot of a bucket (zrepl semantics).
        // When the first bucket is longer than the snapshot interval and
        // not `keep=all`, every new snapshot lands in a bucket that already
        // holds an older one and is destroyed on the spot: the job would
        // create a snapshot every cycle and never retain the newest state.
        // zrepl's documented idiom is a leading `1x<interval>(keep=all)`.
        if let KeepRule::Grid { grid, .. } = k
            && let Some(first) = grid.0.first()
            && first.keep_count != KeepCount::All
            && first.length > s.snapshotting().interval
        {
            return Err(format!(
                "jobs[{idx}].pruning.keep[{ki}].grid: the first bucket ({}) is longer than \
                 the snapshot interval ({}) and is not keep=all, so every new snapshot would be \
                 destroyed as soon as it is taken; start the grid with \
                 `1x{}(keep=all)` or a bucket no longer than the interval",
                humantime::format_duration(first.length),
                humantime::format_duration(s.snapshotting().interval),
                humantime::format_duration(first.length),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Config, ConfigError> {
        load_from_str(s, "<test>")
    }

    const MIN_OK: &str = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
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
interval = "1s"
prefix = "x_"
[jobs.pruning]
keep = []
"#;
        let err = parse(s).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("duplicate"));
    }

    fn snap_job_named(name: &str) -> String {
        format!(
            r#"
[[jobs]]
type = "snap"
name = "{name}"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
interval = "1s"
prefix = "x_"
[jobs.pruning]
keep = []
"#
        )
    }

    #[test]
    fn ordinary_job_names_are_accepted() {
        for name in ["push_to_mira", "snapshot_arch0", "nas-remote", "a.b:c"] {
            parse(&snap_job_named(name)).unwrap_or_else(|e| panic!("{name:?} rejected: {e}"));
        }
    }

    // The name reaches SSH_ORIGINAL_COMMAND, which the dispatcher splits
    // on whitespace, and becomes a bare positional in zfs argv.
    #[test]
    fn job_names_that_break_the_dispatcher_or_zfs_are_rejected() {
        for name in ["two words", "-rf", "a/b", "a@b", "a#b", ""] {
            assert!(
                parse(&snap_job_named(name)).is_err(),
                "{name:?} should be rejected"
            );
        }
    }

    // `is_cursor_bookmark_leaf` matches on `_J_<job>_P_<peer>`, so a job
    // named `x_J_y` produces a cursor that a job named `y` also claims —
    // and destroys as a stale one of its own, costing incrementality.
    #[test]
    fn the_cursor_scheme_markers_are_rejected_in_names() {
        let err = format!("{}", parse(&snap_job_named("x_J_y")).unwrap_err());
        assert!(err.contains("_J_"), "got: {err}");
        assert!(parse(&snap_job_named("x_P_y")).is_err());
    }

    #[test]
    fn over_long_names_are_rejected() {
        let long = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        assert!(parse(&snap_job_named(&long)).is_err());
        assert!(parse(&snap_job_named(&"a".repeat(MAX_IDENTIFIER_LEN))).is_ok());
    }

    fn prune_job(filesystems: &str, keep: &str, interval: &str) -> String {
        format!(
            r#"
[[jobs]]
type = "prune"
name = "p"
interval = "{interval}"
{filesystems}
[jobs.pruning]
keep = [{keep}]
"#
        )
    }

    const FS: &str = "[[jobs.filesystems]]\npath = \"tank\"";
    const KEEP: &str = "{ type = \"grid\", grid = \"1x1d\", regex = \"^x_\" }";

    #[test]
    fn a_well_formed_prune_job_is_accepted() {
        parse(&prune_job(FS, KEEP, "1h")).expect("valid prune job");
    }

    // Each of these is a job that loads cleanly and then does nothing
    // useful for as long as it runs.
    #[test]
    fn prune_jobs_that_would_be_silent_no_ops_are_rejected() {
        assert!(parse(&prune_job("", KEEP, "1h")).is_err(), "no filesystems");
        assert!(parse(&prune_job(FS, "", "1h")).is_err(), "no keep rules");
    }

    #[test]
    fn a_zero_prune_interval_is_rejected() {
        assert!(parse(&prune_job(FS, KEEP, "0s")).is_err());
    }

    fn snap_job_with_grid(interval: &str, grid: &str) -> String {
        format!(
            r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
interval = "{interval}"
prefix = "x_"
[jobs.pruning]
keep = [{{ type = "grid", grid = "{grid}", regex = "^x_" }}]
"#
        )
    }

    // The grid keeps the oldest snapshot per bucket, so a first bucket
    // longer than the snapshot interval destroys every new snapshot the
    // moment it is taken. zrepl's idiom is a leading keep=all bucket.
    #[test]
    fn a_first_bucket_longer_than_the_interval_needs_keep_all() {
        let err = format!(
            "{}",
            parse(&snap_job_with_grid("15m", "24x1h | 14x1d")).unwrap_err()
        );
        assert!(err.contains("keep=all"), "got: {err}");
        assert!(err.contains("1h"), "got: {err}");

        parse(&snap_job_with_grid("15m", "1x1h(keep=all) | 24x1h")).expect("keep=all lead");
        parse(&snap_job_with_grid("15m", "4x15m | 24x1h")).expect("bucket equals interval");
        parse(&snap_job_with_grid("4h", "4x15m | 24x1h")).expect("bucket shorter than interval");
    }

    // The dispatcher takes the first identity that matches, so the later
    // entry — which an operator added to change that client's grants —
    // never applies.
    #[test]
    fn duplicate_allowed_client_identities_rejected() {
        let s = r#"
[[allowed_clients]]
identity = "laptop"
jobs = ["sink"]
operations = ["recv"]
root_fs = "tank/backups"

[[allowed_clients]]
identity = "laptop"
jobs = ["sink"]
operations = ["recv", "control"]
root_fs = "tank/backups"
"#;
        let err = format!("{}", parse(s).unwrap_err());
        assert!(err.contains("duplicate identity"), "got: {err}");
    }

    // A snap job with its own prefix and no [jobs.pruning] used to get
    // keep-rules built from [defaults].prefix. With protect_non_prefixed
    // on, that aimed the grid at the DEFAULT prefix — another job's
    // snapshots — while classifying this job's own as "non-prefixed" and
    // so protecting them from every rule, forever.
    #[test]
    fn default_pruning_follows_the_jobs_own_prefix() {
        let s = r#"
[defaults]
prefix = "arctern_"
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "1x1h(keep=all) | 24x1h"
protect_non_prefixed = true

[[jobs]]
type = "snap"
name = "myapp"
[[jobs.filesystems]]
path = "tank/myapp"
[jobs.snapshotting]
interval = "15m"
prefix = "myapp_"
"#;
        let c = parse(s).expect("parses");
        let job = match &c.jobs[0] {
            JobConfig::Snap(s) => s,
            other => panic!("expected a snap job, got {other:?}"),
        };
        for rule in &job.pruning.keep {
            let regex = match rule {
                KeepRule::Grid { regex, .. } | KeepRule::Regex { regex, .. } => regex,
            };
            assert!(
                regex.contains("myapp_"),
                "rule aimed at the wrong prefix: {regex}"
            );
            assert!(
                !regex.contains("arctern_"),
                "rule still aimed at the defaults prefix: {regex}"
            );
        }
    }

    // A prune job does not snapshot, so the defaults' prefix is what it
    // is meant to clean up after.
    #[test]
    fn a_standalone_prune_job_still_uses_the_defaults_prefix() {
        let s = r#"
[defaults]
prefix = "arctern_"
[defaults.pruning]
grid = "24x1h"

[[jobs]]
type = "prune"
name = "p"
interval = "1h"
[[jobs.filesystems]]
path = "tank"
"#;
        let c = parse(s).expect("parses");
        let job = match &c.jobs[0] {
            JobConfig::Prune(p) => p,
            other => panic!("expected a prune job, got {other:?}"),
        };
        let KeepRule::Grid { regex, .. } = &job.pruning.keep[0] else {
            panic!("expected a grid rule");
        };
        assert!(regex.contains("arctern_"), "got: {regex}");
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
[[peers]]
name = "home"
ssh_target = "user@host"

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
        // Legacy `peer = "home"` is normalised into targets at load.
        assert!(p.peer.is_none());
        assert_eq!(p.targets, vec!["home".to_string()]);
        assert_eq!(p.target.root_fs, "okdata/backups/laptop");
        assert_eq!(p.snapshot_filter().prefix.as_deref(), Some("zrepl_"));
    }

    #[test]
    fn allowed_client_with_recv_requires_root_fs() {
        let s = r#"
[[allowed_clients]]
identity = "laptop"
jobs = ["backup"]
operations = ["recv"]
"#;
        let err = parse(s).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("allowed_clients[0].root_fs"), "got: {msg}");
    }

    #[test]
    fn allowed_client_with_control_requires_root_fs() {
        let s = r#"
[[allowed_clients]]
identity = "laptop"
jobs = ["backup"]
operations = ["control"]
"#;
        let err = parse(s).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("allowed_clients[0].root_fs"), "got: {msg}");
    }

    #[test]
    fn allowed_client_with_root_fs_parses() {
        let s = r#"
[[allowed_clients]]
identity = "laptop"
jobs = ["backup"]
operations = ["control", "recv"]
root_fs = "tank/backups/laptop"
"#;
        let c = parse(s).unwrap();
        assert_eq!(
            c.allowed_clients[0].root_fs.as_deref(),
            Some("tank/backups/laptop")
        );
    }

    #[test]
    fn push_dry_run_defaults_false() {
        let c = parse(PUSH_OK).unwrap();
        let JobConfig::Push(p) = &c.jobs[0] else {
            panic!("expected Push")
        };
        assert!(!p.dry_run);
    }

    #[test]
    fn push_dry_run_can_be_enabled() {
        let s = r#"
[[peers]]
name = "home"
ssh_target = "user@host"

[[jobs]]
type = "push"
name = "p"
peer = "home"
interval = "5m"
dry_run = true
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "okdata/backups/laptop"
[jobs.snapshot_filter]
prefix = "zrepl_"
"#;
        let c = parse(s).unwrap();
        let JobConfig::Push(p) = &c.jobs[0] else {
            panic!("expected Push")
        };
        assert!(p.dry_run);
    }

    #[test]
    fn allowed_client_root_fs_must_be_relative_dataset_path() {
        let s = r#"
[[allowed_clients]]
identity = "laptop"
jobs = ["backup"]
operations = ["recv"]
root_fs = "/tank/backups/laptop"
"#;
        let err = parse(s).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must not start or end"), "got: {msg}");
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
[[peers]]
name = "p"
ssh_target = "user@host"

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
    fn defaults_fill_in_snap_snapshotting_and_pruning() {
        let s = r#"
[defaults]
prefix = "zrepl_"
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "4x15m | 24x1h"

[[jobs]]
type = "snap"
name = "minimal"
[[jobs.filesystems]]
path = "tank"
"#;
        let c = parse(s).unwrap();
        let JobConfig::Snap(snap) = &c.jobs[0] else {
            panic!("expected Snap")
        };
        let snap_cfg = snap.snapshotting();
        assert_eq!(snap_cfg.interval, std::time::Duration::from_secs(15 * 60));
        assert_eq!(snap_cfg.prefix, "zrepl_");
        // Default protect_non_prefixed = true → two rules.
        let keep = &snap.pruning().keep;
        assert_eq!(keep.len(), 2);
        matches!(keep[0], KeepRule::Grid { .. });
        matches!(keep[1], KeepRule::Regex { negate: true, .. });
    }

    #[test]
    fn defaults_fill_in_push_snapshot_filter() {
        let s = r#"
[defaults]
prefix = "zrepl_"

[[peers]]
name = "home"
ssh_target = "user@host"

[[jobs]]
type = "push"
name = "p"
peer = "home"
interval = "5m"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "okdata/backups/laptop"
"#;
        let c = parse(s).unwrap();
        let JobConfig::Push(p) = &c.jobs[0] else {
            panic!("expected Push")
        };
        assert_eq!(p.snapshot_filter().prefix.as_deref(), Some("zrepl_"));
    }

    #[test]
    fn defaults_protect_non_prefixed_false_drops_negate_rule() {
        let s = r#"
[defaults]
prefix = "zrepl_"
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "4x15m"
protect_non_prefixed = false

[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
"#;
        let c = parse(s).unwrap();
        let JobConfig::Snap(snap) = &c.jobs[0] else {
            panic!("expected Snap")
        };
        assert_eq!(snap.pruning().keep.len(), 1);
    }

    #[test]
    fn filesystems_map_form_translates_to_filters() {
        let s = r#"
[defaults]
prefix = "zrepl_"
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "4x15m"

[[jobs]]
type = "snap"
name = "arch0"
filesystems = { "novafs/arch0/" = true, "novafs/arch0" = false, "novafs/arch0/data" = false }
"#;
        let c = parse(s).unwrap();
        let JobConfig::Snap(snap) = &c.jobs[0] else {
            panic!()
        };
        assert_eq!(snap.filesystems.len(), 1);
        let f = &snap.filesystems[0];
        assert_eq!(f.path, "novafs/arch0");
        assert!(f.recursive);
        assert!(f.exclude.contains(&"novafs/arch0".to_string()));
        assert!(f.exclude.contains(&"novafs/arch0/data".to_string()));
    }

    #[test]
    fn filesystems_map_bare_key_is_exact_match() {
        let s = r#"
[defaults]
prefix = "zrepl_"
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "4x15m"

[[jobs]]
type = "snap"
name = "x"
filesystems = { "tank/data/home" = true }
"#;
        let c = parse(s).unwrap();
        let JobConfig::Snap(snap) = &c.jobs[0] else {
            panic!()
        };
        assert_eq!(snap.filesystems.len(), 1);
        let f = &snap.filesystems[0];
        assert_eq!(f.path, "tank/data/home");
        assert!(!f.recursive);
        assert!(f.exclude.is_empty());
    }

    #[test]
    fn filesystems_map_orphan_exclude_errors() {
        let s = r#"
[defaults]
prefix = "zrepl_"
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "4x15m"

[[jobs]]
type = "snap"
name = "x"
filesystems = { "tank/data" = true, "tank/other" = false }
"#;
        let err = parse(s).unwrap_err();
        assert!(
            matches!(err, ConfigError::Validate { .. }),
            "orphan exclude is a schema error, not a syntax error: {err}"
        );
        let text = err.to_string();
        assert!(text.contains("tank/other"), "got: {text}");
        assert!(text.contains("line 9:"), "got: {text}");
        assert!(!text.contains("TOML parse error"), "got: {text}");
    }

    #[test]
    fn malformed_toml_is_a_parse_error() {
        let err = parse("[[jobs]\ntype = \"snap\"").unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }), "got: {err}");
    }

    #[test]
    fn unknown_field_is_a_validate_error() {
        let s = format!("{MIN_OK}\nbogus = 1\n");
        let err = parse(&s).unwrap_err();
        assert!(matches!(err, ConfigError::Validate { .. }), "got: {err}");
        assert!(err.to_string().contains("bogus"), "got: {err}");
    }

    #[test]
    fn push_multi_target_parses() {
        let s = r#"
[defaults]
prefix = "zrepl_"

[[peers]]
name = "home"
ssh_target = "u@h1"
[[peers]]
name = "remote"
ssh_target = "u@h2"

[[jobs]]
type = "push"
name = "p"
targets = ["home", "remote"]
interval = "5m"
filesystems = { "tank/data" = true }
[jobs.target]
root_fs = "okdata/backups/laptop"
"#;
        let c = parse(s).unwrap();
        let JobConfig::Push(p) = &c.jobs[0] else {
            panic!()
        };
        assert_eq!(p.targets, vec!["home".to_string(), "remote".to_string()]);
    }

    #[test]
    fn push_target_referencing_unknown_peer_rejected() {
        let s = r#"
[defaults]
prefix = "zrepl_"

[[peers]]
name = "home"
ssh_target = "u@h1"

[[jobs]]
type = "push"
name = "p"
targets = ["hoem"]
interval = "5m"
filesystems = { "tank/data" = true }
[jobs.target]
root_fs = "okdata/backups/laptop"
"#;
        let err = parse(s).unwrap_err();
        assert!(
            err.to_string().contains("does not match any [[peers]]"),
            "got: {err}"
        );
    }

    #[test]
    fn duplicate_peer_names_rejected() {
        let s = r#"
[[peers]]
name = "home"
ssh_target = "u@h1"
[[peers]]
name = "home"
ssh_target = "u@h2"
"#;
        let err = parse(s).unwrap_err();
        assert!(err.to_string().contains("duplicate"), "got: {err}");
    }

    #[test]
    fn peer_shorthand_normalises_to_single_route() {
        let s = r#"
[[peers]]
name = "home"
ssh_target = "u@h1"
"#;
        let c = parse(s).unwrap();
        assert!(c.peers[0].ssh_target.is_none());
        assert_eq!(c.peers[0].routes.len(), 1);
        assert_eq!(c.peers[0].routes[0].name, "default");
        assert_eq!(c.peers[0].routes[0].ssh_target, "u@h1");
        assert!(c.peers[0].routes[0].auto);
    }

    #[test]
    fn peer_routes_parse_in_order_with_auto_flag() {
        let s = r#"
[[peers]]
name = "mira"
mode = "auto"
auto_interval = "1d"
[[peers.routes]]
name = "lan"
ssh_target = "arctern-mira-lan"
[[peers.routes]]
name = "wg"
ssh_target = "arctern-mira-wg"
auto = false
"#;
        let c = parse(s).unwrap();
        let p = &c.peers[0];
        assert_eq!(p.routes.len(), 2);
        assert_eq!(p.routes[0].name, "lan");
        assert!(p.routes[0].auto);
        assert_eq!(p.routes[1].name, "wg");
        assert!(!p.routes[1].auto);
    }

    #[test]
    fn peer_ssh_target_and_routes_both_rejected() {
        let s = r#"
[[peers]]
name = "mira"
ssh_target = "u@h"
[[peers.routes]]
name = "lan"
ssh_target = "u@h2"
"#;
        let err = parse(s).unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "got: {err}");
    }

    #[test]
    fn peer_without_connection_rejected() {
        let s = r#"
[[peers]]
name = "mira"
"#;
        let err = parse(s).unwrap_err();
        assert!(err.to_string().contains("missing connection"), "got: {err}");
    }

    #[test]
    fn peer_duplicate_route_names_rejected() {
        let s = r#"
[[peers]]
name = "mira"
[[peers.routes]]
name = "lan"
ssh_target = "a"
[[peers.routes]]
name = "lan"
ssh_target = "b"
"#;
        let err = parse(s).unwrap_err();
        assert!(err.to_string().contains("duplicate route"), "got: {err}");
    }

    #[test]
    fn socket_path_optional() {
        let c = parse("socket = \"/run/arctern/arctern.sock\"\n").unwrap();
        assert_eq!(
            c.socket.as_deref().map(|p| p.display().to_string()),
            Some("/run/arctern/arctern.sock".to_string())
        );
    }

    #[test]
    fn peer_with_empty_ssh_target_rejected() {
        let s = r#"
[[peers]]
name = "home"
ssh_target = ""
"#;
        let err = parse(s).unwrap_err();
        assert!(err.to_string().contains("ssh_target"), "got: {err}");
    }

    #[test]
    fn push_peer_and_targets_both_given_rejected() {
        let s = r#"
[defaults]
prefix = "zrepl_"

[[jobs]]
type = "push"
name = "p"
peer = "home"
targets = ["home"]
interval = "5m"
filesystems = { "tank/data" = true }
[jobs.target]
root_fs = "okdata/backups/laptop"
"#;
        let err = parse(s).unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "got: {err}");
    }

    #[test]
    fn push_neither_peer_nor_targets_rejected() {
        let s = r#"
[defaults]
prefix = "zrepl_"

[[jobs]]
type = "push"
name = "p"
interval = "5m"
filesystems = { "tank/data" = true }
[jobs.target]
root_fs = "okdata/backups/laptop"
"#;
        let err = parse(s).unwrap_err();
        assert!(err.to_string().contains("target peer"), "got: {err}");
    }

    #[test]
    fn missing_snapshotting_without_defaults_errors() {
        let s = r#"
[[jobs]]
type = "snap"
name = "x"
[[jobs.filesystems]]
path = "tank"
"#;
        let err = parse(s).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("snapshotting"), "got: {msg}");
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
            assert!(
                format!("{err}").contains("root_fs"),
                "expected root_fs error for {root:?}"
            );
        }
    }

    #[test]
    fn invalid_filesystem_paths_are_rejected() {
        let s = MIN_OK.replace("path = \"tank\"", "path = \"tank/data//escape\"");
        let err = parse(&s).unwrap_err();
        assert!(
            format!("{err}").contains("invalid dataset name"),
            "got: {err}"
        );
    }

    #[test]
    fn invalid_filesystem_excludes_are_rejected() {
        let s = r#"
[[jobs]]
type = "push"
name = "p"
peer = "p"
interval = "1s"
[[jobs.filesystems]]
path = "tank/data"
recursive = true
exclude = ["tank/data#bookmark"]
[jobs.target]
root_fs = "tank/sink"
[jobs.snapshot_filter]
prefix = "x_"
"#;
        let err = parse(s).unwrap_err();
        assert!(format!("{err}").contains("invalid dataset name"));
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
interval = "4 fortnights"
prefix = "x_"
[jobs.pruning]
keep = []
"#;
        assert!(parse(s).is_err());
    }
}
