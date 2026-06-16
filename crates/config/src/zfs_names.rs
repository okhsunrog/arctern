//! Minimal ZFS target-name validation for Arctern policy boundaries.
//!
//! This is intentionally conservative. Palimpsest remains the low-level ZFS
//! command wrapper; these helpers decide what Arctern accepts from config and
//! remote clients before invoking Palimpsest.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTarget<'a> {
    pub dataset: &'a str,
    pub snapshot: &'a str,
}

pub fn validate_dataset_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("dataset name must not be empty".into());
    }
    if name.starts_with('/') || name.ends_with('/') {
        return Err("dataset name must not start or end with '/'".into());
    }
    if name.contains('@') || name.contains('#') {
        return Err("dataset name must not contain '@' or '#'".into());
    }
    if name.split('/').any(|part| part.is_empty()) {
        return Err("dataset name must not contain empty path components".into());
    }
    for part in name.split('/') {
        validate_component(part).map_err(|e| format!("dataset component {part:?}: {e}"))?;
        if part == "." || part == ".." {
            return Err(format!("dataset component {part:?} is not allowed"));
        }
    }
    Ok(())
}

pub fn parse_snapshot_target(name: &str) -> Result<SnapshotTarget<'_>, String> {
    let Some((dataset, snapshot)) = name.split_once('@') else {
        return Err("snapshot target must be dataset@snapshot".into());
    };
    if snapshot.contains('@') || dataset.contains('@') {
        return Err("snapshot target must contain exactly one '@'".into());
    }
    if dataset.contains('#') || snapshot.contains('#') {
        return Err("snapshot target must not contain bookmark syntax '#'".into());
    }
    validate_dataset_name(dataset).map_err(|e| format!("snapshot dataset: {e}"))?;
    validate_snapshot_leaf(snapshot).map_err(|e| format!("snapshot name: {e}"))?;
    Ok(SnapshotTarget { dataset, snapshot })
}

pub fn validate_snapshot_leaf(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("snapshot name must not be empty".into());
    }
    if name.contains('/') || name.contains('@') || name.contains('#') {
        return Err("snapshot name must not contain '/', '@', or '#'".into());
    }
    validate_component(name)
}

fn validate_component(component: &str) -> Result<(), String> {
    if component.is_empty() {
        return Err("component must not be empty".into());
    }
    // ZFS requires names to begin with an alphanumeric. Enforcing it also
    // closes argument injection: zfs/zpool take names as bare positionals
    // with no `--` terminator, so a leading `-` (e.g. "-F", "-R") would be
    // parsed as a flag.
    if !component
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric())
    {
        return Err("component must begin with an alphanumeric character".into());
    }
    if component.chars().any(char::is_whitespace) {
        return Err("component must not contain whitespace".into());
    }
    if !component
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':' | '+'))
    {
        return Err("component contains unsupported characters".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_name_accepts_common_zfs_paths() {
        validate_dataset_name("tank/backups/laptop/root").unwrap();
        validate_dataset_name("rpool/ROOT/arch-2026.05.15").unwrap();
        validate_dataset_name("pool/user:home+data").unwrap();
    }

    #[test]
    fn dataset_name_rejects_ambiguous_or_unsafe_paths() {
        for name in [
            "",
            "/tank/data",
            "tank/data/",
            "tank//data",
            "tank/../data",
            "tank/data@s1",
            "tank/data#b1",
            "tank/data with spaces",
            // Leading-dash components would be parsed as zfs flags.
            "-tank/data",
            "tank/-data",
            "-F",
        ] {
            assert!(validate_dataset_name(name).is_err(), "accepted {name:?}");
        }
    }

    #[test]
    fn snapshot_leaf_rejects_leading_dash() {
        assert!(validate_snapshot_leaf("-nv").is_err());
        assert!(parse_snapshot_target("tank/data@-R").is_err());
    }

    #[test]
    fn snapshot_target_accepts_dataset_at_snapshot() {
        let parsed = parse_snapshot_target("tank/data@snap_2026-05-15").unwrap();
        assert_eq!(parsed.dataset, "tank/data");
        assert_eq!(parsed.snapshot, "snap_2026-05-15");
    }

    #[test]
    fn snapshot_target_rejects_non_snapshots() {
        for name in [
            "tank/data",
            "tank/data@",
            "@snap",
            "tank/data@snap/child",
            "tank/data@snap@other",
            "tank/data#bookmark",
        ] {
            assert!(parse_snapshot_target(name).is_err(), "accepted {name:?}");
        }
    }
}
