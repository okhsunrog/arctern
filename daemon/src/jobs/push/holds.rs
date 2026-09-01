//! Step holds and the replication cursor bookmark — the naming
//! scheme and the two operations that maintain them. See the module
//! docs in `mod.rs` for what each protects.

use arctern_transport::SnapshotEntry;
use tracing::warn;
use zfskit::dataset::ListOptions;
use zfskit::models::DatasetType;
use zfskit::runner::CommandRunner;

pub(super) fn step_hold_tag(job_name: &str, peer: &str) -> String {
    format!("arctern_step_J_{job_name}_P_{peer}")
}

/// GUID-suffixed cursor name (zrepl's scheme): a new cursor is created
/// under a fresh name *before* stale ones are destroyed, so a crash in
/// between leaves at least one cursor alive.
pub(super) fn cursor_bookmark_name(dataset: &str, job_name: &str, peer: &str, guid: u64) -> String {
    format!("{dataset}#arctern_cursor_G_{guid:x}_J_{job_name}_P_{peer}")
}

/// Matches any cursor bookmark leaf for this (job, peer), regardless of
/// GUID. Used to find stale cursors after advancing.
pub(super) fn is_cursor_bookmark_leaf(leaf: &str, job_name: &str, peer: &str) -> bool {
    leaf.starts_with("arctern_cursor_G_") && leaf.ends_with(&format!("_J_{job_name}_P_{peer}"))
}

/// Plan one filesystem cycle against the receiver. Pure planner glue
/// over zfskit + the control channel. Returns the plan plus the
/// filtered sender snapshot list (the executor's hold sweep reuses it).
pub(super) async fn advance_cursor(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    job_name: &str,
    peer_name: &str,
    to_snap: &str,
    to_guid: u64,
) {
    let cursor = cursor_bookmark_name(sender_dataset, job_name, peer_name, to_guid);
    if let Err(e) = zfskit::bookmark::create(runner, to_snap, &cursor).await {
        warn!(snapshot = %to_snap, bookmark = %cursor, error = %e, "create cursor bookmark");
        // Keep the old cursor rather than risk destroying the only one.
        return;
    }
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Bookmark],
        roots: vec![sender_dataset.to_string()],
        ..ListOptions::default()
    };
    let bookmarks = match zfskit::dataset::list(runner, &opts).await {
        Ok(v) => v,
        Err(e) => {
            warn!(dataset = %sender_dataset, error = %e, "list bookmarks for cursor sweep");
            return;
        }
    };
    for b in &bookmarks {
        let Some((_, leaf)) = b.name.split_once('#') else {
            continue;
        };
        if b.name != cursor
            && is_cursor_bookmark_leaf(leaf, job_name, peer_name)
            && let Err(e) = zfskit::bookmark::destroy(runner, &b.name).await
        {
            warn!(bookmark = %b.name, error = %e, "destroy stale cursor bookmark");
        }
    }
}

/// Release this (job, peer)'s step-hold tag from every filtered sender
/// snapshot — the current `to` plus any stale holds left by earlier
/// failed cycles (a failed cycle keeps its hold; the next cycle usually
/// targets a newer snapshot, so without the sweep the old hold would
/// pin its snapshot against prune forever). One `zfs holds` invocation
/// for the whole set, then one release per actual holder.
pub(super) async fn sweep_step_holds(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    sender_snaps: &[SnapshotEntry],
    to_snap_full: &str,
    tag: &str,
) {
    let mut names: Vec<String> = sender_snaps
        .iter()
        .map(|s| format!("{sender_dataset}@{}", s.name))
        .collect();
    if !names.iter().any(|n| n == to_snap_full) {
        names.push(to_snap_full.to_string());
    }
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let holds = match zfskit::hold::list_holds_many(runner, &refs).await {
        Ok(h) => h,
        Err(e) => {
            warn!(dataset = %sender_dataset, error = %e, "step-hold sweep holds query failed");
            return;
        }
    };
    for h in holds.iter().filter(|h| h.tag == tag) {
        if let Err(e) = zfskit::hold::release(runner, &h.dataset, tag).await {
            warn!(snapshot = %h.dataset, tag = %tag, error = %e, "release step hold");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_hold_tag_includes_peer_for_multi_target_isolation() {
        assert_eq!(
            step_hold_tag("backup", "home"),
            "arctern_step_J_backup_P_home"
        );
    }

    #[test]
    fn cursor_bookmark_name_includes_guid_job_and_peer() {
        let name = cursor_bookmark_name("tank/data", "backup", "home", 0x2a);
        assert_eq!(name, "tank/data#arctern_cursor_G_2a_J_backup_P_home");
        let leaf = name.split_once('#').unwrap().1;
        assert!(is_cursor_bookmark_leaf(leaf, "backup", "home"));
        assert!(!is_cursor_bookmark_leaf(leaf, "backup", "other"));
        assert!(!is_cursor_bookmark_leaf(leaf, "other", "home"));
    }
}
