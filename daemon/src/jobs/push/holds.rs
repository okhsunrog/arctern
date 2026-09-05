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

/// Create the new GUID-named cursor bookmark, then destroy stale cursors
/// for the same (job, peer). Returns whether the new cursor exists: a
/// failed sweep of the old ones is only untidiness, a failed create
/// means nothing on the sender records what the receiver now has.
pub(super) async fn advance_cursor(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    job_name: &str,
    peer_name: &str,
    to_snap: &str,
    to_guid: u64,
) -> bool {
    let cursor = cursor_bookmark_name(sender_dataset, job_name, peer_name, to_guid);
    if let Err(e) = zfskit::bookmark::create(runner, to_snap, &cursor).await {
        warn!(snapshot = %to_snap, bookmark = %cursor, error = %e, "create cursor bookmark");
        // Keep the old cursor rather than risk destroying the only one.
        return false;
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
            return true;
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
    true
}

/// After a successful send: advance the cursor, then release the step
/// holds. The order is the point. The `to` hold is what keeps prune off
/// the snapshot the receiver now has; the cursor bookmark takes over that
/// role. If the bookmark could not be created (pool full, feature
/// missing), releasing the hold anyway left nothing protecting
/// incrementality: prune destroyed `to`, the stale cursor pointed below
/// the receiver's head, and every later incremental was refused.
#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_step(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    job_name: &str,
    peer_name: &str,
    sender_snaps: &[SnapshotEntry],
    to_snap: &str,
    to_guid: u64,
    tag: &str,
) {
    if advance_cursor(
        runner,
        sender_dataset,
        job_name,
        peer_name,
        to_snap,
        to_guid,
    )
    .await
    {
        release_all_step_holds(runner, sender_dataset, sender_snaps, to_snap, tag).await;
    } else {
        warn!(
            snapshot = %to_snap,
            tag = %tag,
            "cursor bookmark not created; keeping the step hold on the sent snapshot until a later cycle records it"
        );
        sweep_stale_step_holds(runner, sender_dataset, sender_snaps, &[to_snap], tag).await;
    }
}

/// Release this (job, peer)'s step-hold tag from every filtered sender
/// snapshot — the current `to` plus any stale holds left by earlier
/// failed cycles (a failed cycle keeps its hold; the next cycle usually
/// targets a newer snapshot, so without the sweep the old hold would
/// pin its snapshot against prune forever). One `zfs holds` invocation
/// for the whole set, then one release per actual holder.
/// Full snapshot names to consider: the dataset's filtered snapshots
/// plus anything named explicitly (a snapshot the filter no longer
/// matches is still ours to clean up).
fn hold_candidates(
    sender_dataset: &str,
    sender_snaps: &[SnapshotEntry],
    extra: &[&str],
) -> Vec<String> {
    let mut names: Vec<String> = sender_snaps
        .iter()
        .map(|s| format!("{sender_dataset}@{}", s.name))
        .collect();
    for e in extra {
        if !names.iter().any(|n| n == e) {
            names.push((*e).to_string());
        }
    }
    names
}

/// Release `tag` from every candidate except `keep`.
async fn release_holds(
    runner: &dyn CommandRunner,
    candidates: &[String],
    keep: &[&str],
    tag: &str,
) {
    let refs: Vec<&str> = candidates.iter().map(String::as_str).collect();
    let holds = match zfskit::hold::list_holds_many(runner, &refs).await {
        Ok(h) => h,
        Err(e) => {
            warn!(error = %e, "step-hold sweep holds query failed");
            return;
        }
    };
    for h in holds
        .iter()
        .filter(|h| h.tag == tag && !keep.contains(&h.dataset.as_str()))
    {
        if let Err(e) = zfskit::hold::release(runner, &h.dataset, tag).await {
            warn!(snapshot = %h.dataset, tag = %tag, error = %e, "release step hold");
        }
    }
}

/// Before sending: drop the holds this (job, peer) left behind on other
/// snapshots, keeping only the ones this step needs.
///
/// A failed step deliberately keeps its hold so the retry can still find
/// the snapshot — but the planner then picks the NEWEST snapshot, not
/// the one it failed on, so every failing cycle held one more snapshot
/// and prune skipped all of them. A peer that is reachable but failing
/// (receiver out of space, a drop mid-send) accumulated one permanent
/// hold per cycle until the next success finally swept them.
///
/// This cannot simply run first: a resumable receive names a sender
/// snapshot in its resume token, and releasing that hold would let prune
/// destroy it and break the resume. So the step's own holds go on first,
/// and only then is everything else released.
pub(super) async fn sweep_stale_step_holds(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    sender_snaps: &[SnapshotEntry],
    keep: &[&str],
    tag: &str,
) {
    let candidates = hold_candidates(sender_dataset, sender_snaps, keep);
    release_holds(runner, &candidates, keep, tag).await;
}

/// After a successful step: the cursor bookmark now protects
/// incrementality, so the step tag is released everywhere.
pub(super) async fn release_all_step_holds(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    sender_snaps: &[SnapshotEntry],
    to_snap_full: &str,
    tag: &str,
) {
    let candidates = hold_candidates(sender_dataset, sender_snaps, &[to_snap_full]);
    release_holds(runner, &candidates, &[], tag).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use zfskit::runner::Cmd;

    /// Answers `zfs holds -p -H <snap>...` from a fixed table and records
    /// every `zfs release` it is asked to perform.
    struct HoldsRunner {
        held: Vec<(String, String)>,
        released: Mutex<Vec<String>>,
        /// Make `zfs bookmark` fail the way a full pool does.
        bookmark_fails: bool,
    }

    impl HoldsRunner {
        fn new(held: &[(&str, &str)]) -> Self {
            Self {
                held: held
                    .iter()
                    .map(|(s, t)| ((*s).to_string(), (*t).to_string()))
                    .collect(),
                released: Mutex::new(Vec::new()),
                bookmark_fails: false,
            }
        }
        fn with_failing_bookmark(mut self) -> Self {
            self.bookmark_fails = true;
            self
        }
        fn released(&self) -> Vec<String> {
            let mut v = self.released.lock().unwrap().clone();
            v.sort();
            v
        }
    }

    fn ok(stdout: String) -> std::process::Output {
        use std::os::unix::process::ExitStatusExt;
        std::process::Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: stdout.into_bytes(),
            stderr: Vec::new(),
        }
    }

    fn failed(stderr: &str) -> std::process::Output {
        use std::os::unix::process::ExitStatusExt;
        std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        }
    }

    #[async_trait::async_trait]
    impl CommandRunner for HoldsRunner {
        async fn run(&self, cmd: Cmd) -> Result<std::process::Output, std::io::Error> {
            let args: Vec<String> = cmd
                .args_list()
                .iter()
                .map(|a| a.to_string_lossy().into())
                .collect();
            match args.first().map(String::as_str) {
                Some("holds") => {
                    let asked: Vec<&String> = args.iter().skip(3).collect();
                    let body: String = self
                        .held
                        .iter()
                        .filter(|(snap, _)| asked.contains(&snap))
                        .map(|(snap, tag)| format!("{snap}\t{tag}\t1700000000\n"))
                        .collect();
                    Ok(ok(body))
                }
                Some("release") => {
                    self.released.lock().unwrap().push(args[2].clone());
                    Ok(ok(String::new()))
                }
                Some("bookmark") if self.bookmark_fails => Ok(failed(
                    "cannot create bookmark 'tank/data#cursor': out of space",
                )),
                other => panic!("unexpected zfs subcommand {other:?}"),
            }
        }
    }

    fn snaps(names: &[&str]) -> Vec<SnapshotEntry> {
        names
            .iter()
            .map(|n| SnapshotEntry {
                name: (*n).to_string(),
                guid: 1,
                createtxg: 1,
            })
            .collect()
    }

    const TAG: &str = "arctern_step_J_push_J_P_mira";

    // Each failing cycle used to leave its hold behind while the planner
    // moved on to the newest snapshot, so holds accumulated one per cycle
    // and prune skipped every one of them.
    #[tokio::test]
    async fn stale_holds_from_earlier_failed_cycles_are_released() {
        let r = HoldsRunner::new(&[
            ("tank/data@s1", TAG),
            ("tank/data@s2", TAG),
            ("tank/data@s3", TAG),
        ]);
        sweep_stale_step_holds(
            &r,
            "tank/data",
            &snaps(&["s1", "s2", "s3"]),
            &["tank/data@s3"],
            TAG,
        )
        .await;
        assert_eq!(r.released(), vec!["tank/data@s1", "tank/data@s2"]);
    }

    // A resumable receive names a sender snapshot in its resume token;
    // releasing that hold would let prune destroy it mid-resume.
    #[tokio::test]
    async fn the_snapshots_this_step_needs_are_kept() {
        let r = HoldsRunner::new(&[
            ("tank/data@old", TAG),
            ("tank/data@from", TAG),
            ("tank/data@to", TAG),
        ]);
        sweep_stale_step_holds(
            &r,
            "tank/data",
            &snaps(&["old", "from", "to"]),
            &["tank/data@from", "tank/data@to"],
            TAG,
        )
        .await;
        assert_eq!(r.released(), vec!["tank/data@old"]);
    }

    // Another job's or peer's tag on the same snapshot is not ours.
    #[tokio::test]
    async fn holds_belonging_to_another_job_are_left_alone() {
        let r = HoldsRunner::new(&[
            ("tank/data@s1", "arctern_step_J_other_P_mira"),
            ("tank/data@s1", "arctern_last_J_push"),
            ("tank/data@s2", TAG),
        ]);
        sweep_stale_step_holds(&r, "tank/data", &snaps(&["s1", "s2"]), &[], TAG).await;
        assert_eq!(r.released(), vec!["tank/data@s2"]);
    }

    // On success the cursor bookmark protects incrementality, so the step
    // tag comes off everywhere including the snapshot just sent.
    #[tokio::test]
    async fn a_successful_step_releases_the_tag_everywhere() {
        let r = HoldsRunner::new(&[("tank/data@s1", TAG), ("tank/data@s2", TAG)]);
        release_all_step_holds(&r, "tank/data", &snaps(&["s1"]), "tank/data@s2", TAG).await;
        assert_eq!(r.released(), vec!["tank/data@s1", "tank/data@s2"]);
    }

    // The step hold on `to` is released because the cursor bookmark takes
    // over protecting incrementality. When the bookmark cannot be created
    // there is no cursor to take over, and releasing anyway let prune
    // destroy the one snapshot the receiver now shares with the sender.
    #[tokio::test]
    async fn a_failed_cursor_bookmark_keeps_the_hold_on_the_sent_snapshot() {
        let r = HoldsRunner::new(&[("tank/data@old", TAG), ("tank/data@to", TAG)])
            .with_failing_bookmark();
        commit_step(
            &r,
            "tank/data",
            "push",
            "mira",
            &snaps(&["old", "to"]),
            "tank/data@to",
            0x2a,
            TAG,
        )
        .await;
        // Stale holds from earlier cycles still go; `to` stays pinned.
        assert_eq!(r.released(), vec!["tank/data@old"]);
    }

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
