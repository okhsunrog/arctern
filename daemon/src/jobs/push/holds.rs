//! Step holds and the replication cursor bookmark for one (dataset,
//! job, peer). See the module docs in `mod.rs` for what each protects.

use arctern_transport::SnapshotEntry;
use tracing::warn;
use zfskit::ZfsError;
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
/// GUID.
pub(super) fn is_cursor_bookmark_leaf(leaf: &str, job_name: &str, peer: &str) -> bool {
    leaf.starts_with("arctern_cursor_G_") && leaf.ends_with(&format!("_J_{job_name}_P_{peer}"))
}

/// The hold namespace of one replication step: one dataset, one job,
/// one peer.
pub(super) struct HoldScope<'a> {
    pub(super) runner: &'a dyn CommandRunner,
    pub(super) dataset: &'a str,
    pub(super) job_name: &'a str,
    pub(super) peer_name: &'a str,
}

impl HoldScope<'_> {
    pub(super) fn tag(&self) -> String {
        step_hold_tag(self.job_name, self.peer_name)
    }

    /// Place the step hold on `snapshot`. Idempotent at the zfskit
    /// layer: a tag that already exists on the snapshot is a no-op.
    pub(super) async fn place(&self, snapshot: &str) -> Result<(), ZfsError> {
        zfskit::hold::hold(self.runner, snapshot, &self.tag()).await
    }

    /// Release the step tag from every filtered snapshot except `keep`.
    ///
    /// A failed step keeps its holds so the retry can still find the
    /// snapshot, but the planner then picks the newest snapshot rather
    /// than the one it failed on, so a peer that is reachable but
    /// failing would accumulate one permanent hold per cycle. Runs
    /// after the current step's own holds are placed: a resumable
    /// receive names a sender snapshot in its token, and releasing
    /// that first would let prune destroy it.
    pub(super) async fn sweep_stale(&self, sender_snaps: &[SnapshotEntry], keep: &[&str]) {
        let candidates = self.candidates(sender_snaps, keep);
        self.release(&candidates, keep).await;
    }

    /// After a successful send: advance the cursor, then release the
    /// step holds. The order is the point. The `to` hold is what keeps
    /// prune off the snapshot the receiver now has; the cursor bookmark
    /// takes over that role. If the bookmark could not be created
    /// (pool full, feature missing) the hold on `to` stays, because
    /// nothing else on the sender would record what the receiver has.
    pub(super) async fn commit(&self, sender_snaps: &[SnapshotEntry], to_snap: &str, to_guid: u64) {
        if self.advance_cursor(to_snap, to_guid).await {
            let candidates = self.candidates(sender_snaps, &[to_snap]);
            self.release(&candidates, &[]).await;
        } else {
            warn!(
                snapshot = %to_snap,
                tag = %self.tag(),
                "cursor bookmark not created; keeping the step hold on the sent snapshot until a later cycle records it"
            );
            self.sweep_stale(sender_snaps, &[to_snap]).await;
        }
    }

    /// Create the new GUID-named cursor bookmark, then destroy stale
    /// cursors for the same (job, peer). Returns whether the new cursor
    /// exists: a failed sweep of the old ones is only untidiness, a
    /// failed create means nothing on the sender records what the
    /// receiver now has.
    async fn advance_cursor(&self, to_snap: &str, to_guid: u64) -> bool {
        let cursor = cursor_bookmark_name(self.dataset, self.job_name, self.peer_name, to_guid);
        if let Err(e) = zfskit::bookmark::create(self.runner, to_snap, &cursor).await {
            warn!(snapshot = %to_snap, bookmark = %cursor, error = %e, "create cursor bookmark");
            return false;
        }
        let opts = ListOptions {
            recursive: false,
            types: vec![DatasetType::Bookmark],
            roots: vec![self.dataset.to_string()],
            ..ListOptions::default()
        };
        let bookmarks = match zfskit::dataset::list(self.runner, &opts).await {
            Ok(v) => v,
            Err(e) => {
                warn!(dataset = %self.dataset, error = %e, "list bookmarks for cursor sweep");
                return true;
            }
        };
        for b in &bookmarks {
            let Some((_, leaf)) = b.name.split_once('#') else {
                continue;
            };
            if b.name != cursor
                && is_cursor_bookmark_leaf(leaf, self.job_name, self.peer_name)
                && let Err(e) = zfskit::bookmark::destroy(self.runner, &b.name).await
            {
                warn!(bookmark = %b.name, error = %e, "destroy stale cursor bookmark");
            }
        }
        true
    }

    /// Full snapshot names to consider: the dataset's filtered snapshots
    /// plus anything named explicitly (a snapshot the filter no longer
    /// matches is still ours to clean up).
    fn candidates(&self, sender_snaps: &[SnapshotEntry], extra: &[&str]) -> Vec<String> {
        let mut names: Vec<String> = sender_snaps
            .iter()
            .map(|s| format!("{}@{}", self.dataset, s.name))
            .collect();
        for e in extra {
            if !names.iter().any(|n| n == e) {
                names.push((*e).to_string());
            }
        }
        names
    }

    /// One `zfs holds` invocation for the whole set, then one release
    /// per actual holder of our tag outside `keep`.
    async fn release(&self, candidates: &[String], keep: &[&str]) {
        let tag = self.tag();
        let refs: Vec<&str> = candidates.iter().map(String::as_str).collect();
        let holds = match zfskit::hold::list_holds_many(self.runner, &refs).await {
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
            if let Err(e) = zfskit::hold::release(self.runner, &h.dataset, &tag).await {
                warn!(snapshot = %h.dataset, tag = %tag, error = %e, "release step hold");
            }
        }
    }
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

    fn scope(runner: &HoldsRunner) -> HoldScope<'_> {
        HoldScope {
            runner,
            dataset: "tank/data",
            job_name: "push",
            peer_name: "mira",
        }
    }

    const TAG: &str = "arctern_step_J_push_P_mira";

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
        scope(&r)
            .sweep_stale(&snaps(&["s1", "s2", "s3"]), &["tank/data@s3"])
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
        scope(&r)
            .sweep_stale(
                &snaps(&["old", "from", "to"]),
                &["tank/data@from", "tank/data@to"],
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
        scope(&r).sweep_stale(&snaps(&["s1", "s2"]), &[]).await;
        assert_eq!(r.released(), vec!["tank/data@s2"]);
    }

    // On success the cursor bookmark protects incrementality, so the step
    // tag comes off everywhere including the snapshot just sent.
    #[tokio::test]
    async fn a_successful_step_releases_the_tag_everywhere() {
        let r = HoldsRunner::new(&[("tank/data@s1", TAG), ("tank/data@s2", TAG)]);
        let s = scope(&r);
        let candidates = s.candidates(&snaps(&["s1"]), &["tank/data@s2"]);
        s.release(&candidates, &[]).await;
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
        scope(&r)
            .commit(&snaps(&["old", "to"]), "tank/data@to", 0x2a)
            .await;
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
