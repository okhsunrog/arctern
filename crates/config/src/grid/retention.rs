//! Direct port of zrepl's retention-grid algorithm. Bucket entries by
//! interval-from-`now` (where `now` is the youngest matching entry's
//! `creation`), then within each bucket retain at most `keep_count`
//! OLDEST entries. Entries older than the oldest bucket are removed.
//! Entries dated *after* `now` (clock-skew defence) are unconditionally
//! kept.
//!
//! Oldest, not youngest, is what makes the grid a grid: the survivor of a
//! bucket ages out of it into the next one, where it becomes that
//! bucket's oldest and survives again. Keeping the youngest instead means
//! every bucket's survivor is displaced the moment a newer snapshot ages
//! into it, so nothing ever reaches the second bucket and a
//! `4x15m | 24x1h | 14x1d` grid over 15-minute snapshots retained five
//! snapshots spanning one hour instead of ~42 spanning two weeks.
//!
//! See `zrepl/internal/pruning/retentiongrid/retentiongrid.go`. We
//! return *indices* into the caller's slice so the caller does not have
//! to clone snapshot names.

use time::OffsetDateTime;

use super::{GridSpec, KeepCount};

/// Minimal shape the retention algorithm needs. Built by the snap job
/// from `zfskit::dataset::list` output.
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    pub name: String,
    pub creation: OffsetDateTime,
}

struct Bucket {
    keep_count: KeepCount,
    younger_than: OffsetDateTime,
    older_than_or_eq: OffsetDateTime,
    indices: Vec<usize>,
}

impl Bucket {
    fn contains(&self, when: OffsetDateTime) -> bool {
        // (when <= older_than_or_eq) && (when > younger_than)
        when <= self.older_than_or_eq && when > self.younger_than
    }
}

impl GridSpec {
    /// Returns `(keep_indices, destroy_indices)`.
    pub fn fit(&self, entries: &[SnapshotEntry]) -> (Vec<usize>, Vec<usize>) {
        let mut keep: Vec<usize> = Vec::new();
        let mut destroy: Vec<usize> = Vec::new();

        if entries.is_empty() {
            return (keep, destroy);
        }

        // `now` = youngest entry's creation (zrepl uses youngest entry as
        // the reference, NOT wall-clock — protects against clock skew).
        let now = entries.iter().map(|e| e.creation).max().expect("non-empty");

        let intervals = &self.0;
        let mut buckets: Vec<Bucket> = Vec::with_capacity(intervals.len());
        let mut prev_younger = now;
        for iv in intervals {
            let older_than_or_eq = prev_younger;
            // Saturate instead of panicking: an absurdly long grid would
            // otherwise overflow OffsetDateTime's representable range. The
            // floored bucket still covers everything older, so no entry is
            // mis-bucketed into the destroy set.
            let younger_than = time::Duration::try_from(iv.length)
                .ok()
                .and_then(|d| older_than_or_eq.checked_sub(d))
                .unwrap_or_else(|| time::PrimitiveDateTime::MIN.assume_utc());
            buckets.push(Bucket {
                keep_count: iv.keep_count,
                younger_than,
                older_than_or_eq,
                indices: Vec::new(),
            });
            prev_younger = younger_than;
        }

        'next_entry: for (idx, e) in entries.iter().enumerate() {
            // Future entries unconditionally kept.
            if e.creation > now {
                keep.push(idx);
                continue;
            }
            for b in buckets.iter_mut() {
                if b.contains(e.creation) {
                    b.indices.push(idx);
                    continue 'next_entry;
                }
            }
            // Older than the oldest bucket: destroy.
            destroy.push(idx);
        }

        // Apply per-bucket keep_count: keep the oldest `keep_count`,
        // destroy the younger rest (zrepl's
        // `RemoveYoungerSnapsExceedingKeepCount`).
        for b in buckets.iter_mut() {
            match b.keep_count {
                KeepCount::All => {
                    keep.extend(b.indices.iter().copied());
                }
                KeepCount::Exactly(n) => {
                    // Sort youngest-to-oldest by creation.
                    b.indices
                        .sort_by(|a, c| entries[*c].creation.cmp(&entries[*a].creation));
                    let n = n as usize;
                    if b.indices.len() <= n {
                        keep.extend(b.indices.iter().copied());
                    } else {
                        let excess = b.indices.len() - n;
                        destroy.extend(b.indices[..excess].iter().copied());
                        keep.extend(b.indices[excess..].iter().copied());
                    }
                }
            }
        }

        keep.sort_unstable();
        destroy.sort_unstable();
        (keep, destroy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn epoch_plus(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).unwrap()
    }

    fn entry(name: &str, t: i64) -> SnapshotEntry {
        SnapshotEntry {
            name: name.into(),
            creation: epoch_plus(t),
        }
    }

    #[test]
    fn empty_input_is_noop() {
        let g = GridSpec::parse("3x1h").unwrap();
        let (k, d) = g.fit(&[]);
        assert!(k.is_empty() && d.is_empty());
    }

    fn names<'a>(entries: &'a [SnapshotEntry], idx: &[usize]) -> Vec<&'a str> {
        idx.iter().map(|i| entries[*i].name.as_str()).collect()
    }

    // zrepl keeps the OLDEST `keep` snapshots of a bucket and destroys the
    // newer ones first. The previous version of this test asserted the
    // opposite, and with it the grid retained one hour of history.
    #[test]
    fn keeps_the_oldest_per_bucket_by_default() {
        // 3 buckets of 1 hour each. Now = 3600 (youngest).
        // Buckets: (0,3600] | (-3600,0] | (-7200,-3600]
        let g = GridSpec::parse("3x1h").unwrap();
        let entries = vec![
            entry("s_now", 3600),
            entry("s_older_in_same_bucket", 3500),
            entry("s_old", -10000), // older than oldest bucket
        ];
        let (keep, destroy) = g.fit(&entries);
        assert_eq!(names(&entries, &keep), vec!["s_older_in_same_bucket"]);
        assert_eq!(names(&entries, &destroy), vec!["s_now", "s_old"]);
    }

    // The worked example from zrepl's docs (configuration/prune.rst,
    // "Policy grid"): `1x1h(keep=all) | 2x2h | 1x3h` over snapshots @a
    // (newest) .. @D (almost 9h older). Expected survivors: a b c | i | p | z.
    #[test]
    fn matches_the_worked_example_from_zrepl_docs() {
        let g = GridSpec::parse("1x1h(keep=all) | 2x2h | 1x3h").unwrap();
        // Bucket edges from `now`: 1h, 3h, 5h, 8h. Place the letters at
        // the ages the diagram shows, in minutes before @a.
        let placement: [(&str, i64); 30] = [
            ("a", 0),
            ("b", 20),
            ("c", 40),
            ("d", 70),
            ("e", 90),
            ("f", 110),
            ("g", 130),
            ("h", 150),
            ("i", 170),
            ("j", 190),
            ("k", 210),
            ("l", 230),
            ("m", 250),
            ("n", 270),
            ("o", 280),
            ("p", 295),
            ("q", 310),
            ("r", 330),
            ("s", 350),
            ("t", 370),
            ("u", 390),
            ("v", 410),
            ("w", 430),
            ("x", 450),
            ("y", 465),
            ("z", 475),
            ("A", 490),
            ("B", 505),
            ("C", 520),
            ("D", 535),
        ];
        let now = 9 * 3600;
        let entries: Vec<SnapshotEntry> = placement
            .iter()
            .map(|(n, minutes_ago)| entry(n, now - minutes_ago * 60))
            .collect();
        let (keep, _destroy) = g.fit(&entries);
        assert_eq!(names(&entries, &keep), vec!["a", "b", "c", "i", "p", "z"]);
    }

    // Prune runs after every snapshot in production, so the grid has to be
    // judged over time, not on one static set. This replays 20 days of a
    // 15-minute snap job through the example config's grid and expects the
    // grid's shape to actually materialise: four quarter-hours, one per
    // hour for a day, one per day for two weeks.
    #[test]
    fn a_snap_job_replayed_over_time_fills_every_bucket() {
        let g = GridSpec::parse("4x15m | 24x1h | 14x1d").unwrap();
        let step = 15 * 60;
        let mut snaps: Vec<SnapshotEntry> = Vec::new();
        let mut t = 0i64;
        while t <= 20 * 86_400 {
            snaps.push(entry(&format!("s{t}"), t));
            let (_keep, destroy) = g.fit(&snaps);
            let doomed: std::collections::BTreeSet<usize> = destroy.into_iter().collect();
            snaps = snaps
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !doomed.contains(i))
                .map(|(_, e)| e)
                .collect();
            t += step;
        }
        let now = snaps.iter().map(|e| e.creation).max().unwrap();
        let mut ages_h: Vec<i64> = snaps
            .iter()
            .map(|e| (now - e.creation).whole_minutes() / 60)
            .collect();
        ages_h.sort_unstable();
        // 4 in the first hour, then hourly through a day, then daily.
        assert_eq!(snaps.len(), 4 + 24 + 14, "ages (h): {ages_h:?}");
        assert_eq!(*ages_h.last().unwrap(), 15 * 24, "ages (h): {ages_h:?}");
        assert_eq!(
            &ages_h[..8],
            &[0, 0, 0, 0, 1, 2, 3, 4],
            "ages (h): {ages_h:?}"
        );
    }

    #[test]
    fn keep_all_retains_every_bucket_entry() {
        // 1x1h: bucket is (0, 3600]. Entry "d" at -1 is older.
        let g = GridSpec::parse("1x1h(keep=all)").unwrap();
        let entries = vec![
            entry("a", 3600),
            entry("b", 3500),
            entry("c", 3000),
            entry("d", -1), // older than bucket — destroy
        ];
        let (keep, destroy) = g.fit(&entries);
        let kn: Vec<&str> = keep.iter().map(|i| entries[*i].name.as_str()).collect();
        let dn: Vec<&str> = destroy.iter().map(|i| entries[*i].name.as_str()).collect();
        assert!(kn.contains(&"a") && kn.contains(&"b") && kn.contains(&"c"));
        assert!(dn.contains(&"d"));
    }

    // `now` is the youngest entry, so no entry can be "in the future": the
    // newest snapshot is simply the young end of the first bucket, and
    // with keep=1 it is the one that goes.
    #[test]
    fn the_newest_snapshot_is_not_special() {
        let g = GridSpec::parse("1x1h").unwrap();
        let entries = vec![entry("older", 3600), entry("newest", 4000)];
        let (keep, destroy) = g.fit(&entries);
        assert_eq!(names(&entries, &keep), vec!["older"]);
        assert_eq!(names(&entries, &destroy), vec!["newest"]);
    }

    #[test]
    fn duration_arithmetic_uses_time_crate() {
        // sanity-check our use of time::Duration vs std::time::Duration
        let g = GridSpec(vec![super::super::RetentionInterval {
            length: Duration::from_secs(3600),
            keep_count: KeepCount::Exactly(1),
        }]);
        let (k, d) = g.fit(&[entry("a", 3600), entry("b", 0)]);
        assert!(!k.is_empty());
        assert!(!d.is_empty());
    }
}
