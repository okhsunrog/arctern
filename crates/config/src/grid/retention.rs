//! Direct port of zrepl's retention-grid algorithm. Bucket entries by
//! interval-from-`now` (where `now` is the youngest matching entry's
//! `creation`), then within each bucket retain at most `keep_count`
//! youngest entries. Entries older than the oldest bucket are removed.
//! Entries dated *after* `now` (clock-skew defence) are unconditionally
//! kept.
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

        // Apply per-bucket keep_count: keep youngest `keep_count`,
        // destroy the rest.
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
                        keep.extend(b.indices[..n].iter().copied());
                        destroy.extend(b.indices[n..].iter().copied());
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

    #[test]
    fn keeps_one_per_bucket_by_default() {
        // 3 buckets of 1 hour each. Now = 3600 (youngest).
        // Buckets: (2700,3600] | (-900,2700] | (-4500,-900]
        let g = GridSpec::parse("3x1h").unwrap();
        let entries = vec![
            entry("s_now", 3600),
            entry("s_youngest_extra", 3500),
            entry("s_old", -10000), // older than oldest bucket (younger_than = -7200)
        ];
        let (keep, destroy) = g.fit(&entries);
        let names_keep: Vec<&str> = keep.iter().map(|i| entries[*i].name.as_str()).collect();
        let names_destroy: Vec<&str> = destroy.iter().map(|i| entries[*i].name.as_str()).collect();
        assert!(names_keep.contains(&"s_now"));
        assert!(names_destroy.contains(&"s_youngest_extra"));
        assert!(names_destroy.contains(&"s_old"));
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

    #[test]
    fn future_entries_unconditionally_kept() {
        let g = GridSpec::parse("1x1h").unwrap();
        let entries = vec![entry("present", 3600), entry("future", 4000)];
        let (keep, _destroy) = g.fit(&entries);
        let kn: Vec<&str> = keep.iter().map(|i| entries[*i].name.as_str()).collect();
        assert!(kn.contains(&"future"));
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
