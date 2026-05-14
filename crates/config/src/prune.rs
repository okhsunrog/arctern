//! Combine `KeepRule`s into a final destroy set.
//!
//! Per-rule semantics (matching zrepl's `internal/pruning/keep_grid.go`
//! and `keep_regex.go`):
//!
//! - `Grid { regex, grid }`: snapshots whose name does NOT match `regex`
//!   are NOT kept by this rule (they are in this rule's destroy list).
//!   Among those that match, the grid algorithm decides which to keep.
//! - `Regex { regex, negate=false }`: snapshots whose name MATCHES
//!   `regex` are kept by this rule; those that DO NOT match are in its
//!   destroy list. (The regex picks the *protected* set.) With
//!   `negate=true`, the polarity flips: snapshots that MATCH are in the
//!   destroy list, and those that do not match are protected.
//!   This matches `zrepl/internal/pruning/keep_regex.go` exactly:
//!   `negate=true` is the typical zrepl idiom paired with a grid rule
//!   so that manual (non-`zrepl_`) snapshots are unconditionally kept
//!   by the negate rule, while the grid decides which `zrepl_` ones to
//!   destroy.
//!
//! Combination rule (matches zrepl's `internal/pruning/pruning.go::PruneSnapshots`):
//! a snapshot is destroyed iff EVERY rule's destroy list contains it.
//! Intersection across rules; if even one rule wants to keep it, it
//! survives. This is what gives the user's "grid for `^zrepl_.*` +
//! regex-negate to keep manual snapshots" config its expected meaning.

use std::collections::BTreeSet;

use regex::Regex;

use crate::grid::SnapshotEntry;
use crate::schema::KeepRule;

#[derive(Debug, thiserror::Error)]
pub enum PruneError {
    #[error("invalid regex {pattern:?}: {source}")]
    Regex {
        pattern: String,
        #[source]
        source: regex::Error,
    },
}

impl KeepRule {
    /// Indices into `entries` this rule wants destroyed.
    pub fn destroy_set(&self, entries: &[SnapshotEntry]) -> Result<BTreeSet<usize>, PruneError> {
        match self {
            KeepRule::Grid { grid, regex } => {
                let re = compile(regex)?;
                let mut destroy: BTreeSet<usize> = BTreeSet::new();
                let mut matching: Vec<(usize, SnapshotEntry)> = Vec::new();
                for (i, e) in entries.iter().enumerate() {
                    if re.is_match(&e.name) {
                        matching.push((i, e.clone()));
                    } else {
                        // Non-matching entries are in this rule's destroy
                        // list — the grid keeps no opinion on them; only
                        // the intersection across all rules can save them.
                        destroy.insert(i);
                    }
                }
                if matching.is_empty() {
                    return Ok(destroy);
                }
                let only_entries: Vec<SnapshotEntry> =
                    matching.iter().map(|(_, e)| e.clone()).collect();
                let (_keep, grid_destroy) = grid.fit(&only_entries);
                for inner_idx in grid_destroy {
                    let outer_idx = matching[inner_idx].0;
                    destroy.insert(outer_idx);
                }
                Ok(destroy)
            }
            KeepRule::Regex { regex, negate } => {
                let re = compile(regex)?;
                let mut destroy: BTreeSet<usize> = BTreeSet::new();
                for (i, e) in entries.iter().enumerate() {
                    let matches = re.is_match(&e.name);
                    // negate=false: regex picks the protected set,
                    // non-matching go in destroy.
                    // negate=true: matching go in destroy, non-matching protected.
                    let in_destroy = if *negate { matches } else { !matches };
                    if in_destroy {
                        destroy.insert(i);
                    }
                }
                Ok(destroy)
            }
        }
    }
}

fn compile(pattern: &str) -> Result<Regex, PruneError> {
    Regex::new(pattern).map_err(|source| PruneError::Regex {
        pattern: pattern.to_string(),
        source,
    })
}

/// Intersection of every rule's destroy set. With zero rules, no
/// snapshot is destroyed (vacuous: an empty intersection over the
/// snapshot universe is the universe itself, but we explicitly return
/// empty — pruning with no rules means "do not prune").
pub fn evaluate(rules: &[KeepRule], entries: &[SnapshotEntry]) -> Result<Vec<usize>, PruneError> {
    if rules.is_empty() {
        return Ok(Vec::new());
    }
    let mut iter = rules.iter();
    let first = iter
        .next()
        .expect("non-empty by check above")
        .destroy_set(entries)?;
    let mut acc = first;
    for r in iter {
        let next = r.destroy_set(entries)?;
        acc = acc.intersection(&next).copied().collect();
    }
    Ok(acc.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grid::GridSpec;
    use time::OffsetDateTime;

    fn entry(name: &str, t: i64) -> SnapshotEntry {
        SnapshotEntry {
            name: name.into(),
            creation: OffsetDateTime::from_unix_timestamp(t).unwrap(),
        }
    }

    #[test]
    fn regex_negate_destroys_matched() {
        let rule = KeepRule::Regex {
            regex: "^zrepl_".into(),
            negate: true,
        };
        let entries = vec![entry("zrepl_a", 1), entry("manual_b", 1)];
        let d = rule.destroy_set(&entries).unwrap();
        // negate=true: matching go in destroy; non-matching protected.
        assert!(d.contains(&0));
        assert!(!d.contains(&1));
    }

    #[test]
    fn regex_default_destroys_unmatched() {
        let rule = KeepRule::Regex {
            regex: "^zrepl_".into(),
            negate: false,
        };
        let entries = vec![entry("zrepl_a", 1), entry("manual_b", 1)];
        let d = rule.destroy_set(&entries).unwrap();
        assert!(!d.contains(&0));
        assert!(d.contains(&1));
    }

    #[test]
    fn intersection_protects_manual_snapshots() {
        // Mirrors the user's databak config:
        //   grid keeps some zrepl_* snapshots
        //   regex(negate=true) keeps everything not matching ^zrepl_*
        // → manual snapshots survive (one of the rules always keeps them).
        let rules = vec![
            KeepRule::Grid {
                grid: GridSpec::parse("1x1h").unwrap(),
                regex: "^zrepl_".into(),
            },
            KeepRule::Regex {
                regex: "^zrepl_".into(),
                negate: true,
            },
        ];
        let entries = vec![
            entry("zrepl_old", 0),       // grid would destroy (older than 1h from now=3600)
            entry("zrepl_recent", 3600), // grid keeps (in bucket)
            entry("manual_snapshot", 100), // grid would destroy (non-matching), regex(negate) keeps
        ];
        let destroy = evaluate(&rules, &entries).unwrap();
        let names: Vec<&str> = destroy.iter().map(|i| entries[*i].name.as_str()).collect();
        assert!(names.contains(&"zrepl_old"));
        assert!(!names.contains(&"zrepl_recent"));
        assert!(!names.contains(&"manual_snapshot"));
    }

    #[test]
    fn no_rules_means_no_destroy() {
        let entries = vec![entry("a", 1)];
        let destroy = evaluate(&[], &entries).unwrap();
        assert!(destroy.is_empty());
    }

    #[test]
    fn invalid_regex_surfaces_error() {
        let rule = KeepRule::Regex {
            regex: "(".into(),
            negate: false,
        };
        let entries = vec![entry("a", 1)];
        assert!(rule.destroy_set(&entries).is_err());
    }
}
