//! Retention grid: `"6x4h | 14x1d"`-style expressions parsed into a
//! sequence of `RetentionInterval`s, plus the bucketing/keep-count
//! algorithm. Source of truth: `zrepl/internal/config/retentiongrid.go`
//! (parser) + `zrepl/internal/pruning/retentiongrid/retentiongrid.go`
//! (algorithm). Both are small and a faithful port is preferable to a
//! parser-combinator dep.

mod retention;

use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};
use thiserror::Error;

pub use retention::SnapshotEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepCount {
    All,
    Exactly(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionInterval {
    pub length: Duration,
    pub keep_count: KeepCount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GridSpec(pub Vec<RetentionInterval>);

#[derive(Debug, Error)]
pub enum GridParseError {
    #[error("term {term_index} ({term:?}): {message}")]
    Term {
        term_index: usize,
        term: String,
        message: String,
    },
    #[error(
        "interval lengths must be monotonically non-decreasing (saw {prev:?} then {next:?}); a `keep=all` prefix run is the only exception"
    )]
    NonMonotonic { prev: Duration, next: Duration },
    #[error("grid expression must contain at least one term")]
    Empty,
}

// Single regex for the per-term shape, compiled once. zrepl uses the
// same pattern; we keep field names compatible.
fn term_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^\s*(\d+)\s*x\s*([^(]+?)\s*(?:\((.*)\))?\s*$").expect("static regex compiles")
    })
}

fn keep_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*keep\s*=\s*(.+?)\s*$").expect("static regex compiles"))
}

impl GridSpec {
    pub fn parse(input: &str) -> Result<Self, GridParseError> {
        let mut intervals: Vec<RetentionInterval> = Vec::new();
        let term_re = term_regex();
        let keep_re = keep_regex();

        let terms: Vec<&str> = input.split('|').collect();
        if terms.iter().all(|t| t.trim().is_empty()) {
            return Err(GridParseError::Empty);
        }

        for (idx, raw) in terms.iter().enumerate() {
            let trimmed = raw.trim();
            let caps = term_re
                .captures(trimmed)
                .ok_or_else(|| GridParseError::Term {
                    term_index: idx,
                    term: trimmed.to_string(),
                    message: "does not match `<count>x<duration>(keep=...)?`".to_string(),
                })?;

            let count: u32 = caps[1].parse().map_err(|e| GridParseError::Term {
                term_index: idx,
                term: trimmed.to_string(),
                message: format!("count: {e}"),
            })?;
            if count == 0 {
                return Err(GridParseError::Term {
                    term_index: idx,
                    term: trimmed.to_string(),
                    message: "count must be > 0".into(),
                });
            }

            let dur_str = caps[2].trim();
            let length = humantime::parse_duration(dur_str).map_err(|e| GridParseError::Term {
                term_index: idx,
                term: trimmed.to_string(),
                message: format!("duration {dur_str:?}: {e}"),
            })?;

            let keep_count = if let Some(modifier) = caps.get(3) {
                let kc =
                    keep_re
                        .captures(modifier.as_str())
                        .ok_or_else(|| GridParseError::Term {
                            term_index: idx,
                            term: trimmed.to_string(),
                            message: format!(
                                "modifier {:?}: only `keep=N` or `keep=all` is supported",
                                modifier.as_str()
                            ),
                        })?;
                let val = kc[1].trim();
                if val.eq_ignore_ascii_case("all") {
                    KeepCount::All
                } else {
                    let n: u32 = val.parse().map_err(|e| GridParseError::Term {
                        term_index: idx,
                        term: trimmed.to_string(),
                        message: format!("keep count {val:?}: {e}"),
                    })?;
                    if n == 0 {
                        return Err(GridParseError::Term {
                            term_index: idx,
                            term: trimmed.to_string(),
                            message: "keep count must be > 0 or `all`".into(),
                        });
                    }
                    KeepCount::Exactly(n)
                }
            } else {
                KeepCount::Exactly(1)
            };

            for _ in 0..count {
                intervals.push(RetentionInterval { length, keep_count });
            }
        }

        if intervals.is_empty() {
            return Err(GridParseError::Empty);
        }

        // Monotonic-non-decreasing check with the "all preceding are keep=all"
        // carve-out (zrepl's behaviour: `4x15m(keep=all) | 24x1h` is legal
        // even though 15m < 1h, because nothing earlier survives the bucket).
        let mut last = Duration::ZERO;
        for (i, iv) in intervals.iter().enumerate() {
            if iv.length < last {
                let all_prev_keep_all = intervals[..i]
                    .iter()
                    .all(|p| matches!(p.keep_count, KeepCount::All));
                if !all_prev_keep_all {
                    return Err(GridParseError::NonMonotonic {
                        prev: last,
                        next: iv.length,
                    });
                }
            }
            last = iv.length;
        }

        Ok(GridSpec(intervals))
    }
}

impl<'de> Deserialize<'de> for GridSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        GridSpec::parse(&s).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Duration {
        humantime::parse_duration(s).unwrap()
    }

    #[test]
    fn parses_simple() {
        let g = GridSpec::parse("6x4h | 14x1d").unwrap();
        assert_eq!(g.0.len(), 20);
        assert_eq!(g.0[0].length, d("4h"));
        assert_eq!(g.0[0].keep_count, KeepCount::Exactly(1));
        assert_eq!(g.0[6].length, d("1d"));
    }

    #[test]
    fn parses_keep_all() {
        let g = GridSpec::parse("4x15m(keep=all) | 24x1h | 3x1d").unwrap();
        assert_eq!(g.0.len(), 31);
        assert_eq!(g.0[0].keep_count, KeepCount::All);
        assert_eq!(g.0[4].length, d("1h"));
        assert_eq!(g.0[4].keep_count, KeepCount::Exactly(1));
    }

    #[test]
    fn parses_keep_n() {
        let g = GridSpec::parse("3x1h(keep=2)").unwrap();
        assert_eq!(g.0[0].keep_count, KeepCount::Exactly(2));
    }

    #[test]
    fn rejects_zero_count() {
        assert!(matches!(
            GridSpec::parse("0x4h"),
            Err(GridParseError::Term { .. })
        ));
    }

    #[test]
    fn rejects_bad_duration() {
        assert!(matches!(
            GridSpec::parse("6x4z"),
            Err(GridParseError::Term { .. })
        ));
    }

    #[test]
    fn rejects_descending_without_keep_all() {
        // 4h then 1m: monotonic violation.
        assert!(matches!(
            GridSpec::parse("1x4h | 1x1m"),
            Err(GridParseError::NonMonotonic { .. })
        ));
    }

    #[test]
    fn permits_descending_after_keep_all() {
        // First all-keep_all run is exempt.
        GridSpec::parse("2x1h(keep=all) | 1x10m(keep=all) | 1x1d").unwrap();
    }

    #[test]
    fn empty_string_is_error() {
        assert!(matches!(GridSpec::parse(""), Err(GridParseError::Empty)));
        assert!(matches!(GridSpec::parse("   "), Err(GridParseError::Empty)));
    }

    #[test]
    fn rejects_unknown_modifier() {
        assert!(GridSpec::parse("1x1h(foo=bar)").is_err());
    }
}
