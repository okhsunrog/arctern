//! Resolve a `FilesystemFilter` against a flat list of dataset names.
//!
//! The snap job calls `palimpsest::dataset::list` once per cycle (with
//! `recursive = true`) to materialize every dataset under each filter's
//! `path`, then asks `resolve_all` to compute the union the job should
//! act on. Doing the matching in-process means we never have to issue
//! one `zfs list` per filter.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use crate::schema::FilesystemFilter;

/// `filesystems` accepts two shapes:
///
/// 1. The original array of tables (`[[jobs.filesystems]] path=...`),
///    which deserializes straight into `Vec<FilesystemFilter>`.
/// 2. A zrepl-style inline map:
///
///    ```toml
///    filesystems = {
///      "novafs/arch0/" = true,
///      "novafs/arch0" = false,
///      "novafs/arch0/data" = false,
///    }
///    ```
///
///    Keys ending in `/` mean "include this subtree" (recursive=true).
///    Bare keys mean "include this exact dataset" (recursive=false).
///    `false` values are excludes for whichever `true` subtree they sit
///    under, with the same suffix rule: a bare key excludes exactly that
///    dataset (its children stay included), a trailing `/` excludes the
///    whole subtree. An exclude with no matching parent is a config error.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum FilesystemsInput {
    List(Vec<FilesystemFilter>),
    Map(BTreeMap<String, bool>),
}

pub fn deserialize_filesystems<'de, D>(d: D) -> Result<Vec<FilesystemFilter>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let input = FilesystemsInput::deserialize(d)?;
    match input {
        FilesystemsInput::List(v) => Ok(v),
        FilesystemsInput::Map(m) => map_to_filters(m).map_err(serde::de::Error::custom),
    }
}

fn map_to_filters(m: BTreeMap<String, bool>) -> Result<Vec<FilesystemFilter>, String> {
    // (resolved-path, recursive)
    let mut trues: Vec<(String, bool)> = Vec::new();
    let mut falses: Vec<String> = Vec::new();
    for (k, v) in m {
        if v {
            if let Some(stripped) = k.strip_suffix('/') {
                if stripped.is_empty() {
                    return Err(
                        "filesystems: empty key with `/` suffix is not a valid path".to_string()
                    );
                }
                trues.push((stripped.to_string(), true));
            } else {
                trues.push((k, false));
            }
        } else {
            // Keep the trailing `/` — `excluded()` reads it as
            // "exclude the subtree" vs the bare "exclude exactly this".
            falses.push(k);
        }
    }

    let mut used: BTreeSet<String> = BTreeSet::new();
    let mut out: Vec<FilesystemFilter> = Vec::new();
    for (path, recursive) in &trues {
        let mut exclude: Vec<String> = Vec::new();
        if *recursive {
            for f in &falses {
                let name = f.trim_end_matches('/');
                if name == path || name.starts_with(&format!("{path}/")) {
                    exclude.push(f.clone());
                    used.insert(f.clone());
                }
            }
        }
        out.push(FilesystemFilter {
            path: path.clone(),
            recursive: *recursive,
            exclude,
        });
    }
    for f in &falses {
        if !used.contains(f) {
            return Err(format!(
                "filesystems: exclude {f:?} has no matching `\"<parent>/\" = true` to belong to"
            ));
        }
    }
    Ok(out)
}

impl FilesystemFilter {
    /// Returns the subset of `candidates` selected by this filter.
    pub fn resolve<'a>(&self, candidates: &[&'a str]) -> Vec<&'a str> {
        let path = self.path.as_str();
        let mut out: Vec<&'a str> = Vec::new();
        for c in candidates {
            if !is_under(c, path, self.recursive) {
                continue;
            }
            if self.recursive && excluded(c, &self.exclude, &self.path) {
                continue;
            }
            out.push(c);
        }
        out
    }
}

/// Resolve every filter and dedupe (preserving first-seen order).
pub fn resolve_all<'a>(filters: &[FilesystemFilter], candidates: &[&'a str]) -> Vec<&'a str> {
    let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    let mut out: Vec<&'a str> = Vec::new();
    for f in filters {
        for d in f.resolve(candidates) {
            if seen.insert(d) {
                out.push(d);
            }
        }
    }
    out
}

fn is_under(candidate: &str, root: &str, recursive: bool) -> bool {
    if candidate == root {
        return true;
    }
    if !recursive {
        return false;
    }
    let prefix = format!("{root}/");
    candidate.starts_with(&prefix)
}

/// Exclude-entry semantics mirror the include keys: a bare name drops
/// exactly that dataset (children stay included), a trailing `/` drops
/// the whole subtree. This is what lets a config say "everything under
/// arch0 except the two container datasets themselves" without also
/// losing the containers' children.
fn excluded(candidate: &str, excludes: &[String], _root: &str) -> bool {
    for e in excludes {
        if let Some(name) = e.strip_suffix('/') {
            if candidate == name || candidate.starts_with(&format!("{name}/")) {
                return true;
            }
        } else if candidate == e.as_str() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(path: &str, recursive: bool, exclude: &[&str]) -> FilesystemFilter {
        FilesystemFilter {
            path: path.into(),
            recursive,
            exclude: exclude.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    #[test]
    fn non_recursive_exact_match() {
        let f = f("tank/data", false, &[]);
        let cands = vec!["tank", "tank/data", "tank/data/home"];
        assert_eq!(f.resolve(&cands), vec!["tank/data"]);
    }

    #[test]
    fn recursive_includes_descendants() {
        let f = f("tank/data", true, &[]);
        let cands = vec!["tank", "tank/data", "tank/data/home", "tank/other"];
        assert_eq!(f.resolve(&cands), vec!["tank/data", "tank/data/home"]);
    }

    #[test]
    fn recursive_excludes_root_keeps_descendants() {
        let f = f("tank/data", true, &["tank/data"]);
        let cands = vec!["tank/data", "tank/data/home", "tank/data/var"];
        assert_eq!(f.resolve(&cands), vec!["tank/data/home", "tank/data/var"]);
    }

    #[test]
    fn recursive_excludes_subtree_with_trailing_slash() {
        let f = f("tank", true, &["tank/data/"]);
        let cands = vec!["tank", "tank/data", "tank/data/home", "tank/var"];
        assert_eq!(f.resolve(&cands), vec!["tank", "tank/var"]);
    }

    #[test]
    fn bare_exclude_drops_only_that_dataset() {
        let f = f("tank", true, &["tank/data"]);
        let cands = vec!["tank", "tank/data", "tank/data/home", "tank/var"];
        assert_eq!(
            f.resolve(&cands),
            vec!["tank", "tank/data/home", "tank/var"]
        );
    }

    #[test]
    fn container_excludes_keep_their_children() {
        // The laptop idiom: snapshot everything under novafs/arch0
        // except the two container datasets themselves — their children
        // (data/home, data/root) stay in.
        let f = f("novafs/arch0", true, &["novafs/arch0", "novafs/arch0/data"]);
        let cands = vec![
            "novafs",
            "novafs/arch0",
            "novafs/arch0/data",
            "novafs/arch0/data/home",
            "novafs/arch0/data/root",
            "novafs/arch0/root",
            "novafs/arch0/vm",
            "novafs/arch0/docker",
        ];
        assert_eq!(
            f.resolve(&cands),
            vec![
                "novafs/arch0/data/home",
                "novafs/arch0/data/root",
                "novafs/arch0/root",
                "novafs/arch0/vm",
                "novafs/arch0/docker",
            ],
        );
    }

    #[test]
    fn resolve_all_dedupes() {
        let f1 = f("tank/data", false, &[]);
        let f2 = f("tank", true, &[]);
        let cands = vec!["tank", "tank/data", "tank/var"];
        // f2 catches everything; f1's tank/data is already there.
        let out = resolve_all(&[f1, f2], &cands);
        assert_eq!(out.len(), 3);
        assert!(out.contains(&"tank/data"));
    }
}
