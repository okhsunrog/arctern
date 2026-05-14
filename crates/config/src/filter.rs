//! Resolve a `FilesystemFilter` against a flat list of dataset names.
//!
//! The snap job calls `palimpsest::dataset::list` once per cycle (with
//! `recursive = true`) to materialize every dataset under each filter's
//! `path`, then asks `resolve_all` to compute the union the job should
//! act on. Doing the matching in-process means we never have to issue
//! one `zfs list` per filter.

use crate::schema::FilesystemFilter;

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

fn excluded(candidate: &str, excludes: &[String], root: &str) -> bool {
    for e in excludes {
        // Special case from FR-019: excluding `path` itself means
        // "snapshot only descendants" — drop the root, keep its
        // children.
        if e == root {
            if candidate == root {
                return true;
            }
            continue;
        }
        if candidate == e {
            return true;
        }
        let prefix = format!("{e}/");
        if candidate.starts_with(&prefix) {
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
    fn recursive_excludes_subtree() {
        let f = f("tank", true, &["tank/data"]);
        let cands = vec!["tank", "tank/data", "tank/data/home", "tank/var"];
        assert_eq!(f.resolve(&cands), vec!["tank", "tank/var"]);
    }

    #[test]
    fn zrepl_tree_pattern_with_excludes_translates() {
        // zrepl yaml: { "novafs/arch0<": true, "novafs/arch0": false,
        //              "novafs/arch0/data": false }
        // arctern toml: path="novafs/arch0", recursive=true,
        //               exclude=["novafs/arch0", "novafs/arch0/data"]
        // Net effect: every descendant of novafs/arch0 except the
        // root itself and the data subtree.
        let f = f(
            "novafs/arch0",
            true,
            &["novafs/arch0", "novafs/arch0/data"],
        );
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
