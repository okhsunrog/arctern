//! `zfs list` invocations shared by the jobs and the receiver-side
//! control channel: which datasets a job's filters cover, and the
//! GUID-carrying snapshot and bookmark inventories the planner
//! intersects.

use std::collections::BTreeSet;

use arctern_config::FilesystemFilter;
use arctern_transport::SnapshotEntry;
use zfskit::ZfsError;
use zfskit::dataset::{ListOptions, ZfsListEntry};
use zfskit::models::DatasetType;
use zfskit::runner::CommandRunner;

/// One sender-side bookmark. `leaf` is the part after `#`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookmarkRef {
    pub leaf: String,
    pub guid: u64,
    pub createtxg: u64,
}

/// The pools a filter set touches. Listing is scoped to these rather
/// than the whole host so unrelated pools stay out of the result.
pub fn pool_roots(filters: &[FilesystemFilter]) -> Vec<String> {
    let pools: BTreeSet<String> = filters
        .iter()
        .map(|f| f.path.split('/').next().unwrap_or(&f.path).to_string())
        .collect();
    pools.into_iter().collect()
}

/// Every filesystem and volume under the pools `filters` reference.
pub async fn list_filesystems(
    runner: &dyn CommandRunner,
    filters: &[FilesystemFilter],
) -> Result<Vec<ZfsListEntry>, ZfsError> {
    let opts = ListOptions {
        recursive: true,
        types: vec![DatasetType::Filesystem, DatasetType::Volume],
        roots: pool_roots(filters),
        ..ListOptions::default()
    };
    zfskit::dataset::list(runner, &opts).await
}

/// A snapshot list entry as the planner sees it: leaf name, GUID,
/// createtxg. None when the entry is not a snapshot or lacks a
/// parseable GUID.
pub fn snapshot_entry(e: &ZfsListEntry) -> Option<SnapshotEntry> {
    let name = e.snapshot_name.clone()?;
    let guid = e.properties.get("guid")?.value.parse::<u64>().ok()?;
    let createtxg = e.createtxg.parse::<u64>().ok()?;
    Some(SnapshotEntry {
        name,
        guid,
        createtxg,
    })
}

/// `dataset`'s snapshots whose leaf name passes `keep`, oldest first.
pub async fn list_snapshots(
    runner: &dyn CommandRunner,
    dataset: &str,
    keep: impl Fn(&str) -> bool,
) -> Result<Vec<SnapshotEntry>, ZfsError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(runner, &opts).await?;
    let mut snaps: Vec<SnapshotEntry> = entries
        .iter()
        .filter_map(snapshot_entry)
        .filter(|s| keep(&s.name))
        .collect();
    snaps.sort_by_key(|s| s.createtxg);
    Ok(snaps)
}

/// Every bookmark of `dataset` with its GUID. Unfiltered by name on
/// purpose: the planner matches by GUID, and foreign bookmarks (zrepl
/// cursors) are exactly the migration case.
pub async fn list_bookmarks(
    runner: &dyn CommandRunner,
    dataset: &str,
) -> Result<Vec<BookmarkRef>, ZfsError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Bookmark],
        roots: vec![dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(runner, &opts).await?;
    Ok(entries
        .into_iter()
        .filter_map(|e| {
            let leaf = e.name.split_once('#').map(|(_, l)| l.to_string())?;
            let guid = e.properties.get("guid")?.value.parse::<u64>().ok()?;
            let createtxg = e.createtxg.parse::<u64>().ok()?;
            Some(BookmarkRef {
                leaf,
                guid,
                createtxg,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(path: &str) -> FilesystemFilter {
        FilesystemFilter {
            path: path.into(),
            recursive: true,
            exclude: Vec::new(),
        }
    }

    #[test]
    fn pool_roots_dedupe_and_sort() {
        let roots = pool_roots(&[
            filter("tank/data"),
            filter("tank/home"),
            filter("backup/x"),
            filter("tank"),
        ]);
        assert_eq!(roots, vec!["backup", "tank"]);
    }
}
