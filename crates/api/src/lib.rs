//! Shared request/response types for the arctern HTTP API.
//!
//! Wire types decouple the daemon's HTTP surface from `palimpsest`'s
//! internal models so palimpsest can refactor freely without breaking
//! the API. Both the in-process axum router and the `arctern-client`
//! crate consume these types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Slim projection of [`palimpsest::ZfsListEntry`] suitable for HTTP +
/// OpenAPI. Native ZFS properties carry typed data (bytes, bool, …) but
/// `BTreeMap<String, String>` serializes more cleanly through utoipa;
/// consumers parse property values as needed.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatasetSummary {
    pub name: String,
    /// `"filesystem" | "volume" | "snapshot" | "bookmark"` — lowercase
    /// to match `zfs(8)`'s output and avoid leaking palimpsest's enum repr.
    pub dataset_type: String,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

impl From<palimpsest::dataset::ZfsListEntry> for DatasetSummary {
    fn from(entry: palimpsest::dataset::ZfsListEntry) -> Self {
        let properties = entry
            .properties
            .into_iter()
            .map(|(k, v)| (k, v.value))
            .collect();
        Self {
            name: entry.name,
            dataset_type: entry.kind.cli_name().to_string(),
            properties,
        }
    }
}

/// Body shape for `4xx`/`5xx` responses from the daemon. `error` is a
/// short machine-readable category (`spawn`, `dataset_not_found`, …);
/// `message` is a human-readable description.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorBody {
    pub error: String,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list_entry(name: &str, kind: palimpsest::models::DatasetType) -> palimpsest::dataset::ZfsListEntry {
        palimpsest::dataset::ZfsListEntry {
            name: name.into(),
            kind,
            pool: name.split('/').next().unwrap().to_string(),
            createtxg: "1".into(),
            dataset: None,
            snapshot_name: None,
            properties: Default::default(),
        }
    }

    #[test]
    fn from_zfs_list_entry_lowercases_kind() {
        let s = DatasetSummary::from(list_entry("tank", palimpsest::models::DatasetType::Filesystem));
        assert_eq!(s.name, "tank");
        assert_eq!(s.dataset_type, "filesystem");
    }

    #[test]
    fn serde_roundtrip() {
        let s = DatasetSummary {
            name: "tank/data".into(),
            dataset_type: "filesystem".into(),
            properties: [("compression".to_string(), "lz4".to_string())]
                .into_iter()
                .collect(),
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: DatasetSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, s.name);
        assert_eq!(back.dataset_type, s.dataset_type);
        assert_eq!(back.properties.get("compression").map(String::as_str), Some("lz4"));
    }
}
