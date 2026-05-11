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

/// String constant for the `snap` job kind. The wire field is a
/// `String` (not an enum) so that adding a future job kind in a later
/// slice does not break clients pinned to an older `JobKind` enum
/// definition.
pub const JOB_KIND_SNAP: &str = "snap";

/// String constant for the `sink` job kind. See `JOB_KIND_SNAP` for the
/// rationale (string field on the wire so adding kinds is non-breaking).
pub const JOB_KIND_SINK: &str = "sink";

/// String constant for the `push` job kind. See `JOB_KIND_SNAP` for the
/// rationale.
pub const JOB_KIND_PUSH: &str = "push";

/// One entry in the response of `GET /api/v1/jobs`. RFC3339 timestamps
/// are nullable: `last_run` is null until the job has completed at
/// least one cycle; `next_run` is set as soon as the loop knows when
/// it will fire next; `last_error` is null when the most recent cycle
/// finished cleanly.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobStatus {
    pub name: String,
    pub kind: String,
    pub last_run: Option<String>,
    pub next_run: Option<String>,
    pub last_error: Option<String>,
}

/// One row of `job_runs` returned by `GET /api/v1/jobs/{name}/runs`.
/// `started_at` / `finished_at` are unix seconds; `bytes_sent` is set
/// only by push jobs that finished cleanly.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobRun {
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub status: String,
    pub error_message: Option<String>,
    pub bytes_sent: Option<i64>,
}

/// Body shape for `4xx`/`5xx` responses from the daemon. `error` is a
/// short machine-readable category (`spawn`, `dataset_not_found`, …);
/// `message` is a human-readable description.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorBody {
    pub error: String,
    pub message: String,
}

/// Reachability classification for one configured peer. The daemon
/// updates this from its background reconnect loop; the UI surfaces
/// it in the Peers tab.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PeerReachability {
    /// SSH session is up and the control channel is responding.
    Connected,
    /// Background task is between reconnect attempts.
    Reconnecting {
        /// RFC3339 timestamp the link first went down.
        since: String,
    },
    /// Last connect attempt failed; the loop is sleeping before retrying.
    Failed {
        /// RFC3339 timestamp the link first went down (or last failed).
        since: String,
        last_error: String,
    },
}

/// One row in `GET /api/v1/peers`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PeerSummary {
    pub name: String,
    pub ssh_target: String,
    pub reachability: PeerReachability,
}

/// One snapshot returned by `GET /api/v1/peers/{peer}/snapshots`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PeerSnapshotEntry {
    pub name: String,
    /// ZFS GUID, serialized as a u64 string to stay safe across
    /// JSON parsers that downgrade large integers to f64.
    pub guid: String,
    pub createtxg: u64,
}

/// One row in `GET /api/v1/events` (and the proxied
/// `GET /api/v1/peers/{peer}/events`). Mirrors
/// `arctern_transport::EventWire` but lives in the public API surface
/// so clients don't pull in the transport crate.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LogEvent {
    pub id: u64,
    /// Unix seconds.
    pub timestamp: i64,
    pub level: String,
    pub job_name: Option<String>,
    pub message: String,
}

/// Request body for `POST /api/v1/datasets/{name}/snapshots`. The path
/// segment carries the parent dataset; this struct carries everything
/// else. `recursive` and `properties` default so a minimal client can
/// post `{"snapshot_name":"…"}` and get the common case.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct CreateSnapshotRequest {
    pub snapshot_name: String,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
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
    fn create_snapshot_request_defaults() {
        let req: CreateSnapshotRequest =
            serde_json::from_str(r#"{"snapshot_name":"s1"}"#).unwrap();
        assert_eq!(req.snapshot_name, "s1");
        assert!(!req.recursive);
        assert!(req.properties.is_empty());
    }

    #[test]
    fn create_snapshot_request_full_roundtrip() {
        let req = CreateSnapshotRequest {
            snapshot_name: "manual-2026-05-09".into(),
            recursive: true,
            properties: [("user:reason".to_string(), "manual".to_string())]
                .into_iter()
                .collect(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: CreateSnapshotRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.snapshot_name, req.snapshot_name);
        assert!(back.recursive);
        assert_eq!(back.properties.get("user:reason").map(String::as_str), Some("manual"));
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
