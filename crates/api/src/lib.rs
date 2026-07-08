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
    /// True while a cycle is currently executing (e.g. a multi-hour
    /// full send). `last_*` fields describe the previous cycle.
    #[serde(default)]
    pub running: bool,
    /// True while the job is paused: the current transfer was aborted
    /// (resumably) and scheduled cycles are suspended until resumed.
    #[serde(default)]
    pub paused: bool,
    /// In-flight transfers, one per parallel send slot. UI derives
    /// speed from `bytes_sent` deltas between polls.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transfers: Vec<TransferInfo>,
    /// Push jobs: per-target replication policy + last outcome.
    /// Empty for snap/prune jobs.
    #[serde(default)]
    pub targets: Vec<TargetStatus>,
}

/// Progress of an in-flight `zfs send` stream.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferInfo {
    pub dataset: String,
    pub peer: String,
    /// `"full" | "incremental" | "resume"`.
    pub kind: String,
    pub bytes_sent: u64,
    /// Dry-run estimate. None for resume sends (no estimate available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    /// Unix seconds.
    pub started_at: i64,
}

/// One replication target of a push job.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TargetStatus {
    pub peer: String,
    /// `"auto" | "manual"`.
    pub mode: String,
    pub connected: bool,
    /// Active route name while connected (e.g. `"lan"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    /// Whether the active route permits scheduled replication.
    #[serde(default)]
    pub route_auto: bool,
    /// For auto mode: the configured `auto_interval` in seconds. The
    /// next auto sync is `last_success + auto_interval_secs` (or the
    /// next planner tick when unset/no history).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_interval_secs: Option<u64>,
    /// Unix seconds of the last successful sync to this peer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// One pool's slot in `GET /api/v1/pools`. Numeric fields are
/// `zpool`-formatted strings (e.g. `"608G"`, `"1.48T"`) rather than
/// raw bytes because that's what `zpool` emits and round-tripping
/// through bytes risks rounding mismatches in the UI.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PoolSummary {
    pub name: String,
    /// `"ONLINE"`, `"DEGRADED"`, `"FAULTED"`, …
    pub state: String,
    /// Aggregate error count across all vdevs.
    pub error_count: String,
    pub alloc_space: String,
    pub total_space: String,
    /// Most recent scrub/resilver status if zpool reports one.
    pub scan: Option<ScanSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScanSummary {
    /// `"SCRUB"`, `"RESILVER"`, `"NONE"`.
    pub function: String,
    /// `"SCANNING"`, `"FINISHED"`, `"CANCELED"`.
    pub state: String,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub to_examine: Option<String>,
    pub examined: Option<String>,
    pub errors: Option<String>,
    pub pass_start: Option<String>,
    pub scrub_pause: Option<String>,
    pub issued: Option<String>,
}

/// `GET /api/v1/pools/{name}` — full status: scrub + recursive vdev tree.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PoolStatus {
    pub name: String,
    pub state: String,
    pub error_count: String,
    pub pool_guid: String,
    pub txg: String,
    pub scan: Option<ScanSummary>,
    pub vdevs: Vec<VdevNode>,
}

/// Recursive vdev tree as a flat list of trees. Wire-friendlier than
/// palimpsest's map<name, VdevStatus> for UIs that want to render in
/// declared order.
///
/// `children` carries the `#[schema(no_recursion)]` attribute so
/// utoipa's auto-collector stops at the cycle and emits a `$ref` to
/// `VdevNode` itself instead of inlining the type — without this
/// `ApiDoc::openapi()` infinite-recurses and overflows the stack at
/// startup. See utoipa docs on recursive schemas.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct VdevNode {
    pub name: String,
    pub vdev_type: String,
    pub state: String,
    pub alloc_space: String,
    pub total_space: String,
    pub read_errors: String,
    pub write_errors: String,
    pub checksum_errors: String,
    pub path: Option<String>,
    #[schema(no_recursion)]
    pub children: Vec<VdevNode>,
}

/// Body of `POST /api/v1/pools/{name}/scrub`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScrubRequest {
    /// `"start"`, `"pause"`, `"resume"`, or `"stop"`.
    pub action: String,
}

/// One hold entry returned by
/// `GET /api/v1/datasets/{name}/snapshots/{snapshot}/holds`.
/// `timestamp` is unix seconds.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SnapshotHold {
    pub tag: String,
    pub timestamp: u64,
}

/// `GET /api/v1/system/arc` — a typed echo of the kernel's
/// `/proc/spl/kstat/zfs/arcstats`, plus a precomputed hit_ratio
/// (NaN encoded as `null` for empty caches). Fields mirror the
/// palimpsest `ArcStats` struct; raw map omitted from the wire to
/// keep responses small.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArcStats {
    pub size: u64,
    pub c: u64,
    pub c_min: u64,
    pub c_max: u64,
    pub hits: u64,
    pub misses: u64,
    pub demand_data_hits: u64,
    pub demand_data_misses: u64,
    pub demand_metadata_hits: u64,
    pub demand_metadata_misses: u64,
    pub prefetch_data_hits: u64,
    pub prefetch_data_misses: u64,
    pub prefetch_metadata_hits: u64,
    pub prefetch_metadata_misses: u64,
    pub mru_hits: u64,
    pub mfu_hits: u64,
    pub mru_ghost_hits: u64,
    pub mfu_ghost_hits: u64,
    pub l2_size: u64,
    pub l2_hits: u64,
    pub l2_misses: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    /// `hits / (hits + misses)`, `None` when the cache has had no
    /// traffic yet (avoids leaking JSON `NaN`).
    pub hit_ratio: Option<f64>,
}

impl From<palimpsest::system::ArcStats> for ArcStats {
    fn from(s: palimpsest::system::ArcStats) -> Self {
        let ratio = s.hit_ratio();
        Self {
            hit_ratio: if ratio.is_finite() { Some(ratio) } else { None },
            size: s.size,
            c: s.c,
            c_min: s.c_min,
            c_max: s.c_max,
            hits: s.hits,
            misses: s.misses,
            demand_data_hits: s.demand_data_hits,
            demand_data_misses: s.demand_data_misses,
            demand_metadata_hits: s.demand_metadata_hits,
            demand_metadata_misses: s.demand_metadata_misses,
            prefetch_data_hits: s.prefetch_data_hits,
            prefetch_data_misses: s.prefetch_data_misses,
            prefetch_metadata_hits: s.prefetch_metadata_hits,
            prefetch_metadata_misses: s.prefetch_metadata_misses,
            mru_hits: s.mru_hits,
            mfu_hits: s.mfu_hits,
            mru_ghost_hits: s.mru_ghost_hits,
            mfu_ghost_hits: s.mfu_ghost_hits,
            l2_size: s.l2_size,
            l2_hits: s.l2_hits,
            l2_misses: s.l2_misses,
            compressed_size: s.compressed_size,
            uncompressed_size: s.uncompressed_size,
        }
    }
}

/// One row of `GET /api/v1/system/arc/history`. Slim by design —
/// only the fields the dashboard chart consumes. Add more columns
/// (and migrate `arcstats_history`) when a future view needs them.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArcHistoryPoint {
    /// Unix seconds.
    pub timestamp: i64,
    pub size: u64,
    pub c: u64,
    pub hits: u64,
    pub misses: u64,
}

/// Body of `GET /api/v1/config` — the on-disk TOML the daemon was
/// started with, plus its absolute path. Read-only: there is no
/// write-back endpoint, so this is a faithful echo of what's loaded,
/// not what the daemon may have parsed (you can spot drift by
/// comparing to the other endpoints).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ConfigView {
    pub path: String,
    pub content_toml: String,
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

/// Body of `POST /api/v1/datasets/{name}/snapshots/{snapshot}/holds`.
/// `arctern_*` tags are reserved for the replication machinery and
/// rejected.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateHoldRequest {
    pub tag: String,
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

/// One network route of a peer, in priority order (first = preferred).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PeerRoute {
    pub name: String,
    pub ssh_target: String,
    /// Whether scheduled (auto) replication may run over this route.
    pub auto: bool,
    /// `"connected" | "failed" | "unknown"` — last connect result for
    /// this route. Lower-priority routes are only probed on failover /
    /// re-rank, so `unknown` is the common idle state.
    pub health: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// RFC3339 timestamp of the last connect attempt, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked: Option<String>,
}

/// One row in `GET /api/v1/peers`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PeerSummary {
    pub name: String,
    pub reachability: PeerReachability,
    /// Name of the route the live link currently runs over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_route: Option<String>,
    pub routes: Vec<PeerRoute>,
}

/// One completed inbound transfer, as recorded by the recv channel on
/// this host. `GET /api/v1/transfers/recent`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RecvTransfer {
    pub id: i64,
    /// Unix seconds.
    pub completed_at: i64,
    /// Receiver-side job name the sender addressed.
    pub job: String,
    /// Sender identity from `[[allowed_clients]]`.
    pub identity: String,
    pub dataset: String,
    pub to_snapshot: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_snapshot: Option<String>,
    pub bytes: i64,
    pub duration_ms: i64,
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

    fn list_entry(
        name: &str,
        kind: palimpsest::models::DatasetType,
    ) -> palimpsest::dataset::ZfsListEntry {
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
        let s = DatasetSummary::from(list_entry(
            "tank",
            palimpsest::models::DatasetType::Filesystem,
        ));
        assert_eq!(s.name, "tank");
        assert_eq!(s.dataset_type, "filesystem");
    }

    #[test]
    fn create_snapshot_request_defaults() {
        let req: CreateSnapshotRequest = serde_json::from_str(r#"{"snapshot_name":"s1"}"#).unwrap();
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
        assert_eq!(
            back.properties.get("user:reason").map(String::as_str),
            Some("manual")
        );
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
        assert_eq!(
            back.properties.get("compression").map(String::as_str),
            Some("lz4")
        );
    }
}
