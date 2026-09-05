//! Shared request/response types for the arctern HTTP API.
//!
//! Pure serde + utoipa types: no zfskit, no I/O. Both the in-process
//! axum router and the `arctern-client` crate consume these, and the
//! admin UI's TypeScript client is generated from their OpenAPI schema.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// A string that is not one of an enum's wire values. Returned by the
/// `FromStr` impls below when a stored value (SQLite) or a peer's reply
/// predates the current set of variants.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{value:?} is not a valid {kind}")]
pub struct UnknownVariant {
    pub kind: &'static str,
    pub value: String,
}

/// A closed set of lowercase snake_case wire values with a string
/// round-trip for SQLite columns and log fields.
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident => $text:literal ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $text ),+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl std::str::FromStr for $name {
            type Err = UnknownVariant;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $( $text => Ok(Self::$variant), )+
                    other => Err(UnknownVariant {
                        kind: stringify!($name),
                        value: other.to_string(),
                    }),
                }
            }
        }
    };
}

wire_enum! {
    /// `zfs list -t` kinds, lowercase as `zfs(8)` prints them.
    DatasetType {
        Filesystem => "filesystem",
        Volume => "volume",
        Snapshot => "snapshot",
        Bookmark => "bookmark",
    }
}

/// Slim projection of a `zfs list` entry suitable for HTTP + OpenAPI.
/// Native ZFS properties carry typed data (bytes, bool, …) but
/// `BTreeMap<String, String>` serializes more cleanly through utoipa;
/// consumers parse property values as needed.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatasetSummary {
    pub name: String,
    pub dataset_type: DatasetType,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// Response of `GET /api/v1/system/info`: identity of the daemon serving
/// this API. Host-scoped like every other endpoint, so a peer's console
/// (fetched through the proxy) reports that peer's daemon version.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SystemInfo {
    /// The daemon's crate version (`CARGO_PKG_VERSION`), e.g. `"0.2.2"`.
    pub version: String,
}

wire_enum! {
    /// The `type` of a `[[jobs]]` entry.
    JobKind {
        Snap => "snap",
        Push => "push",
        Prune => "prune",
    }
}

wire_enum! {
    /// Terminal (or in-flight) state of one job cycle, as stored in
    /// `job_runs.status` and `push_syncs.status`.
    RunStatus {
        Ok => "ok",
        Error => "error",
        /// Only in `job_runs`: the cycle is still executing.
        Running => "running",
        /// The operator stopped or paused the cycle, or the daemon shut
        /// down. Not a failure.
        Cancelled => "cancelled",
        /// A push cycle in `dry_run` mode that completed without errors.
        /// Never counts as a sync.
        DryRun => "dry_run",
        /// Only in `job_runs`: the daemon died between start and finish
        /// and the row was reconciled at the next startup.
        Interrupted => "interrupted",
    }
}

/// Fields every job kind reports.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct PeriodicJobStatus {
    pub name: String,
    /// RFC3339; null until the job has completed at least one cycle.
    pub last_run: Option<String>,
    /// RFC3339; set as soon as the loop knows when it fires next.
    pub next_run: Option<String>,
    /// Null when the most recent cycle finished cleanly.
    pub last_error: Option<String>,
    /// True while a cycle is currently executing. `last_*` describe the
    /// previous cycle.
    #[serde(default)]
    pub running: bool,
}

/// Status of a push job: the periodic fields plus transfer state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct PushJobStatus {
    pub name: String,
    pub last_run: Option<String>,
    pub next_run: Option<String>,
    pub last_error: Option<String>,
    #[serde(default)]
    pub running: bool,
    /// True while the job is paused: the current transfer was aborted
    /// (resumably) and scheduled cycles are suspended until resumed.
    #[serde(default)]
    pub paused: bool,
    /// True while a cancel request would actually abort something. False
    /// once every in-flight transfer has passed the point where cancel is
    /// a no-op (finalizing/committing).
    #[serde(default)]
    pub cancellable: bool,
    /// Configured with `dry_run = true`: every cycle plans and logs but
    /// sends nothing, so the job can never be "synced".
    #[serde(default)]
    pub dry_run: bool,
    /// In-flight transfers, one per parallel send slot. UI derives
    /// speed from `bytes_sent` deltas between live snapshots.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transfers: Vec<TransferInfo>,
    /// Per-target replication policy + last outcome.
    #[serde(default)]
    pub targets: Vec<TargetStatus>,
}

/// One entry in the response of `GET /api/v1/jobs`, discriminated by
/// `kind`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JobStatus {
    Snap(PeriodicJobStatus),
    Prune(PeriodicJobStatus),
    Push(PushJobStatus),
}

impl JobStatus {
    pub fn name(&self) -> &str {
        match self {
            JobStatus::Snap(s) | JobStatus::Prune(s) => &s.name,
            JobStatus::Push(s) => &s.name,
        }
    }

    pub fn kind(&self) -> JobKind {
        match self {
            JobStatus::Snap(_) => JobKind::Snap,
            JobStatus::Prune(_) => JobKind::Prune,
            JobStatus::Push(_) => JobKind::Push,
        }
    }
}

wire_enum! {
    /// What kind of `zfs send` stream a transfer carries.
    TransferKind {
        Full => "full",
        Incremental => "incremental",
        Resume => "resume",
    }
}

wire_enum! {
    /// Where the executor is in one transfer. Phases past `finalizing`
    /// cannot be cancelled: the bytes are with the receiver.
    TransferPhase {
        Sending => "sending",
        /// `zfs send` has not produced the next record for a while.
        WaitingSender => "waiting_sender",
        /// The SSH channel is applying backpressure: network, or the
        /// receiver's `zfs recv` / storage.
        WaitingReceiver => "waiting_receiver",
        /// All bytes are written; waiting for the receiver's verdict.
        Finalizing => "finalizing",
        /// Advancing the cursor bookmark and releasing step holds.
        Committing => "committing",
        /// Cancel requested; waiting for the remote `zfs recv` to exit.
        Cancelling => "cancelling",
    }
}

/// Progress of an in-flight `zfs send` stream.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransferInfo {
    pub dataset: String,
    pub peer: String,
    pub kind: TransferKind,
    pub bytes_sent: u64,
    /// Dry-run estimate. None for resume sends (no estimate available).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    /// Unix seconds.
    pub started_at: i64,
    #[serde(default = "default_transfer_phase")]
    pub phase: TransferPhase,
    /// Unix seconds when `phase` last changed. Lets clients render a live
    /// wait duration even while no byte-count events are arriving.
    #[serde(default)]
    pub phase_since: i64,
}

fn default_transfer_phase() -> TransferPhase {
    TransferPhase::Sending
}

wire_enum! {
    /// Replication policy for one peer of a push job.
    PeerMode {
        Auto => "auto",
        Manual => "manual",
    }
}

/// One replication target of a push job.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TargetStatus {
    pub peer: String,
    pub mode: PeerMode,
    pub connected: bool,
    /// Active route name while connected (e.g. `"lan"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    /// Whether the active route permits scheduled replication.
    #[serde(default)]
    pub route_auto: bool,
    /// A manual push to this peer is queued and will run on the next
    /// cycle. Set from the moment the request is accepted until the
    /// cycle that drains it starts.
    #[serde(default)]
    pub manual_queued: bool,
    /// For auto mode: the configured `auto_interval` in seconds. The
    /// next auto sync is `last_success + auto_interval_secs` (or the
    /// next planner tick when unset/no history).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_interval_secs: Option<u64>,
    /// Unix seconds of the last successful sync to this peer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success: Option<i64>,
    /// Unix seconds of the most recent attempted sync, regardless of outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt: Option<i64>,
    /// Outcome of the most recent attempt: `ok`, `error`, `cancelled`
    /// or `dry_run`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_outcome: Option<RunStatus>,
    /// Human-readable context for the most recent non-successful attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message: Option<String>,
    /// Compatibility field for clients that only understand failures.
    /// Unlike `last_message`, this is populated only for a real error.
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
/// zfskit's map<name, VdevStatus> for UIs that want to render in
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

wire_enum! {
    /// `zpool scrub` verbs.
    ScrubAction {
        Start => "start",
        Pause => "pause",
        Resume => "resume",
        Stop => "stop",
    }
}

/// Body of `POST /api/v1/pools/{name}/scrub`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScrubRequest {
    pub action: ScrubAction,
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
/// (NaN encoded as `null` for empty caches).
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

/// One row of `GET /api/v1/system/arc/history`. Slim by design —
/// only the fields the dashboard chart consumes.
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
/// started with, plus its absolute path. Read-only.
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
    pub status: RunStatus,
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

wire_enum! {
    /// Last connect result for one route. Lower-priority routes are only
    /// probed on failover / re-rank, so `unknown` is the common idle
    /// state.
    RouteHealth {
        Unknown => "unknown",
        Connected => "connected",
        Failed => "failed",
    }
}

/// One network route of a peer, in priority order (first = preferred).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PeerRoute {
    pub name: String,
    pub ssh_target: String,
    /// Whether scheduled (auto) replication may run over this route.
    pub auto: bool,
    pub health: RouteHealth,
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

    #[test]
    fn wire_enums_round_trip_through_their_strings() {
        for status in [
            RunStatus::Ok,
            RunStatus::Error,
            RunStatus::Running,
            RunStatus::Cancelled,
            RunStatus::DryRun,
            RunStatus::Interrupted,
        ] {
            assert_eq!(status.as_str().parse::<RunStatus>().unwrap(), status);
            // serde and as_str agree, so a value written by one reader
            // is accepted by the other.
            assert_eq!(
                serde_json::to_string(&status).unwrap(),
                format!("\"{}\"", status.as_str())
            );
        }
        let err = "bogus".parse::<RunStatus>().unwrap_err();
        assert_eq!(err.kind, "RunStatus");
        assert_eq!(err.value, "bogus");
    }

    #[test]
    fn job_status_is_tagged_by_kind() {
        let status = JobStatus::Push(PushJobStatus {
            name: "push".into(),
            ..Default::default()
        });
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["kind"], "push");
        assert_eq!(json["name"], "push");
        let back: JobStatus = serde_json::from_value(json).unwrap();
        assert_eq!(back.kind(), JobKind::Push);
        assert_eq!(back.name(), "push");

        let snap: JobStatus = serde_json::from_str(r#"{"kind":"snap","name":"s"}"#).unwrap();
        assert!(matches!(snap, JobStatus::Snap(_)));
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
    fn transfer_info_defaults_old_payload_to_sending() {
        let transfer: TransferInfo = serde_json::from_str(
            r#"{
                "dataset":"tank/data",
                "peer":"backup",
                "kind":"incremental",
                "bytes_sent":42,
                "started_at":100
            }"#,
        )
        .unwrap();
        assert_eq!(transfer.phase, TransferPhase::Sending);
        assert_eq!(transfer.phase_since, 0);
    }

    #[test]
    fn serde_roundtrip() {
        let s = DatasetSummary {
            name: "tank/data".into(),
            dataset_type: DatasetType::Filesystem,
            properties: [("compression".to_string(), "lz4".to_string())]
                .into_iter()
                .collect(),
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains(r#""dataset_type":"filesystem""#));
        let back: DatasetSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, s.name);
        assert_eq!(back.dataset_type, DatasetType::Filesystem);
        assert_eq!(
            back.properties.get("compression").map(String::as_str),
            Some("lz4")
        );
    }
}
