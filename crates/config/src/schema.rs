//! TOML schema for `arctern.toml`. Field shapes are inspired by zrepl's
//! YAML schema but Rust-idiomatic — see `docs/example-config.toml` for
//! the mapping. `#[serde(deny_unknown_fields)]` everywhere so a typo in
//! the operator's file fails loud, not silent.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::grid::GridSpec;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where the daemon stores per-host state (SQLite, future replication
    /// cursors). The daemon resolves `None` to its hard-coded default
    /// `/var/lib/arctern` (see daemon main).
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    /// Path of the daemon's UNIX API socket. The daemon binds here
    /// unless overridden by `--socket`; `stdinserver-dispatch` uses it
    /// to reach the local daemon when proxying peer requests (job list,
    /// wakeup) — the two processes have no other rendezvous point.
    /// `None` falls back to `$XDG_RUNTIME_DIR/arctern.sock`, then
    /// `/run/arctern.sock`.
    #[serde(default)]
    pub socket: Option<PathBuf>,
    /// Host-wide defaults applied to every job at config-load time. Lets
    /// an operator write 5-line jobs that say only what differs from
    /// the standard zrepl idiom.
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
    /// Receiver-side ACL for the SSH transport. `arctern stdinserver-
    /// dispatch <identity>` looks up the row whose `identity` matches its
    /// argv before serving any control or recv channel. Empty (the laptop
    /// host's typical case) means no inbound clients are allowed.
    #[serde(default, rename = "allowed_clients")]
    pub allowed_clients: Vec<AllowedClient>,
    /// Outbound SSH peers reachable via the system `ssh(1)`. Push jobs
    /// reference these by `name` instead of carrying connect details
    /// inline. Empty on a server-only host.
    #[serde(default, rename = "peers")]
    pub peers: Vec<PeerConfig>,
}

/// Host-wide defaults. Resolved into every job at load time
/// (`Config::resolve_defaults`); jobs that override a field win.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Defaults {
    /// Snapshot tag prefix shared by snap, push, and prune jobs. A
    /// per-job `prefix` (in snapshotting or snapshot_filter) wins.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Snapshotting cadence for snap jobs that don't specify their own.
    #[serde(default)]
    pub snapshotting: Option<SnapshottingDefaults>,
    /// Prune defaults — both the grid and whether to auto-inject the
    /// "protect manual snapshots" rule.
    #[serde(default)]
    pub pruning: Option<PruningDefaults>,
    /// Default `zfs send` flags. The four flags default true here as
    /// they do on `SendFlagsConfig`; spelling out `[defaults.send]`
    /// lets an operator flip one off host-wide.
    #[serde(default)]
    pub send: SendFlagsConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshottingDefaults {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PruningDefaults {
    pub grid: GridSpec,
    /// When true (the default), the resolver appends a
    /// `{ type: regex, regex: "^<prefix>.*", negate: true }` rule so
    /// snapshots without the configured prefix survive prune. Mirrors
    /// the standard zrepl idiom.
    #[serde(default = "yes")]
    pub protect_non_prefixed: bool,
}

/// One outbound peer: a physical host the daemon can open an SSH
/// session to, possibly reachable over several network routes (LAN,
/// WireGuard, ...). Exactly one of `ssh_target` (single-route
/// shorthand) or `routes` must be given; the loader normalises the
/// shorthand into a one-entry `routes` list.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub name: String,
    /// Single-route shorthand: equivalent to
    /// `routes = [{ name = "default", ssh_target = "..." }]`.
    /// Mutually exclusive with `routes`; `None` after config load.
    #[serde(default)]
    pub ssh_target: Option<String>,
    /// Ordered routes to the same physical host, highest priority
    /// first. The peer link connects the first reachable route and
    /// re-ranks back to a higher one when it returns. Cursor bookmarks,
    /// step holds and sync history are keyed by the PEER name — routes
    /// are pure transport and leave no trace in ZFS state.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    /// Replication policy for push jobs targeting this peer.
    #[serde(default)]
    pub mode: PeerMode,
    /// For `mode = "auto"`: don't auto-replicate to this peer more
    /// often than this (measured from the last successful sync).
    /// Unset = every push cycle. Combined with per-route `auto`
    /// eligibility, route reachability is the locality signal — a
    /// LAN-only route is only reachable at home, so "auto at home,
    /// manual on the road" needs no network-detection config at all.
    #[serde(default, with = "humantime_serde::option")]
    pub auto_interval: Option<Duration>,
}

/// One network route to a peer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    pub name: String,
    /// `[user@]host[:port]` or any string `ssh(1)` accepts (including
    /// an alias from `~/.ssh/config`). The daemon does NOT parse this;
    /// it hands it verbatim to openssh.
    pub ssh_target: String,
    /// Whether scheduled (auto) replication may run while this route is
    /// the active one. `false` = the route carries only manual
    /// "Send now" pushes — e.g. a metered WireGuard path.
    #[serde(default = "yes")]
    pub auto: bool,
}

/// When a push job replicates to a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerMode {
    /// Replicate whenever the peer is reachable and `auto_interval`
    /// (if set) has elapsed since the last successful sync.
    #[default]
    Auto,
    /// Replicate only on an explicit trigger (UI "Send now" /
    /// `POST /api/v1/jobs/{name}/push/{peer}`).
    Manual,
}

/// One inbound client entry. `identity` is the literal argv passed to
/// `stdinserver-dispatch` from the matching `authorized_keys` line.
/// `jobs` and `operations` are allow-lists; the dispatcher rejects any
/// `(job, op)` pair that isn't covered. `root_fs`, when set, restricts
/// recv operations to that subtree.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedClient {
    pub identity: String,
    /// Optional defense-in-depth: SHA256 fingerprint of the SSH key the
    /// client is expected to authenticate with, e.g.
    /// `"SHA256:abc123..."`. When set, the dispatcher compares it to
    /// `SSH_AUTH_INFO_0` before granting access.
    #[serde(default)]
    pub fingerprint: Option<String>,
    pub jobs: Vec<String>,
    pub operations: Vec<String>,
    #[serde(default)]
    pub root_fs: Option<String>,
    /// Per-client recv-side tuning. Empty defaults match the
    /// historical hardcoded behaviour: unmounted, no property mutation
    /// beyond the implicit `mountpoint=none` on placeholders.
    #[serde(default)]
    pub recv: RecvConfig,
}

/// Receiver-side `zfs recv` knobs for a given client. Maps zrepl's
/// `recv.properties.inherit` / `recv.properties.override` 1:1, and
/// translates to palimpsest's `RecvArgs::property_inherit` /
/// `property_override` at recv time.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecvConfig {
    /// Property keys passed to `zfs recv -x <key>`. The received
    /// dataset will inherit each key from its parent on the receiver.
    #[serde(default)]
    pub inherit_properties: Vec<String>,
    /// Property `k=v` pairs passed to `zfs recv -o <k>=<v>`. The
    /// received dataset is forced to take each value, ignoring any
    /// value in the send stream.
    #[serde(default)]
    pub override_properties: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobConfig {
    Snap(SnapJobConfig),
    Push(PushJobConfig),
    /// Prune-only job. Walks `filesystems`, evaluates `pruning` rules,
    /// destroys victims — no `zfs snapshot` calls. Designed for the
    /// receiver-side scenario where the local node is not the snapshot
    /// source (the sender owns that) but still needs retention.
    Prune(PruneJobConfig),
}

impl JobConfig {
    pub fn name(&self) -> &str {
        match self {
            JobConfig::Snap(s) => &s.name,
            JobConfig::Push(s) => &s.name,
            JobConfig::Prune(s) => &s.name,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PruneJobConfig {
    pub name: String,
    #[serde(deserialize_with = "crate::filter::deserialize_filesystems")]
    pub filesystems: Vec<FilesystemFilter>,
    /// How often the prune loop fires.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// `keep` empty after parsing means "use [defaults.pruning]";
    /// loader fills it. Non-Option for the TOML-subtable reason
    /// above.
    #[serde(default)]
    pub pruning: PruningConfig,
}

impl PruneJobConfig {
    pub fn pruning(&self) -> &PruningConfig {
        &self.pruning
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapJobConfig {
    pub name: String,
    #[serde(deserialize_with = "crate::filter::deserialize_filesystems")]
    pub filesystems: Vec<FilesystemFilter>,
    /// Use `SnapshottingConfig::Unset` (the `Default`) when missing
    /// from TOML; `resolve_defaults` replaces with the host-wide
    /// `[defaults.snapshotting]` block. Downstream callers go through
    /// the `snapshotting()` accessor.
    ///
    /// (This is `T` rather than `Option<T>` so TOML's subtable form
    /// `[jobs.snapshotting]` parses; serde-toml drops nested subtables
    /// for `Option<T>` fields. Same pattern below for `pruning`.)
    #[serde(default)]
    pub snapshotting: SnapshottingConfig,
    #[serde(default)]
    pub pruning: PruningConfig,
}

impl SnapJobConfig {
    pub fn snapshotting(&self) -> &SnapshottingConfig {
        &self.snapshotting
    }
    pub fn pruning(&self) -> &PruningConfig {
        &self.pruning
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemFilter {
    pub path: String,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// Periodic snapshot creation. Currently the only mode (cron / manual
/// modes deferred until somebody actually asks); kept as a flat struct
/// rather than a tagged enum so a job can override one field without
/// re-declaring `type = "periodic"`.
///
/// Default value (both fields zero/empty) is the "unset" sentinel
/// `resolve_defaults` replaces with `[defaults.snapshotting]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshottingConfig {
    // humantime accepts "15m", "4h", "1d", etc.
    #[serde(default, with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default)]
    pub prefix: String,
}

impl SnapshottingConfig {
    /// True for the `Default::default()` sentinel — both fields blank.
    /// Used by `resolve_defaults` to decide when to substitute from
    /// `[defaults.snapshotting]`.
    pub fn is_unset(&self) -> bool {
        self.interval == Duration::ZERO && self.prefix.is_empty()
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PruningConfig {
    #[serde(default)]
    pub keep: Vec<KeepRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum KeepRule {
    Grid {
        grid: GridSpec,
        regex: String,
    },
    Regex {
        regex: String,
        #[serde(default)]
        negate: bool,
    },
}

/// Push job — active sender. Each cycle, lists local matching snapshots
/// per filesystem, asks the peer over its SSH control channel what it
/// has, intersects by GUID, and pipes a full / incremental / resume
/// `zfs send` into a fresh recv channel on the peer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushJobConfig {
    pub name: String,
    /// Legacy single-target form (`peer = "home"`). Equivalent to
    /// `targets = ["home"]` and resolved into that during config load;
    /// mutually exclusive with `targets`. Kept for backwards
    /// compatibility with configs written before multi-target landed.
    #[serde(default)]
    pub peer: Option<String>,
    /// Peer names this job replicates to. Each cycle selects EVERY due
    /// target: manual requests always run; an auto peer runs when its
    /// active route is auto-eligible and `auto_interval` has elapsed.
    /// Route failover within one physical host lives in `[[peers]]`
    /// routes, not here. Each peer keeps its own per-dataset cursor
    /// bookmark, so a peer that's been offline for a week catches up
    /// from where it left off when it comes back.
    #[serde(default)]
    pub targets: Vec<String>,
    /// How often the planner cycle fires. The wakeup endpoint
    /// (POST /api/v1/jobs/{name}/wakeup) re-enters the cycle on demand.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(deserialize_with = "crate::filter::deserialize_filesystems")]
    pub filesystems: Vec<FilesystemFilter>,
    pub target: PushTarget,
    #[serde(default)]
    pub send: SendFlagsConfig,
    /// Plan-only mode. The job lists local/remote snapshots and logs the
    /// chosen action for each filesystem, but does not discard partial
    /// receives, open recv channels, run `zfs send`, create holds, or update
    /// cursor bookmarks. Intended for first-run verification.
    #[serde(default)]
    pub dry_run: bool,
    /// Both fields `None` after parsing means "build from
    /// `[defaults].prefix`"; resolved at load time. (Non-Option for
    /// the same TOML-subtable-vs-Option reason as `snapshotting`.)
    #[serde(default)]
    pub snapshot_filter: SnapshotFilterConfig,
}

impl PushJobConfig {
    pub fn snapshot_filter(&self) -> &SnapshotFilterConfig {
        &self.snapshot_filter
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushTarget {
    /// Receiver-side root. Target dataset = `<root_fs>/<sender_path>`
    /// (literal concatenation, no path stripping). Documented in
    /// docs/example-config.toml.
    pub root_fs: String,
}

/// `zfs send` replication flags. All four default `true` because the
/// canonical zrepl push job uses all four; off-by-default would force
/// most operators to type a 5-line block to get behaviour they want
/// anyway.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendFlagsConfig {
    #[serde(default = "yes")]
    pub encrypted: bool,
    #[serde(default = "yes")]
    pub embedded_data: bool,
    #[serde(default = "yes")]
    pub compressed: bool,
    #[serde(default = "yes")]
    pub large_blocks: bool,
}

impl Default for SendFlagsConfig {
    fn default() -> Self {
        Self {
            encrypted: true,
            embedded_data: true,
            compressed: true,
            large_blocks: true,
        }
    }
}

fn yes() -> bool {
    true
}

/// Per-job snapshot filter. Exactly one of `prefix` (sugar for
/// `^<prefix>`) or `regex` must be present — enforced in `validate`
/// after `resolve_defaults` fills in `prefix` from `[defaults].prefix`
/// when neither is given.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotFilterConfig {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub regex: Option<String>,
}

impl SnapshotFilterConfig {
    /// Materialise the filter as a regex string ready to send on the
    /// wire (and to compile on the planner side). `prefix = "zrepl_"`
    /// becomes `^zrepl_`. `regex` passes through verbatim. Returns
    /// `None` if neither is set; xor-validation in `validate_push`
    /// makes that path unreachable from a valid config.
    pub fn as_regex_str(&self) -> Option<String> {
        if let Some(p) = &self.prefix {
            return Some(format!("^{}", regex::escape(p)));
        }
        self.regex.clone()
    }
}
