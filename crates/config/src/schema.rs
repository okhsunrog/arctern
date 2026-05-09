//! TOML schema for `arctern.toml`. Field shapes are inspired by zrepl's
//! YAML schema but Rust-idiomatic — see `docs/example-config.toml` for
//! the mapping. `#[serde(deny_unknown_fields)]` everywhere so a typo in
//! the operator's file fails loud, not silent.

use std::collections::BTreeMap;
use std::net::SocketAddr;
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

/// One outbound peer the daemon can open an SSH session to. Field
/// shapes mirror `ssh(1)`'s positional target so an entry can be
/// hand-validated with `ssh -G <ssh_target> exit`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub name: String,
    /// `[user@]host[:port]` or any string `ssh(1)` accepts (including
    /// an alias from `~/.ssh/config`). The daemon does NOT parse this;
    /// it hands it verbatim to openssh.
    pub ssh_target: String,
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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobConfig {
    Snap(SnapJobConfig),
    Sink(SinkJobConfig),
    Push(PushJobConfig),
}

impl JobConfig {
    pub fn name(&self) -> &str {
        match self {
            JobConfig::Snap(s) => &s.name,
            JobConfig::Sink(s) => &s.name,
            JobConfig::Push(s) => &s.name,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapJobConfig {
    pub name: String,
    pub filesystems: Vec<FilesystemFilter>,
    pub snapshotting: SnapshottingConfig,
    pub pruning: PruningConfig,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshottingConfig {
    Periodic {
        // humantime accepts "15m", "4h", "1d", etc.
        #[serde(with = "humantime_serde")]
        interval: Duration,
        prefix: String,
    },
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

/// Sink job — passive receiver. Binds a QUIC listener on `listen` and
/// writes inbound `zfs send` streams under `root_fs` (target dataset
/// path comes from the wire header).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkJobConfig {
    pub name: String,
    pub listen: SocketAddr,
    pub root_fs: String,
    #[serde(default)]
    pub recv: RecvConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecvConfig {
    #[serde(default)]
    pub properties: RecvProperties,
}

/// Mirrors zrepl's `recv.properties.{override, inherit}`. Slice 005
/// closes the wiring (see slice 005 plan D22 → T008): both fields are
/// passed to palimpsest's RecvArgs::property_override / property_inherit
/// builders inside SinkJob::handle_send.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecvProperties {
    /// `-o property=value` overrides applied at recv time.
    #[serde(default, rename = "override")]
    pub overrides: BTreeMap<String, String>,
    /// `-x property` inheritance — drop the property from the stream.
    #[serde(default)]
    pub inherit: Vec<String>,
}

/// Push job — active sender. Each cycle, lists local matching snapshots
/// per filesystem, opens a QUIC LIST request to the peer to learn what
/// it already has, intersects by GUID, and emits a full or incremental
/// `zfs send` over a second QUIC stream into the peer's sink.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushJobConfig {
    pub name: String,
    /// Receiver peer name. Must match a `[[peers]]` entry. Optional in
    /// this commit so existing QUIC-shape configs still parse; the
    /// daemon refuses to run a push job whose peer is missing once the
    /// SSH executor lands in step 9.
    #[serde(default)]
    pub peer: Option<String>,
    /// Legacy QUIC sink address. Ignored under the SSH transport;
    /// retained as Optional so existing TOMLs deserialise. Field will
    /// be removed in a follow-up.
    #[serde(default)]
    pub connect: Option<SocketAddr>,
    /// How often the planner cycle fires. The wakeup endpoint
    /// (POST /api/v1/jobs/{name}/wakeup) re-enters the cycle on demand.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Legacy QUIC SNI. Ignored under the SSH transport; default
    /// retained for backward-compat parsing.
    #[serde(default = "default_server_name")]
    pub server_name: String,
    pub filesystems: Vec<FilesystemFilter>,
    pub target: PushTarget,
    #[serde(default)]
    pub send: SendFlagsConfig,
    pub snapshot_filter: SnapshotFilterConfig,
}

fn default_server_name() -> String {
    "arctern".into()
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
/// `^<prefix>`) or `regex` must be present — enforced in `validate`.
#[derive(Debug, Clone, Deserialize)]
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
