# Implementation Plan: Push job — active sender + LIST-based discovery

**Branch**: `005-push-job` | **Date**: 2026-05-09 | **Spec**: [spec.md](./spec.md)
**Input**: `specs/005-push-job/spec.md`

## Summary

Add a `push` job that periodically lists the sender's matching snapshots, opens a QUIC stream to a sink with `op = "list"` to learn the receiver's current snapshot set, intersects by GUID to choose full vs incremental, then opens a second QUIC stream with `op = "send"` to stream `zfs send` bytes into `palimpsest::recv` on the receiver. The wire protocol gains an `op` field on the existing header (default `"send"` for slice 004 backward compat). Sink dispatches on `op` — old `"send"` path is unchanged, new `"list"` path runs `palimpsest::dataset::list` filtered by `prefix_regex` and writes the snapshot list back. `JOB_KIND_PUSH` lands in `crates/api`; `POST /api/v1/jobs/{name}/wakeup` lands in the daemon's HTTP surface so the dashboard and integration test can trigger immediate cycles. Slice 004's D22 (recv property override / inherit) closes this slice — palimpsest now exposes `properties_override` + `properties_inherit` on `RecvArgs`, the sink wires its `RecvProperties` through.

## Technical Context

**Language/Version**: Rust 1.95, edition 2024.
**Primary Dependencies**: existing `axum` 0.8, `clap`, `tokio`, `tracing`, `serde`, `palimpsest`, `tokio-util`, `time`, `arctern-config`, `arctern-transport`, `quinn`, `rustls`, `rcgen`. No new top-level crate this slice — push lives in `daemon/src/jobs/push.rs` and reuses `crates/transport` primitives. `cargo add` may pull `regex` into `crates/transport` (the LIST handler compiles `prefix_regex` to filter snapshots) — covered by NFR-002 + the constitution-IV grep allowlist update.
**Storage**: TOML config on disk; `<state_dir>/{cert,key}.pem` already managed by slice 004. No new persistence — receiver snapshot state is queried fresh each cycle.
**Testing**: `cargo test --workspace` for unit tests (planner GUID intersection, wire protocol round-trip for the new types, config schema). `cargo test -p arctern-daemon --features integration -- --test-threads=1` for the end-to-end push test against the palimpsest VM.
**Target Platform**: Linux x86_64.
**Project Type**: Cargo workspace; no new members.
**Performance Goals**: A single small-snapshot push completes in <2 seconds wall-clock against the test VM (LIST round-trip + handshake + send + recv finalize). Per-cycle planner overhead is one `palimpsest::dataset::list` per filesystem on the sender plus one LIST request per filesystem.
**Constraints**: Constitution principles I-V apply — see Constitution Check. Async-only. No `tokio::process::Command` in arctern source. `crates/transport` already on the constitution-IV grep allowlist; this slice extends the regex allowlist to include it (it now compiles `prefix_regex`).
**Scale/Scope**: ~1500-2000 LoC arctern source + tests + 10 commits.

## Constitution Check

*GATE: passes before implementation.*

| Principle | Compliance |
|---|---|
| I. QUIC With HTTP Semantics | Bulk send bytes flow over raw QUIC streams (slice 004 carried forward). The new `op = "list"` request is small JSON request/response, also over a QUIC stream — fits the constitution's "raw streams where HTTP framing doesn't help" principle. The new `POST /jobs/{name}/wakeup` endpoint lives on the unix-socket axum router (HTTP semantics where they help). |
| II. One API for Browser and Daemons | `JOB_KIND_PUSH` is a string constant in `crates/api`. `POST /api/v1/jobs/{name}/wakeup` is part of the OpenAPI-described HTTP surface. The new wire types (`ListRequest`, `ListResponse`, `SnapshotEntry`, `SendHeader`) live in `crates/transport` — daemon-internal, NOT in `crates/api` because they are not browser-facing (matching the slice 004 D-decision). |
| III. Web UI Replaces the CLI | No new CLI verbs. The wakeup endpoint serves both the dashboard and `curl` (and the integration test). |
| IV. ZFS Through palimpsest | All `zfs send` / `zfs list` invocations go through palimpsest. Sender's planner uses `palimpsest::dataset::list`, executor uses `palimpsest::send::send`. Sink's LIST handler uses `palimpsest::dataset::list`. The constitution-IV grep gate covers `crates/{api,client,transport} daemon/src/`. **No prep gap** — all required palimpsest APIs were already present after the recv `-o`/`-x` prep commit (sole palimpsest change for this slice). |
| V. Local-Only by Default, Auth Opt-In | The push side is a CLIENT, no new bind. Reuses the slice 004 accept-any TLS verifier; WG remains the security perimeter. The new wakeup endpoint binds nothing new — it's served on the existing unix socket whose perms are already 0600. |
| VI. Live Data Over SSE | Not applicable this slice. Slice 008 will push per-fs progress over SSE. |
| VII. ZFS Metadata Compatibility | Wire protocol is greenfield (constitution VII explicit). NO cursor bookmarks planted — the receiver's `palimpsest::dataset::list` output is the source of truth each cycle. zrepl's `#zrepl_CURSOR_<guid>` bookmarks are not honoured (their presence is harmless; they are simply not used). The receiver pool is bit-identical to a zrepl-managed pool when arctern takes over. |

All applicable principles pass. Deferred work tracked in spec's "Out of scope".

## Project Structure

### Documentation (this feature)

```text
specs/005-push-job/
├── spec.md     # done
├── plan.md     # this file
└── tasks.md    # next, via speckit-tasks
```

### Source code (repository root)

```text
arctern/
├── crates/
│   ├── api/src/lib.rs                  # +pub const JOB_KIND_PUSH
│   ├── client/                         # unchanged this slice
│   ├── config/src/schema.rs            # +PushJobConfig + PushTarget + SendFlags + SnapshotFilter
│   ├── config/src/lib.rs               # +validate_push, dispatch in validate()
│   └── transport/
│       ├── Cargo.toml                  # +regex (cargo add)
│       └── src/
│           ├── lib.rs                  # re-export new protocol types
│           └── protocol.rs             # +Op enum, +ListRequest/Response/SnapshotEntry/SendHeader,
│                                       #  ReceiveHeader.op field, helpers for SEND-stream header
├── daemon/
│   ├── Cargo.toml                      # no new deps (quinn, transport already present)
│   └── src/
│       ├── main.rs                     # +construct PushJob in the job-build loop;
│                                       #  pass identity (already loaded) to push too
│       ├── jobs/
│       │   ├── mod.rs                  # +pub mod push; +Job::wakeup() default no-op
│       │   ├── snap.rs                 # implement Job::wakeup -> notify_one()
│       │   ├── sink.rs                 # +dispatch on Op; +wire RecvProperties through palimpsest
│       │   └── push.rs                 # NEW: PushJob, planner, executor
│       ├── handlers/
│       │   ├── mod.rs                  # add jobs::wakeup route
│       │   └── jobs.rs                 # +POST /jobs/{name}/wakeup handler
│       └── router.rs                   # +route registration
└── daemon/tests/
    └── integration_quic_push.rs        # NEW: end-to-end full + incremental push
```

**Structure Decision**:

- Push job is not its own crate. It's a `daemon/src/jobs/push.rs` module that composes `arctern_transport::protocol` (wire types), `palimpsest::dataset::list` + `palimpsest::send::send` (planner + executor), and the existing `Job` trait. Coupling = three crates which can only meet inside the daemon binary; same rationale as slice 004's `SinkJob`.
- The sink's LIST handler lives in `daemon/src/jobs/sink.rs` (not `crates/transport`) for the same reason — it composes transport + palimpsest + the daemon's tracing surface.
- The wakeup endpoint lives in `daemon/src/handlers/jobs.rs` next to the existing `GET /jobs` handler — a one-handler addition, no new module.

## Phase 0: Research

Spot-checks done at planning time:

- **palimpsest send streaming API** (`palimpsest/src/send/mod.rs`): `pub async fn send(runner, args) -> Result<ChildHandle, ZfsError>`. ChildHandle's `stdout: Option<Box<dyn AsyncRead + Unpin + Send>>`; full / incremental / resume_token forms all present in `SendArgs`; `raw`, `embedded`, `compressed`, `large_blocks`, `properties`, `replicate` builder methods all present. **No prep needed for send.**
- **palimpsest recv `-o` / `-x` flags**: missing in slice-004's palimpsest snapshot. Closed in this slice's prep commit (`feat(recv): -o property=value + -x property in RecvArgs` on palimpsest master). `RecvArgs` now exposes `properties_override: BTreeMap<String, String>` + `properties_inherit: Vec<String>` plus `property_override(k, v)` and `property_inherit(k)` builder methods. Args are emitted in deterministic key order (BTreeMap iteration).
- **palimpsest dataset list with GUID** (`palimpsest/src/dataset/list.rs` + `models/dataset.rs`): `ZfsListEntry` carries `createtxg: String` at top level and arbitrary properties in `properties: PropertyMap`. To get GUID, set `ListOptions.properties = vec!["guid".into()]` (and `createtxg` for the redundancy — the top-level field exists too). **VERIFIED IN VM**: `zfs list -j -p -t snapshot -o name,guid,createtxg <ds>` returns `properties.guid.value` as a string holding a u64 (e.g., `"11587258101628135412"` — exceeds `i64::MAX`). Arctern parses as `u64`. **No palimpsest prep needed for list.**
- **quinn client API**: `quinn::Endpoint::client((Ipv4Addr::UNSPECIFIED, 0).into())?`, install `client_config_accept_any()` from `crates/transport`, `endpoint.connect(addr, server_name)?.await?` for `Connection`. Open bi streams via `connection.open_bi().await?` (returns `(SendStream, RecvStream)`). Closing the send half: `send.finish()?` (sync) followed by reading the response from the receive half until EOF.
- **`tokio::sync::Notify`**: the wakeup primitive. `notify.notified().await` returns when `notify.notify_one()` is called. Stored on the job, exposed via a new `Job::wakeup(&self)` trait method. SnapJob's loop becomes a `select!` over sleep + cancel + wakeup; PushJob's loop is the same shape.
- **GUID parsing**: `s.properties.get("guid").and_then(|p| p.value.parse::<u64>().ok())`. The fallback path (no guid property — should be impossible if asked for it) treats the snapshot as un-intersectable; planner skips it.
- **Sink's LIST handler stream lifecycle**: receive header → run palimpsest list → write JSON response → `send.finish()`. No bulk bytes. The existing per-stream task framework in `SinkJob` handles this — the inner function dispatches on `op`.
- **Wire compat for slice 004 sinks**: `ReceiveHeader.op` is `#[serde(default = "default_op")]` returning `Op::Send`. A slice-004 client sending the slice-004 header (no `op` field) hits the new sink and it dispatches to the existing send path. A slice-005 client sending a header with `op: "send"` to a slice-004 sink is rejected because slice 004 used `#[serde(deny_unknown_fields)]` — ACCEPTED RISK: slice 004 never shipped, the only slice 004 sinks in existence are the ones from this codebase, which we're updating in lockstep.

## Phase 1: Design artifacts

### TOML schema additions

```toml
[[jobs]]
type = "push"
name = "push_to_server"
connect = "10.77.77.100:8888"
interval = "15m"
server_name = "arctern"             # default

[[jobs.filesystems]]
path = "okdata/data/home"
recursive = false

[jobs.target]
root_fs = "okdata/backups/laptop"

[jobs.send]
encrypted = true
embedded_data = true
compressed = true
large_blocks = true

[jobs.snapshot_filter]
prefix = "zrepl_"                   # OR regex = "^zrepl_.*" — exactly one
```

### Rust types — config

```rust
// crates/config/src/schema.rs
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobConfig {
    Snap(SnapJobConfig),
    Sink(SinkJobConfig),
    Push(PushJobConfig),                 // NEW
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushJobConfig {
    pub name: String,
    pub connect: SocketAddr,
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default = "default_server_name")]
    pub server_name: String,
    pub filesystems: Vec<FilesystemFilter>,
    pub target: PushTarget,
    #[serde(default)]
    pub send: SendFlagsConfig,
    pub snapshot_filter: SnapshotFilterConfig,
}

fn default_server_name() -> String { "arctern".into() }

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushTarget { pub root_fs: String }

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendFlagsConfig {
    #[serde(default = "yes")] pub encrypted: bool,
    #[serde(default = "yes")] pub embedded_data: bool,
    #[serde(default = "yes")] pub compressed: bool,
    #[serde(default = "yes")] pub large_blocks: bool,
}
impl Default for SendFlagsConfig { /* all true */ }
fn yes() -> bool { true }

// Exactly-one validation lives in lib.rs validate_push.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotFilterConfig {
    #[serde(default)] pub prefix: Option<String>,
    #[serde(default)] pub regex: Option<String>,
}
```

### Rust types — wire protocol

```rust
// crates/transport/src/protocol.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op { Send, List }
fn default_op() -> Op { Op::Send }   // slice-004 wire compat

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiveHeader {
    pub version: u32,
    #[serde(default = "default_op")]
    pub op: Op,
    pub target_dataset: String,
    /// Present only when op == List. None means no filtering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_regex: Option<String>,
    /// Present only when op == Send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send: Option<SendHeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendHeader {
    pub send_kind: SendKind,                                 // full | incremental
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_snap: Option<SnapshotRef>,
    pub to_snap: SnapshotRef,
    pub flags: SendFlagsWire,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SendKind { Full, Incremental }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRef { pub name: String, pub guid: u64 }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendFlagsWire {
    pub raw: bool, pub embedded: bool, pub compressed: bool, pub large_blocks: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ListResponse {
    Ok { snapshots: Vec<SnapshotEntry> },
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub name: String,
    pub guid: u64,
    pub createtxg: u64,
}
```

### Quickstart (developer)

```bash
cd ~/code/palimpsest && just vm-up
cd ~/code/arctern

# Sink (terminal 1)
cat > /tmp/arctern-sink.toml <<'EOF'
state_dir = "/tmp/arctern-sink-state"
[[jobs]]
type = "sink"
name = "sink"
listen = "127.0.0.1:8888"
root_fs = "tank/sink"
EOF
PALIMPSEST_SSH_TARGET=root@localhost:2226 PALIMPSEST_SSH_PASSWORD="" \
  cargo run -p arctern-daemon -- daemon \
    --config /tmp/arctern-sink.toml \
    --socket /tmp/arctern-sink.sock

# Sender (terminal 2)
cat > /tmp/arctern-sender.toml <<'EOF'
state_dir = "/tmp/arctern-sender-state"

[[jobs]]
type = "snap"
name = "snap"
[[jobs.filesystems]]
path = "tank/data"
[jobs.snapshotting]
type = "periodic"
interval = "10s"
prefix = "test_"
[jobs.pruning]
keep = []

[[jobs]]
type = "push"
name = "push"
connect = "127.0.0.1:8888"
interval = "10s"
[[jobs.filesystems]]
path = "tank/data"
[jobs.target]
root_fs = "tank/sink"
[jobs.snapshot_filter]
prefix = "test_"
EOF
PALIMPSEST_SSH_TARGET=root@localhost:2226 PALIMPSEST_SSH_PASSWORD="" \
  cargo run -p arctern-daemon -- daemon \
    --config /tmp/arctern-sender.toml \
    --socket /tmp/arctern-sender.sock

# Trigger an immediate cycle
curl -X POST --unix-socket /tmp/arctern-sender.sock \
  http://_/api/v1/jobs/push/wakeup
```

CI:

```bash
cd ~/code/arctern && just test-vm
```

## Phase 2: Tasks

Generated by `speckit-tasks` into `specs/005-push-job/tasks.md`. Expected ordering (10 tasks):

1. T001 — `feat(transport)`: extend wire protocol with `Op` enum, `ListRequest`/`Response`/`SnapshotEntry`, `SendHeader`, `ReceiveHeader.op` field with `#[serde(default)]`. cargo add `regex` to crates/transport.
2. T002 — `feat(daemon)`: SinkJob handles `op = "list"` alongside `op = "send"`; LIST runs palimpsest list, filters via `prefix_regex`, returns `ListResponse`.
3. T003 — `feat(config)`: PushJobConfig + PushTarget + SendFlagsConfig + SnapshotFilterConfig + validate_push (xor on prefix/regex, root_fs shape, regex compiles).
4. T004 — `feat(daemon)`: jobs/push.rs — planner (palimpsest list of sender, LIST request to receiver, intersect by GUID, choose Full/Incremental).
5. T005 — `feat(daemon)`: jobs/push.rs — executor (open SEND stream, write header, spawn palimpsest send, copy to stream, await response).
6. T006 — `feat(daemon)`: wire PushJob into JobManager; cycle loop with cancellation + wakeup; Job::wakeup() trait method default no-op + impls on snap+push.
7. T007 — `feat(api)`: JOB_KIND_PUSH constant; `POST /api/v1/jobs/{name}/wakeup` handler.
8. T008 — `feat(daemon)`: wire RecvProperties into palimpsest RecvArgs in sink (closes slice 004 D22).
9. T009 — `docs(config)`: example-config.toml gains a push job translating the user's zrepl `push_to_local` shape.
10. T010 — `test(integration)`: end-to-end full + incremental push between two LoopbackPools.

## Decisions made beyond the slice ticket's D1-D15

- **D16** (formalised at planning): the LIST path's "dataset doesn't exist" returns `Ok { snapshots: [] }` not `Error`. This is a server-side mapping of `palimpsest::ZfsError::DatasetNotFound` to the empty-snapshot case. Reason: first-replication is a normal state, not an error. The sink's `palimpsest::dataset::list` rooted at a missing dataset returns an `ZfsError`; the LIST handler matches on the variant and converts. Any other error variant becomes `Error { message }`.
- **D17**: cursor bookmarks NOT planted on the sender (zrepl plants `#zrepl_CURSOR_<guid>`). Reason: the LIST round-trip already gives us authoritative receiver state at single-digit-ms cost over WG; cursor bookmarks add a second source of truth that has to be reconciled when they disagree. Receiver state wins. Documented in spec NFR-001 (constitution VII).
- **D18**: LIST request and SEND request go on SEPARATE QUIC streams within a single filesystem cycle (one stream = one operation). Reason: each stream has its own response framing; reusing a stream would force a more complex protocol with intra-stream framing for "list response, then send header, then send bytes, then send response". The LIST cost is one extra `accept_bi` on the receiver — microseconds. Worth it for the simplicity.
- **D19**: GUIDs are wire-typed as `u64` (not `String`). Reason: `serde_json` round-trips `u64` exactly via numeric-or-string per the spec, but `serde_json::Number` is f64 by default which loses precision above 2^53. Using `u64` with `#[serde(serialize_with = "u64_as_string", deserialize_with = "string_or_u64")]` would be belt-and-suspenders, but `serde_json` actually handles `u64` correctly via its `arbitrary_precision` feature; we'll test that the parse round-trips a value > i64::MAX (the VM-captured one: 11587258101628135412). If that test fails, we add a string-or-int custom deserializer in the same commit.
- **D20**: `connect` is exactly one `SocketAddr`. zrepl supports a `Servers` array; arctern defers multi-peer fan-out. Operators wanting fan-out today use one push job per peer.
- **D21**: `server_name` defaults to `"arctern"` because the slice 004 self-signed cert in `crates/transport::identity` is generated with subject CN/SAN = `"arctern"`. The accept-any verifier ignores SAN matching anyway, but rustls still requires a legal SNI string at connection time; `"arctern"` is the value that round-trips through `quinn::Endpoint::connect(addr, "arctern")`.
- **D22**: `Job::wakeup(&self)` is a new trait method with a default no-op implementation. SnapJob and PushJob override it to call `notify.notify_one()`. SinkJob keeps the default — sinks are event-driven. Reason: a single trait method is the simplest extension that lets the wakeup HTTP handler dispatch generically across job kinds without a kind-aware match.
- **D23**: the wakeup HTTP handler is `POST /api/v1/jobs/{name}/wakeup` returning `204 No Content` on success and `404` on unknown name. No body. No request payload — the act of POSTing IS the wakeup. Same shape as zrepl's `signal wakeup` CLI verb but routed over HTTP per constitution III.
- **D24**: per-fs error reporting at the cycle level uses `errors.join("; ")` accumulated in the cycle loop and stored in `JobStatusInner.last_error` — same pattern as `SnapJob` from slice 003. Per-fs sub-status (each filesystem has its own `last_error`/`last_pushed_snapshot`) is deferred to slice 008 per the spec's "Out of scope".
- **D25**: SEND-stream header carries `from_snap` + `to_snap` as `SnapshotRef { name, guid }`. The receiver does NOT validate that the incoming stream's GUIDs match the header's claims (`zfs recv` already enforces stream integrity); the header is informational + future-proofing. Logging `from_snap.guid` and `to_snap.guid` lets operators correlate receiver logs with sender-side decisions.
- **D26** (regex grep allowlist extension): `crates/transport` now `cargo add regex` to compile `prefix_regex` server-side. The constitution-IV grep gate command is now `! grep -RnE '^use regex' --include='*.rs' crates/api crates/client daemon/src/` — `crates/transport` joins `crates/config` on the regex allowlist. Reason: `crates/transport` parses untrusted user-controlled regex strings off the wire; rejecting bad regex with a clean `ListResponse::Error` is the right behaviour and requires `regex::Regex::new`.
- **D27**: snapshot pre-creation for the integration test uses the slice-002 snapshot HTTP endpoint (`POST /api/v1/datasets/{ds}/snapshots`) on the sender daemon. Reason: avoids reaching for `palimpsest` directly inside the test, exercising the public daemon HTTP surface. Two birds, one stone.
- **D28**: integration test wakeup-and-wait pattern is a poll loop with a generous timeout (~10s) checking the receiver's snapshot list grew. Reason: the cycle is asynchronous — the wakeup returns 204 immediately, but the actual send + recv complete some time later. Polling is simpler than a notify channel and the test is already wall-clock heavy from VM ops.

## Verification

```bash
# Inside arctern repo
cargo check --workspace
cargo clippy --workspace --all-targets --features integration -- -D warnings
cargo test --workspace                          # unit tests

# Constitution principle IV gates
! grep -RnE 'tokio::process::Command' --include='*.rs' crates/api crates/client crates/transport daemon/src/
! grep -RnE '^use regex' --include='*.rs' crates/api crates/client daemon/src/
# crates/config + crates/transport are exempt — they parse user input.

# Integration (requires VM)
just vm-up
just test-integration
just vm-down
```

## Risks

- **u64 GUID JSON precision**: noted in D19. Mitigation: a deliberate unit test that round-trips `SnapshotEntry { guid: 11587258101628135412, ... }` and asserts equality. If `serde_json` mangles it, swap in a `string_or_u64` custom deserializer in the same commit.
- **Sink's LIST handler on a non-existent dataset**: palimpsest's `list` returns `ZfsError::DatasetNotFound` for `roots = ["<missing>"]` (verified). The handler maps that variant to `Ok { snapshots: [] }`. Mitigation: a unit test with a `RecordingRunner` returning a "no such dataset" exit. (Already covered by palimpsest's own classifier; we test the arctern-side mapping.)
- **Slice 004 wire compat with slice 005 sink**: see Phase 0 final bullet — slice 004 used `deny_unknown_fields`, so a slice-005 client header sent to a slice-004 sink would be rejected. Accepted because slice 004 never shipped externally and the only running sinks are the ones we're updating now.
- **Snap-job interval racing the push-job interval in the integration test**: with both at 1s, the push job might fire before the snap job has finished its first cycle. Mitigation: the test does NOT use the snap job to create the test snapshots; it manually POSTs `POST /api/v1/datasets/{ds}/snapshots` with a deterministic name (`test_001`, `test_002`). Snap job is only present in the demo quickstart, not the integration test config.
- **`zfs send` against an encrypted dataset without `-w` and no loaded key**: noted in spec User Story 5.3 — fails cleanly per-fs, cycle continues. Operator sets `encrypted = true`. No silent breakage.
- **`palimpsest::send::send` ChildHandle drop-on-cancel ordering**: `kill_on_drop` is set in palimpsest's `RealRunner`. Confirmed in palimpsest's `runner` module (slice-004 prep commit `ChildHandle::start_kill + kill_on_drop`). Cancellation drops the QUIC stream (which errors `tokio::io::copy`), then the per-fs task drops the ChildHandle, which kills `zfs send`. Verified at the integration-test SIGTERM path.
- **Receiver's per-stream task spawning unbounded LIST tasks**: a misbehaving (or compromised) sender could open many LIST streams and DoS the receiver via accumulated `palimpsest::dataset::list` invocations. Mitigation deferred — same threat model as slice 004's unbounded recv concurrency (slice 004 D21). WG is the perimeter.
