# Tasks: Push job — active sender + LIST-based discovery

**Feature**: `005-push-job`
**Input**: [spec.md](./spec.md), [plan.md](./plan.md)

Each task = one logical commit. Per-task verification commands listed.

## T001 — `feat(transport)`: extend wire protocol with `Op` + LIST + SendHeader

**Why first**: every later task imports these types.

**Changes**:

- `crates/transport/`: `cargo add regex` (LIST handler will need it later, and we want the dep landed in this crate's commit, not the daemon's).
- `crates/transport/src/protocol.rs`:
  - Add `pub enum Op { Send, List }` with `#[serde(rename_all = "snake_case")]`.
  - Extend `ReceiveHeader`: add `#[serde(default = "default_op")] pub op: Op`, `#[serde(default, skip_serializing_if = "Option::is_none")] pub prefix_regex: Option<String>`, `#[serde(default, skip_serializing_if = "Option::is_none")] pub send: Option<SendHeader>`. Drop the slice-004 `send_flags: Option<SendFlags>` placeholder (was `SendFlags {}` empty struct — superseded by the typed `SendHeader`).
  - Add `SendHeader { send_kind: SendKind, from_snap: Option<SnapshotRef>, to_snap: SnapshotRef, flags: SendFlagsWire }`.
  - Add `pub enum SendKind { Full, Incremental }` with `#[serde(rename_all = "snake_case")]`.
  - Add `SnapshotRef { name: String, guid: u64 }`.
  - Add `SendFlagsWire { raw: bool, embedded: bool, compressed: bool, large_blocks: bool }`.
  - Add `pub enum ListResponse { Ok { snapshots: Vec<SnapshotEntry> }, Error { message: String } }` with `#[serde(tag = "status", rename_all = "snake_case")]`.
  - Add `SnapshotEntry { name: String, guid: u64, createtxg: u64 }`.
  - Add `pub async fn write_list_response<W: AsyncWrite + Unpin>(w: &mut W, resp: &ListResponse) -> Result<(), ProtocolError>` and `read_list_response<R: AsyncRead + Unpin>(r) -> Result<ListResponse, ProtocolError>` — same shape as the existing `write_response`/`read_response` for `ReceiveResponse`.
- `crates/transport/src/lib.rs`: re-export `Op`, `SendHeader`, `SendKind`, `SnapshotRef`, `SendFlagsWire`, `ListResponse`, `SnapshotEntry`, `write_list_response`, `read_list_response`.
- Unit tests:
  - `op_field_defaults_to_send_when_absent`: deserialize a slice-004 shape (no `op` key) and assert `header.op == Op::Send` (slice-004 wire compat).
  - `header_with_op_list_roundtrip`: write + read back a `{op: List, prefix_regex: Some(...)}` header.
  - `send_header_roundtrip`: full + incremental send headers.
  - `list_response_roundtrip`: Ok + Error variants.
  - `guid_above_i64_max_roundtrips`: a `SnapshotEntry { guid: 11587258101628135412, ... }` round-trips through serde_json without precision loss. **D19 risk** — if this fails, swap in a custom string-or-u64 deserializer in the same commit.

**Verify**:
```
cargo test -p arctern-transport protocol
cargo clippy -p arctern-transport --all-targets -- -D warnings
```

**Commit**: `feat(transport): op + LIST + SendHeader wire types (T001)`

## T002 — `feat(daemon)`: SinkJob dispatches on Op, handles `op = "list"`

**Changes**:

- `daemon/src/jobs/sink.rs`:
  - Inside `handle_stream_inner`, after `read_header`, branch on `header.op`:
    - `Op::Send`: existing path (target validation + zfs_recv + io::copy + wait). Refactor into `handle_send(...)`.
    - `Op::List`: new path. Call `handle_list(job, runner, header.target_dataset, header.prefix_regex, send).await`.
  - `handle_list`:
    1. Validate `target_dataset == job.config.root_fs || target_dataset.starts_with(format!("{}/", root_fs))` — same prefix logic as send. Reject otherwise with `ListResponse::Error`. (List of root_fs itself IS allowed — it's a meaningful query.)
    2. Compile the optional `prefix_regex` to a `regex::Regex`. On compile error, write `ListResponse::Error { message: "<re-error>" }` and finish.
    3. Call `palimpsest::dataset::list(runner, &ListOptions { recursive: false, types: vec![DatasetType::Snapshot], roots: vec![target_dataset.clone()], properties: vec!["guid".into()], ..Default::default() })`.
    4. On `Err(ZfsError::DatasetNotFound { .. })`: write `ListResponse::Ok { snapshots: vec![] }` (D16). On other errors: `ListResponse::Error { message }`.
    5. On `Ok(entries)`: filter by `prefix_regex.is_match(snapshot_name)` where snapshot_name is the part after `@` (use `entry.snapshot_name.as_deref().unwrap_or(...)` — palimpsest splits this for snapshot entries). Build `Vec<SnapshotEntry>` parsing `properties.guid.value` as `u64` (skip + warn on parse failure) and `entry.createtxg` as `u64`.
    6. `write_list_response(&mut send, &ListResponse::Ok { snapshots })`.
  - Unit tests: pure-function-style — `filter_snapshots(entries, &Some("^test_"))` returns expected names. The async path is exercised by T010 (integration).

**Verify**:
```
cargo test -p arctern-daemon sink
cargo clippy -p arctern-daemon --all-targets -- -D warnings
```

**Commit**: `feat(daemon): sink dispatches on Op; handle op=list with palimpsest list (T002)`

## T003 — `feat(config)`: PushJobConfig + validation

**Changes**:

- `crates/config/src/schema.rs`:
  - `JobConfig::Push(PushJobConfig)` variant.
  - `PushJobConfig { name, connect: SocketAddr, interval (humantime_serde), server_name (default "arctern"), filesystems: Vec<FilesystemFilter>, target: PushTarget, send: SendFlagsConfig (default), snapshot_filter: SnapshotFilterConfig }`.
  - `PushTarget { root_fs: String }`.
  - `SendFlagsConfig { encrypted, embedded_data, compressed, large_blocks }` — all `bool`, all default `true`. Implement `Default`.
  - `SnapshotFilterConfig { prefix: Option<String>, regex: Option<String> }`.
  - Update `JobConfig::name()` to cover the Push arm.
- `crates/config/src/lib.rs`:
  - Extend `validate(...)`: dispatch on JobConfig::Push, call `validate_push(idx, &cfg)`.
  - `validate_push`: name non-empty; `target.root_fs` non-empty + no leading/trailing `/`; `snapshot_filter` has exactly one of `prefix` xor `regex` (both empty rejected, both present rejected); if `regex` present, compile via `regex::Regex::new` to surface bad patterns; for each filesystem entry, the same `exclude requires recursive` + descendant checks as snap.
  - Re-export `PushJobConfig`, `PushTarget`, `SendFlagsConfig`, `SnapshotFilterConfig`.
- Unit tests:
  - `minimal_push_parses` — schema as in plan's quickstart parses + validates.
  - `push_send_flags_default_true_when_omitted`.
  - `push_filter_neither_prefix_nor_regex_rejected`.
  - `push_filter_both_prefix_and_regex_rejected`.
  - `push_bad_regex_rejected`.
  - `push_bad_root_fs_rejected` — empty + leading-slash + trailing-slash.
  - `push_bad_connect_rejected`.
  - `push_bad_interval_rejected`.

**Verify**:
```
cargo test -p arctern-config push
cargo test -p arctern-config             # all
```

**Commit**: `feat(config): push job schema + xor snapshot_filter validation (T003)`

## T004 — `feat(daemon)`: jobs/push.rs — planner

**Changes**:

- `daemon/src/jobs/push.rs` (new): planner-only this commit. Executor is T005, cycle loop is T006.
- `pub struct SnapshotPlan` enum with `Nothing`, `Full { to: SnapshotRef }`, `Incremental { from: SnapshotRef, to: SnapshotRef }`.
- `pub async fn plan_one_filesystem(runner, sender_path: &str, target_dataset: &str, snapshot_filter: &CompiledFilter, connection: &quinn::Connection) -> Result<SnapshotPlan, PlanError>`:
  1. `palimpsest::dataset::list` rooted at `sender_path`, type Snapshot, properties `["guid"]`.
  2. Filter by `snapshot_filter` (matches on the bare snapshot name after `@`).
  3. Sort by `createtxg` ascending.
  4. If empty: return `SnapshotPlan::Nothing`.
  5. Open a bi stream on the connection. Write a `ReceiveHeader { version: 1, op: Op::List, target_dataset: target_dataset.to_string(), prefix_regex: snapshot_filter.regex_str().map(String::from), send: None }`. `send.finish()`.
  6. `read_list_response(&mut recv)`. On `ListResponse::Error { message }`: return `Err(PlanError::Receiver { message })`. On `Ok { snapshots }`: continue.
  7. Build a `BTreeMap<u64, &SnapshotEntry>` of receiver snapshots by GUID.
  8. Walk sender snapshots from highest createtxg to lowest, find first with a GUID in the receiver map. That is `from`. The latest sender snapshot is `to`.
  9. If `from.guid == to.guid` (which means latest sender snap is already on the receiver): return `SnapshotPlan::Nothing`.
  10. If no `from` found: return `SnapshotPlan::Full { to }`.
  11. Else: `SnapshotPlan::Incremental { from, to }`.
- `pub struct CompiledFilter { regex: Option<regex::Regex> }` with `compile(prefix: Option<&str>, regex: Option<&str>) -> Result<Self, regex::Error>`. The compiled filter materialises `regex_str` for the wire (we send the original string, not the compiled form). Lives in `daemon/src/jobs/push.rs` because the planner owns it; the daemon (not the leaf transport crate) is allowed to compile regex.
- Unit tests (no QUIC required — the GUID-intersection algorithm is pure):
  - Extract a small helper `pub(crate) fn pick_plan(sender: &[(u64, u64, String)], receiver: &[(u64, String)]) -> SnapshotPlan` where args are `(createtxg, guid, name)` triples for sender and `(guid, name)` for receiver. Test:
    - empty sender → Nothing
    - sender has snaps, receiver empty → Full(latest)
    - sender == receiver latest → Nothing
    - sender ahead by 2, receiver has the older one → Incremental(receiver_latest → sender_latest)
    - GUIDs disjoint → Full(latest)
    - VM-captured u64 GUID values exceeding i64::MAX intersect correctly

**Verify**:
```
cargo test -p arctern-daemon push::planner
cargo clippy -p arctern-daemon --all-targets -- -D warnings
```

**Commit**: `feat(daemon): push planner — palimpsest list + GUID intersection (T004)`

## T005 — `feat(daemon)`: jobs/push.rs — executor

**Changes**:

- `daemon/src/jobs/push.rs`: add executor on top of T004's planner.
- `pub async fn execute_one_plan(runner, plan: &SnapshotPlan, target_dataset: &str, sender_dataset: &str, send_flags: &SendFlagsConfig, connection: &quinn::Connection) -> Result<(), ExecError>`:
  1. Match `plan`; on `Nothing`: log + return Ok.
  2. Open a bi stream.
  3. Build the wire header: `op = Op::Send`, `target_dataset = target_dataset.to_string()`, `send = Some(SendHeader { send_kind, from_snap, to_snap, flags: SendFlagsWire { raw, embedded, compressed, large_blocks } })` derived from the plan + `send_flags`.
  4. `write_header(&mut send_stream, &header)`.
  5. Build palimpsest `SendArgs::new(format!("{sender_dataset}@{to.name}"))` with the four flags. For Incremental, call `.incremental(format!("{sender_dataset}@{from.name}"))`.
  6. `palimpsest::send::send(runner, &args)` → ChildHandle.
  7. Take stdout from the handle. Spawn a stderr-drain task into a `Vec<u8>`.
  8. `tokio::io::copy(&mut child_stdout, &mut send_stream).await`.
  9. `send_stream.finish()`. Wait for `child.wait()`. Await the stderr drain.
  10. `read_response(&mut recv_stream)` (the existing slice-004 helper for `ReceiveResponse`).
  11. On `ReceiveResponse::Ok` AND child exit success: log success + return Ok.
  12. On any error: build `ExecError` with both child stderr and receiver message (whichever applies).
- The cycle-level loop is in T006; T005 is just the executor function.
- Unit tests: minimal — most of the executor is async I/O against quinn, exercised by T010. Add a small wire-construction test: given a `SnapshotPlan::Incremental` and `SendFlagsConfig::default()`, the constructed `SendHeader` carries the right `send_kind` + `flags`.

**Verify**:
```
cargo test -p arctern-daemon push
cargo clippy -p arctern-daemon --all-targets -- -D warnings
```

**Commit**: `feat(daemon): push executor — open SEND stream + pipe palimpsest send (T005)`

## T006 — `feat(daemon)`: PushJob struct + cycle loop + Job::wakeup

**Changes**:

- `daemon/src/jobs/mod.rs`:
  - `pub mod push;`.
  - Add a default `fn wakeup(&self) {}` method to the `Job` trait.
- `daemon/src/jobs/snap.rs`:
  - Add `wakeup: Arc<tokio::sync::Notify>` field. `wakeup()` calls `notify_one()`.
  - `select!` arm in the loop: `_ = wakeup.notified() => {}` between `cancel.cancelled()` and `sleep(interval)`.
- `daemon/src/jobs/push.rs`:
  - `pub struct PushJob { config: PushJobConfig, identity: Arc<TransportIdentity>, status: Mutex<JobStatusInner>, wakeup: Arc<tokio::sync::Notify>, filter: CompiledFilter }`. (Compile the filter once at construction.)
  - `pub fn new(config: PushJobConfig, identity: Arc<TransportIdentity>) -> Result<Self, regex::Error>`. Bubble compile errors to the caller (daemon main).
  - `impl Job for PushJob`: `name`, `kind = JOB_KIND_PUSH`, `status` mirror SnapJob; `wakeup` calls `notify_one()`.
  - `run` loop:
    1. Loop with `select!` on `cancel`, `sleep(interval)`, `wakeup.notified()`.
    2. Each cycle: open one `quinn::Endpoint::client((Ipv4Addr::UNSPECIFIED, 0).into())?` per cycle (one connection across all filesystems within the cycle). `endpoint.set_default_client_config(client_config_accept_any())`. `connect(self.config.connect, &self.config.server_name)?.await?` for the connection.
    3. For each `[[jobs.filesystems]]` entry: resolve via `arctern_config::filter::resolve_all` against a fresh sender-side dataset list (use a per-filesystem `palimpsest::dataset::list` rooted at the path for `recursive` + `exclude` semantics matching snap).
    4. For each resolved sender path: target_dataset = `format!("{root_fs}/{sender_path}")`. Call `plan_one_filesystem(...)`. Then `execute_one_plan(...)`. Accumulate any `Err` into a per-cycle `Vec<String>`.
    5. Close the connection (`connection.close(0u32.into(), b"cycle done"); endpoint.wait_idle().await`).
    6. Update `JobStatusInner` with `last_run = now`, `next_run = now + interval`, `last_error = errors.join("; ")` if non-empty.
- `daemon/src/main.rs`:
  - Construct push jobs alongside snap and sink in the build loop. Push needs the same `Arc<TransportIdentity>` that sink uses (it's a TLS client; the identity carries the cert too, but only the client config's verifier is used — accept-any). Actually push only needs `client_config_accept_any()`, NOT the identity. **Update `needs_identity` to also check for sink jobs (only sinks need a cert)**; push doesn't trigger cert generation.
  - Compile filter at construction; bubble regex errors to `eyre::eyre!`.

**Verify**:
```
cargo check -p arctern-daemon
cargo clippy -p arctern-daemon --all-targets -- -D warnings
cargo test -p arctern-daemon
```

**Commit**: `feat(daemon): PushJob — cycle loop with cancellation + wakeup (T006)`

## T007 — `feat(api+daemon)`: JOB_KIND_PUSH + POST /jobs/{name}/wakeup

**Changes**:

- `crates/api/src/lib.rs`: `pub const JOB_KIND_PUSH: &str = "push";`.
- `daemon/src/jobs/push.rs`: `pub const KIND: &str = arctern_api::JOB_KIND_PUSH;`.
- `daemon/src/jobs/mod.rs`: extend `JobManager` with a new method `pub fn wakeup_by_name(&self, name: &str) -> bool`. Looks up the job by name in `handles`, calls `job.wakeup()` on the trait object via `status_ref` — actually we need a separate trait object per slot. Alternative: store an additional `Arc<dyn Job>` in `JobHandle` (we currently have `status_ref: Arc<dyn StatusRead>`). Add `job_ref: Arc<dyn Job>` (extending the existing pattern). `wakeup_by_name` returns `false` if no handle with that name exists, else calls `job.wakeup()` and returns `true`.
  - Spawn signature stays the same (`spawn<J: Job>` already takes `Arc<J>`); store `job.clone() as Arc<dyn Job>` alongside `status_ref` in the handle.
- `daemon/src/handlers/jobs.rs`: add `pub async fn wakeup(State(state): State<AppState>, Path(name): Path<String>) -> StatusCode` returning `204` on hit, `404` on miss.
- `daemon/src/router.rs`: register `.route("/api/v1/jobs/{name}/wakeup", post(wakeup))`.
- Unit tests: a router-level test using `axum::http::Request::builder()` + a `JobManager` with one `NoopJob` (extending the existing test in `daemon/src/jobs/mod.rs` — give NoopJob a wakeup-observable flag).

**Verify**:
```
cargo test -p arctern-daemon
cargo test -p arctern-api
cargo clippy --workspace --all-targets -- -D warnings
```

**Commit**: `feat(api+daemon): JOB_KIND_PUSH + POST /jobs/{name}/wakeup (T007)`

## T008 — `feat(daemon)`: wire RecvProperties into palimpsest RecvArgs

**Changes**:

- `daemon/src/jobs/sink.rs`:
  - In `handle_send` (refactored out of `handle_stream_inner` in T002), construct `RecvArgs` as:
    ```rust
    let mut args = RecvArgs::new(header.target_dataset.clone()).unmounted();
    for (k, v) in &job.config.recv.properties.overrides {
        args = args.property_override(k, v);
    }
    for k in &job.config.recv.properties.inherit {
        args = args.property_inherit(k);
    }
    ```
  - Always pass `.unmounted()` — sink-side mounts are inappropriate (we don't know the operator's mountpoint policy; the dataset can be mounted later by the operator). Justified in the function-level comment.
- Unit tests:
  - In `daemon/src/jobs/sink.rs` tests: add a build-args fixture confirming the override+inherit chain produces the expected `RecvArgs.build_args()` output via palimpsest. (Calls into palimpsest directly with a synthetic `RecvProperties`.)

**Verify**:
```
cargo test -p arctern-daemon sink
cargo clippy -p arctern-daemon --all-targets -- -D warnings
```

**Commit**: `feat(daemon): wire RecvProperties into palimpsest RecvArgs (T008, closes 004 D22)`

## T009 — `docs(config)`: example-config.toml gains a push job

**Changes**:

- `docs/example-config.toml`: append a `push_to_local` job at the bottom that mirrors what the user's zrepl `push_to_local` would look like in arctern. Include comments explaining:
  - The `target.root_fs` mapping (`<root_fs>/<sender_path>` literal concat).
  - Why all four `[jobs.send]` flags default true.
  - The `snapshot_filter.prefix` vs `regex` xor.
  - That `interval` is the planner-cycle interval, NOT the snapshot interval (snap job's interval is independent).

**Verify**:
```
# parse the doc through arctern_config
cargo run -p arctern-daemon -- configcheck docs/example-config.toml
```

**Commit**: `docs(config): example-config.toml gains a push job (T009)`

## T010 — `test(integration)`: end-to-end full + incremental push

**Changes**:

- `daemon/tests/common/mod.rs`: extend `spawn_daemon_uds_with_config` (or add a `_with_quic` variant) to support multiple daemons in the same test — distinct UDS paths, distinct state dirs. Already half-supported; just confirm.
- `daemon/tests/integration_quic_push.rs` (new):
  1. Boot two `LoopbackPool`s as `sender_pool` and `receiver_pool`.
  2. On the receiver pool, create `<receiver_pool>/sink` (`zfs create -o mountpoint=none`).
  3. On the sender pool, create `<sender_pool>/data` (`zfs create -o mountpoint=none`).
  4. Spawn the **sink** daemon: state_dir = `/tmp/arctern_sink_<nanos>`, UDS = `/tmp/arctern_sink_<nanos>.sock`, sink job listens on `127.0.0.1:0` with `root_fs = <receiver_pool>/sink`. Read `LISTEN_QUIC <port>` from stdout.
  5. Spawn the **sender** daemon: state_dir = `/tmp/arctern_sender_<nanos>`, UDS = `/tmp/arctern_sender_<nanos>.sock`, push job (`interval = "1h"` — we'll use wakeup, so the interval should be long enough not to fire concurrently with the test) connecting to `127.0.0.1:<sink_port>`, target.root_fs = `<receiver_pool>/sink`, snapshot_filter.prefix = `"test_"`, [[jobs.filesystems]] path = `<sender_pool>/data`.
  6. **Phase 1 — full send**:
     - On the sender pool (via SSH directly): `zfs snapshot <sender_pool>/data@test_001`.
     - `POST /api/v1/jobs/push/wakeup` to the sender daemon.
     - Poll receiver via SSH (`zfs list -H -t snapshot -o name <receiver_pool>/sink/<sender_pool>/data` after a `<receiver_pool>/sink/<sender_pool>/data` directory exists) until at least one snapshot named `test_001` is present, with a 15-second timeout.
     - Assert receiver has exactly one snapshot, name = `test_001`.
  7. **Phase 2 — incremental send**:
     - Snapshot sender as `test_002`.
     - Wakeup again.
     - Poll until receiver has two snapshots.
     - Assert names = `["test_001", "test_002"]`, GUIDs match the sender's snapshots' GUIDs.
  8. Tear down: kill both daemons (SIGTERM via signal_kill on the Child), `loopback.destroy()` for both pools.
- `#[tokio::test(flavor = "multi_thread")]`. `#![cfg(feature = "integration")]` at top.
- This test is sensitive to ordering — keep snap creation, wakeup, and assertion serial. Use `tokio::time::timeout` for the poll loops.

**Verify**:
```
just vm-up
just test-integration
just vm-down
```

**Commit**: `test(daemon): integration test for full + incremental push (T010)`

## Dependency graph

```
T001 (transport: Op + LIST + SendHeader) ──┬──> T002 (sink op=list dispatch)
                                            └──> T004 (planner) ──> T005 (executor) ──> T006 (PushJob loop)
T003 (config: PushJobConfig) ───────────────────────────────────────────────────────────────┘
T006 ──> T007 (JOB_KIND_PUSH + wakeup endpoint)
T002 ──> T008 (sink wires RecvProperties)
T009 (docs) — independent, can land any time after T003
T010 (integration) — last; needs T002 + T006 + T007 + T008
```

T001 strictly first. T002 needs T001. T003 independent (config crate). T004 needs T001 + T003. T005 needs T004. T006 needs T005 + T003. T007 needs T006 (uses Job::wakeup). T008 needs T002. T009 needs T003. T010 needs everything operational.

## Done when

All of: `cargo test --workspace` green, `cargo clippy --workspace --all-targets --features integration -- -D warnings` clean, `just test-integration` exits 0, all 10 commits land on the slice branch, the constitution-IV grep returns no matches in `crates/{api,client,transport} daemon/src/`, and `! grep -RnE '^use regex' --include='*.rs' crates/api crates/client daemon/src/` returns no matches.
