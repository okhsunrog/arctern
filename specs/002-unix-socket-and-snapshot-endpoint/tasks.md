# Tasks: UNIX socket + POST /datasets/{name}/snapshots

**Feature**: `002-unix-socket-and-snapshot-endpoint`
**Input**: [spec.md](./spec.md), [plan.md](./plan.md)

Each task = one logical commit. Per-task verification commands listed.

## T001 — `crates/api`: add `CreateSnapshotRequest`

**Why first**: `daemon` and `arctern-client` both depend on the type. Landing the wire type alone keeps the commit small and unblocks the next two tasks in parallel.

**Changes**:
- `crates/api/src/lib.rs`: add `CreateSnapshotRequest { snapshot_name: String, recursive: bool, properties: BTreeMap<String, String> }` deriving `Debug, Clone, Default, Serialize, Deserialize, ToSchema`. `recursive` and `properties` use `#[serde(default)]`.
- Unit test: serde round-trip with `{"snapshot_name":"s1"}` (defaults for the rest) and a fully-populated form.

**Verify**:
```
cargo test -p arctern-api
```

**Commit**: `feat(api): CreateSnapshotRequest wire type (T001)`

## T002 — `daemon`: `--socket` flag + UNIX-listener bind + `LISTEN unix:` handshake

**Changes**:
- `daemon/Cargo.toml`: ensure `tokio` features include `signal` and `net` (already on with `full`; explicit comment if pruned later).
- `daemon/src/main.rs`:
  - Extend `Command::Daemon` to `Daemon { #[arg(long)] socket: Option<PathBuf> }`.
  - New private fn `resolve_socket_path(cli: Option<PathBuf>) -> PathBuf`: returns `cli` if set; else `$XDG_RUNTIME_DIR/arctern.sock` if `$XDG_RUNTIME_DIR` is set and points to an existing writable dir; else `/run/arctern.sock`.
  - In `run_daemon`: call `resolve_socket_path`, `remove_file` (ignore `NotFound`, fatal otherwise), `UnixListener::bind`, `set_permissions(0o600)`, print `LISTEN unix:<absolute-path>` and flush, then `axum::serve(listener, app.into_make_service_with_connect_info::<PeerCredentials>())` (PeerCredentials added in T003 — temporarily use a unit `()` connect-info type and swap in T003).
  - SIGTERM/SIGINT cleanup branch (best-effort `remove_file`).
- Slice-001's TCP-bind path is removed, NOT kept alongside (FR-001).

**Verify**:
```
cargo build -p arctern-daemon
cargo run -p arctern-daemon -- daemon --socket /tmp/arctern_t2.sock &
sleep 0.5; cat /tmp/arctern_t2.sock_listen 2>/dev/null   # informal: stdout shows LISTEN unix:/tmp/arctern_t2.sock
kill %1; rm -f /tmp/arctern_t2.sock
```

**Commit**: `feat(daemon): bind UNIX socket; LISTEN unix:<path> handshake (T002)`

## T003 — `daemon`: PeerCredentials + PeerAuth tower layer

**Changes**:
- `daemon/src/auth.rs` (new):
  - `#[derive(Clone, Debug)] pub struct PeerCredentials { pub uid: u32 }`.
  - `impl Connected<IncomingStream<'_, UnixListener>> for PeerCredentials` calling `stream.io().peer_cred()` and unwrapping the `UCred` uid (panic-on-error is acceptable here: kernel guarantees `SO_PEERCRED` for `AF_UNIX`).
  - `pub async fn enforce_same_uid(req, next) -> Response` middleware (`axum::middleware::from_fn`) that reads `ConnectInfo<PeerCredentials>` from the request extensions, compares to `nix::unistd::geteuid().as_raw()` or libc `geteuid()` (use libc directly via `unsafe { libc::geteuid() }` — no extra dep), returns `(StatusCode::FORBIDDEN, Json(ApiErrorBody { error: "peer_uid_mismatch", message: ... })).into_response()` on mismatch.
- `daemon/src/router.rs`: layer the entire router with `axum::middleware::from_fn(enforce_same_uid)` so EVERY route (including `/api-docs/openapi.json`) is protected.
- `daemon/src/main.rs`: swap the temporary `()` connect-info type for `PeerCredentials`.
- `daemon/Cargo.toml`: `cargo add libc` (avoids pulling all of nix for one syscall).
- Unit test in `daemon/src/auth.rs`: trivial — assert mismatched uid yields 403, matched yields next.run-through. Skip if mocking the layer becomes more code than the layer itself; the integration tests cover the live behaviour.

**Verify**:
```
cargo build -p arctern-daemon
cargo clippy -p arctern-daemon -- -D warnings
```

**Commit**: `feat(daemon): peer-uid auth via SO_PEERCRED tower layer (T003)`

## T004 — `daemon`: POST /api/v1/datasets/{name}/snapshots handler

**Changes**:
- `daemon/src/handlers/mod.rs`: `pub mod snapshots;`.
- `daemon/src/handlers/snapshots.rs`:
  - Handler signature: `pub async fn create_snapshot(Path(name): Path<String>, Json(req): Json<CreateSnapshotRequest>) -> Result<(StatusCode, Json<DatasetSummary>), ApiError>`.
  - Build `palimpsest::dataset::SnapshotOptions` from `req` (recursive + properties).
  - Construct the `SshCommandRunner` via `from_env()` (same shape as slice 001).
  - Call `palimpsest::dataset::snapshot(&runner, &format!("{name}@{}", req.snapshot_name), &opts).await?`.
  - Materialize the response: `palimpsest::dataset::list(&runner, &ListOptions { roots: vec![format!("{name}@{}", req.snapshot_name)], types: vec![DatasetType::Snapshot], ..Default::default() }).await?` — take the single entry, map via `DatasetSummary::from`.
  - Return `(StatusCode::CREATED, Json(summary))`.
  - `#[utoipa::path(post, path = "/api/v1/datasets/{name}/snapshots", request_body = CreateSnapshotRequest, params(("name" = String, Path, ...)), responses((status = 201, body = DatasetSummary), (status = 409, body = ApiErrorBody), (status = 404, body = ApiErrorBody), (status = 500, body = ApiErrorBody)))]`.
- `daemon/src/router.rs`: register via `routes!(handlers::snapshots::create_snapshot)`. Add `CreateSnapshotRequest` to `components(schemas(...))`.

**Verify**:
```
cargo build -p arctern-daemon
cargo clippy -p arctern-daemon -- -D warnings
```

**Commit**: `feat(daemon): POST /api/v1/datasets/{name}/snapshots handler (T004)`

## T005 — `crates/client`: hand-rolled UDS transport + `list_datasets` + `create_snapshot`

**Changes**:
- `crates/client/Cargo.toml`: drop `reqwest`. `cargo add hyper --features client,http1`. `cargo add hyper-util --features tokio,http1,client-legacy`. `cargo add http-body-util`. `cargo add http`. `cargo add tokio --features net,rt`. (Keep `serde_json`, `arctern-api`, `thiserror`.)
- `crates/client/src/lib.rs`:
  - Replace TCP `list_datasets(base: &str)` with `list_datasets(socket: &Path) -> Result<Vec<DatasetSummary>, ClientError>`.
  - Add `create_snapshot(socket: &Path, dataset: &str, req: &CreateSnapshotRequest) -> Result<DatasetSummary, ClientError>`. URL-encodes `dataset` (`/` → `%2F`); JSON-encodes `req`; expects `201`.
  - Private helper `async fn request_uds(socket: &Path, method: hyper::Method, path: &str, body: Option<Vec<u8>>) -> Result<(StatusCode, Vec<u8>), ClientError>`. Opens `tokio::net::UnixStream::connect(socket)`, wraps in `hyper_util::rt::TokioIo`, runs `hyper::client::conn::http1::handshake`, spawns the connection driver, sends the request, collects the body via `http_body_util::BodyExt::collect`.
  - `ClientError`: keep `Status { code: u16, body: String }` so callers detect 409 by matching on `code == 409`. Add `Io(std::io::Error)`, `Hyper(hyper::Error)`, `Http(http::Error)` variants as needed.
  - URL encoding: hand-encode `/` → `%2F` (no extra dep needed for the one character). The `Host: _` header satisfies hyper's HTTP/1.1 expectation.

**Verify**:
```
cargo check -p arctern-client
cargo clippy -p arctern-client -- -D warnings
```

**Commit**: `feat(client): UDS transport + create_snapshot + list_datasets(socket) (T005)`

## T006 — `daemon/tests/common`: extend `spawn_daemon` for UDS

**Changes**:
- `daemon/tests/common/mod.rs`:
  - Change `pub fn spawn_daemon() -> (Child, String)` to `pub fn spawn_daemon_uds() -> (Child, PathBuf)` (keeping the old name aliased only if both tests still need it during the transition — they don't; T007 migrates the old test).
  - The new helper picks a unique socket path `/tmp/arctern_test_<nanos>_<seq>.sock`, passes it via `--socket`, parses `LISTEN unix:<path>` from stdout, returns `(child, socket_path)`.
  - The 10s spawn deadline + zombie-process guard logic stays the same.

**Verify**:
```
cargo check -p arctern-daemon --features integration --tests
```

**Commit**: `test(daemon): extend spawn_daemon helper for UDS (T006)`

## T007 — Migrate slice-001 datasets test to UDS + add snapshot test

**Changes**:
- `daemon/tests/integration_datasets_endpoint.rs`: switch from `reqwest::get(format!("{base}/..."))` to `arctern_client::list_datasets(&socket_path)` and `serde_json` parsing of `/api-docs/openapi.json` via the same UDS helper (or a small inline UDS GET — keep arctern-client focused on the typed APIs).
- `daemon/tests/integration_snapshots_endpoint.rs` (new):
  - `#![cfg(feature = "integration")]`. `mod common;`.
  - Boot a `LoopbackPool`. Spawn the daemon via `spawn_daemon_uds`. Call `arctern_client::create_snapshot(&socket, pool.name(), &CreateSnapshotRequest { snapshot_name: "s1", ..Default::default() })`. Assert returned `DatasetSummary.name == format!("{}@s1", pool.name())` and `dataset_type == "snapshot"`.
  - Repeat the same call; assert `Err(ClientError::Status { code: 409, .. })`.
  - Call `list_datasets(&socket)`; assert the snapshot appears in the result.
  - Tear down: `child.kill(); child.wait(); pool.destroy().await`.

**Verify**:
```
just vm-up
just test-integration
```

**Commit**: `test(daemon): integration test for snapshot endpoint over UDS; migrate datasets test (T007)`

## T008 — Final verification (constitution-IV grep + full test suite)

**Changes**: None (verification-only).

**Verify**:
```
# Principle IV: ZFS through palimpsest only
! grep -RnE 'tokio::process::Command|^use regex' --include='*.rs' crates/ daemon/src/

cargo check --workspace
cargo clippy --workspace --all-targets --features integration -- -D warnings
cargo test --workspace                                  # unit tests
just vm-up
just test-integration                                   # integration tests (slice 001 + 002)
just vm-down
```

If anything fails, fix and re-run; do not commit a broken state.

**Commit**: (no commit needed; verification only)

## Dependency graph

```
T001 (api type) ─┐
                 ├─> T004 (handler) ──> T007 (integration tests)
T002 (UDS bind) ─┤                       ▲
                 │                       │
T003 (peer auth) ┘                       │
T005 (client UDS) ───────────────────────┘
T006 (test helper) ──────────────────────┘
T008 (verify)
```

T001/T002 can land in either order; T003 depends on T002 (needs the UDS listener); T004 depends on T001 + T003; T005 is independent of the daemon-side tasks; T006 depends on T002; T007 depends on T004 + T005 + T006; T008 depends on everything.

## Done when

All of: `cargo test --workspace` green, `cargo clippy --workspace --all-targets --features integration -- -D warnings` clean, `just test-integration` exits 0, all 7 commits land on the slice branch, the constitution-IV grep returns no matches.
