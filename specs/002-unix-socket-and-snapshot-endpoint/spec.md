# Feature Specification: UNIX socket + POST /datasets/{name}/snapshots

**Feature Branch**: `002-unix-socket-and-snapshot-endpoint`
**Created**: 2026-05-09
**Status**: Draft
**Input**: Slice 002 of arctern. Replace slice-001's TCP loopback bind with a UNIX socket. Add the first mutating endpoint: `POST /api/v1/datasets/{name}/snapshots`. Authorization is peer-uid via `SO_PEERCRED`. Extend `arctern-client` to speak HTTP-over-UDS and call the new endpoint. End-to-end integration test runs the daemon over a per-test socket.

## Why this slice

Slice 001 left the daemon listening on TCP loopback with no auth — a safety hole that constitution principle V (Local-Only by Default, Auth Opt-In) explicitly disallows for any non-local-only deployment, and a poor default even for a single-host config. This slice fixes that by switching to a UNIX socket whose access is governed by file-system permissions plus peer-uid checking, and proves the model end-to-end by adding the smallest meaningful mutating endpoint: snapshot creation.

The mutating endpoint also locks in patterns that every later mutator will copy — request DTO, idempotent error mapping (`SnapshotExists` → 409), response shape (return the created resource as a `DatasetSummary`), and a client method consumable by both the upcoming admin-ui (via the generated TS client) and daemon-to-daemon code.

TCP binding, bearer tokens, and mTLS are deliberately deferred to a later slice (see Non-Goals).

## User Scenarios & Testing *(mandatory)*

The "users" of this slice are the future admin-ui (over local UDS once the binary is bundled), automation scripts running as the same user as the daemon (`arctern-cli`-style consumers built on `arctern-client`), and arctern's own developers driving integration tests.

### User Story 1 — Daemon binds a UNIX socket only the daemon's user can use (Priority: P1)

An operator launches `arctern daemon` on their machine. The daemon creates a UNIX socket at a predictable path under their user's runtime directory. Other local users cannot reach the API by default; the daemon's own user can.

**Why this priority**: Without this, the daemon is either bound to TCP loopback with no auth (constitution-V violation) or non-functional. Every other story this slice depends on the socket existing.

**Independent Test**: Start the daemon. Confirm the socket exists at the expected path. Confirm a process running as the daemon's uid can connect; confirm a process running as a different (non-root) uid cannot.

**Acceptance Scenarios**:

1. **Given** `$XDG_RUNTIME_DIR` is set, **When** `arctern daemon` is invoked with no `--socket` flag, **Then** the daemon creates a socket at `$XDG_RUNTIME_DIR/arctern.sock` and prints exactly one line to stdout: `LISTEN unix:<absolute-path>` (newline-terminated).
2. **Given** `$XDG_RUNTIME_DIR` is unset, **When** `arctern daemon` is invoked with no `--socket` flag, **Then** the daemon falls back to `/run/arctern.sock`.
3. **Given** the operator passes `--socket /tmp/foo.sock`, **When** the daemon starts, **Then** the socket is created at exactly that path and the LISTEN line reflects it.
4. **Given** a stale socket already exists at the chosen path, **When** the daemon starts, **Then** it removes the stale entry and rebinds (idempotent restart).
5. **Given** the daemon is running, **When** a process running as a different uid attempts to call any endpoint, **Then** the daemon rejects the connection with HTTP `403 Forbidden` and a JSON body identifying the failure as a peer-uid mismatch.
6. **Given** the daemon is running, **When** the daemon's own user calls `GET /api/v1/datasets`, **Then** the request is served as in slice 001.

### User Story 2 — Create a snapshot of a dataset via HTTP (Priority: P1)

A consumer (admin-ui, script, or daemon-to-daemon code) submits a request to create a snapshot of a named dataset. On success the consumer receives the snapshot's wire summary so it can render or chain further operations without a follow-up `GET`. If the snapshot already exists, the consumer is told so explicitly (HTTP 409) instead of silently succeeding — letting the caller decide whether that's fatal.

**Why this priority**: First mutating endpoint. Locks in the patterns (request DTO, status-code policy, idempotency mapping, response shape) every later mutator copies. Until this exists, arctern cannot do anything *to* a pool — only read it.

**Independent Test**: With a test pool present, `POST /api/v1/datasets/<test-pool>/snapshots` with a fresh snapshot name returns `201` and a `DatasetSummary` describing the new snapshot. Repeating the same request returns `409 Conflict`.

**Acceptance Scenarios**:

1. **Given** dataset `tank/data` exists and snapshot `manual-2026-05-09` does not, **When** the consumer issues `POST /api/v1/datasets/tank%2Fdata/snapshots` with `{ "snapshot_name": "manual-2026-05-09" }`, **Then** the response is `201 Created` and the body is a `DatasetSummary` with `name = "tank/data@manual-2026-05-09"` and `dataset_type = "snapshot"`.
2. **Given** the same snapshot already exists, **When** the same request is repeated, **Then** the response is `409 Conflict` with body `{ "error": "snapshot_exists", "message": "..." }`.
3. **Given** the request body has `"recursive": true`, **When** the request is processed, **Then** the underlying ZFS operation passes `-r` and snapshots all descendants atomically; the response body still describes the parent snapshot only (descendants are discoverable via `GET /api/v1/datasets`).
4. **Given** the dataset name in the path does not exist, **When** the request is sent, **Then** the response is `404 Not Found` with `{ "error": "dataset_not_found", ... }`.
5. **Given** the request body is missing `snapshot_name`, **When** the request is sent, **Then** the response is `400 Bad Request` (axum's default JSON-rejection mapping is acceptable).
6. **Given** properties are supplied (`{ "snapshot_name": "s1", "properties": { "user:reason": "manual" } }`), **When** the request is processed, **Then** the underlying ZFS invocation receives `-o user:reason=manual` and the property is set on the new snapshot.
7. **Given** a successful snapshot creation, **When** a subsequent `GET /api/v1/datasets` is issued, **Then** the response includes an entry whose name matches the newly-created snapshot.

### User Story 3 — Programmatic client over UDS (Priority: P1)

A Rust consumer of `arctern-client` can call `create_snapshot(socket_path, dataset, request)` and `list_datasets(socket_path)` and get typed results back, without writing any HTTP, hyper, or socket plumbing themselves.

**Why this priority**: Both the integration test for slice 002 and every future daemon-side consumer (admin-ui-bundled CLI, replication peer initialization) depend on the client speaking UDS. If the client only speaks TCP, every consumer rebuilds the UDS plumbing.

**Independent Test**: Spawn the daemon on a per-test socket; call `arctern_client::create_snapshot(socket, "tank/data", req)` from an integration test; assert the returned `DatasetSummary`.

**Acceptance Scenarios**:

1. **Given** the daemon is listening on `/tmp/arctern_test_<nanos>.sock`, **When** the integration test calls `arctern_client::list_datasets("/tmp/arctern_test_<nanos>.sock")`, **Then** it returns `Vec<DatasetSummary>` deserialized from the daemon's JSON response.
2. **Given** the daemon is listening, **When** the test calls `arctern_client::create_snapshot(socket, "tank/data", CreateSnapshotRequest { snapshot_name: "s1", recursive: false, properties: Default::default() })`, **Then** it returns a `DatasetSummary` for `tank/data@s1`.
3. **Given** the daemon returns `409 Conflict`, **When** the client decodes the response, **Then** it returns `Err(ClientError::Status { code: 409, body })` so the caller can treat it as non-fatal if desired.

### User Story 4 — Integration test boots a daemon over a per-test socket and exercises the new endpoint (Priority: P1)

The repo's integration suite spawns the daemon with `--socket /tmp/arctern_test_<nanos>.sock`, waits for the LISTEN handshake, creates a snapshot via the new endpoint, lists datasets, and tears down the pool and the daemon.

**Why this priority**: Proves the whole new pipeline (socket bind → peer-uid check → handler → palimpsest snapshot → response) works against real ZFS in the VM. Becomes the template for every later mutator's integration test.

**Independent Test**: With `just vm-up` running, `just test-integration` exits 0.

**Acceptance Scenarios**:

1. **Given** the palimpsest VM is up on port 2226 and a fresh loopback test pool exists, **When** the integration test runs, **Then** it spawns the daemon, calls `POST /api/v1/datasets/<pool>/snapshots` via `arctern-client`, asserts the response, then calls `GET /api/v1/datasets` and asserts the snapshot is present.
2. **Given** the integration test reuses the existing slice-001 daemon-spawn helper, **When** it asks for a daemon, **Then** it can request a UDS socket path and the helper parses `LISTEN unix:<path>` (slice-001 parsed `LISTEN <addr>`; the helper extends, not replaces).

### Edge Cases

- **Socket path contains `:`** (legal on Linux): the LISTEN handshake parser MUST split on the first space only, NOT on `:`, since the path may itself contain colons. Use `LISTEN unix:<path>` with everything after `unix:` taken literally.
- **Concurrent identical snapshot requests**: ZFS itself serializes; whichever loses the race gets `EEXIST` → 409. No coordination needed in arctern this slice.
- **Recursive snapshot with mid-tree permission denial**: `palimpsest::ZfsError::PermissionDenied` → 403. Same mapping as slice 001.
- **Dataset name needs URL encoding**: `tank/data` → `tank%2Fdata` in the path. Axum decodes; the handler sees `tank/data`. Document that callers MUST URL-encode `/` in path segments.
- **Daemon UID 0 (root)**: still accepted; the same-uid policy is "peer uid == daemon uid", which is uniformly true for root-owned daemons.
- **Stale socket from a prior crash**: daemon removes the file before binding. If `unlink(2)` fails for a non-`ENOENT` reason, the daemon exits non-zero with a clear stderr.
- **Missing `$XDG_RUNTIME_DIR`** but `/run` not writable (e.g., running as a non-root user with no XDG): daemon exits non-zero with a stderr message asking the operator to pass `--socket <path>`.
- **Snapshot name contains `@`**: rejected by ZFS; surface palimpsest's classified error as 500 (or as 400 if a specific classifier exists). Don't try to be clever in arctern.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The daemon MUST bind a UNIX domain socket and MUST NOT bind any TCP port. The socket path is determined by the following resolution order: `--socket <path>` flag (highest priority); else `$XDG_RUNTIME_DIR/arctern.sock` if `$XDG_RUNTIME_DIR` is set and writable; else `/run/arctern.sock`.
- **FR-002**: Before binding, if a regular file or socket already exists at the chosen path, the daemon MUST attempt to remove it. Failure to remove (other than `ENOENT`) MUST cause the daemon to exit non-zero with a stderr message naming the path and the OS error.
- **FR-003**: After successful bind, the daemon MUST print exactly one line to stdout in the form `LISTEN unix:<absolute-path>` (newline-terminated, line-buffered). This replaces slice-001's `LISTEN <addr>` TCP handshake.
- **FR-004**: The daemon MUST set the socket's filesystem permissions to `0600` (owner read/write only) immediately after bind, before serving any traffic.
- **FR-005**: For every accepted connection, the daemon MUST read the peer's uid via `SO_PEERCRED` and reject the request with HTTP `403 Forbidden` and JSON body `{ "error": "peer_uid_mismatch", "message": "..." }` if the peer uid does not equal the daemon's effective uid. (Default policy: same uid only. Future slices may add an allowlist; not in scope here.)
- **FR-006**: The peer-uid check MUST run as axum middleware (or equivalent layer) so it applies uniformly to every route registered under the router, including `/api-docs/openapi.json`.
- **FR-007**: The router MUST register `POST /api/v1/datasets/{name}/snapshots` returning the new snapshot's `DatasetSummary` on success. The path parameter `{name}` is the dataset name (callers URL-encode `/` as `%2F`).
- **FR-008**: The request body for the new endpoint MUST deserialize as `CreateSnapshotRequest { snapshot_name: String, recursive: bool (default false), properties: BTreeMap<String, String> (default empty) }`. `CreateSnapshotRequest` lives in `arctern-api` with `serde + utoipa::ToSchema`.
- **FR-009**: On successful creation, the response MUST be `201 Created` with body `DatasetSummary` describing `<name>@<snapshot_name>` (the new snapshot itself, with `dataset_type = "snapshot"`). The handler obtains the summary by calling `palimpsest::dataset::list` scoped to the new snapshot.
- **FR-010**: `palimpsest::ZfsError::SnapshotExists` MUST map to HTTP `409 Conflict` with `{ "error": "snapshot_exists", "message": "..." }`. The handler MUST NOT silently treat this as success; the caller decides whether 409 is fatal. All other `ZfsError` mappings from slice 001 (`Spawn` → 503, `DatasetNotFound` → 404, `PermissionDenied` → 403, etc.) carry over unchanged.
- **FR-011**: `arctern-client` MUST expose `pub async fn create_snapshot(socket_path: &Path, dataset: &str, req: &CreateSnapshotRequest) -> Result<DatasetSummary, ClientError>`. `ClientError` MUST surface 409 distinctly enough that callers can detect "already exists" (e.g., `Status { code: 409, body }` is acceptable; matching on `code == 409` is the documented contract).
- **FR-012**: `arctern-client` MUST expose `pub async fn list_datasets(socket_path: &Path) -> Result<Vec<DatasetSummary>, ClientError>`. The slice-001 TCP variant is replaced (not kept alongside) — UDS is the only transport this slice supports.
- **FR-013**: The client's HTTP-over-UDS implementation MUST use a tokio `UnixStream` driven by `hyper`'s low-level client. No dependency on `hyperlocal` if a clean ~30-line in-crate implementation is feasible; otherwise `hyperlocal` is acceptable as long as it is added via `cargo add` (no hand-edited Cargo.toml).
- **FR-014**: There MUST be exactly one new integration test file under `daemon/tests/` (e.g., `integration_snapshots_endpoint.rs`) that: brings up a fresh `LoopbackPool`, spawns the daemon with `--socket /tmp/arctern_test_<nanos>.sock`, calls `create_snapshot` via `arctern-client`, asserts `DatasetSummary.name == "<pool>@<tag>"` and `dataset_type == "snapshot"`, calls `list_datasets`, asserts the snapshot appears, then tears down (kill daemon, destroy pool).
- **FR-015**: The slice-001 integration test (`integration_datasets_endpoint.rs`) MUST be updated to use UDS — either replaced or migrated. After this slice, no integration test in the repo binds TCP.
- **FR-016**: The daemon-spawn test helper (`daemon/tests/common/mod.rs::spawn_daemon`) MUST be extended to accept a socket path argument and parse `LISTEN unix:<path>` lines. The signature change is acceptable (single caller to update from slice 001).
- **FR-017**: The daemon's `Cli` MUST accept an optional `--socket <PATH>` argument on the `daemon` subcommand. `arctern --help` and `arctern daemon --help` MUST document it.
- **FR-018**: On `SIGTERM` or `SIGINT`, the daemon SHOULD attempt to remove its socket file before exiting (best-effort; failure is non-fatal). Slice 001 had no shutdown semantics; integration tests still use `Child::kill` which won't trigger this — that's acceptable, the next process startup unlinks the stale entry per FR-002.

### Non-Functional Requirements

- **NFR-001**: Total slice size: ~400-700 LoC of Rust + spec-kit artifacts. The hand-rolled hyper/UDS client should be ~30-60 LoC; the peer-uid layer ~40-60 LoC; the snapshot handler ~30 LoC; the rest is wiring and tests.
- **NFR-002**: No `tokio::process::Command` calls inside arctern source (constitution principle IV, carried over from slice 001).
- **NFR-003**: No code under `daemon/src/` or `crates/` matches stderr against any regex. Error classification stays in palimpsest (constitution principle IV).
- **NFR-004**: No `anyhow`/`eyre` in `crates/api` or `crates/client`. The daemon binary may continue to use `eyre` at the top level only.
- **NFR-005**: The peer-uid layer MUST NOT block the runtime. `SO_PEERCRED` is a non-blocking getsockopt, but if a future implementation wants to log uid→username, that lookup MUST be `tokio::task::spawn_blocking` or omitted.

### Key Entities

- **CreateSnapshotRequest** (in `crates/api`): wire type for the snapshot endpoint's request body. `{ snapshot_name: String, recursive: bool, properties: BTreeMap<String, String> }`. `serde + utoipa::ToSchema`.
- **PeerCredentials** (in `daemon`): captured per-connection extension carrying the peer uid (and optionally pid/gid for future logging). Populated by the `Connected` impl on `tokio::net::UnixListener`.
- **PeerAuth** layer (in `daemon`): axum tower layer that reads the per-connection `PeerCredentials` and short-circuits with 403 if the uid policy fails. Default policy: same-uid-only. Encapsulating it as a layer (not handler-level checks) means future routes inherit the check by construction.
- **DatasetSummary** (re-used from slice 001, in `crates/api`): unchanged; the snapshot endpoint returns this.
- **ApiErrorBody** (re-used from slice 001, in `crates/api`): unchanged.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `cargo check --workspace`, `cargo clippy --workspace --all-targets --features integration -- -D warnings`, and `cargo test --workspace` all exit 0 on the resulting branch.
- **SC-002**: With `just vm-up` running, `just test-integration` exits 0 across 5 consecutive runs without flake.
- **SC-003**: A snapshot created via `POST /api/v1/datasets/<dataset>/snapshots` is observable in a subsequent `GET /api/v1/datasets` response within the same test run.
- **SC-004**: Repeating a successful snapshot request with the same body returns `409 Conflict` (NOT `200 OK` and NOT `201 Created`).
- **SC-005**: A connection from a different uid (manually verified, not in the automated test suite this slice) is rejected with `403 Forbidden` and the body identifies the failure as a peer-uid mismatch.
- **SC-006**: Constitution-IV grep (`! grep -RnE 'tokio::process::Command|^use regex' --include='*.rs' crates/ daemon/src/`) exits 0.
- **SC-007**: Total slice size lands in the 400-700 LoC range stated in NFR-001 (informational; not a hard gate).

## Assumptions

- The host kernel supports `SO_PEERCRED` on `AF_UNIX` sockets (Linux always does; macOS and BSD use `LOCAL_PEERCRED` / `getpeereid`). arctern targets Linux only this slice; non-Linux is deferred and out of scope.
- `axum` 0.8's `tokio::net::UnixListener` `Listener` impl + `Connected<IncomingStream<UnixListener>>` trait are stable and sufficient to surface peer credentials per-request via a `Request` extension. Verified at planning time; if the surface is insufficient, fall back to a custom `Listener` wrapper that pre-reads `peer_cred()` and stashes it on the connection's request extensions.
- `hyper-util`'s low-level client `Client::builder().build(...)` over a custom `Connect` impl driving `tokio::net::UnixStream` is sufficient for `arctern-client`. If the boilerplate climbs above ~60 LoC, switch to `hyperlocal` (added via `cargo add`).
- The integration VM (port 2226) is up-to-date with palimpsest's expectations from slice 001. No new VM-side requirements this slice.
- The daemon and the integration test process always run as the same uid — a safe assumption since `cargo test` spawns the daemon as a child of the test binary. No need for the test harness to manipulate uids.

## Out of scope (Non-Goals)

These are deliberately deferred and MUST NOT creep into this slice:

- TCP binding (loopback or otherwise). Future slice when daemon-to-daemon QUIC lands.
- Bearer tokens, mTLS, OAuth, or any non-peer-uid auth.
- Configurable peer-uid allowlist (multi-uid policy). Default same-uid-only is sufficient for slice 002.
- Snapshot deletion / destroy endpoint.
- `zfs send` / `zfs recv` endpoints.
- Hold / release / bookmark endpoints.
- Replication job model, cursors, scheduling.
- Vue 3 admin-ui (lands in slice 005+).
- Daemon-side metrics, audit log, or SSE log tail (slice 003+).
- macOS / BSD support (peer-cred APIs differ).
- systemd socket activation (operator passes `--socket` if they want a specific path; activation can be added later without breaking the CLI).
