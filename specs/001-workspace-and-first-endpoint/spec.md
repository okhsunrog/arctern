# Feature Specification: Workspace migration + GET /api/v1/datasets

**Feature Branch**: `001-workspace-and-first-endpoint`
**Created**: 2026-05-09
**Status**: Draft
**Input**: First arctern slice that converts the single-bin stub into the workspace shape described in `CLAUDE.md`, exposes one read-only HTTP endpoint backed by `palimpsest::dataset::list`, and proves the pipeline end-to-end with an integration test against a real-ZFS VM.

## Why this slice

`CLAUDE.md` and the constitution describe a Cargo workspace (`crates/{api,client}` + `daemon/` + `admin-ui/`) with a single axum router serving both HTTP/2 (browser) and HTTP/3 (daemon-to-daemon), all administration through a web UI, all ZFS access through `palimpsest`. The current state is an 8-line stub.

Going straight to the full feature surface (job model, QUIC, SSE, web UI) would land thousands of lines before anything is exercised end-to-end. This slice is the smallest vertical that proves the load-bearing structural decisions:

- arctern compiles as the documented workspace.
- `palimpsest = { path = "../palimpsest" }` integrates cleanly.
- The axum router + utoipa OpenAPI emission work for one handler.
- An integration test can boot the daemon, call the endpoint against real ZFS in a VM, and assert correctness — using the `SshCommandRunner`/loopback-pool harness palimpsest just shipped.

Everything else (UNIX socket binding, auth, SSE, mutating endpoints, QUIC, admin-ui, replication) lands in subsequent slices on top of this foundation.

## User Scenarios & Testing *(mandatory)*

The "users" of this slice are downstream consumers (the future browser SPA, future daemon-to-daemon RPC) and arctern's own developers. Stories are consumption scenarios.

### User Story 1 — A working build of the documented workspace (Priority: P1)

A developer who clones arctern can run `cargo build --workspace` and `cargo test --workspace` and have all member crates compile against the local `palimpsest` checkout.

**Why this priority**: Without the workspace shape working, no further slice can land. This is the foundational scaffolding. Every slice past this one assumes `cargo build --workspace` succeeds.

**Independent Test**: `cd ~/code/arctern && cargo check --workspace && cargo clippy --workspace -- -D warnings && cargo test --workspace`. All commands exit 0.

**Acceptance Scenarios**:

1. **Given** arctern's `Cargo.toml` is a workspace root and `crates/api/`, `crates/client/`, `daemon/`, and `admin-ui/` exist as members (`admin-ui` may be empty placeholder), **When** `cargo check --workspace` runs, **Then** all members compile.
2. **Given** the daemon depends on `palimpsest` via `path = "../palimpsest"`, **When** `cargo build -p arctern-daemon` runs, **Then** the binary is produced without surprises (single resolved `palimpsest` version in the lockfile).
3. **Given** the workspace, **When** `cargo clippy --workspace -- -D warnings` runs, **Then** there are no warnings.

### User Story 2 — One read-only endpoint backed by palimpsest (Priority: P1)

A consumer can issue `GET /api/v1/datasets` against the running daemon and receive a JSON list of ZFS datasets visible to the daemon's `palimpsest::CommandRunner`. The OpenAPI spec for this endpoint is discoverable.

**Why this priority**: Validates the whole stack — axum scaffold, handler structure, palimpsest integration, JSON serialization shape, OpenAPI generation. If this works, every additional read endpoint is a copy-paste away.

**Independent Test**: Boot the daemon pointing at a VM with one or more datasets. `curl -s http://127.0.0.1:<port>/api/v1/datasets | jq .` returns a JSON array containing at least the test pool. `curl -s http://127.0.0.1:<port>/api-docs/openapi.json | jq '.paths."/api/v1/datasets"'` returns a non-null path entry.

**Acceptance Scenarios**:

1. **Given** the daemon is running and a `palimpsest_test_*` pool exists in the daemon's runner target, **When** the consumer calls `GET /api/v1/datasets`, **Then** the JSON response is a `200 OK` array containing at least one entry whose `name` matches the test pool.
2. **Given** the daemon, **When** `GET /api-docs/openapi.json` is called, **Then** the response contains a `paths."/api/v1/datasets"` entry with a `200` response schema referencing `DatasetSummary`.
3. **Given** the daemon's underlying `zfs list` errors (e.g., target unreachable), **When** `GET /api/v1/datasets` is called, **Then** the response is `500 Internal Server Error` with a JSON body containing an error category derived from `palimpsest::ZfsError` (not the raw stderr).

### User Story 3 — End-to-end integration test against real ZFS (Priority: P1)

The repo includes an integration test that boots the daemon as a subprocess, points it at a VM-hosted ZFS pool via `PALIMPSEST_SSH_TARGET`, calls the endpoint over HTTP, and asserts on the JSON response.

**Why this priority**: Proves the runner→daemon→handler→palimpsest→VM pipeline works without manual smoke-testing. Becomes the template every future endpoint slice copies.

**Independent Test**: `just vm-up && PALIMPSEST_SSH_TARGET=root@localhost:2226 PALIMPSEST_SSH_PASSWORD="" cargo test -p arctern-daemon --features integration -- --test-threads=1 && just vm-down` exits 0.

**Acceptance Scenarios**:

1. **Given** the palimpsest VM is running on port 2226 with a loopback test pool, **When** the integration test runs, **Then** it spawns the daemon, reads the bound port from stdout, calls the endpoint, asserts the test pool appears in the response, and shuts the daemon down cleanly.
2. **Given** an arctern `justfile` mirroring palimpsest's vm management, **When** a developer runs `just test-integration` (after `just vm-up`), **Then** the integration test runs against the shared VM.

### User Story 4 — Three CLI subcommands present as stubs (Priority: P3)

The daemon binary exposes the three subcommands documented in `CLAUDE.md` ("Out-of-scope CLI"): `arctern daemon`, `arctern stdinserver <ident>`, `arctern configcheck <path>`. Only `daemon` does real work this slice; the other two are placeholders that exit cleanly with a "not implemented" message.

**Why this priority**: Locks in the CLI surface so future slices can flesh out the placeholders without breaking the binary's invocation contract.

**Independent Test**: `arctern --help` lists the three subcommands. `arctern stdinserver foo` and `arctern configcheck /etc/hostname` exit 0 with a recognizable "not implemented" output.

**Acceptance Scenarios**:

1. **Given** the daemon binary, **When** `arctern --help` is invoked, **Then** the help text lists `daemon`, `stdinserver`, `configcheck`.
2. **Given** the binary, **When** `arctern stdinserver test-ident` is invoked, **Then** it exits 0 with stderr containing "not implemented in slice 001".

### Edge Cases

- **Empty dataset list**: VM has no pools. Endpoint returns `[]` with `200 OK`. Integration test handles this (the test creates its own pool first).
- **palimpsest returns ZfsError::Spawn**: SSH unreachable or `zfs` binary missing. Daemon returns `503 Service Unavailable` (distinct from generic `500`) with a category tag.
- **Daemon binding fails**: port 0 should always succeed; if the OS denies, `arctern daemon` exits non-zero with a clear stderr.
- **Multiple concurrent requests during a `zfs list`**: each request constructs its own `SshCommandRunner` (cheap — just a wrapper over an SSH subprocess). No shared mutable state in the handler.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The repository root `Cargo.toml` MUST be a Cargo workspace with members `crates/api`, `crates/client`, `daemon`. The `admin-ui/` directory MUST exist (empty placeholder, `.gitkeep`-only).
- **FR-002**: `crates/api` MUST export a `DatasetSummary` type with `serde::{Serialize, Deserialize}` and `utoipa::ToSchema`, projecting `palimpsest::ZfsListEntry` to `{ name: String, dataset_type: String, properties: BTreeMap<String, String> }` (string-mapped property values for OpenAPI compatibility).
- **FR-003**: `crates/client` MUST export a thin async client (`reqwest`) with `pub async fn list_datasets(base: &str) -> Result<Vec<DatasetSummary>, _>` calling `GET <base>/api/v1/datasets`.
- **FR-004**: `daemon` MUST be the binary crate. Library + bin pattern is acceptable but not required.
- **FR-005**: `daemon` MUST depend on `palimpsest = { path = "../palimpsest" }`.
- **FR-006**: The daemon MUST expose three CLI subcommands via `clap`: `daemon`, `stdinserver <ident>`, `configcheck <path>`. Only `daemon` is fully implemented this slice; the other two print a "not implemented in slice 001" message and exit 0.
- **FR-007**: `arctern daemon` MUST bind axum on `127.0.0.1:0` (random port). After binding, it MUST print exactly one line to stdout in the form `LISTEN 127.0.0.1:<port>` (newline-terminated) so an integration test can parse it.
- **FR-008**: The axum router MUST serve `GET /api/v1/datasets` returning `Vec<DatasetSummary>` as `200 OK` JSON. Implemented by calling `palimpsest::dataset::list` with default `ListOptions` against an `SshCommandRunner` constructed from `PALIMPSEST_SSH_TARGET` (and `PALIMPSEST_SSH_PASSWORD` if set).
- **FR-009**: The router MUST serve `GET /api-docs/openapi.json` returning the utoipa-generated OpenAPI 3 document. The document MUST include `DatasetSummary` as a component schema and `/api/v1/datasets` in the paths.
- **FR-010**: Errors from `palimpsest::dataset::list` MUST be mapped to HTTP responses via a thin `IntoResponse` wrapper. `ZfsError::Spawn(_)` → `503`. All other variants → `500`. Body is `{ "error": "<category>", "message": "<short>" }` JSON.
- **FR-011**: There MUST be exactly one integration test under `daemon/tests/` that: spawns the daemon as a subprocess, reads the `LISTEN ...` line from stdout, issues `GET /api/v1/datasets` via `reqwest`, asserts the response contains the test pool, then signals the daemon to shut down.
- **FR-012**: A `justfile` MUST exist in arctern with at minimum `vm-up` / `vm-down` / `vm-ssh` / `test-integration` / `test-vm` recipes. They MAY share semantics with palimpsest's justfile (same VM port 2226) — the VM is a shared dev resource, not a per-project one.

### Non-Functional Requirements

- **NFR-001**: Total slice size: ~600-900 LoC of Rust + ~50 lines of justfile + spec-kit artifacts. If significantly over, the slice has crept.
- **NFR-002**: First `cargo check --workspace` may take 60-120 s due to fresh axum/utoipa download. Subsequent builds < 10 s. (Informative; not a build-system requirement.)
- **NFR-003**: No `tokio::process::Command` calls inside arctern source. All ZFS access through palimpsest. (Constitution principle IV.)
- **NFR-004**: No code under `daemon/src/` matches stderr against any regex. Error classification stays inside palimpsest. (Constitution principle IV.)

### Key Entities

- **DatasetSummary** (in `crates/api`): serde + utoipa wire type. Projection of `palimpsest::ZfsListEntry`. Decoupled from palimpsest's internal model so palimpsest can refactor freely without breaking the API.
- **AppState** (in `daemon`): holds the `SshCommandRunner` factory (just the env-var lookup) and any future shared state. For this slice, effectively empty.
- **ApiError** (in `daemon`): newtype around `palimpsest::ZfsError` implementing `axum::response::IntoResponse`. Confines error mapping to one place.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `cargo check --workspace`, `cargo clippy --workspace -- -D warnings`, and `cargo test --workspace` all exit 0 on the resulting branch.
- **SC-002**: With `just vm-up` running, `cargo test -p arctern-daemon --features integration` exits 0 and the test produces no flakes across 5 consecutive runs.
- **SC-003**: `arctern --help` lists `daemon`, `stdinserver`, `configcheck`. The latter two exit 0 with a "not implemented in slice 001" message.
- **SC-004**: `curl http://127.0.0.1:<port>/api/v1/datasets` returns valid JSON conforming to the `DatasetSummary` schema in the OpenAPI doc.
- **SC-005**: Total slice size is approximately 600-900 LoC of arctern source + tests, plus the spec-kit `spec/plan/tasks.md` artifacts.

## Assumptions

- palimpsest is reachable at `../palimpsest` from arctern's repo root and is at the version with `SshCommandRunner` + `dataset::list` already available (Slice A + Stage 1 finishing).
- The development host has `qemu-system-x86_64`, `edk2-ovmf`, `sshpass`, and the archzfs test ISO already built (per palimpsest's CLAUDE.md). Integration tests reuse palimpsest's VM lifecycle on port 2226.
- `axum` 0.8 + `utoipa-axum` 0.x cohabit cleanly. (Verify via `cargo add` resolution at implementation time; if not, pin axum to whatever utoipa-axum's current major release supports.)
- No dependency on a Nix dev shell — arctern can be built with a vanilla cargo on the host. (NixOS test framework is deferred per the project's testing discussion.)

## Open Questions

1. **Daemon shutdown signal in integration test**: SIGTERM to the spawned process, or an in-band `POST /api/v1/shutdown` endpoint? SIGTERM is simpler and standard; in-band is more testable. Default to SIGTERM (`tokio::process::Child::kill`); revisit if it's flaky.
2. **`AppState` design for future cancellation**: arctern will eventually need a `CancellationToken` for jobs. This slice has no jobs, so AppState is effectively empty. Wait until slice 003 (first mutating endpoint) to introduce it.
3. **Whether `crates/api` should re-export `palimpsest::ZfsError`'s variants in its own error type or define an HTTP-shaped wire error**: defer to slice 002 when the second endpoint exists and the pattern shape is clearer. For this slice, errors stay daemon-internal.
4. **utoipa-axum version**: 0.x track moves fast. Pin loosely (`utoipa-axum = "0"`) and let `cargo add` choose. Lock in via `Cargo.lock`.

## Out of scope (deferred to later slices)

- UNIX socket binding + loopback TCP per constitution principle V (slice 002).
- Session-cookie auth (slice 002).
- SSE log tail (slice 003+).
- Mutating endpoints (snapshot/destroy/send/recv) — slice 003+.
- Daemon-to-daemon QUIC transport (slice 004+).
- Vue 3 admin-ui (slice 005+).
- Replication cursor logic (lives in arctern's daemon, not palimpsest; lands when first replication slice does).
- NixOS VM test framework (deferred; this slice rides on palimpsest's SSH+QEMU harness).
