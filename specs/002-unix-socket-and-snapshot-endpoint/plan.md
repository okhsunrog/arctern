# Implementation Plan: UNIX socket + POST /datasets/{name}/snapshots

**Branch**: `002-unix-socket-and-snapshot-endpoint` | **Date**: 2026-05-09 | **Spec**: [spec.md](./spec.md)
**Input**: `specs/002-unix-socket-and-snapshot-endpoint/spec.md`

## Summary

Switch the daemon's transport from TCP loopback (slice 001) to a UNIX domain socket and add the first mutating endpoint, `POST /api/v1/datasets/{name}/snapshots`. Authorization is peer-uid via `SO_PEERCRED` enforced by an axum tower layer (default policy: peer uid must equal daemon uid). `arctern-client` gains a tiny hand-rolled hyper-over-`tokio::net::UnixStream` transport and exposes `create_snapshot(socket, dataset, req)` plus a UDS-flavoured `list_datasets(socket)`. Exactly one new integration test exercises the full pipeline against the existing palimpsest VM harness; the slice-001 integration test is migrated to UDS so the repo holds no TCP-binding tests.

## Technical Context

**Language/Version**: Rust 1.95, edition 2024.
**Primary Dependencies**: existing `axum` 0.8, `utoipa`, `utoipa-axum`, `clap`, `tokio`, `tracing`, `serde`, `palimpsest = { path = "../../palimpsest" }` (slice 001). New: `hyper` (low-level client), `hyper-util` (`TokioIo` + tokio runtime glue), `http-body-util` (collecting response bodies), `tower` (for the axum layer/middleware shape).
**Storage**: None this slice (ZFS metadata + ephemeral socket inode).
**Testing**: `cargo test --workspace` for unit tests; `cargo test -p arctern-daemon --features integration -- --test-threads=1` for VM-driven tests via the slice-001 SSH harness.
**Target Platform**: Linux x86_64 (Linux-only because of `SO_PEERCRED` semantics; non-Linux deferred per spec).
**Project Type**: Cargo workspace (slice 001's shape — `crates/api`, `crates/client`, `daemon` — unchanged).
**Performance Goals**: Not applicable. The endpoint is a thin pass-through; latency dominated by `zfs snapshot` execution time.
**Constraints**: Constitution principles I-V apply (see Constitution Check). Async-only. No `tokio::process::Command`/regex in arctern source.
**Scale/Scope**: ~400-700 LoC of arctern source + tests.

## Constitution Check

*GATE: passes before implementation.*

| Principle | Compliance |
|---|---|
| I. QUIC With HTTP Semantics | Not applicable this slice (no daemon-to-daemon RPC yet). |
| II. One API for Browser and Daemons | `CreateSnapshotRequest` lives in `crates/api` with `serde + utoipa::ToSchema`. Both the daemon's handler and `arctern-client::create_snapshot` consume it. The future TS client will pick it up from the OpenAPI doc. |
| III. Web UI Replaces the CLI | No new CLI subcommands. The added `--socket` flag is on the existing `daemon` subcommand only. |
| IV. ZFS Through palimpsest | New handler calls `palimpsest::dataset::snapshot` and `palimpsest::dataset::list`. No `tokio::process::Command` or stderr regex in arctern. End-of-slice grep gates this in CI: `! grep -RnE 'tokio::process::Command\|^use regex' --include='*.rs' crates/ daemon/src/`. |
| V. Local-Only by Default, Auth Opt-In | UNIX socket only this slice. Peer-uid auth via `SO_PEERCRED` is the access-control mechanism (kernel-enforced; no tokens). Loopback-TCP-with-auth and remote auth land in a later slice. |
| VI. Live Data Over SSE | Not applicable this slice. |
| VII. ZFS Metadata Compatibility | Not applicable this slice (no replication). |

All applicable principles pass. Deferred work for I and VI tracked in spec's Non-Goals.

## Project Structure

### Documentation (this feature)

```text
specs/002-unix-socket-and-snapshot-endpoint/
├── spec.md     # done
├── plan.md     # this file
├── tasks.md    # next, via speckit-tasks
└── checklists/
    └── requirements.md
```

### Source code (repository root)

```text
arctern/
├── crates/
│   ├── api/src/lib.rs          # add CreateSnapshotRequest (serde + utoipa::ToSchema)
│   └── client/
│       ├── Cargo.toml          # +hyper, +hyper-util, +http-body-util, +http
│       └── src/lib.rs          # UDS transport + list_datasets(path) + create_snapshot(path, ds, req)
├── daemon/
│   ├── Cargo.toml              # +tower (axum re-exports it but explicit dep is cleaner for the layer)
│   ├── src/
│   │   ├── main.rs             # add --socket flag; bind UnixListener; print LISTEN unix:<path>
│   │   ├── router.rs           # register POST snapshot route; wrap router with PeerAuth layer
│   │   ├── auth.rs             # NEW: PeerCredentials extractor, PeerAuth tower layer
│   │   ├── handlers/
│   │   │   ├── mod.rs          # add `pub mod snapshots;`
│   │   │   ├── datasets.rs     # unchanged
│   │   │   └── snapshots.rs    # NEW: POST /api/v1/datasets/{name}/snapshots handler
│   │   └── error.rs            # unchanged (mappings already cover SnapshotExists -> 409)
│   └── tests/
│       ├── common/mod.rs       # extend spawn_daemon to take Option<&Path>; parse `LISTEN unix:<path>`
│       ├── integration_datasets_endpoint.rs   # migrate to UDS (use new client list_datasets)
│       └── integration_snapshots_endpoint.rs  # NEW
└── specs/002-...               # spec-kit artifacts
```

**Structure Decision**: Slice 001's workspace layout is unchanged. New code is one new module per concept (`auth.rs`, `handlers/snapshots.rs`), one new integration test, one extension to the test helper, and one new request DTO in `crates/api`.

## Phase 0: Research

Resolved decisions (from the slice ticket D1-D6, plus library-API spot-checks done at planning time):

- **Axum 0.8 ships first-class `tokio::net::UnixListener` `Listener` impl** (`axum-0.8.9/src/serve/listener.rs:46`). `axum::serve(unix_listener, router)` Just Works.
- **Per-connection peer credentials via the `Connected` trait**: implement `Connected<IncomingStream<'_, UnixListener>> for PeerCredentials`, call `stream.io().peer_cred()` (a `tokio::net::unix::UCred`), capture the uid. Wire in via `router.into_make_service_with_connect_info::<PeerCredentials>()`. The handler/middleware reads it via `Extension<PeerCredentials>` or `ConnectInfo<PeerCredentials>`.
- **PeerAuth layer**: tower-style `from_fn` middleware that reads the per-request `ConnectInfo<PeerCredentials>`, compares to the daemon's `getuid()`, returns 403 if mismatched. Implementation is ~20 lines.
- **`reqwest` 0.13 has no first-class UDS transport.** Hand-roll using `hyper::client::conn::http1::handshake` over `hyper_util::rt::TokioIo<tokio::net::UnixStream>`. One connection per request is fine for slice 002's test load (no pooling needed; the test issues a few requests and exits). ~30 LoC.
- **Body collection**: use `http_body_util::BodyExt::collect()` then `.to_bytes()`. Standard hyper 1.x pattern.
- **Response shape for snapshot creation**: after `palimpsest::dataset::snapshot` succeeds, call `palimpsest::dataset::list` with `roots = vec!["<dataset>@<tag>".into()]` and `types = vec![DatasetType::Snapshot]` to materialize the `DatasetSummary`. One extra `zfs list -j` call per create — acceptable; future optimization can return a synthesized summary if profiling demands.
- **Socket cleanup on stale path**: `std::fs::remove_file` returning `ErrorKind::NotFound` is ignored; any other error is fatal. After bind, `std::os::unix::fs::PermissionsExt::set_permissions(path, 0o600)`.
- **`SIGTERM` cleanup** (FR-018): use `tokio::signal::unix::{signal, SignalKind}` to await SIGINT/SIGTERM concurrently with `axum::serve`. On signal, drop the listener and `remove_file` the socket. Best-effort.

## Phase 1: Design artifacts

### Data model additions

`crates/api`:

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct CreateSnapshotRequest {
    pub snapshot_name: String,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}
```

Re-exported alongside `DatasetSummary` and `ApiErrorBody`.

### API contract additions

`POST /api/v1/datasets/{name}/snapshots`

- Path parameter `name`: dataset name (URL-encoded `/` as `%2F`).
- Request body: `CreateSnapshotRequest`.
- Responses:
  - `201 Created` body `DatasetSummary` describing the new snapshot.
  - `400 Bad Request` for malformed JSON / missing `snapshot_name` (axum's default rejection).
  - `403 Forbidden` for peer-uid mismatch OR `ZfsError::PermissionDenied`.
  - `404 Not Found` for `ZfsError::DatasetNotFound`.
  - `409 Conflict` for `ZfsError::SnapshotExists` (D3 idempotency policy: NOT 200/201).
  - `500 Internal Server Error` / `503 Service Unavailable` per slice-001 mappings.

Also: every existing route gains the implicit 403-for-peer-mismatch precondition via the PeerAuth layer.

OpenAPI doc registers `CreateSnapshotRequest` as a component schema and the new path under `paths`.

### Quickstart (developer)

```bash
cd ~/code/palimpsest && just vm-up         # boots VM on port 2226 (shared)
cd ~/code/arctern
SOCK=$(mktemp -u /tmp/arctern.XXXXXX.sock)
cargo run -p arctern-daemon -- daemon --socket "$SOCK" &
# wait until: "LISTEN unix:$SOCK" appears on stdout
curl --unix-socket "$SOCK" http://_/api/v1/datasets | jq .
curl --unix-socket "$SOCK" -X POST -H 'Content-Type: application/json' \
  -d '{"snapshot_name":"manual-1"}' \
  "http://_/api/v1/datasets/$(printf %s 'tank/data' | jq -sRr @uri)/snapshots" | jq .
kill %1
cd ~/code/palimpsest && just vm-down
```

CI-style:

```bash
cd ~/code/arctern
just test-vm     # vm-up + integration tests (slice-001 + slice-002) + vm-down
```

## Phase 2: Tasks

Generated by `speckit-tasks` into `specs/002-*/tasks.md`. Expected ordering:

1. `crates/api`: add `CreateSnapshotRequest`.
2. `daemon`: add `--socket` flag + `UnixListener` bind + `LISTEN unix:` handshake (slice-001 TCP path goes away).
3. `daemon`: PeerCredentials `Connected` impl + PeerAuth tower layer wired into the router.
4. `daemon`: `POST /api/v1/datasets/{name}/snapshots` handler + utoipa registration.
5. `crates/client`: hand-rolled UDS transport + `list_datasets(path)` + `create_snapshot(path, ds, req)`. Slice-001 TCP `list_datasets` is replaced.
6. `daemon/tests/common`: extend `spawn_daemon` to accept a socket path; parse `LISTEN unix:<path>`.
7. `daemon/tests`: migrate slice-001 datasets test to UDS; add `integration_snapshots_endpoint.rs`.
8. End-of-slice verification (constitution-IV grep + full test suite).

## Risks

- **`UCred::uid()` returns `Option<u32>` historically**: tokio currently returns `u32` directly, but the API has shifted across versions. Plan for both shapes; pin tokio (already pinned via lockfile) and write the layer against whatever the current API yields.
- **`Connected` impl boilerplate**: `IncomingStream` borrows from the listener; the trait impl needs care with lifetimes. If it fights, fall back to a custom `Listener` wrapper that pre-reads `peer_cred()` and stashes it on a wrapping stream type with its own `Connected`.
- **Hand-rolled hyper UDS client growing past ~60 LoC**: pivot to `hyperlocal` (added via `cargo add`) without re-architecting; the public API in `arctern-client` does not change.
- **Integration test flake from socket path collisions**: `/tmp/arctern_test_<nanos>_<pid>.sock` is collision-resistant. The daemon unlinks any pre-existing entry per FR-002 anyway.
- **`SIGTERM` cleanup vs. `Child::kill` in tests**: tests use `Child::kill` (SIGKILL) which won't run the cleanup branch; the next process startup unlinks via FR-002. Acceptable.

## Decisions made beyond the slice ticket's D1-D6

- **D7** (added at planning): use `Router::into_make_service_with_connect_info::<PeerCredentials>()` rather than a custom service factory, to ride axum's built-in plumbing. If it proves insufficient, fall back to a hand-written `MakeService`.
- **D8** (added at planning): the snapshot handler issues a follow-up `zfs list -j` to materialize the response `DatasetSummary` rather than synthesizing one from the request. One extra subprocess invocation per create; trades latency for correctness (the snapshot's properties as ZFS sees them).
- **D9** (added at planning): tokio `signal::unix` for SIGTERM/SIGINT-driven socket cleanup. Best-effort; not a hard requirement (FR-018 is SHOULD).
- **D10** (added at planning): no client-side connection pooling. Each `arctern-client` call opens a new `UnixStream` and `http1::handshake`. Sufficient for slice 002's test load and simpler code; revisit if a future high-rate consumer needs pooling.

## Verification

```bash
# Inside arctern repo
cargo check --workspace
cargo clippy --workspace --all-targets --features integration -- -D warnings
cargo test --workspace                        # unit tests (incl. crates/api round-trips)

# Constitution principle IV gate
! grep -RnE 'tokio::process::Command|^use regex' --include='*.rs' crates/ daemon/src/

# Integration (requires VM)
just vm-up
just test-integration
just vm-down
```

End-of-slice manual smoke:

```bash
just vm-up
SOCK=$(mktemp -u /tmp/arctern.XXXXXX.sock)
PALIMPSEST_SSH_TARGET=root@localhost:2226 PALIMPSEST_SSH_PASSWORD="" \
  cargo run -p arctern-daemon -- daemon --socket "$SOCK" 2>/tmp/d.log &
sleep 1
# create a snapshot
curl --unix-socket "$SOCK" -X POST -H 'Content-Type: application/json' \
  -d '{"snapshot_name":"smoke"}' \
  "http://_/api/v1/datasets/$(printf %s 'tank' | jq -sRr @uri)/snapshots" | jq .
# repeat -> 409
curl --unix-socket "$SOCK" -X POST -H 'Content-Type: application/json' \
  -d '{"snapshot_name":"smoke"}' \
  "http://_/api/v1/datasets/$(printf %s 'tank' | jq -sRr @uri)/snapshots" -w '%{http_code}\n'
kill %1
just vm-down
```
