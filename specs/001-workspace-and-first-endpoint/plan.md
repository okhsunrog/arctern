# Implementation Plan: Workspace migration + GET /api/v1/datasets

**Branch**: `001-workspace-and-first-endpoint` | **Date**: 2026-05-09 | **Spec**: [spec.md](./spec.md)
**Input**: `specs/001-workspace-and-first-endpoint/spec.md`

## Summary

Convert arctern's single-bin stub into the workspace shape from `CLAUDE.md`, expose `GET /api/v1/datasets` backed by `palimpsest::dataset::list`, and add an integration test that runs against the existing palimpsest SSH+QEMU harness on port 2226. Three CLI subcommands documented in the constitution exist as stubs (`daemon` works; `stdinserver`/`configcheck` print "not implemented in slice 001").

## Technical Context

**Language/Version**: Rust 1.95, edition 2024
**Primary Dependencies**: `tokio` (full), `axum` 0.8, `utoipa`, `utoipa-axum`, `clap` (derive), `serde`, `serde_json`, `tracing`, `tracing-subscriber`, `eyre`, `palimpsest = { path = "../palimpsest" }`
**Storage**: None this slice. ZFS metadata is the source of truth.
**Testing**: `cargo test --workspace` for unit tests; `cargo test -p arctern-daemon --features integration` for the VM-driven integration test.
**Target Platform**: Linux x86_64 (host); ZFS commands dispatched via SSH into a QEMU VM running the archzfs test ISO.
**Project Type**: Cargo workspace (multi-crate library + binary).
**Performance Goals**: Not applicable this slice. The endpoint is a thin pass-through; latency dominated by `zfs list -j` execution time.
**Constraints**: All ZFS access via palimpsest (no `tokio::process::Command` in arctern source). All errors via `thiserror`/`palimpsest::ZfsError` (no stderr regex in arctern). Loopback-only binding.
**Scale/Scope**: ~600-900 LoC of arctern source + tests.

## Constitution Check

*GATE: Must pass before implementation begins.*

| Principle | Compliance |
|---|---|
| I. QUIC With HTTP Semantics | Not applicable this slice (no daemon-to-daemon RPC yet). Architecture is set up to allow it later (single axum router). |
| II. One API for Browser and Daemons | `crates/api` defines `DatasetSummary` with `serde + utoipa::ToSchema`. Daemon uses it in handlers; future Rust clients consume it via `crates/client`; future TS client generates from `/api-docs/openapi.json`. ✅ |
| III. Web UI Replaces the CLI | Three CLI subcommands present as stubs per the documented "out-of-scope CLI" set. No additional CLI surface. ✅ |
| IV. ZFS Through palimpsest | Daemon depends on palimpsest; no `tokio::process::Command` in arctern source; no stderr regex in arctern. Verified by an end-of-implementation grep. ✅ |
| V. Local-Only by Default, Auth Opt-In | Daemon binds `127.0.0.1:0` (loopback only). No auth this slice — UNIX socket + auth land in slice 002 per spec's "out of scope". ✅ (with deferred work tracked) |
| VI. Live Data Over SSE | Not applicable this slice (no live data yet). |
| VII. ZFS Metadata Compatibility | Not applicable this slice (no replication/cursors yet). |

All applicable principles pass. Deferred work for principles V (auth) and I/VI (QUIC, SSE) tracked in spec's "out of scope".

## Project Structure

### Documentation (this feature)

```text
specs/001-workspace-and-first-endpoint/
├── spec.md     # done
├── plan.md     # this file
└── tasks.md    # next, via speckit-tasks
```

### Source code (repository root)

```text
arctern/
├── Cargo.toml              [workspace]
├── Cargo.lock
├── crates/
│   ├── api/
│   │   ├── Cargo.toml
│   │   └── src/lib.rs       # DatasetSummary (serde + utoipa::ToSchema)
│   └── client/
│       ├── Cargo.toml
│       └── src/lib.rs       # async fn list_datasets(base) via reqwest
├── daemon/
│   ├── Cargo.toml           # bin "arctern"
│   ├── src/
│   │   ├── main.rs          # clap subcommands; daemon entry point
│   │   ├── router.rs        # axum router + utoipa OpenAPI doc
│   │   ├── handlers/
│   │   │   ├── mod.rs
│   │   │   └── datasets.rs  # GET /api/v1/datasets handler
│   │   └── error.rs         # ApiError newtype + IntoResponse
│   └── tests/
│       ├── common/
│       │   └── mod.rs       # daemon-spawn helper (port-parsing)
│       └── integration_datasets_endpoint.rs
├── admin-ui/.gitkeep        # placeholder; Vue SPA lands later
├── justfile                 # vm-up/vm-down/vm-ssh/test-integration/test-vm
├── specs/                   # spec-kit artifacts
└── .specify/                # spec-kit config (already present)
```

## Phase 0: Research

Done inline in spec.md's Open Questions and the parent project's testing-strategy discussion. Key resolved decisions:

- **Spawn-and-parse-port** for the integration test (not in-band shutdown endpoint). `tokio::process::Child::kill` is the shutdown signal.
- **`utoipa-axum`** for OpenAPI emission tied to the axum router. Pin loosely (`= "0"`) and let cargo resolve.
- **No Nix dev shell or NixOS test framework this slice** (deferred per project-level discussion).
- **Workspace shape matches CLAUDE.md verbatim** — `crates/{api,client}` + `daemon` + `admin-ui`.
- **VM is shared with palimpsest** on port 2226 — no separate VM lifecycle for arctern.

## Phase 1: Design artifacts

### Data model

`crates/api`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatasetSummary {
    pub name: String,
    /// "filesystem" | "volume" | "snapshot" | "bookmark" — lowercase form
    /// used by `zfs(8)`. Avoids leaking palimpsest's enum repr.
    pub dataset_type: String,
    /// String-mapped property values. Native ZFS properties carry typed
    /// data (bytes, bool, …) but utoipa serializes more cleanly with a
    /// uniform string map; consumers parse as needed.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}
```

Conversion `From<palimpsest::ZfsListEntry> for DatasetSummary` lives in `crates/api`.

### API contract

`GET /api/v1/datasets` — no parameters. Response `200 OK` JSON: `Vec<DatasetSummary>`. Errors: `503` for `ZfsError::Spawn(_)`; `500` otherwise. Body schema `ApiErrorBody { error: String, message: String }`.

`GET /api-docs/openapi.json` — utoipa-generated OpenAPI 3 document. Includes `DatasetSummary` and `ApiErrorBody` as components.

### Quickstart (developer)

```bash
# One-time per session:
cd ~/code/palimpsest && just vm-up         # boots VM on port 2226
# Iterate:
cd ~/code/arctern
cargo run -p arctern-daemon -- daemon &    # prints LISTEN 127.0.0.1:<port>
curl http://127.0.0.1:<port>/api/v1/datasets | jq .
kill %1
# When done:
cd ~/code/palimpsest && just vm-down
```

CI-style:

```bash
cd ~/code/arctern
just test-vm                                # vm-up + integration tests + vm-down
```

## Phase 2: Tasks

Generated by `speckit-tasks` into `specs/001-*/tasks.md`. Expected ordering:

1. Workspace skeleton (root `Cargo.toml`, member dirs, placeholder lib.rs files).
2. `crates/api`: `DatasetSummary` type + `From` impl.
3. `crates/client`: list_datasets fn (skeleton; not used this slice but present per CLAUDE.md target layout).
4. `daemon`: clap CLI surface + `LISTEN ...` print + axum scaffold.
5. `daemon`: handler + ApiError + utoipa wiring.
6. `daemon/tests/`: integration test (spawn daemon, parse port, hit endpoint).
7. `justfile`: vm-up/down/ssh/test-integration/test-vm.
8. CLAUDE.md update if anything changed in target layout.
9. End-of-slice grep: confirm no `tokio::process::Command` and no stderr regex in arctern.

## Risks

- **Workspace migration ordering**: must convert root `Cargo.toml` to a workspace BEFORE adding member crates, or `cargo add` will refuse to add deps to the wrong file. Plan for root `Cargo.toml` change to be the first staged commit.
- **utoipa version drift vs axum 0.8**: utoipa-axum 0.x track is fast-moving. If `cargo add` lands incompatible versions, pin axum and utoipa-axum to a known-good pair found by `cargo search`.
- **Integration test port-parsing race**: if the daemon prints `LISTEN ...` to stdout but the test reads stdout asynchronously, there's a startup-window race. Mitigation: use line-buffered stdout (`writeln!` + flush) and a bounded read loop in the test.
- **Daemon shutdown via `Child::kill` may race with in-flight requests**: not relevant for this slice (test issues one request, then shuts down). Revisit when streaming endpoints land.

## Verification

```bash
# Inside arctern repo
cargo check --workspace
cargo clippy --workspace -- -D warnings
cargo test --workspace
just vm-up        # if not already running
just test-integration
just vm-down
```

End-of-slice manual smoke (per spec SC-004):

```bash
just vm-up
cargo run -p arctern-daemon -- daemon 2>/tmp/daemon.log &
PORT=$(awk '/^LISTEN/ {split($2,a,":"); print a[2]; exit}' < /tmp/daemon.log)
curl -s http://127.0.0.1:$PORT/api/v1/datasets | jq .
curl -s http://127.0.0.1:$PORT/api-docs/openapi.json | jq '.paths | keys'
kill %1
just vm-down
```

Constitution-IV grep (run after implementation):

```bash
grep -rn "tokio::process::Command" daemon/src crates/ ; echo "exit=$?"   # expect exit=1 (no matches)
grep -rn "regex::" daemon/src crates/ ; echo "exit=$?"                    # expect exit=1
```
