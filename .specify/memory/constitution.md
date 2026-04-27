# arctern Constitution

A modern ZFS replication daemon: replicate snapshots between hosts, manage retention, expose a web UI for all administration. Greenfield, not wire-compatible with Go zrepl.

## Core Principles

### I. QUIC With HTTP Semantics Where Helpful

All inter-host communication runs over a single QUIC connection per peer. Daemons reach each other over HTTP/3-over-QUIC, served by axum. Browsers reach the daemon over HTTP/2-over-TLS (or plain HTTP on loopback). Both clients hit the **same** axum router; one API surface, one OpenAPI spec, one set of types. Bulk ZFS data flows on **raw QUIC unidirectional streams**, not over HTTP — the control RPC returns a stream id; the sender opens a uni stream; the receiver pipes it into `zfs recv`. HTTP framing where it helps (typed handlers, status codes, SSE), raw streams where it doesn't (gigabytes of `zfs send` output).

### II. One API for Browser and Daemons

A `crates/api/` crate defines all request/response types with `serde` + `utoipa::ToSchema`. The axum router uses these types in handlers. Daemon-to-daemon Rust clients consume the same types via a thin `reqwest` (or h3) wrapper — no codegen needed because both ends are Rust. The browser TS client is generated from the OpenAPI spec via `@hey-api/openapi-ts`. Adding an endpoint changes one type and one handler; both clients pick it up. Two-API drift is structurally impossible.

### III. Web UI Replaces the CLI

The daemon binary exposes only the CLI subcommands a web UI cannot replace: `arctern daemon` (runs the daemon), `arctern stdinserver` (SSH transport entry point invoked by `sshd`), `arctern configcheck` (one-shot YAML validation for CI). Everything else — status, signal, wakeup, snapshot listing, log tail, replication progress — is the web UI. There is no `arctern status` command; visit the dashboard.

### IV. ZFS Through palimpsest

All ZFS interaction goes through the `palimpsest` sibling crate. arctern itself contains no `tokio::process::Command` calls to `zfs`/`zpool` and no stderr regex matching. If palimpsest doesn't cover an operation arctern needs, the change goes into palimpsest, not into arctern. This keeps the ZFS surface unified across both downstream projects (arctern and archinstall_zfs).

### V. Local-Only by Default, Auth Opt-In

The default bind is a UNIX socket plus loopback TCP — filesystem permissions and kernel-enforced loopback are the only access control. Operators who bind to a network address must enable session-cookie auth (HttpOnly, SameSite=Strict, constant-time credential comparison). The web UI surfaces a clear visual indicator when running in network-exposed mode without auth.

### VI. Live Data Over SSE, Not WebSockets

Job state changes, replication progress, and the log tail are pushed to the browser via Server-Sent Events. axum has first-class SSE support; SSE survives proxies and middleboxes that mangle WebSockets; data flows server→client almost exclusively. Internally backed by `tokio::sync::broadcast` channels. Logs are hooked via a custom `tracing_subscriber::Layer` that pushes events into the same broadcast alongside stdout/file outlets.

### VII. ZFS Metadata Compatibility, Nothing Else

The crate maintains compatibility *only* at the ZFS metadata layer (hold tag conventions, replication cursor bookmark naming, resume token handling) so an arctern instance can take over a pool previously managed by Go zrepl without re-replicating multi-TB datasets. **Nothing else is wire- or config-compatible.** YAML config schema is greenfield. Control socket protocol is greenfield. Wire protocol is greenfield (QUIC, not TCP+TLS).

## Job Model and Topology

| Job type | Side    | Initiates connection? | Direction of data |
|----------|---------|------------------------|--------------------|
| push     | active  | yes                    | sender             |
| sink     | passive | no                     | receiver           |
| pull     | active  | yes                    | receiver           |
| source   | passive | no                     | sender             |
| snap     | local-only — snapshotting + pruning, no network |

Pairs: push↔sink, pull↔source. Both peers run the same binary; each runs its own web UI. The active side is the RPC client; the passive side is the RPC server. With QUIC, data direction is independent of connection direction — for `pull`, the active receiver dials, but the passive source opens uni streams to push the data back.

## Web UI Feature Surface

1. Dashboard — per-job state, last/next run, last error.
2. Job detail — per-FS step list with progress, wakeup/reset buttons.
3. Log viewer — live tail with level/job filters, ring-buffered in memory.
4. Snapshot explorer — sender↔receiver snapshot graph, holds, bookmarks, replication cursor.
5. Pruning preview — what would be destroyed if pruning ran now; no-op against ZFS.
6. Config viewer (read-only first; editor with validate-then-apply later).

## Build and Deploy

Single binary, embedded UI. `build.rs` calls `memory_serve::load_directory("admin-ui/dist")`. A `justfile` orchestrates `just openapi` (regenerates TS client from utoipa spec), `just build-ui` (vite build), `just build` (cargo build with embedded assets), `just deploy` (rsync + systemctl restart).

## Deferred Decisions

- Whether to use HTTP/3 + axum-on-h3, or fall back to HTTP/2 over TLS for the control plane while keeping QUIC purely for raw data streams. Spike before committing — h3+axum integration is younger than h2+axum.
- Persistent state. Likely none (ZFS metadata is the source of truth) but revisit when implementing the replication driver.
- Federation: one daemon's UI proxying to another's API. v2 material.
- Hooks (pre/post-replication shell scripts). Port from Go zrepl when needed.
- Whether the snap job type should run as a separate scheduling unit or be embedded in push/pull/source/sink.

## Status

In design. Implementation is **blocked on palimpsest** providing usable surface for: dataset list/get, snapshot/rollback, holds, bookmarks, send (with raw/properties/large-block/compressed/embedded/replicate/resume flags + dry-run sizing), recv (with resumability), resume token parsing and validation, encryption status, and replication-cursor bookmark management.

## Governance

Pre-1.0. Breaking changes are allowed in any minor version. Amendments to this constitution are decided in PR review with explicit reference to which principle is being modified and why.

**Version**: 0.1.0 | **Ratified**: 2026-04-27 | **Last Amended**: 2026-04-27
