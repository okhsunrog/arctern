# arctern

ZFS replication daemon. Async Rust, SSH transport, web UI for all administration. Inspired by zrepl; not wire-compatible.

See `ARCHITECTURE.md` for the durable design (transport, protocol, ACL model, state storage). Read it before changing code. CLAUDE.md is project conventions and how-to-work-in-this-repo.

## Status

Mid-pivot from QUIC transport to SSH transport. Slices 001-006 shipped a working QUIC-based laptop→server replication pipeline (snap + push + sink + resume tokens). The QUIC transport is now being torn out and replaced with multi-channel SSH per `ARCHITECTURE.md`. Replication semantics (planner, GUID intersection, resume tokens, `discard_partial_recv`) and the snap job are preserved verbatim.

The spec-kit workflow is dropped. Future work goes straight to feature commits — no `specs/00X-*` directories, no spec/plan/tasks ceremony.

## Stack

- `tokio` — runtime
- `axum` 0.8 — HTTP server, browser-facing on loopback only
- `openssh` — SSH session + multi-channel client (uses system `ssh(1)`, ControlMaster)
- `tokio_util::codec::LengthDelimitedCodec` — framing on the control channel
- `serde_json` — payload encoding (readable in logs; postcard later if size matters)
- `sqlx` (sqlite + runtime-tokio) — observability state at `<state_dir>/state.db`
- `utoipa` + `utoipa-axum` — OpenAPI generation for the local UI
- `zfskit` — ZFS toolkit (sibling crate, `path = "../zfskit"` during development)
- `tracing` + `tracing-subscriber` — structured logging; SQLite layer for INFO+, journald for the rest
- `serde` + `thiserror` — types and errors
- `tokio_util::sync::CancellationToken` — graceful shutdown / job interruption

Frontend: Vue 3 + TypeScript + Nuxt UI v4 + Tailwind v4, built with Vite + bun, embedded into the binary via `memory-serve` in `build.rs`. TS client generated from the OpenAPI spec via `@hey-api/openapi-ts`.

## Conventions

- Rust edition 2024.
- Async-only. Same disciplines as zfskit.
- Add deps via `cargo add`; do not hand-edit Cargo.toml.
- Errors via `thiserror` in library code; `eyre` only at `main.rs`.
- Comment WHY, never WHAT. Default to no comment.
- No emojis in code, comments, or commit messages.
- TS client is auto-generated; never hand-edit files under `admin-ui/src/client/`.
- All ZFS work goes through zfskit. If a primitive is missing, add it to zfskit first as a separate commit on `master`, push, then use it here.

## CLI surface

The daemon binary exposes only:

- `arctern daemon` — runs the daemon (which serves the local web UI).
- `arctern stdinserver-dispatch <identity>` — SSH transport entry point, invoked by `sshd` via `authorized_keys` `command="..."`. Reads `SSH_ORIGINAL_COMMAND` to determine `<job> <op>`, validates the identity against config, dispatches to the control or recv handler.
- `arctern configcheck <path>` — one-shot config validation for CI / pre-deploy scripts.

Everything else (status, signal, wakeup, snapshot listing, log tail) is web UI.

## Layout

```
crates/
  api/         HTTP API request/response types (serde + utoipa::ToSchema)
  config/      TOML schema, filter resolver, prune algorithm, grid retention
  transport/   wire protocol enums (RequestFrame, ResponseFrame, RecvHeader, SendHeader),
               LengthDelimitedCodec wrapper. Pure types; no I/O.
daemon/        binary crate
  src/
    main.rs                  daemon + dispatch entry points (split via subcommand)
    auth.rs                  PeerCredentials connect-info for UDS
    handlers/                axum handlers (local + proxied to peers)
    jobs/                    JobManager, snap, push
    peer/                    PeerLink, ControlClient, RecvChannel, reconnect
    stdinserver/             dispatch + control + recv handlers
    state/                   SQLite pool, migrations, queries
    router.rs                axum wiring
    error.rs                 ApiError → HTTP response mapping
admin-ui/                    Vue 3 SPA, embedded via build.rs
docs/                        deploy-snap-only.md, deploy-full-mirror.md, example-config.toml
packaging/systemd/           arctern.service unit
```

## Commands

- `cargo check --workspace`
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo add <crate>` for deps
- `just vm-up` / `just vm-down` / `just test-integration` — VM-driven integration tests (shared with zfskit, port 2226)

## Runner override

The daemon's local `CommandRunner` defaults to `zfskit::runner::RealRunner` — the SSH-transport pivot puts the daemon on the actual ZFS host. Setting `ZFSKIT_SSH_TARGET=user@host:port` (and optionally `ZFSKIT_SSH_PASSWORD`) swaps in `SshCommandRunner` instead. This is the override the integration tests use to drive the daemon against the test VM from the developer's host; production deployments leave both env vars unset.
