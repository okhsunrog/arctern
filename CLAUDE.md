# arctern

ZFS replication daemon. Async Rust, SSH transport, web UI for all administration. Inspired by zrepl; not wire-compatible.

See `ARCHITECTURE.md` for the durable design (transport, protocol, ACL model, state storage). Read it before changing code. CLAUDE.md is project conventions and how-to-work-in-this-repo.

## Status

Transport is multi-channel SSH per `ARCHITECTURE.md`: a long-lived tarpc control channel plus per-step recv channels and a one-way events channel, all multiplexed over a single `openssh` session with ControlMaster. The QUIC→SSH pivot is complete; the original QUIC transport has been fully removed. Replication semantics (planner, GUID intersection, resume tokens, `discard_partial_recv`) and the snap job were preserved verbatim across the pivot.

The spec-kit workflow is dropped. Future work goes straight to feature commits — no `specs/00X-*` directories, no spec/plan/tasks ceremony.

## Stack

- `tokio` — runtime
- `axum` 0.8 — HTTP server, browser-facing on loopback only
- `openssh` — SSH session + multi-channel client (uses system `ssh(1)`, ControlMaster)
- `tokio_util::codec::LengthDelimitedCodec` — framing on the control channel
- `serde_json` — payload encoding (readable in logs; postcard later if size matters)
- `sqlx` (sqlite + runtime-tokio) — observability state at `<state_dir>/state.db`
- `utoipa` + `utoipa-axum` — OpenAPI generation for the local UI
- `zfskit` — ZFS toolkit, from crates.io. To develop against the sibling clone (~/code/zfskit), add a LOCAL `[patch.crates-io] zfskit = { path = "../zfskit" }` to the workspace Cargo.toml — never commit or push a path/patch to an external crate; main must build from the registry alone.
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
- All ZFS work goes through zfskit. If a primitive is missing, add it to zfskit first, publish a release, then use it here (local `[patch.crates-io]` while iterating).

## CLI surface

The daemon binary exposes only:

- `arctern daemon` — runs the daemon (which serves the local web UI).
- `arctern stdinserver-dispatch <identity>` — SSH transport entry point, invoked by `sshd` via `authorized_keys` `command="..."`. Reads `SSH_ORIGINAL_COMMAND` to determine `<job> <op>`, validates the identity against config, dispatches to the control or recv handler.
- `arctern configcheck <path>` — one-shot config validation for CI / pre-deploy scripts.
- `arctern openapi` — print the OpenAPI spec as JSON to stdout and exit; used to regenerate the UI's typed client.

Everything else (status, signal, wakeup, snapshot listing, log tail) is web UI.

## Layout

```
crates/
  api/         HTTP API request/response types (serde + utoipa::ToSchema)
  client/      thin async HTTP/1.1-over-UNIX-socket client (used by the stdinserver proxy)
  config/      TOML schema, filter resolver, prune algorithm, grid retention
  transport/   the tarpc `ArcternControl` service definition plus recv/event framing
               types (ResponseFrame, RecvHeader, SendHeader) and the
               LengthDelimitedCodec + JSON transport helper. Pure types; no I/O.
daemon/        binary crate
  src/
    main.rs                  daemon + dispatch entry points (split via subcommand)
    auth.rs                  PeerCredentials connect-info for UDS
    handlers/                axum handlers (local + proxied to peers)
    jobs/                    JobManager, snap, push, prune
    peer/                    PeerLink, ControlClient, RecvChannel, reconnect
    stdinserver/             dispatch + control + recv handlers
    state/                   SQLite pool, migrations, queries
    router.rs                axum wiring
    error.rs                 ApiError → HTTP response mapping
admin-ui/                    Vue 3 SPA, embedded via build.rs
docs/                        install.md, deploy-snap-only.md, deploy-full-mirror.md,
                             migrate-from-zrepl.md, roadmap.md, example-config.toml (+ diagrams/, screenshots/)
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
