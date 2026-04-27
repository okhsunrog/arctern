# arctern

ZFS replication daemon. Async Rust, QUIC transport, web UI for all administration. Greenfield; not wire-compatible with Go zrepl.

See `.specify/memory/constitution.md` for durable design decisions and `specs/` for per-slice specifications. CLAUDE.md is project conventions and how-to-work-in-this-repo.

## Status

In design. Implementation is blocked on `palimpsest` (sibling repo at `~/code/palimpsest/`) reaching a usable surface for: dataset list/get, snapshot/rollback, holds, bookmarks, send (with all flags + dry-run sizing), recv, resume token parsing, replication cursor management. Until palimpsest covers those, arctern's source is intentionally a single-file stub.

## Stack

- `tokio` — runtime
- `axum` 0.8 — HTTP server, single Router served on both HTTP/2-over-TLS (browser) and HTTP/3-over-QUIC (daemon-to-daemon)
- `quinn` — QUIC transport
- `h3` (+ glue layer) — HTTP/3 over quinn
- `utoipa` + `utoipa-axum` — OpenAPI generation
- `palimpsest` — ZFS toolkit (sibling crate, `path = "../palimpsest"` during development)
- `tracing` + `tracing-subscriber` — structured logging; ring-buffered for SSE log tail
- `serde` + `thiserror` — types and errors
- `tokio_util::sync::CancellationToken` — graceful shutdown / job interruption

Frontend: Vue 3 + TypeScript + Nuxt UI v4 + Tailwind v4, built with Vite + bun, embedded into the binary via `memory-serve` in `build.rs`. TS client generated from the OpenAPI spec via `@hey-api/openapi-ts`.

## Conventions

- Rust edition 2024.
- Async-only. Same disciplines as palimpsest.
- Add deps via `cargo add`; do not hand-edit Cargo.toml.
- Errors via `thiserror` in library code; `eyre` only at `main.rs`.
- Comment WHY, never WHAT. Default to no comment.
- No emojis in code, comments, or commit messages.
- TS client is auto-generated; never hand-edit files under `admin-ui/src/client/`.

## Out-of-scope CLI

The daemon binary still exposes a few CLI subcommands for things a web UI cannot replace:

- `arctern daemon` — runs the daemon (which serves the web UI).
- `arctern stdinserver <ident>` — SSH transport entry point, invoked by `sshd` via `authorized_keys` `command="..."`.
- `arctern configcheck <path>` — one-shot YAML validation for CI / pre-deploy scripts.

Everything else (status, signal, wakeup, snapshot listing, log tail) is web UI.

## Layout (current → target)

Currently a single `bin` crate. Splits into a workspace once palimpsest covers what arctern needs:

```
crates/
  api/        request/response types (serde + utoipa::ToSchema). One source of truth
              for both the browser and daemon-to-daemon RPC.
  client/     thin reqwest+h3 wrapper using crates/api types (used by daemon→daemon).
daemon/       binary crate; axum router, AppState, SSE, signals, snapper, control logic.
admin-ui/     Vue 3 SPA, embedded via build.rs. Auto-generated TS client in src/client/.
```

## Commands

- `cargo check`
- `cargo test`
- `cargo clippy --all-targets -- -D warnings`
- `cargo add <crate>` for deps
- (Future) `just openapi`, `just build-ui`, `just build`, `just deploy`

<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
<!-- SPECKIT END -->
