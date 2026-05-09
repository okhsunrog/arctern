# Tasks: Workspace migration + GET /api/v1/datasets

**Feature**: `001-workspace-and-first-endpoint`
**Input**: [spec.md](./spec.md), [plan.md](./plan.md)

Each task = one logical commit. Per-task verification commands listed.

## T001 — Convert root `Cargo.toml` into a workspace

**Why first**: `cargo add -p <member>` requires the workspace root to already exist. Adding members before the root is a workspace breaks `cargo`.

**Changes**:
- Replace `[package] arctern` block in `Cargo.toml` with `[workspace] resolver = "3", members = ["crates/api", "crates/client", "daemon"]`.
- Move (rename) `src/main.rs` to `daemon/src/main.rs` *after* the workspace is in place; or delete `src/` and let T004 recreate the daemon binary fresh.
- Decision: delete `src/main.rs` outright in this task. T004 builds `daemon/` fresh. Cleaner than a move-then-overwrite.
- Create empty member dirs with `Cargo.toml` + minimal `lib.rs` so `cargo check --workspace` resolves: `crates/api`, `crates/client`. The `daemon` crate exists but stays empty until T004.
- `admin-ui/.gitkeep` (placeholder).

**Verify**:
```
cargo check --workspace
```

**Commit**: `feat(workspace): convert arctern to cargo workspace per slice 001`

## T002 — `crates/api`: `DatasetSummary` type

**Changes**:
- `crates/api/Cargo.toml` adds `serde` (derive) + `utoipa` + `palimpsest = { path = "../../palimpsest" }` (for the `From` impl).
- `crates/api/src/lib.rs`: define `DatasetSummary { name, dataset_type, properties: BTreeMap<String, String> }`. Implement `From<palimpsest::ZfsListEntry> for DatasetSummary` mapping the dataset type to its lowercase string form and stringifying property values.
- Unit tests: round-trip a synthetic `ZfsListEntry` through `DatasetSummary` + `serde_json` and back; assert key fields preserved.

**Verify**:
```
cargo test -p arctern-api
```

**Commit**: `feat(api): DatasetSummary wire type with From<ZfsListEntry>`

## T003 — `crates/client`: skeleton `list_datasets`

**Changes**:
- `crates/client/Cargo.toml` adds `reqwest` (default features: `json`), `serde_json`, `arctern-api = { path = "../api" }`, `thiserror`.
- `crates/client/src/lib.rs`: `pub async fn list_datasets(base: &str) -> Result<Vec<DatasetSummary>, ClientError>` calling `GET <base>/api/v1/datasets`.
- `ClientError` enum: `Http(reqwest::Error)`, `Decode(serde_json::Error)`, `Status { code: u16, body: String }`.
- No tests this slice (used by future daemon-to-daemon code; exercised via integration tests in later slices).

**Verify**:
```
cargo check -p arctern-client
```

**Commit**: `feat(client): skeleton reqwest client with list_datasets`

## T004 — `daemon`: clap CLI surface + LISTEN print + axum scaffold

**Changes**:
- `daemon/Cargo.toml`: bin name `arctern`, deps `tokio` (full), `axum` 0.8, `utoipa`, `utoipa-axum`, `clap` (derive), `serde`, `serde_json`, `tracing`, `tracing-subscriber`, `eyre`, `arctern-api`, `palimpsest = { path = "../../palimpsest" }`.
- `daemon/src/main.rs`: clap-derived `Cli` with three subcommands: `Daemon`, `Stdinserver { ident: String }`, `Configcheck { path: PathBuf }`.
  - `Daemon`: build router (T005), bind `127.0.0.1:0`, print `LISTEN <addr>` line to stdout (line-buffered), serve.
  - `Stdinserver`: `eprintln!("not implemented in slice 001"); Ok(())`.
  - `Configcheck`: `eprintln!("not implemented in slice 001"); Ok(())`.
- `daemon/src/router.rs`: `pub fn build_router(state: AppState) -> Router` returning a router with `/api/v1/datasets` (handler from T005) and `/api-docs/openapi.json` (utoipa). `AppState` is unit-shaped (or `()` if utoipa allows).
- Tracing: install `tracing_subscriber::fmt()` at `INFO` for human-readable startup logs.

**Verify**:
```
cargo build -p arctern-daemon
target/debug/arctern --help                                    # lists 3 subcommands
target/debug/arctern stdinserver foo; echo $?                  # exit 0
```

**Commit**: `feat(daemon): clap CLI + axum scaffold + LISTEN port-handshake`

## T005 — `daemon`: GET /api/v1/datasets handler + ApiError

**Changes**:
- `daemon/src/error.rs`: `ApiError(palimpsest::ZfsError)` newtype with `IntoResponse`. `Spawn(_)` → 503; everything else → 500. Body `ApiErrorBody { error, message }`.
- `daemon/src/handlers/datasets.rs`: `#[utoipa::path(get, path = "/api/v1/datasets", responses((status = 200, body = Vec<DatasetSummary>)))]` async fn. Constructs `palimpsest::SshCommandRunner::from_env()` per request (no shared mutable state), calls `palimpsest::dataset::list` with default `ListOptions`, maps result via `DatasetSummary::from` per entry.
- `daemon/src/router.rs`: wire the handler + register the utoipa path. Expose `/api-docs/openapi.json` via `utoipa_axum::router::OpenApiRouter` or equivalent.

**Verify**:
```
cargo build -p arctern-daemon
cargo clippy -p arctern-daemon -- -D warnings
```

**Commit**: `feat(daemon): GET /api/v1/datasets backed by palimpsest`

## T006 — Integration test: spawn daemon, parse port, hit endpoint

**Changes**:
- `daemon/Cargo.toml`: add `[features] integration = []` and `[dev-dependencies]` `reqwest` (json), `tokio` (full + test-util), `serde_json`.
- `daemon/tests/common/mod.rs`: helper `spawn_daemon() -> (Child, String /* base_url */)`. Spawns `target/debug/arctern daemon` with `PALIMPSEST_SSH_TARGET` + `PALIMPSEST_SSH_PASSWORD` from env, reads stdout until `LISTEN <addr>` arrives, returns `(child, format!("http://{addr}"))`.
- `daemon/tests/integration_datasets_endpoint.rs`: gated `#![cfg(feature = "integration")]`. Boots a `LoopbackPool` (copy from `palimpsest/tests/common/mod.rs` — fine for now per D4), spawns the daemon, calls `GET /api/v1/datasets`, asserts the test pool name appears in the response, kills the daemon, destroys the pool.
- Daemon binary path resolution: use `env!("CARGO_BIN_EXE_arctern")` so cargo provides the absolute path at compile time — no PATH or current-dir games.

**Verify**:
```
just vm-up
just test-integration
```

**Commit**: `test(daemon): integration test for GET /api/v1/datasets against VM`

## T007 — `justfile`

**Changes**:
- New `justfile` at arctern repo root mirroring palimpsest's vm-management recipes (same port 2226, same env-var contract).
- Recipes: `default` (list), `check`, `test`, `lint`, `fmt`, `vm-up`, `vm-down`, `vm-ssh`, `vm-log`, `test-integration`, `test-vm`, `test-cleanup`.
- `vm-up` / `vm-down` may simply delegate to palimpsest's justfile via `just --justfile ../palimpsest/justfile vm-up` (DRY) OR be self-contained copies (independence). Decide: delegate for `vm-up`/`vm-down` (one source of truth), self-contained for `test-integration` (it's arctern-specific).

**Verify**: `just --list` shows recipes; `just check` runs `cargo check --workspace`.

**Commit**: `chore(arctern): justfile mirroring palimpsest vm management`

## T008 — Constitution-IV grep + final verification

**Changes**: None (verification step).

**Verify**:
```
# Principle IV: ZFS through palimpsest only
grep -rn "tokio::process::Command\|std::process::Command" daemon/src crates/ ; echo $?   # expect 1
grep -rn "regex::" daemon/src crates/ ; echo $?                                          # expect 1

cargo check --workspace
cargo clippy --workspace -- -D warnings
cargo test --workspace                                  # unit tests
just vm-up
just test-integration                                   # integration test
just vm-down
```

If anything fails, fix and re-run; do not commit a broken state.

**Commit**: (no commit needed; verification only)

## Optional / deferred

- T009 — README touch-up if anything in `CLAUDE.md`'s target layout was rephrased. Skip if unchanged.

## Done when

All of: `cargo test --workspace` green, `cargo clippy --workspace -- -D warnings` clean, `just test-integration` exits 0, all 7 commits land on the slice branch.
