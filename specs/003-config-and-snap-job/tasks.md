# Tasks: Config file + periodic snap jobs with grid pruning

**Feature**: `003-config-and-snap-job`
**Input**: [spec.md](./spec.md), [plan.md](./plan.md)

Each task = one logical commit. Per-task verification commands listed.

## T001 — `chore(workspace)`: add `crates/config` crate

**Why first**: every later task either lives in this crate or imports it.

**Changes**:
- `cargo new --lib crates/config` (or hand-create with the conventional layout).
- `crates/config/Cargo.toml`: `name = "arctern-config"`, `version.workspace = true`, `edition.workspace = true`, `publish.workspace = true`. Add `serde`, `thiserror`, `toml`, `humantime-serde`, `regex`, `time` (with `formatting`+`parsing`+`macros` features). Use `cargo add` from inside `crates/config/`.
- `crates/config/src/lib.rs`: empty `pub` re-exports skeleton.
- `Cargo.toml` (workspace): add `"crates/config"` to `members`.

**Verify**:
```
cargo check -p arctern-config
```

**Commit**: `chore(workspace): add crates/config crate (T001)`

## T002 — `feat(config)`: TOML schema + serde + `load_from_path`

**Changes**:
- `crates/config/src/schema.rs`: `Config`, `JobConfig` (tagged enum, `Snap` only), `SnapJobConfig`, `FilesystemFilter`, `SnapshottingConfig` (tagged enum, `Periodic` only), `PruningConfig`, `KeepRule`. All with `#[serde(deny_unknown_fields)]` per FR-006/008.
- `crates/config/src/lib.rs`: `pub use schema::*;`. Add `pub fn load_from_path(path: &Path) -> Result<Config, ConfigError>`. `ConfigError` is a thiserror enum with variants for IO, parse (carrying field path / span), and semantic-validation (`UnknownJobType`, `DuplicateJobName`, `InvalidExclude`, etc.).
- Add a top-level `validate(&Config) -> Result<(), ConfigError>` invoked by `load_from_path` after deserialization. This slice's checks: duplicate job names; for each `FilesystemFilter`, `exclude.is_empty() || recursive`; for each excluded path, `is == path || starts_with(path + "/")`.
- Unit tests in `schema.rs` (or `tests/schema.rs`): round-trip the example config from spec; reject the negative cases listed in spec edge cases (unknown top-level field, unknown job `type`, missing `name`, `exclude` without `recursive`).

**Verify**:
```
cargo test -p arctern-config schema
```

**Commit**: `feat(config): TOML schema + serde + load_from_path (T002)`

## T003 — `feat(config)`: `FilesystemFilter::resolve`

**Changes**:
- `crates/config/src/filter.rs`:
  - Trait-light input: define a small trait `pub trait DatasetEntry { fn name(&self) -> &str; }` so the resolver can run against `palimpsest::dataset::ZfsListEntry` without depending on palimpsest. Or accept `&[&str]` — choose `&[&str]` to avoid the trait dance and keep the crate leaf.
  - `impl FilesystemFilter { pub fn resolve<'a>(&self, candidates: &'a [&'a str]) -> Vec<&'a str> }` per FR-018/019.
  - `pub fn resolve_all<'a>(filters: &[FilesystemFilter], candidates: &'a [&'a str]) -> Vec<&'a str>` deduplicates across filters.
- Unit tests: include exact match (non-recursive); recursive match with descendant set; recursive-with-exclude including the `path` itself; exclude-not-a-descendant (caller's responsibility — already rejected at validate, but resolve treats it as a no-op).

**Verify**:
```
cargo test -p arctern-config filter
```

**Commit**: `feat(config): FilesystemFilter resolver with recursive + exclude (T003)`

## T004 — `feat(pruning)`: grid expression parser

**Changes**:
- `crates/config/src/grid/mod.rs`:
  - `pub struct GridSpec(pub Vec<RetentionInterval>);`
  - `pub struct RetentionInterval { pub length: Duration, pub keep_count: KeepCount }` where `KeepCount = All | Exactly(u32)`.
  - `impl GridSpec { pub fn parse(s: &str) -> Result<Self, GridParseError> }`. Hand-roll using one `regex::Regex` for the per-term shape; split terms on `|`. `humantime::parse_duration` for the duration.
  - `impl<'de> Deserialize<'de> for GridSpec` — visits a string, calls `parse`, maps the error to `de::Error::custom`.
  - Validation: monotonic-increase rule with the all-`keep=all` carve-out (FR-014). Rejected expressions name the offending term in the error.
- Unit tests: parse the spec's examples (`6x4h | 14x1d`, `7x1d`, `4x15m(keep=all) | 24x1h | 3x1d`); reject `6x4z` (bad duration), `0x4h` (zero count), `1x4h | 1x1m` (descending without keep=all).

**Verify**:
```
cargo test -p arctern-config grid
```

**Commit**: `feat(config): grid retention expression parser (T004)`

## T005 — `feat(pruning)`: retention algorithm + KeepRule + PrunePolicy

**Changes**:
- `crates/config/src/grid/retention.rs`:
  - `pub struct SnapshotEntry { pub name: String, pub creation: time::OffsetDateTime }`.
  - `impl GridSpec { pub fn fit(&self, entries: &[SnapshotEntry]) -> (Vec<usize>, Vec<usize>) }` returning `(keep_indices, destroy_indices)` per zrepl's `FitEntries`. Direct port: youngest's date is `now`; bucket entries by interval-from-now; per-bucket keep-count selection.
- `crates/config/src/prune.rs`:
  - `impl KeepRule { pub fn destroy_set(&self, entries: &[SnapshotEntry]) -> std::collections::BTreeSet<usize> }` — per-rule destroy-list.
  - `pub struct PrunePolicy<'a>(&'a [KeepRule]);`
  - `impl<'a> PrunePolicy<'a> { pub fn evaluate(&self, entries: &[SnapshotEntry]) -> Vec<usize> }` — intersection across rules per FR-016.
  - Compile regex strings once (cache or expect callers to pre-compile). Simplest: compile inside `destroy_set` per call; the regex crate caches compilation reasonably and the per-cycle cost is negligible. Document in a comment.
- Unit tests: a fixture with synthetic timestamps reproduces zrepl's expected keep/destroy split for `6x4h | 14x1d`; intersection with a `regex(negate=true)` rule preserves the non-matching snapshots.

**Verify**:
```
cargo test -p arctern-config -- retention prune
```

**Commit**: `feat(config): grid retention algorithm + KeepRule + PrunePolicy (T005)`

## T006 — `feat(daemon)`: Job trait + JobManager + JobContext

**Changes**:
- `daemon/Cargo.toml`: `cargo add tokio-util --features rt`. `cargo add time --features formatting,parsing,macros`. `cargo add arctern-config` (path = `../crates/config`).
- `daemon/src/jobs/mod.rs`:
  - `pub trait Job: Send + Sync { fn name(&self) -> &str; fn kind(&self) -> &'static str; fn status(&self) -> JobStatusInner; async fn run(self: Arc<Self>, ctx: JobContext, cancel: CancellationToken); }`. `async fn` in trait via 2024 edition is fine.
  - `pub struct JobContext { pub runner: Arc<dyn palimpsest::runner::CommandRunner> }`.
  - `pub struct JobStatusInner { pub last_run: Option<OffsetDateTime>, pub next_run: Option<OffsetDateTime>, pub last_error: Option<String> }` (Mutex-guarded inside the SnapJob).
  - `pub struct JobManager { tasks: Vec<JobHandle>, ... }` with `pub fn spawn_all(...)`, `pub async fn shutdown(self, deadline: Duration)`, `pub fn statuses(&self) -> Vec<(String, &'static str, JobStatusInner)>`.
- Unit test: a noop-Job impl confirms `JobManager::spawn_all` + `shutdown` joins cleanly with `CancellationToken`.

**Verify**:
```
cargo test -p arctern-daemon jobs
```

**Commit**: `feat(daemon): Job trait + JobManager + CancellationToken lifecycle (T006)`

## T007 — `feat(daemon)`: SnapJob impl

**Changes**:
- `daemon/src/jobs/snap.rs`:
  - `pub struct SnapJob { config: SnapJobConfig, status: Mutex<JobStatusInner> }`.
  - `impl Job for SnapJob`:
    - `kind` returns `arctern_api::JOB_KIND_SNAP`.
    - `run`: startup-immediate check (FR-023.1), then loop with `tokio::select!` per FR-023.2/3.
    - Per cycle: list datasets via `palimpsest::dataset::list` with `recursive = true` (or scoped to the union of filter roots — pick whichever is simpler; recursive across all roots is fine). Resolve filters (T003). For each matched dataset: `snapshot` (idempotent on `SnapshotExists`); then list snapshots of that dataset with `properties = ["creation"]`; build `Vec<SnapshotEntry>`; run `PrunePolicy::evaluate`; `destroy` each victim (idempotent on `SnapshotHeld`).
    - Snapshot tag: `format!("{}{}", prefix, OffsetDateTime::now_utc().format(&Rfc3339)?.replace(':', ""))`.
    - Update `JobStatusInner` after every cycle.
    - Emit tracing events per FR-027.
- Unit tests: impossible without ZFS — covered by T012 integration test.

**Verify**:
```
cargo check -p arctern-daemon
cargo clippy -p arctern-daemon -- -D warnings
```

**Commit**: `feat(daemon): SnapJob implementation (T007)`

## T008 — `feat(daemon)`: --config flag + load + spawn jobs

**Changes**:
- `daemon/src/main.rs`:
  - Add `--config <PATH>` to the `Daemon` subcommand variant. Default `/etc/arctern/arctern.toml`.
  - In `run_daemon`: call `arctern_config::load_from_path(&config_path)?` BEFORE binding the socket. Print error to stderr and exit non-zero on validation failure.
  - Construct `Arc<dyn CommandRunner>` once from `palimpsest::SshCommandRunner::from_env()`.
  - Build `Vec<Box<dyn Job>>` from the parsed config (one `SnapJob` per `JobConfig::Snap`).
  - Instantiate `JobManager::spawn_all(...)`. Stash the `Arc<JobManager>` in a new `AppState` extension and pass it to `router::build_router(state)`.
  - Extend the existing `tokio::select!` shutdown branch to also `manager.shutdown(Duration::from_secs(5)).await` after the HTTP server stops.

**Verify**:
```
cargo check -p arctern-daemon
```

**Commit**: `feat(daemon): --config flag, load + spawn jobs in run_daemon (T008)`

## T009 — `feat(daemon)`: configcheck subcommand

**Changes**:
- `daemon/src/configcheck.rs` (new): `pub fn run(path: &Path) -> eyre::Result<()>`. Calls `arctern_config::load_from_path`. On `Ok`, prints `ok` to stdout and returns `Ok(())`. On `Err`, formats the error per D19 and returns `Err(eyre::eyre!("{e}"))` so the binary's `main` propagates non-zero exit.
- `daemon/src/main.rs`: replace the slice-002 stub branch `Command::Configcheck { path } => ...` with `configcheck::run(&path)`.

**Verify**:
```
cargo build -p arctern-daemon
echo '[[jobs]]\ntype="snap"\nname="x"\n[jobs.snapshotting]\ntype="periodic"\ninterval="1s"\nprefix="x_"\n[jobs.pruning]\nkeep=[]\n[[jobs.filesystems]]\npath="tank"' > /tmp/cc.toml
cargo run -p arctern-daemon -- configcheck /tmp/cc.toml   # expect: ok
echo 'jobs = "wrong"' > /tmp/bad.toml
cargo run -p arctern-daemon -- configcheck /tmp/bad.toml  # expect: non-zero, stderr names file + field
```

**Commit**: `feat(daemon): configcheck subcommand validates real config (T009)`

## T010 — `feat(api)`: JobStatus + GET /api/v1/jobs

**Changes**:
- `crates/api/src/lib.rs`:
  - `pub const JOB_KIND_SNAP: &str = "snap";`.
  - `pub struct JobStatus { name: String, kind: String, last_run: Option<String>, next_run: Option<String>, last_error: Option<String> }` with `serde + utoipa::ToSchema`.
- `daemon/src/handlers/jobs.rs` (new): handler reads `State<Arc<JobManager>>`, calls `manager.statuses()`, formats `OffsetDateTime`s as RFC3339, returns `Json(Vec<JobStatus>)`. Utoipa registration.
- `daemon/src/router.rs`: extend `build_router` to take an `AppState` (containing `Arc<JobManager>`). Register the new route via `routes!(handlers::jobs::list_jobs)` and add `JobStatus` to `components(schemas(...))`.

**Verify**:
```
cargo check -p arctern-daemon -p arctern-api
cargo test -p arctern-api job_status
```

**Commit**: `feat(api): JobStatus type + GET /api/v1/jobs (T010)`

## T011 — `docs`: example-config.toml

**Changes**:
- `docs/example-config.toml` (new): translate `databak` + `rootbak` from the slice ticket's reference YAML. Comments document:
  - The mapping from zrepl's `{ "okdata/data/nas": true, ... }` to `[[jobs.filesystems]] path = "..."` tables.
  - The mapping from zrepl's `path<` recursive syntax to arctern's `recursive = true` + `exclude = [...]`.
  - That snapshot-tag format (`zrepl_<RFC3339-no-colons>`) is wire-compatible with zrepl, so a host migrating from zrepl can keep its existing snapshot history under arctern.
- A `#[test]` in `crates/config/tests/example_config.rs` parses the file via `arctern_config::load_from_path` and asserts no error.

**Verify**:
```
cargo test -p arctern-config example_config
cargo run -p arctern-daemon -- configcheck docs/example-config.toml   # expect: ok
```

**Commit**: `docs(config): example-config.toml translating databak + rootbak (T011)`

## T012 — `test(integration)`: snap-job end-to-end

**Changes**:
- `daemon/tests/common/mod.rs`: extend `spawn_daemon_uds` (or add `spawn_daemon_uds_with_config`) to optionally pass `--config <path>`. Default behaviour (no config) MUST still work for slice 002's tests; do this via an `Option<PathBuf>` arg or a builder. **However**, slice 003's `run_daemon` REQUIRES a config (FR-002). To keep slice-002 tests green, write a default `/tmp/arctern_test_<nanos>.toml` containing zero jobs when no explicit config is requested.
- `daemon/tests/integration_snap_job.rs` (new):
  - `#![cfg(feature = "integration")]`. `mod common;`.
  - Boot a `LoopbackPool`. Create `tank/data` as a child. Write a TOML config to `/tmp/arctern_test_<nanos>.toml` with `interval = "1s"`, `prefix = "test_"`, `path = "<pool>/data"`, and a `1x1s` keep grid. Spawn the daemon via `spawn_daemon_uds_with_config`. Sleep 3s. Use `palimpsest::dataset::list` to assert ≥2 snapshots whose names start with `test_` exist on `<pool>/data`.
  - Tear down: `child.kill(); child.wait(); pool.destroy().await`.

**Verify**:
```
just vm-up
just test-integration
just vm-down
```

**Commit**: `test(daemon): integration test for snap job end-to-end (T012)`

## Dependency graph

```
T001 (workspace) ──> T002 (schema) ──> T003 (filter resolver) ─┐
                                  └──> T004 (grid parser) ──> T005 (retention) ─┐
                                                                                ├─> T007 (SnapJob)
T006 (Job/Manager) ─────────────────────────────────────────────────────────────┘
                                              │
T008 (--config wiring) <────────────── T002, T006, T007
T009 (configcheck) <───────────────── T002
T010 (API + handler) <─────────────── T006
T011 (example) <───────────────────── T002, T009
T012 (integration) <───────────────── T007, T008, T010
```

T001 strictly first. T002-T005 land in `crates/config` order. T006 can land before T002 (no dependency). T007 needs T002+T005+T006. T008-T010 are wire-up. T011 is docs. T012 is the verification gate.

## Done when

All of: `cargo test --workspace` green, `cargo clippy --workspace --all-targets --features integration -- -D warnings` clean, `just test-integration` exits 0, all 12 commits land on the slice branch, the constitution-IV grep returns no matches in `crates/api crates/client daemon/src/`.
