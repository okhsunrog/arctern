# Feature Specification: Config file + periodic snap jobs with grid pruning

**Feature Branch**: `003-config-and-snap-job`
**Created**: 2026-05-09
**Status**: Draft
**Input**: Slice 003 of arctern. Read a TOML config file at `/etc/arctern/arctern.toml` (override `--config <path>`), parse it via serde, then run periodic snapshot jobs with grid-based pruning as background tokio tasks. Replaces zrepl's `snap`-type jobs end-to-end. No networking, no replication this slice.

## Why this slice

Slices 001 + 002 stood up the daemon's HTTP surface and proved the mutating-endpoint pattern over UDS. Until the daemon does periodic work *on its own*, it's just a thin shell over `palimpsest`. Periodic snapshot + retention is also the smallest job type from the constitution's job table (`snap`: local-only, no peer, no transport) — landing it first lets the job runtime, config schema, and pruning algorithm bake before the network slices (push/pull/source/sink) layer on top.

The user's actual workload is a Go zrepl install with two `snap` jobs (`databak` + `rootbak`). This slice's success criterion is that those two jobs are expressible verbatim in arctern's TOML schema and produce the same retention behaviour. zrepl-config wire compatibility is explicitly NOT a goal (constitution VII); only the metadata convention (snapshot prefix `zrepl_`) and the grid-keep semantics carry over.

QUIC, daemon-to-daemon RPC, replication cursors, sender/receiver endpoints, hooks, bandwidth limits, and the snapshot-explorer UI are deferred to later slices (see Non-Goals).

## User Scenarios & Testing *(mandatory)*

The "user" of this slice is the operator who today edits `/etc/zrepl/zrepl.yml` and reloads zrepl. They drop the equivalent file at `/etc/arctern/arctern.toml`, restart `arctern daemon`, and snapshots start happening on the schedule they declared. A second user is `arctern configcheck`, run from CI / pre-deploy automation.

### User Story 1 — Operator declares periodic-snap jobs in TOML and the daemon honours them (Priority: P1)

An operator writes `/etc/arctern/arctern.toml` describing one or more snap jobs (filesystems to snapshot, interval, snapshot-name prefix, retention rules). They start `arctern daemon`. Snapshots appear on the declared interval; old snapshots disappear per the retention rules. No further action.

**Why this priority**: This *is* the slice. Every other story below supports it.

**Independent Test**: Boot a fresh loopback pool, write a config pointing at it with `interval = 1s` and a one-row keep grid, run the daemon for ~3 seconds, observe ≥2 snapshots created and pruning executed.

**Acceptance Scenarios**:

1. **Given** a TOML config declaring one snap job with `interval = "1s"`, `prefix = "arctern_"`, and one filesystem, **When** the daemon runs for 3 seconds, **Then** ≥2 snapshots whose names start with `arctern_` exist on that filesystem.
2. **Given** the daemon has been running and accumulated more snapshots than the keep-grid permits, **When** the next cycle runs, **Then** the surplus snapshots are destroyed and only the grid-permitted set remains; snapshots NOT matching the keep-rule regex are left untouched.
3. **Given** the daemon has just started and the most recent matching snapshot on a target filesystem is older than `interval`, **When** the daemon starts, **Then** it takes an immediate snapshot rather than waiting a full interval (so a daemon restart does not lose the snapshot window).
4. **Given** the daemon receives `SIGTERM`/`SIGINT` mid-cycle, **When** it shuts down, **Then** the in-progress snapshot completes (or the cancellation token aborts cleanly between snapshots), background tasks join within a 5s deadline, and the socket is removed.
5. **Given** a job's snapshot creation race-loses (`ZfsError::SnapshotExists` because two daemons or a manual user took it), **When** the job processes the error, **Then** the daemon logs a `warn`, treats the snapshot as already present, and continues to the next filesystem (idempotent).
6. **Given** a job's filesystem filter declares `recursive = true` plus `exclude = [...]`, **When** the job runs, **Then** the snapshotter operates on every descendant of the recursive root EXCEPT the listed excludes (and their descendants).

### User Story 2 — `arctern configcheck <path>` validates a config without starting the daemon (Priority: P1)

A CI / pre-deploy script invokes `arctern configcheck /etc/arctern/arctern.toml`. The command parses + validates the file, prints either `ok` (exit 0) or a structured error (exit ≠ 0). It performs **no** ZFS operations and does **not** require the daemon to be running.

**Why this priority**: An operator must be able to validate a change before applying it. The slice 002 stub (`Configcheck { path }: not implemented`) becomes real here; this is the only blocker preventing the command from being a footgun.

**Independent Test**: `arctern configcheck` against a valid file exits 0 with `ok` on stdout. Against a malformed grid expression (`"6x4z"`), it exits non-zero with a stderr message naming the offending field.

**Acceptance Scenarios**:

1. **Given** a syntactically valid TOML file with one snap job, **When** the operator runs `arctern configcheck <path>`, **Then** it exits 0 and stdout is `ok\n`.
2. **Given** a file with a malformed grid spec (`"6x4z"`), **When** validated, **Then** the command exits non-zero and stderr contains the file path, the field path (e.g., `jobs[0].pruning.keep[0].grid`), and the parser's error message.
3. **Given** a file with a malformed regex in a keep rule, **When** validated, **Then** the command exits non-zero with the same shape of error.
4. **Given** a file whose `interval` value cannot be parsed by `humantime` (e.g., `"4 fortnights"`), **When** validated, **Then** the command exits non-zero with a message naming the field.
5. **Given** a file path that does not exist, **When** validated, **Then** the command exits non-zero with a clear "file not found" stderr line.

### User Story 3 — Operator can see job state via the HTTP API (Priority: P2)

After the daemon starts the snap jobs from the config, the operator wants to confirm they are scheduled and observe last-run / next-run / last-error per job without reading the log. They GET `/api/v1/jobs` over the UDS and receive a list of job statuses.

**Why this priority**: Constitution III ("Web UI Replaces the CLI") makes job status a UI concern, not a CLI concern. Slice 003 does not ship the UI yet, but it MUST ship the API the UI will consume. Skipping this would mean the only way to see the snap loop running is to grep logs.

**Independent Test**: With a daemon running a one-job config, `GET /api/v1/jobs` returns a JSON array containing one entry with `kind = "snap"`, `name = "<job-name>"`, and `last_run` / `next_run` populated after the first cycle.

**Acceptance Scenarios**:

1. **Given** the daemon is running with two configured snap jobs, **When** a client GETs `/api/v1/jobs`, **Then** the response is a 200 with a JSON array of two `JobStatus` entries; each includes `name`, `kind`, `last_run` (nullable RFC3339), `next_run` (nullable RFC3339), `last_error` (nullable string).
2. **Given** the daemon has not yet run any cycle (just started), **When** the client GETs `/api/v1/jobs`, **Then** entries appear with `last_run = null`, `next_run = <startup_time + interval>` (or null if startup-immediate-snapshot logic hasn't set it), `last_error = null`.
3. **Given** a job's most recent cycle errored, **When** the client GETs `/api/v1/jobs`, **Then** `last_error` is the error's `Display` string.

### User Story 4 — Example config translates the operator's existing zrepl jobs (Priority: P1)

A new arctern user reading the docs sees `docs/example-config.toml` and finds the `databak` + `rootbak` jobs from the reference zrepl YAML translated into arctern's schema, with comments explaining the mapping (especially the filesystem-filter syntax change).

**Why this priority**: The whole slice's worth depends on operators being able to translate their existing zrepl config. Without a working example, every adopter rediscovers the schema from the spec.

**Independent Test**: `cat docs/example-config.toml | cargo run -p arctern-daemon -- configcheck /dev/stdin` exits 0 (or an equivalent test in the test suite that loads the file and asserts it parses).

**Acceptance Scenarios**:

1. **Given** the example file exists in the repo at `docs/example-config.toml`, **When** `configcheck` is run against it, **Then** the command exits 0.
2. **Given** the example file, **When** an operator reads it, **Then** the `databak` job lists three filesystems (`okdata/data/nas`, `okdata/data/root`, `okdata/data/home`) with `interval = "4h"`, `prefix = "zrepl_"`, and pruning keep rules for `"6x4h | 14x1d"` plus a negate-regex rule.
3. **Given** the example file, **When** an operator reads the comments, **Then** the mapping from zrepl's `path<` recursive-include syntax to arctern's `recursive = true` + `exclude = [...]` is documented inline.

### Edge Cases

- **Config file missing AND no `--config` override**: daemon exits non-zero with stderr identifying the default path (`/etc/arctern/arctern.toml`) and suggesting `--config`. No silent default-empty config — the operator must be explicit.
- **Config file present but contains zero jobs**: daemon starts, logs `info` "no jobs configured", serves the HTTP API, and idles. Not an error (the operator may be staging a config). `GET /api/v1/jobs` returns `[]`.
- **Two jobs with the same `name`**: validation rejects with a clear error. Names are the public identifier in `/api/v1/jobs` and in log spans; uniqueness is required.
- **Filesystem filter matches zero datasets**: per-job warning at startup; the job still runs (so a future snapshot of a future-created dataset gets picked up), but every cycle logs an `info` "no datasets matched". Not fatal.
- **Snapshot name collision with a manually-taken snapshot of the same name**: `ZfsError::SnapshotExists` → log `warn`, treat as no-op, continue. The grid-prune step still runs and may include the manual snapshot in the keep set if the regex matches.
- **Pruning would destroy ALL snapshots matching the regex**: allowed (keep grid says so). The constitution does not require a "minimum keep" floor; that's a config concern.
- **Pruning a snapshot held via `zfs hold`**: `ZfsError::SnapshotHeld` → log `warn`, skip, continue. Holds are a deliberate user-side veto.
- **Clock skew between daemon and ZFS `creation` property**: the prune algorithm uses snapshot `creation` (Unix epoch seconds, parsed from the property) and treats the most recent `creation` as `now` (zrepl's behaviour). Daemon wall-clock is not consulted for prune decisions.
- **Recursive include with overlapping excludes**: `exclude` paths that are not descendants of the `path` are a config error, rejected at validation. `exclude = [path, ...]` (excluding the root itself) is allowed and means "snapshot only the descendants".
- **Grid expression with descending intervals not preceded by `keep=all`**: rejected at validation. Matches zrepl's monotonic-increase requirement (with the `keep=all` carve-out).
- **`(keep=all)` modifier**: required for the `4x15m(keep=all) | 24x1h | 3x1d` shape to validate, because 15m < 1h would otherwise violate monotonic-increase.
- **TOML-side typos** (e.g., `intevral = "4h"`): rejected by `serde(deny_unknown_fields)` at the top level and inside each job; error names the unrecognized key.

## Requirements *(mandatory)*

### Functional Requirements

#### Config loading

- **FR-001**: The daemon MUST load its configuration from a TOML file at startup. Resolution order: `--config <path>` flag (highest); else `/etc/arctern/arctern.toml`. NO XDG fallback, NO embedded default.
- **FR-002**: Failure to read or parse the config (file missing, IO error, syntactically invalid TOML, schema validation error) MUST cause the daemon to exit non-zero before any HTTP listener binds. The error message MUST name the file path; for parse errors it MUST also name the field path inside the file.
- **FR-003**: `arctern configcheck <path>` MUST run the same load + validate pipeline as `arctern daemon` and MUST NOT touch ZFS. On success it prints `ok\n` to stdout and exits 0; on failure it prints the error to stderr and exits 1.
- **FR-004**: Config types MUST live in their own crate `crates/config` (not in `crates/api`, not in `daemon`). The crate exposes a single entry point `arctern_config::load_from_path(&Path) -> Result<Config, ConfigError>` plus the public types it returns. Reason: future slices and the future `arctern-client` will consume these types (e.g., to surface a config-validate command over UDS, or to render the config viewer in the UI).
- **FR-005**: The config crate MUST NOT depend on `palimpsest`, `axum`, `tokio` runtime, or any IO crate beyond what's needed to read the file. (`std::fs::read_to_string` is fine; async file IO is unnecessary for a one-shot load.)

#### TOML schema

- **FR-006**: The top-level config MUST be `Config { jobs: Vec<JobConfig> }` with `#[serde(deny_unknown_fields)]`. Unknown top-level keys are an error.
- **FR-007**: `JobConfig` MUST be a tagged enum `#[serde(tag = "type", rename_all = "snake_case")]`. The only variant this slice MUST accept is `Snap(SnapJobConfig)`. Unknown `type` values MUST produce a clear validation error naming the supported variants.
- **FR-008**: `SnapJobConfig` MUST contain at minimum: `name: String`, `filesystems: Vec<FilesystemFilter>`, `snapshotting: SnapshottingConfig`, `pruning: PruningConfig`. `#[serde(deny_unknown_fields)]` applies.
- **FR-009**: `FilesystemFilter` MUST be `{ path: String, recursive: bool (default false), exclude: Vec<String> (default empty) }`. `recursive = false` makes `exclude` non-applicable (unused fields rejected by `deny_unknown_fields` are NOT triggered because the field exists; semantic validation rejects `exclude.non_empty() && !recursive`). When `recursive = true`, `exclude` paths MUST be descendants of `path` (or equal to `path` itself, meaning "snapshot only descendants"); validation rejects others.
- **FR-010**: `SnapshottingConfig` MUST be a tagged enum `{ type = "periodic", interval: Duration, prefix: String }`. `interval` deserializes via `humantime-serde` (supports `15m`, `4h`, `1d`, etc.). `prefix` MAY be empty; convention is to end with `_` (e.g., `arctern_`).
- **FR-011**: `PruningConfig` MUST be `{ keep: Vec<KeepRule> }`. Empty `keep` is allowed and means "keep nothing matching" — pruning is conservative *only* through explicit rules.
- **FR-012**: `KeepRule` MUST be a tagged enum `#[serde(tag = "type", rename_all = "snake_case")]` with two variants this slice: `Grid { grid: GridSpec, regex: String }` and `Regex { regex: String, negate: bool (default false) }`. `GridSpec` is a wrapper newtype around `Vec<RetentionInterval>` whose `Deserialize` impl parses the zrepl-style expression string. `regex` strings are compiled at validation time; compile failures abort startup.

#### Grid expression + retention algorithm

- **FR-013**: The grid expression parser MUST accept the zrepl syntax: `"<count>x<duration>(keep=<n>|all)? | ..."`. Examples that MUST parse: `"6x4h | 14x1d"`, `"7x1d"`, `"4x15m(keep=all) | 24x1h | 3x1d"`. Whitespace around `x`, `|`, and the parens is tolerated.
- **FR-014**: The grid expression validator MUST reject expressions whose interval lengths decrease without an all-`keep=all` prefix run (zrepl's monotonic-increase rule).
- **FR-015**: The retention algorithm MUST match zrepl's `retentiongrid` semantics: youngest matching snapshot defines `now`; entries are bucketed by interval-from-now; per-bucket `keep_count` decides which to retain; entries older than the oldest bucket are removed; entries dated in the future of `now` are unconditionally kept. Implementation may copy the algorithm verbatim — it is small and well-tested in zrepl.
- **FR-016**: The combined keep-rule evaluator MUST follow zrepl's "intersection" semantics: a snapshot is destroyed iff EVERY keep-rule's destroy-list contains it. The first slice's job loop calls each rule's `keep_rule(snaps) -> destroy_list`, intersects the destroy lists, then issues `zfs destroy` for each result.

#### Filesystem-filter resolution

- **FR-017**: At job-cycle start, the snap job MUST resolve its `Vec<FilesystemFilter>` to a concrete `Vec<String>` of dataset names by querying `palimpsest::dataset::list` once and applying the include + exclude rules locally. Reason: a single `zfs list -j` call with `recursive = true` per job per cycle is cheaper than one call per filter and yields a consistent snapshot of the namespace.
- **FR-018**: A `FilesystemFilter { path, recursive: false, .. }` MUST match exactly the dataset named `path`, no descendants.
- **FR-019**: A `FilesystemFilter { path, recursive: true, exclude }` MUST match `path` and every descendant, MINUS any dataset whose name equals an entry in `exclude` OR is a descendant of one. Excluding `path` itself means "all descendants but not the root".

#### Job runtime

- **FR-020**: Jobs MUST implement `trait Job: Send + Sync { async fn run(&self, ctx: JobContext, cancel: CancellationToken); fn name(&self) -> &str; fn kind(&self) -> JobKind; fn status(&self) -> JobStatus; }`. The `Job` trait lives in `daemon/src/jobs/mod.rs`.
- **FR-021**: A `JobManager` owned by the daemon MUST spawn each `Box<dyn Job>` as a `tokio::task::JoinHandle`, hold one `tokio_util::sync::CancellationToken` per job, and expose `fn statuses(&self) -> Vec<JobStatus>` for the HTTP handler.
- **FR-022**: On daemon shutdown (SIGTERM/SIGINT), the manager MUST trigger every job's cancellation token, then `join` every task with a 5s deadline. Tasks that miss the deadline are dropped (the runtime will abort them when the runtime shuts down); a `warn` is logged.
- **FR-023**: A snap job's run loop MUST:
  1. On entry, take an "immediate" snapshot pass if the most recent matching snapshot on every target filesystem is older than `interval` (FR-startup-immediate).
  2. Loop: `tokio::select! { _ = cancel.cancelled() => break, _ = tokio::time::sleep(interval) => {} }`. After sleep, run a snapshot pass + a prune pass. Update `last_run`, `next_run`, and `last_error` on the shared status.
  3. Each pass first resolves the filesystem filter (FR-017), then operates on the resolved list.
- **FR-024**: Snapshot creation MUST format the snapshot tag as `<prefix><utc-rfc3339-no-colons>` (e.g., `zrepl_20260509T134500Z`). Colons are stripped because some downstream tooling (Windows clients, certain shell pipelines) chokes on them; this is the convention zrepl uses and is wire-compatible at the metadata layer (constitution VII).
- **FR-025**: Snapshot creation goes through `palimpsest::dataset::snapshot` per filesystem (NOT `snapshot_many` — independent failures must not abort the pass). `ZfsError::SnapshotExists` MUST be treated as a no-op + `warn`. Other `ZfsError` variants are recorded in `last_error` for that cycle but do not stop the loop.
- **FR-026**: Prune execution MUST go through `palimpsest::dataset::destroy` per snapshot. `ZfsError::SnapshotHeld` MUST be treated as a no-op + `warn`. Other `ZfsError` variants are recorded in `last_error` for that cycle but do not stop the loop.
- **FR-027**: Each snapshot creation and destruction MUST emit a `tracing` event at INFO level inside a per-job `tracing::span!(Level::INFO, "snap_job", name = %job.name())` span. Events name the dataset and snapshot tag.

#### HTTP API

- **FR-028**: `GET /api/v1/jobs` MUST return `200 OK` with a JSON array of `JobStatus { name: String, kind: String, last_run: Option<String> /* RFC3339 */, next_run: Option<String> /* RFC3339 */, last_error: Option<String> }`. The endpoint reads from the JobManager's shared status snapshot.
- **FR-029**: `JobStatus` MUST live in `crates/api` (so the future TS client picks it up via OpenAPI). `JobKind` is a string in the wire type (not an enum) to keep the type stable across slices that add new job kinds.

#### Wiring

- **FR-030**: The `daemon` subcommand MUST accept an optional `--config <PATH>` flag. Default path is `/etc/arctern/arctern.toml`.
- **FR-031**: `run_daemon` MUST: (1) load + validate the config; (2) bind the UDS socket (slice-002 logic, unchanged); (3) construct the `palimpsest` runner; (4) instantiate the `JobManager` from the config and spawn each job; (5) install signal handlers (SIGTERM/SIGINT) that trigger graceful shutdown of both the HTTP server and the JobManager; (6) `axum::serve` the router (which now includes the `/api/v1/jobs` route).
- **FR-032**: The `palimpsest` runner injection MUST be plumbed via the `JobContext` so jobs do not construct their own runners. Today (slice 002) handlers construct `SshCommandRunner::from_env()` per request; slice 003 keeps that for handlers but introduces a single shared runner for jobs (constructed once at startup). This is a planning-side decision (see plan.md D14).

### Non-Functional Requirements

- **NFR-001**: Total slice size: ~1500-2200 LoC of Rust + spec-kit artifacts + ~100 LoC of example TOML. The grid parser + retention algorithm are ~250 LoC together; JobManager + SnapJob ~300; config types + validation ~300; the rest is wiring and tests.
- **NFR-002**: NO `tokio::process::Command` calls inside `daemon/`, `crates/api/`, or `crates/client/` (constitution principle IV, carried forward). `crates/config` MAY use `regex::` for compile-time grid + keep-rule validation — that is config parsing, not ZFS invocation, and is explicitly allowed (see plan.md D13).
- **NFR-003**: NO `anyhow`/`eyre` in `crates/config`, `crates/api`, or `crates/client`. The daemon binary keeps `eyre` for top-level reporting only.
- **NFR-004**: Per-job background tasks MUST NOT block the runtime. Snapshot + destroy go through the existing `palimpsest` async APIs; status reads go through `parking_lot::Mutex` or `std::sync::Mutex` held only briefly (microseconds); no `std::thread::sleep` / `std::sync::mpsc::recv` in the hot path.
- **NFR-005**: The integration test for the snap loop MUST take ≤10 seconds wall-clock. With `interval = 1s`, three cycles in three seconds + setup/teardown fits this budget.

### Key Entities

- **`Config`** (in `crates/config`): `{ jobs: Vec<JobConfig> }`. Top-level deserialized type.
- **`JobConfig`** (in `crates/config`): tagged enum, `Snap(SnapJobConfig)` only this slice.
- **`SnapJobConfig`** (in `crates/config`): `{ name, filesystems, snapshotting, pruning }`.
- **`FilesystemFilter`** (in `crates/config`): `{ path, recursive, exclude }`.
- **`SnapshottingConfig`** (in `crates/config`): tagged enum, `Periodic { interval: Duration, prefix: String }`.
- **`PruningConfig`** + **`KeepRule`** + **`GridSpec`** + **`RetentionInterval`** (in `crates/config`): see FR-011 to FR-014.
- **`Job` trait** + **`JobManager`** + **`JobContext`** (in `daemon/src/jobs/`): see FR-020 to FR-022.
- **`SnapJob`** (in `daemon/src/jobs/snap.rs`): concrete `Job` impl wrapping `SnapJobConfig` + a runner.
- **`JobStatus`** (in `crates/api`): wire type for `GET /api/v1/jobs` (see FR-028).
- **`JobKind`** (in `crates/api`): convenience constants (`pub const JOB_KIND_SNAP: &str = "snap"`); the wire field is `String` (FR-029).
- **`PrunePolicy`** (in `crates/config` or a new `crates/pruning` — planning-side decision): the resolved keep-rule chain that takes a `Vec<SnapshotEntry>` and returns the destroy set.
- **`SnapshotEntry`** (in `daemon/src/jobs/snap.rs` or `crates/pruning`): minimal `{ name: String, creation: SystemTime }` — the input to the prune algorithm. Source: `palimpsest::dataset::list` filtered to type=snapshot.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `cargo check --workspace`, `cargo clippy --workspace --all-targets --features integration -- -D warnings`, and `cargo test --workspace` all exit 0 on the resulting branch.
- **SC-002**: With `just vm-up` running, `just test-integration` exits 0 across 3 consecutive runs without flake. (The new snap-loop integration test is the only test added this slice.)
- **SC-003**: A snap job with `interval = "1s"` running for 3 seconds against a loopback pool produces ≥2 snapshots whose names start with the configured prefix, observable via `palimpsest::dataset::list`.
- **SC-004**: The grid retention algorithm's unit tests (against fixtures with synthetic snapshot timestamps) MUST exactly match the keep / destroy classifications zrepl's algorithm produces for the same inputs. The fixtures are derived from the zrepl test cases and committed alongside the algorithm.
- **SC-005**: `arctern configcheck docs/example-config.toml` exits 0. The example file expresses the user's `databak` + `rootbak` jobs verbatim (modulo the filesystem-filter syntax change).
- **SC-006**: `arctern configcheck` against a config with a malformed grid expression exits non-zero AND the stderr names the offending field path (e.g., `jobs[0].pruning.keep[0].grid`).
- **SC-007**: Constitution-IV grep: `! grep -RnE 'tokio::process::Command' --include='*.rs' crates/api crates/client daemon/src/` AND `! grep -RnE '^use regex' --include='*.rs' crates/api crates/client daemon/src/` both exit 0. The `crates/config` crate is explicitly excluded — config parsing regex is allowed (see plan.md D13).
- **SC-008**: `GET /api/v1/jobs` over UDS returns a JSON array whose length matches the configured-job count, and entries decode as `arctern_api::JobStatus`.

## Assumptions

- The integration test VM (port 2226) is up to date with palimpsest's expectations from slices 001 + 002. No new VM-side requirements.
- ZFS's `creation` property (Unix epoch seconds when fetched with `-p`) is available on every snapshot via `palimpsest::dataset::list` with `properties = vec!["creation".into()]`. Snapshot ordering for the prune algorithm uses this value.
- The user's existing zrepl install snapshots roughly hourly across ≤10 datasets, prunes a ≤20-snapshot keep set per dataset. Slice 003's algorithm is O(n log n) in snapshot count per cycle and trivially handles thousands of snapshots per dataset; no scaling concerns.
- Default `/etc/arctern/arctern.toml` is acceptable for a single-host deploy. Per-user XDG configs can land in a future slice without breaking anyone (the resolution order would extend, not change).
- `humantime-serde` parses `15m`, `4h`, `1d` to `std::time::Duration`. We do NOT need sub-second precision; the smallest interval users will configure is minutes. Test-side intervals of `1s` are within `humantime`'s grammar.
- Snapshot tags using `T<HHMM>SS` (no colons) are accepted by ZFS (verified — colons are not legal anyway).

## Out of scope (Non-Goals)

These are deliberately deferred and MUST NOT creep into this slice:

- `push`, `pull`, `source`, `sink` job types. Land in slice 004+.
- Replication cursor management / `zrepl_CURSOR_*` bookmarks.
- `zfs send` / `zfs recv` over the wire.
- Per-job hooks (pre-snapshot / post-snapshot shell scripts).
- Bandwidth-limit configuration.
- `zfs hold` integration on snap jobs (zrepl uses holds to defend in-flight replicated snapshots; snap jobs don't replicate, so no holds this slice).
- Manual snapshot trigger via the API (`POST /api/v1/jobs/{name}/wakeup`) — useful but not required to prove the loop. Land in slice 004 or 005.
- A `keep_last_n` rule. zrepl has it; arctern can add it later. Slice 003's two rules (grid + regex) cover the user's actual jobs.
- A `keep_not_replicated` rule. Replication-aware; lands when push/pull does.
- Config hot-reload (SIGHUP). The daemon today loads config once at startup; reload restarts the daemon. Hot-reload can land later without breaking the schema.
- The Vue 3 admin UI. The `/api/v1/jobs` endpoint exists this slice; the UI consuming it lands in slice 005+.
- macOS / BSD support (carried over from slice 002's note).
