# Implementation Plan: Config file + periodic snap jobs with grid pruning

**Branch**: `003-config-and-snap-job` | **Date**: 2026-05-09 | **Spec**: [spec.md](./spec.md)
**Input**: `specs/003-config-and-snap-job/spec.md`

## Summary

Land a TOML config (`/etc/arctern/arctern.toml`, override `--config`), parse it via serde in a new `crates/config` workspace member, then run `Snap` jobs as background tokio tasks owned by a `JobManager`. Each snap job loops on a `humantime`-style interval, snapshots the configured filesystems with a `<prefix><utc-rfc3339-no-colons>` tag, and prunes per a hand-rolled port of zrepl's grid-retention algorithm. `arctern configcheck <path>` becomes real (parses + validates without touching ZFS). `GET /api/v1/jobs` returns per-job status. Networking, replication, hooks, and bandwidth limits stay deferred.

## Technical Context

**Language/Version**: Rust 1.95, edition 2024.
**Primary Dependencies**: existing `axum` 0.8, `clap`, `tokio`, `tracing`, `serde`, `palimpsest`, `utoipa`, `utoipa-axum`. New (added via `cargo add` per CLAUDE.md): `toml` (TOML parse), `humantime-serde` (Duration parse), `regex` (in `crates/config` only), `tokio-util` (`CancellationToken`), `time` (RFC3339 formatting + parsing snapshot `creation` epoch).
**Storage**: TOML config on disk; ZFS metadata as the source of truth for snapshot state.
**Testing**: `cargo test --workspace` for unit tests (grid parser, retention algorithm, filter resolver, schema round-trip). `cargo test -p arctern-daemon --features integration -- --test-threads=1` for the snap-loop integration test against the palimpsest VM.
**Target Platform**: Linux x86_64 (carried over).
**Project Type**: Cargo workspace (slice 001's shape — `crates/api`, `crates/client`, `daemon` — gains `crates/config`).
**Performance Goals**: A snap job cycle's wall-clock is dominated by `zfs snapshot` (per-FS) + `zfs destroy` (per-victim). The arctern-side overhead (filter resolve + grid algorithm + status update) is microseconds for hundreds of snapshots and not optimised further.
**Constraints**: Constitution principles I-V apply — see Constitution Check. Async-only. No `tokio::process::Command` in arctern source. `regex` allowed in `crates/config` (config parsing is not ZFS invocation; see D13).
**Scale/Scope**: ~1500-2200 LoC arctern source + tests + ~100 LoC of example TOML.

## Constitution Check

*GATE: passes before implementation.*

| Principle | Compliance |
|---|---|
| I. QUIC With HTTP Semantics | Not applicable this slice (no daemon-to-daemon RPC; snap is local-only per the constitution's job table). |
| II. One API for Browser and Daemons | `JobStatus` lives in `crates/api` with `serde + utoipa::ToSchema`. The handler and the future TS client both consume it. |
| III. Web UI Replaces the CLI | `GET /api/v1/jobs` is the surface a future UI consumes. The only new CLI work is making `configcheck` real (it was already declared in slice 002 as a stub) — that's a pre-deploy validator the constitution explicitly carves out (III). |
| IV. ZFS Through palimpsest | Snap job calls `palimpsest::dataset::snapshot`, `::destroy`, `::list`. NO `tokio::process::Command` or stderr regex in arctern source. `crates/config` uses `regex` for config parsing (`grid` expression + `KeepRule { regex }`); this is config validation, not ZFS invocation, and the constitution-IV grep gate explicitly excludes `crates/config` (D13). |
| V. Local-Only by Default, Auth Opt-In | Slice 002's UDS + peer-uid setup is unchanged. `GET /api/v1/jobs` inherits the same-uid layer. |
| VI. Live Data Over SSE | Not applicable this slice. A future slice MAY add an SSE topic for job-progress events; the polling `GET /api/v1/jobs` shipped here is the v0 surface. |
| VII. ZFS Metadata Compatibility | Snapshot-tag format is the zrepl convention (`<prefix><RFC3339-no-colons>`). The grid-retention algorithm matches zrepl's semantics so the user's existing keep-rule expectations hold. NOT wire-compatible with zrepl's YAML. |

All applicable principles pass. Deferred work for I, V (multi-uid), and VI tracked in spec's Non-Goals.

## Project Structure

### Documentation (this feature)

```text
specs/003-config-and-snap-job/
├── spec.md     # done
├── plan.md     # this file
├── tasks.md    # next, via speckit-tasks
└── checklists/ # (optional)
```

### Source code (repository root)

```text
arctern/
├── crates/
│   ├── api/src/lib.rs          # add JobStatus + (string consts) JOB_KIND_SNAP
│   ├── client/                 # unchanged this slice (could add list_jobs() but not in scope)
│   └── config/                 # NEW workspace member
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs          # public Config + load_from_path + ConfigError
│           ├── schema.rs       # Config / JobConfig / SnapJobConfig / FilesystemFilter / SnapshottingConfig / PruningConfig / KeepRule
│           ├── filter.rs       # FilesystemFilter::resolve(&[ZfsListEntry]) -> Vec<String>
│           ├── grid/
│           │   ├── mod.rs      # GridSpec + RetentionInterval + parser
│           │   └── retention.rs  # the FitEntries / bucketing algorithm (port of zrepl/internal/pruning/retentiongrid)
│           └── prune.rs        # KeepRule::keep_rule(&[SnapshotEntry]) -> destroy_set;
│                               # PrunePolicy::evaluate(&[SnapshotEntry]) -> destroy_set (intersection of keep rules)
├── daemon/
│   ├── Cargo.toml              # +arctern-config, +tokio-util (CancellationToken), +time
│   ├── src/
│   │   ├── main.rs             # add --config; load it; build JobManager; spawn jobs; signal-handle both
│   │   ├── router.rs           # register GET /api/v1/jobs
│   │   ├── auth.rs             # unchanged
│   │   ├── error.rs            # unchanged
│   │   ├── handlers/
│   │   │   ├── mod.rs          # +pub mod jobs;
│   │   │   ├── datasets.rs     # unchanged
│   │   │   ├── snapshots.rs    # unchanged
│   │   │   └── jobs.rs         # NEW: GET /api/v1/jobs handler reading from JobManager
│   │   ├── jobs/
│   │   │   ├── mod.rs          # NEW: Job trait, JobManager, JobContext, JobStatusInner
│   │   │   └── snap.rs         # NEW: SnapJob impl
│   │   └── configcheck.rs      # NEW: arctern configcheck implementation (load + validate)
│   └── tests/
│       ├── common/mod.rs       # extend spawn_daemon_uds to accept a config-path arg
│       └── integration_snap_job.rs   # NEW: end-to-end snap-loop test against loopback pool
└── docs/
    └── example-config.toml     # NEW: translates the user's databak + rootbak from zrepl
```

**Structure Decision**:

- Config types are their own crate (D2 from the slice ticket, formalized as FR-004). The crate is leaf — no `tokio`, no `palimpsest`. Future slices and the CLI can depend on it.
- Pruning lives **in `crates/config`** rather than a separate `crates/pruning` crate. Reason: the grid expression is config (parsed at startup, validated by `configcheck`); the retention algorithm operates on a tiny `SnapshotEntry { name, creation }` shape that needs no ZFS coupling. Separating "parse" from "evaluate" into two crates would create circular API surfaces (the schema needs the parsed grid; the algorithm needs the schema's grid) for no benefit. If a third consumer ever needs the algorithm without the schema, splitting then is cheap.
- `JobManager` and the `Job` trait live in `daemon/` not `crates/`. They're the daemon's internal scheduling concern; no other crate should construct jobs. The `JobStatus` *wire* type is in `crates/api` (consumed by clients); the *internal* `JobStatusInner` (with `Mutex` and live state) is in `daemon`.

## Phase 0: Research

Spot-checks done at planning time:

- **`toml` crate (0.9)**: `toml::from_str::<Config>(&contents)` returns `Result<Config, toml::de::Error>`. Errors carry `.span()` (line/column) which we surface in `ConfigError`. Tagged enums with `#[serde(tag = "type")]` are supported and produce field-path-shaped errors out of the box.
- **`humantime-serde`**: deserializes a string like `"4h"` into `std::time::Duration` via `#[serde(with = "humantime_serde")]`. Use `humantime_serde::deserialize` in a custom `Deserialize` if we ever need it on a non-field; field-level is sufficient here.
- **zrepl's grid algorithm** (`internal/pruning/retentiongrid/retentiongrid.go`, ~70 lines): direct port. The algorithm uses the youngest matching snapshot's date as `now`, then partitions entries into buckets by interval-from-now. Per-bucket `keep_count == -1` means "keep all in this bucket". A snapshot dated *after* `now` (clock skew on a remote system) is unconditionally kept. The Rust port replaces `time.Time` with `time::OffsetDateTime` and `time.Duration` with `time::Duration`.
- **zrepl's grid expression parser** (`internal/config/retentiongrid.go`, ~120 lines): regex-based. The pattern `^\s*(\d+)\s*x\s*([^(]+)\s*(\((.*)\))?\s*$` matches one term; `|` splits terms. `(keep=N)` or `(keep=all)` is the only modifier. The Rust port uses `regex::Regex` + `humantime::parse_duration` for the duration component (zrepl's `parsePositiveDuration` is roughly equivalent).
- **zrepl's keep-rule combination semantics** (`internal/pruning/pruning.go`, function `PruneSnapshots`): a snapshot is destroyed iff EVERY rule's `KeepRule(snaps)` destroy-list contains it. This intersection means "every rule must agree to delete". Matches the user's expectation: a regex-negate rule's "keep everything not matching `^zrepl_.*`" combines with the grid's "keep some matching `^zrepl_.*`" to mean "keep everything not matching, plus keep what the grid keeps" — i.e., destroy only matching snapshots that the grid says are surplus. Crucial; getting this wrong would delete the user's manual snapshots.
- **`tokio::time::sleep` cancellability**: `tokio::select! { _ = cancel.cancelled() => break, _ = tokio::time::sleep(d) => {} }` is the canonical shape. `CancellationToken::cancelled()` is borrow-friendly.
- **axum + shared state**: `Router::with_state(Arc<AppState>)` carries the `JobManager` handle into the `GET /api/v1/jobs` handler. Slice 002's handlers don't use shared state; slice 003 introduces it for the jobs handler. Datasets/snapshots handlers stay state-free this slice (they construct a runner per request as before; see D14).
- **palimpsest::dataset::list properties**: pass `properties: vec!["creation".into(), "name".into(), "type".into()]` to receive `creation` as a Unix-epoch-seconds string in `entry.properties.get("creation").value`. We parse it via `i64::from_str` then `OffsetDateTime::from_unix_timestamp`.
- **Snapshot tag format**: `time::OffsetDateTime::now_utc().format(&Rfc3339)?` then strip `:`. e.g., `2026-05-09T13:45:00Z` → `20260509T134500Z` after stripping `:` and `-` … actually we keep dashes since they are URL-safe and conventional in zrepl. Final shape: `2026-05-09T134500Z` (only colons stripped). zrepl uses `2006-01-02T15:04:05Z` and strips colons; we match.

## Phase 1: Design artifacts

### TOML schema (excerpt)

```toml
# Top-level
[[jobs]]
type = "snap"
name = "databak"

# One [[jobs.filesystems]] table per filter (no path-as-key map).
[[jobs.filesystems]]
path = "okdata/data/nas"
[[jobs.filesystems]]
path = "okdata/data/root"
[[jobs.filesystems]]
path = "okdata/data/home"

[jobs.snapshotting]
type = "periodic"
interval = "4h"
prefix = "zrepl_"

[[jobs.pruning.keep]]
type = "grid"
grid = "6x4h | 14x1d"
regex = "^zrepl_.*"

[[jobs.pruning.keep]]
type = "regex"
regex = "^zrepl_.*"
negate = true
```

Recursive include with explicit excludes:

```toml
[[jobs.filesystems]]
path = "novafs/arch0"
recursive = true
exclude = ["novafs/arch0", "novafs/arch0/data"]
```

### Rust types (excerpt)

```rust
// crates/config/src/schema.rs
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobConfig {
    Snap(SnapJobConfig),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapJobConfig {
    pub name: String,
    pub filesystems: Vec<FilesystemFilter>,
    pub snapshotting: SnapshottingConfig,
    pub pruning: PruningConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemFilter {
    pub path: String,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshottingConfig {
    Periodic {
        #[serde(with = "humantime_serde")]
        interval: Duration,
        prefix: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PruningConfig {
    #[serde(default)]
    pub keep: Vec<KeepRule>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum KeepRule {
    Grid { grid: GridSpec, regex: String },
    Regex { regex: String, #[serde(default)] negate: bool },
}
```

### API contract additions

`GET /api/v1/jobs` → `200 OK` with body:

```json
[
  { "name": "databak", "kind": "snap", "last_run": "2026-05-09T13:45:00Z",
    "next_run": "2026-05-09T17:45:00Z", "last_error": null }
]
```

`JobStatus` lives in `crates/api`. Errors mappings unchanged.

### Quickstart (developer)

```bash
cd ~/code/palimpsest && just vm-up
cd ~/code/arctern
cat > /tmp/arctern-snap.toml <<'EOF'
[[jobs]]
type = "snap"
name = "smoke"
[[jobs.filesystems]]
path = "tank"
[jobs.snapshotting]
type = "periodic"
interval = "1s"
prefix = "smoke_"
[[jobs.pruning.keep]]
type = "grid"
grid = "3x1s"
regex = "^smoke_.*"
EOF
PALIMPSEST_SSH_TARGET=root@localhost:2226 PALIMPSEST_SSH_PASSWORD="" \
  cargo run -p arctern-daemon -- daemon --config /tmp/arctern-snap.toml \
    --socket /tmp/arctern.sock &
sleep 5
curl --unix-socket /tmp/arctern.sock http://_/api/v1/jobs | jq .
kill %1
cd ~/code/palimpsest && just vm-down
```

CI:

```bash
cd ~/code/arctern
just test-vm
```

## Phase 2: Tasks

Generated by `speckit-tasks` into `specs/003-*/tasks.md`. Expected ordering (12 tasks):

1. T001 — `chore(workspace)`: add `crates/config` to the workspace.
2. T002 — `feat(config)`: TOML schema + serde + humantime-serde + `load_from_path`.
3. T003 — `feat(config)`: `FilesystemFilter::resolve` against a `&[ZfsListEntry]`-shaped input.
4. T004 — `feat(pruning)`: grid expression parser (`6x4h | 14x1d`).
5. T005 — `feat(pruning)`: grid retention algorithm + `KeepRule` evaluator + `PrunePolicy` intersection.
6. T006 — `feat(daemon)`: `Job` trait + `JobManager` + `JobContext` + `CancellationToken` lifecycle.
7. T007 — `feat(daemon)`: `SnapJob` implementation.
8. T008 — `feat(daemon)`: `--config` flag + load + spawn jobs in `run_daemon`.
9. T009 — `feat(daemon)`: `configcheck` subcommand actually validates.
10. T010 — `feat(api)`: `JobStatus` type + `GET /api/v1/jobs` handler.
11. T011 — `docs`: `example-config.toml` translating `databak` + `rootbak`.
12. T012 — `test(integration)`: snap job end-to-end against loopback pool.

## Risks

- **Grid algorithm divergence from zrepl**: subtle bucketing/edge behaviour (e.g., snapshots exactly at a bucket boundary) could yield different keep sets than zrepl. Mitigation: port the algorithm faithfully, then write unit tests with fixtures whose expected outputs come from cross-checking against zrepl's own test cases.
- **Filesystem-filter resolution semantics**: zrepl's `path<` recursive operator vs arctern's `recursive = true` could trip up users. Mitigation: example config documents the mapping inline; validation rejects nonsense (`exclude.non_empty() && !recursive`, `exclude` not a descendant of `path`).
- **Per-job runner lifetime**: the `palimpsest::SshCommandRunner` is constructed per request in slice 002. Slice 003 needs a long-lived runner per job. If `SshCommandRunner` is `!Sync` or holds a connection that times out, the job loop must reconstruct on error. Mitigation: D14 holds the runner in `Arc` per job; if the runner turns out to be problematic, the per-cycle reconstruction cost is negligible (it's a struct, not a connection).
- **Snapshot creation racing with manual user**: handled by FR-025 (treat `SnapshotExists` as no-op).
- **Integration test flakes from `interval = "1s"` + slow VM**: 3-second budget gives ~3 cycles. If the VM is slow, ≥2 snapshots is a more lenient assertion than ≥3 — chosen deliberately. Mitigation: assertion is `>= 2`, not exact count.
- **`time` crate vs `chrono` choice**: stdlib `SystemTime` is awkward for RFC3339 formatting. `time` 0.3 covers both formatting and Unix-epoch conversion in a small dep footprint. `chrono` is heavier and pulls more transitive deps; avoid.

## Decisions made beyond the slice ticket's D1-D12

- **D13** (added at planning, in spec NFR-002 + plan): the constitution-IV grep gates exclude `crates/config`. Grid expression parsing and keep-rule regex compilation are config validation, not ZFS invocation. The CLAUDE.md ground rules already permit this; codifying it here so future contributors don't accidentally tighten the gate.
- **D14** (added at planning): the `JobContext` carries an `Arc<dyn CommandRunner>` constructed once at startup from `palimpsest::SshCommandRunner::from_env()`. HTTP handlers (slice 001 + 002) keep their per-request construction unchanged — refactoring them to share the runner is out of scope (one-line change, but each touch invites scope creep). The shared runner lives in `AppState` alongside `Arc<JobManager>` so the jobs handler can reach it (only the JobManager handle is actually needed for `/api/v1/jobs`; the runner is there for future handlers in 004+).
- **D15** (added at planning): pruning lives in `crates/config` (see Structure Decision rationale). Splitting later is cheap; splitting now creates two crates with identical API surface.
- **D16** (added at planning): job kind is a `String` on the wire (`JobStatus.kind`), not a Rust enum. Rationale: every future job kind addition would otherwise be a wire-breaking enum extension. Constants live in `crates/api` for the daemon to use; clients compare strings. Two slices from now this might warrant promoting to a string-backed enum in OpenAPI, but not yet.
- **D17** (added at planning): no per-job log file or per-job ring buffer this slice. All job events go through the global `tracing` subscriber. The constitution's SSE log-tail (principle VI) lands in a future slice and will subscribe to the same subscriber's output.
- **D18** (added at planning): the snap loop's "startup-immediate" check (FR-023.1) compares the youngest matching snapshot's `creation` time to `interval` ago. If no matching snapshot exists at all, the job takes an immediate snapshot (zrepl's behaviour). This is what users expect after a fresh install: snapshots start happening, not "wait 4 hours then start".
- **D19** (added at planning): when validation fails, errors carry a Display impl that prints `<file_path>: <field_path>: <message>`. The field path comes from `toml::de::Error::span()` for serde errors and from a hand-built path (`jobs[N].pruning.keep[M].grid`) for our own semantic checks. The CLI binary's `eyre::Result` propagation handles printing.
- **D20** (added at planning): the integration test creates the test pool with `tank/data` as a child filesystem (so the snap-job filter has an interesting target) but keeps the per-test pool naming convention from slices 001 + 002. The test only asserts ≥2 snapshots, NOT exactly N, to absorb VM jitter (per Risks).

## Verification

```bash
# Inside arctern repo
cargo check --workspace
cargo clippy --workspace --all-targets --features integration -- -D warnings
cargo test --workspace                          # unit tests (incl. crates/config + crates/api)

# Constitution principle IV gates (D13 — exclude crates/config)
! grep -RnE 'tokio::process::Command' --include='*.rs' crates/api crates/client daemon/src/
! grep -RnE '^use regex' --include='*.rs' crates/api crates/client daemon/src/

# Integration (requires VM)
just vm-up
just test-integration
just vm-down
```
