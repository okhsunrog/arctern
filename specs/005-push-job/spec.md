# Feature Specification: Push job — active sender + LIST-based discovery

**Feature Branch**: `005-push-job`
**Created**: 2026-05-09
**Status**: Draft
**Input**: Slice 005 of arctern. Add a `push` job type that periodically replicates snapshots from a sender to a peer running a `sink` job. Adds an `op = "list"` request to the wire protocol so the sender can discover what the receiver already has, intersect by GUID, and decide between full and incremental sends. End-to-end: with sender + receiver both running arctern, snapshots created by a snap job on the sender land on the receiver under `root_fs`. Replaces zrepl's `internal/replication/`, `internal/daemon/job/active.go` (push side), and the planner+executor in `internal/replication/driver/`.

## Why this slice

Slice 004 stood up the passive receive plane. Slice 005 closes the loop with the active send plane: snap jobs already write `zrepl_<ts>` snapshots locally; this slice lets a peer pull them across QUIC into the sink's pool. The minimum that has to land for arctern to become an actual replication tool — not a snapshotter + receiver in separate boxes that don't talk — is one push job type, one new request shape on the existing QUIC transport, and a planner that picks the next snapshot to send.

zrepl uses a multi-step replication protocol with cursor bookmarks (`#zrepl_CURSOR_<guid>` planted on the sender, walked back by the receiver) plus a sequence of typed RPC calls (Hello/ListFilesystems/ListVersions/Send/Receive). arctern collapses this to two QUIC stream shapes (`op = "list"` and `op = "send"`) and uses the receiver's actual snapshot list — read fresh each cycle — as the source of truth. **No cursor bookmarks are planted.** Round-trip cost is single-digit ms over WireGuard; replication cycles are minutes apart. The simplification is paid for at planning time, not at design or migration time.

The discovery model deliberately intersects by GUID, not name. ZFS snapshot names can collide (a manual `tank/data@snap1` and an arctern-created `tank/data@snap1` are different snapshots with different GUIDs); only the GUID is provably the same identity across pools. Replication state is therefore correct even if an operator destroys + recreates a snapshot with the same name on one side.

## User Scenarios & Testing *(mandatory)*

The "user" is the operator who runs arctern on a host with valuable filesystems and wants snapshots replicated to a separate backup box that already runs an arctern sink. They keep a [[jobs]] type = "snap" block from slice 003 and add a [[jobs]] type = "push" block alongside it. The sink box's IP + QUIC port go in `connect`. Restart the daemon. Snapshots flow.

### User Story 1 — Operator declares a push job and the sender's snapshots appear on the receiver (Priority: P1)

An operator's sender daemon runs a snap job (slice 003) that creates `zrepl_<ts>` snapshots on `okdata/data/home`. They add a push job pointing at a sink at `10.77.77.100:8888` with `root_fs = "okdata/backups/laptop"`. The push job's interval fires; it lists local matching snapshots, opens a QUIC stream, sends a `{op: "list", target_dataset: "okdata/backups/laptop/okdata/data/home", prefix_regex: "^zrepl_.*"}` request, the receiver returns `{status: "ok", snapshots: []}` (no replica yet), the push job opens a second QUIC stream and full-sends the latest snapshot. Next cycle, the receiver returns the GUID of the previously-received snapshot; the planner intersects with the sender's current list, picks the highest common GUID as the from-snap, and incrementally sends the new snapshot.

**Why this priority**: This is the slice. Resume tokens, parallelism, retry are slice 006+.

**Independent Test**: Two `LoopbackPool`s in the same VM as `sender_pool` and `receiver_pool`. Two arctern daemons on the host pointing at the same VM with different `state_dir`, UDS path, and (for the sink daemon) QUIC port. Pre-create `<sender_pool>/data` on the sender, snapshot it once via the slice-002 API to get `<sender_pool>/data@zrepl_<ts1>`. Trigger the push job (`POST /api/v1/jobs/<name>/wakeup` — see User Story 4). Assert `<receiver_pool>/sink/<sender_pool>/data@zrepl_<ts1>` exists. Snapshot again on the sender. Trigger again. Assert `<receiver_pool>/sink/<sender_pool>/data@zrepl_<ts2>` is the only second snapshot present (confirming incremental sent the right delta).

**Acceptance Scenarios**:

1. **Given** a sender with one matching snapshot and a receiver whose target dataset does not exist yet, **When** the push cycle runs, **Then** the receiver returns `snapshots: []`, the planner emits a full send, the executor pipes the bytes over QUIC, the receiver responds `{status: "ok"}`, and the target dataset + snapshot exist on the receiver pool with the same GUID as the sender's snapshot.
2. **Given** the receiver already has the sender's last replicated snapshot (matched by GUID) and the sender has one newer matching snapshot, **When** the push cycle runs, **Then** the planner picks the highest common-GUID snapshot as the incremental from-snap and emits an incremental send, and the receiver gains exactly one new snapshot.
3. **Given** the sender has no matching snapshots on a configured filesystem (e.g., user only created snapshots under a different prefix), **When** the cycle runs, **Then** the cycle logs "nothing to replicate" for that filesystem and continues to the next, no QUIC streams are opened for that filesystem, and `JobStatus.last_error` is None.
4. **Given** the receiver has snapshots that share NO GUID with the sender (e.g., the receiver dataset was rolled back by hand), **When** the cycle runs, **Then** the planner falls back to full send of the sender's latest matching snapshot. The receiver fails the recv (incoming stream's GUID conflicts with existing data) — the per-fs cycle logs the receiver's error and continues. Recovery is operator-driven (destroy + retry, or use `-F` in a future slice). The cycle does NOT silently destroy the receiver's data.
5. **Given** one of three configured filesystems fails (e.g., receiver returns `{status: "error", message: "permission denied"}`), **When** the cycle finishes, **Then** the other two filesystems were processed normally, `JobStatus.last_run` is set to cycle end, `JobStatus.last_error` carries a summary string naming the failed filesystem and the receiver's message, and the next cycle replans from current receiver state — there is no in-cycle retry.
6. **Given** the daemon receives `SIGTERM` mid-cycle (specifically: while a `zfs send` is streaming bytes into a QUIC stream), **When** it shuts down, **Then** the cancellation token cancels the per-fs task, the QUIC stream is dropped (which causes the sender's `tokio::io::copy` to error with broken-pipe), the `zfs send` ChildHandle's `kill_on_drop` terminates the process, and the daemon exits within the JobManager's deadline.
7. **Given** a sender's source dataset has a snapshot whose GUID exceeds `i64::MAX` (a normal occurrence — ZFS GUIDs are u64), **When** the planner parses the LIST response, **Then** parsing succeeds and the GUID is preserved exactly through the intersection step.

### User Story 2 — `arctern configcheck` validates push jobs (Priority: P1)

A pre-deploy script invokes `arctern configcheck /etc/arctern/arctern.toml` against a config that contains a push job. The command parses + validates (push-specific fields: `connect` parses as `SocketAddr`, `interval` parses as humantime, `target.root_fs` non-empty + no leading/trailing `/`, snapshot_filter is either `prefix` xor `regex`, regex compiles), prints `ok`, exits 0; on failure exits non-zero with the offending field in stderr.

**Why this priority**: A config that ships to production must be validatable without standing the daemon up — same rationale as slices 003 + 004 for snap and sink.

**Independent Test**: `cargo run -p arctern-daemon -- configcheck` against a valid push config exits 0; against `connect = "not-a-socket-addr"` exits non-zero naming the field; against `target.root_fs = ""` exits non-zero naming the field.

**Acceptance Scenarios**:

1. **Given** a TOML config with a syntactically valid push job, **When** `configcheck` runs, **Then** it exits 0 with `ok`.
2. **Given** a push job with `connect = "not-an-addr"`, **When** validated, **Then** the command exits non-zero with stderr naming `jobs[N].connect`.
3. **Given** a push job with `target.root_fs = ""`, **When** validated, **Then** the command exits non-zero naming `jobs[N].target.root_fs`.
4. **Given** a push job whose `snapshot_filter` declares both `prefix` AND `regex`, **When** validated, **Then** the command rejects the config naming the conflict.
5. **Given** a push job whose `snapshot_filter.regex` does not compile, **When** validated, **Then** the command rejects the config naming the regex error.

### User Story 3 — `GET /api/v1/jobs` reports push jobs (Priority: P2)

The dashboard polls `GET /api/v1/jobs` and gets back one entry per configured job. After this slice, `kind = "push"` is one of the strings the field can carry. `last_run`, `next_run`, and `last_error` follow the same conventions as snap / sink: `last_run` is the wall-clock end of the last cycle, `next_run` = `last_run + interval`, `last_error` is None on a clean cycle or a summary string on partial / total failure.

**Why this priority**: Visibility — operators need to know when push last ran and whether the last cycle errored.

**Acceptance Scenarios**:

1. **Given** a push job has run at least one cycle, **When** the API is hit, **Then** the response includes `{name: "<name>", kind: "push", last_run: "<RFC3339>", next_run: "<RFC3339>", last_error: <null|string>}`.
2. **Given** a push job has not yet completed its first cycle, **When** the API is hit, **Then** `last_run` and `last_error` are null and `next_run` is null or the configured first-fire time.

### User Story 4 — `POST /api/v1/jobs/{name}/wakeup` triggers an immediate push cycle (Priority: P2)

An operator clicks "wakeup" in the dashboard (or a CI pipeline POSTs to the endpoint) and the named push job starts its next cycle immediately rather than waiting for `interval` to elapse. Returns `204 No Content` on success, `404` if the job doesn't exist.

**Why this priority**: Required for the integration test (D14 in the slice ticket) and a useful operator primitive — push jobs typically have minute+ intervals, and "I just made a snapshot, push it now" should not take a minute.

**Acceptance Scenarios**:

1. **Given** a push job named `push_to_server`, **When** `POST /api/v1/jobs/push_to_server/wakeup` is called, **Then** the response is `204` and the job's cycle loop wakes up before `interval` elapses.
2. **Given** no job named `nope`, **When** `POST /api/v1/jobs/nope/wakeup` is called, **Then** the response is `404`.
3. **Given** a snap or sink job (where wakeup is meaningful for snap, no-op for sink), **When** the endpoint is called, **Then** snap jobs wake up immediately; sink jobs return `204` because the endpoint exists for the kind but the sink loop is event-driven (the wakeup is harmlessly absorbed by a non-blocking notify).

### User Story 5 — Sender chooses the right send flags from config (Priority: P2)

The TOML's `[jobs.send]` block exposes the four replication flags (`raw`, `embedded_data`, `compressed`, `large_blocks`). All default `true` because the user's existing zrepl config uses all four. They map 1:1 to `palimpsest::send::SendArgs`'s `raw`/`embedded`/`compressed`/`large_blocks` builder methods, which become `zfs send -w -e -c -L`. The flags travel in the SEND-stream header so the receiver can know what's on the wire (informational this slice; future slices may use them for property-strip decisions).

**Acceptance Scenarios**:

1. **Given** a push job with `[jobs.send] encrypted = true, embedded_data = true, compressed = true, large_blocks = true`, **When** the executor runs a full send, **Then** `palimpsest::send::send` is called with all four builder methods invoked.
2. **Given** a push job omitting `[jobs.send]`, **When** the executor runs, **Then** all four flags default true.
3. **Given** a push job with `[jobs.send] encrypted = false`, **When** the executor runs against an encrypted dataset, **Then** the send fails (zfs send without `-w` against an encrypted dataset that doesn't have a loaded key returns an error). The per-fs error is logged and the cycle continues. Operator sets `encrypted = true` and retries.

## Functional Requirements *(mandatory)*

- **FR-001 — Push job kind**: TOML accepts `[[jobs]] type = "push"` with the schema below. `JOB_KIND_PUSH = "push"` lands in `crates/api`.
- **FR-002 — Periodic loop**: each push job runs a select-loop on `interval` + `cancel` + `wakeup` notify; cycle = "process every configured filesystem in declared order".
- **FR-003 — Filesystem expansion**: filesystems are resolved through the same `arctern_config::filter::resolve_all` path snap jobs use (with `recursive` + `exclude`) so the operator's `[[jobs.filesystems]]` semantics are identical across job types.
- **FR-004 — Snapshot filter**: per-job snapshot filter accepts EITHER `prefix = "<str>"` (sugar for `regex = "^<str>"`) XOR `regex = "<re>"`. Default = no filter (all sender snapshots are candidates). Rejected at config-validate time if both are present.
- **FR-005 — Path mapping**: `target_dataset = "<root_fs>/<sender_full_path>"`. Concatenation, no stripping. Documented in `docs/example-config.toml`.
- **FR-006 — LIST request**: sender opens a QUIC bi stream, writes a header `{op: "list", target_dataset: "<receiver path>", prefix_regex: "<re>"}` (regex optional, the sender translates `prefix` to a regex before sending; receiver only ever sees `prefix_regex`).
- **FR-007 — LIST response**: receiver replies `{status: "ok", snapshots: [{name, guid, createtxg}, ...]}` or `{status: "error", message: "<reason>"}`. Snapshot list is filtered by `prefix_regex`. **Dataset-not-found maps to `ok` with `snapshots: []`** — first-replication is a normal state, not an error.
- **FR-008 — Planning by GUID**: planner intersects sender's local snapshots (palimpsest list filtered by snapshot_filter, sorted by `createtxg` ascending) with the receiver's response by `guid`. Highest-`createtxg` common GUID wins as the from-snap. If `from_snap == sender_latest`, nothing to do. If no common GUID, full send sender's latest matching snap.
- **FR-009 — SEND header**: executor opens a SECOND QUIC stream and writes header `{op: "send", target_dataset, send_kind: "full"|"incremental", from_snap: <Option<{name, guid}>>, to_snap: {name, guid}, send_flags: {raw, embedded, compressed, large_blocks}}`. Then bulk send bytes flow until FIN.
- **FR-010 — Per-stream isolation**: one stream = one operation (either LIST or SEND). The sender does NOT reuse a single stream for both operations within a filesystem cycle. Keeps the protocol simple and allows trivial parallelism in slice 007.
- **FR-011 — Sequential within a cycle**: filesystems are processed sequentially; no parallel sends. Avoids ZFS contention this slice; slice 007 can parallelize.
- **FR-012 — Retry policy**: on per-fs failure, log + continue to the next filesystem. `JobStatus.last_error` carries a summary on the cycle. No in-cycle retry, no backoff. Next cycle replans from current receiver state.
- **FR-013 — Cancellation**: in-flight LIST or SEND aborts when the cancellation token fires. The QUIC stream drops; the `zfs send` ChildHandle's `kill_on_drop` terminates the child.
- **FR-014 — Wakeup endpoint**: `POST /api/v1/jobs/{name}/wakeup` returns `204` if the job exists (any kind) or `404` otherwise. Snap and push jobs observe the wakeup notify and re-enter their cycle promptly; sink jobs absorb it harmlessly.
- **FR-015 — Sink-side LIST handler**: the existing `SinkJob::handle_stream` dispatches on the new `op` field. `op = "send"` keeps slice-004 behaviour. `op = "list"` runs the new handler (palimpsest list of snapshots under `target_dataset`, filtered by `prefix_regex`, returning snapshot list + each entry's `guid` + `createtxg`).
- **FR-016 — Recv property wiring**: with palimpsest's `RecvArgs::properties_override` + `properties_inherit` now available (slice 004 D22), the sink wires its `RecvProperties` config into recv invocations. This closes the slice 004 D22 gap from the arctern side.
- **FR-017 — Status reporting**: push jobs report `{kind: "push", last_run, next_run, last_error}` via the existing `GET /api/v1/jobs`. Cycle-level summary string only this slice; per-filesystem sub-status is a slice 008 concern.

## Non-Functional Requirements *(mandatory)*

- **NFR-001 — Constitution compliance**:
  - I (QUIC + HTTP semantics): bulk send bytes flow over raw QUIC streams; `op = "list"` is a small JSON request/response also over a QUIC stream — fits the constitution's "raw streams where HTTP framing doesn't help" principle. The control plane is still the unix-socket axum router; the wakeup endpoint goes there.
  - II (One API): `JOB_KIND_PUSH` is a string constant in `crates/api`. Wakeup endpoint is part of the OpenAPI-described HTTP surface.
  - III (Web UI replaces CLI): no new CLI. Wakeup endpoint serves both the dashboard and `curl`.
  - IV (ZFS through palimpsest): all `zfs send` / `zfs list` invocations go through palimpsest. Sender's planner uses `palimpsest::dataset::list`, executor uses `palimpsest::send::send`. The constitution-IV grep extends to cover all current arctern source.
  - V (Local-only by default): the sink already binds a network port; the push side is a CLIENT, no new bind. The existing accept-any TLS verifier in `crates/transport` covers both directions.
  - VI (SSE): not applicable this slice. Per-fs progress streaming is slice 008.
  - VII (ZFS metadata compat): cursor bookmarks NOT planted; receiver's snapshot list is the source of truth. This is a deliberate departure from zrepl. The receiver's pool is bit-identical to a zrepl-managed pool when arctern takes over (zrepl-planted bookmarks are ignored; their presence is harmless).
- **NFR-002 — Regex usage scoped**: the only direct `regex::` import in arctern remains `crates/config` (config validates the user's regex strings). The transport crate handles the LIST request's `prefix_regex` as a passthrough string AND compiles it for filtering on the receiver side — `crates/transport` joins `crates/config` on the regex-allowlist. Documented in plan.md D-grep.
- **NFR-003 — No tokio::process::Command in arctern source**: the constitution-IV grep must show zero matches in `crates/{api,client,transport}` and `daemon/src/`.
- **NFR-004 — Errors via thiserror in libraries**: no `anyhow`/`eyre` outside `daemon/src/main.rs`.

## Out of scope (deferred to slice 006+)

- **Resume tokens**: probe `receive_resume_token` on the receiver before deciding planner kind; if a token is present, decode it and pass `-t <token>` to the sender. Slice 006.
- **Retry / backoff**: in-cycle retry of failed sends with exponential backoff. Slice 006.
- **Parallel multi-stream sends**: per-cycle concurrency knob. Slice 007.
- **Per-filesystem sub-status**: each filesystem within a push job exposes its own `last_pushed_snapshot` + `last_error`. Slice 008.
- **Hold of last-replicated snapshot**: zrepl plants a hold so the snapshot can't be pruned before the next replication round. arctern can do this once snap-job and push-job interaction is exercised more. Slice 008.
- **Connection pooling**: opening a fresh QUIC connection per cycle is fine for minute+ intervals; once intervals shrink (or tests show measurable handshake cost) the cycle can keep one connection open across cycles. Slice 008.
- **Push to multiple peers from one job**: `connect` is a single `SocketAddr` this slice. Multi-peer fan-out is one job per peer for now.

## Wire protocol additions

The slice 004 wire protocol (length-prefixed JSON header → raw send bytes → JSON response) is unchanged on the bulk path. Slice 005 adds:

1. **Header gains `op` field**: `op: "send"` (default if absent — preserves slice 004 wire compatibility) or `op: "list"`.
2. **`op = "list"`**: header is `{version, op: "list", target_dataset, prefix_regex: <Option<String>>}`. NO bulk bytes follow. Receiver writes `ListResponse` and FINs. ListResponse: `{status: "ok", snapshots: [{name, guid, createtxg}, ...]}` or `{status: "error", message}`.
3. **`op = "send"` with `send_flags`**: when present in the header, declares which `zfs send` flags the sender used. Slice 005 receivers log them; future slices may consult them. Backward-compatible — absent means "unknown / slice 004 default".
4. **Backward compat**: the slice 004 sink already accepts the slice 004 header as written; the new `op` field is `#[serde(default)]` to `"send"`, so a slice-005 sink talking to a slice-004 client (or vice versa) keeps working.

`PROTOCOL_VERSION` stays at `1`. The `op` field is an additive change inside the existing protocol envelope.

## TOML schema additions

```toml
state_dir = "/var/lib/arctern"     # carried over from slice 004

[[jobs]]
type = "push"
name = "push_to_server"
connect = "10.77.77.100:8888"      # sink peer's QUIC addr
interval = "15m"
server_name = "arctern"            # SNI; default "arctern" — matches sink cert

[[jobs.filesystems]]
path = "okdata/data/home"
recursive = false
# exclude = [...]                  # same shape as snap job filter

[jobs.target]
root_fs = "okdata/backups/laptop"  # mapped: <root_fs>/<sender_path>

[jobs.send]
encrypted = true                    # raw send -w
embedded_data = true                # -e
compressed = true                   # -c
large_blocks = true                 # -L

[jobs.snapshot_filter]
prefix = "zrepl_"                   # OR regex = "^zrepl_.*" — exactly one
```

## Edge cases (mandatory)

- **First replication when receiver target dataset doesn't exist**: handled — receiver returns `snapshots: []` (D7), planner emits full send.
- **Receiver target dataset exists but with non-overlapping GUIDs** (rolled-back-by-hand case): handled — full send is attempted, `zfs recv` fails because the existing data conflicts, the per-fs cycle logs the error and continues. Operator-driven recovery (no in-arctern destroy).
- **Sender has zero matching snapshots** (empty filter result): handled — log "nothing to replicate", continue.
- **`from_snap == sender_latest`** (already up to date): handled — log "nothing to do", continue.
- **GUID overflow** (ZFS GUIDs are u64; some exceed `i64::MAX`): handled — wire types use `u64`, intersection key is `u64`. **VERIFIED IN VM**: `tank/data@zrepl_001` has guid `11587258101628135412` which exceeds `i64::MAX`.
- **A filesystem in the config does not exist on the sender**: handled — `palimpsest::dataset::list` rooted at the missing path returns an error; the per-fs cycle logs it and continues.
- **One filesystem fails mid-stream** (network drop, disk full on receiver): handled — per-fs cycle catches the error, logs it, accumulates into `last_error` summary, moves on.
- **SIGTERM during a multi-GB send**: handled — cancellation token fires, QUIC stream drops, `tokio::io::copy` errors, `kill_on_drop` on the send ChildHandle kills `zfs send` cleanly.
- **Receiver's prior arctern run died mid-recv leaving a partial dataset with `receive_resume_token`**: deferred to slice 006. This slice's executor does NOT probe for a resume token; the planner always chooses full or incremental from scratch. If a resume token exists, the next full/incremental send fails the same way it would against any conflicting dataset — operator clears the partial recv (`zfs receive -A`) and retries.
- **Snapshot disappearing between LIST and SEND** (e.g., a competing prune destroys the from-snap on the sender between planning and executing): rare, but possible. Outcome: `zfs send` fails; per-fs cycle logs + continues; next cycle replans.

## Acceptance verification (single integration test)

One test in `daemon/tests/integration_quic_push.rs` exercises:

1. Boot two `LoopbackPool`s (`sender_pool`, `receiver_pool`).
2. Spawn the sink daemon: state_dir = `/tmp/arctern_sink_<nanos>`, UDS = `/tmp/arctern_sink_<nanos>.sock`, sink job listens on `127.0.0.1:0` with `root_fs = <receiver_pool>/sink`.
3. Capture the LISTEN_QUIC port from the sink daemon's stdout.
4. Spawn the sender daemon: state_dir = `/tmp/arctern_sender_<nanos>`, UDS = `/tmp/arctern_sender_<nanos>.sock`, snap job (interval = 1s) over `<sender_pool>/data`, push job (interval = 1s) at `127.0.0.1:<sink_port>` with `target.root_fs = <receiver_pool>/sink` and `snapshot_filter.prefix = "test_"`.
5. Pre-create `<sender_pool>/data` (`zfs create -o mountpoint=none`). Manually snapshot via the API with name `test_001`. Manually trigger the sender's push job via wakeup. Wait + assert `<receiver_pool>/sink/<sender_pool>/data` exists with one snapshot.
6. Manually snapshot `<sender_pool>/data` again as `test_002`. Wakeup. Wait + assert receiver now has two snapshots, the second matching the sender's `test_002` GUID.
7. Tear down both daemons; destroy both pools.

The test counts one `#[tokio::test]` and exercises the full+incremental sequence.

## Reused from slice 004

- `crates/transport`: identity, TLS, framing primitives (`read_header`, `write_header`, `write_response`, `read_response`). New types added; existing types kept wire-compatible via `#[serde(default)]` on the `op` field.
- `JobManager`, `JobContext`, `JobStatusInner` from `daemon/src/jobs/mod.rs`. Push job's `Job` impl is the third sibling alongside `SnapJob` and `SinkJob`.
- `arctern_config::filter::resolve_all` from slice 003. Same filtering for `[[jobs.filesystems]]` across snap and push.
- `palimpsest::dataset::list`, `palimpsest::send::send`, `palimpsest::recv::recv`, `palimpsest::runner::ChildHandle` (with kill_on_drop).
