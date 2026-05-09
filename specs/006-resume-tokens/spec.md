# Feature Specification: Resume tokens — pick up interrupted pushes

**Feature Branch**: `006-resume-tokens`
**Created**: 2026-05-09
**Status**: Draft
**Input**: Slice 006 of arctern. When a previous push to a target dataset was interrupted mid-stream (network drop, daemon SIGKILL, sink crash), the next cycle resumes from where the previous one stopped instead of restarting from byte zero. Critical for large initial bootstraps (TB-scale) over flaky links. Replicates the resumable-recv semantics from zrepl's `internal/zfs/zfs.go` + `internal/replication/logic/diff/diff.go` (`WithResumeToken` path).

## Why this slice

Slice 005 closed the active-send plane: planner reads receiver state, picks Full or Incremental, executor pipes `zfs send` over QUIC into `zfs recv`. If the recv is interrupted halfway through a multi-GB stream, slice 005's behaviour is to throw away every received byte on the next cycle and restart. Over a flaky WireGuard link the bootstrap of a large dataset can stall indefinitely — every drop costs the entire stream so far.

ZFS already supports resumable receive natively: `zfs recv -s` keeps the partial state on the destination dataset and exposes a `receive_resume_token` user property. The next sender runs `zfs send -t <token>` to produce a stream that begins at the offset the prior recv stopped at. arctern's job is to wire this through: sink advertises the token over LIST, planner notices it, executor sends the right thing.

The risk surface is that the partial recv on the sink is implicitly tied to a specific `(from_guid, to_guid)` snapshot pair encoded in the token. If the sender no longer has those snapshots (operator pruned, snap job rolled past, manual destroy), the token is stale and the partial state must be cleared before any fresh stream can land. **Verified in OpenZFS 2.4.1 in the test VM**: a fresh full or incremental send into a dataset that still carries `receive_resume_token` is rejected outright — even with `-F`. The error is unmistakable: `destination ... contains partially-complete state from "zfs receive -s"`. The only paths back are `zfs send -t <token>` (continue the partial) or `zfs recv -A <ds>` (abort it).

This means the slice has to handle the stale-token case actively: when the planner decides Full or Incremental despite the receiver advertising a token, the sink needs to call `zfs recv -A` first. zrepl handles this via an explicit `Reset` RPC; arctern handles it via a `discard_partial_recv` boolean on the existing SEND header, keeping the wire vocabulary at the slice-005 footprint.

## User Scenarios & Testing *(mandatory)*

The "user" is the same operator from slice 005: arctern on a sender, arctern sink on a backup box, both behind WireGuard. They've configured a push job for a 4 TB filesystem. Three nights into the bootstrap, their VPN drops. They want the next cycle to pick up at 2.7 TB, not at 0.

### User Story 1 — A push job interrupted mid-stream resumes from the existing partial on the next cycle (Priority: P1)

A sender's push cycle starts a full send of `okdata/data/home@zrepl_001`. After 1.2 GB of a 16 MB dataset (or whatever absurd ratio is convenient for the test), the network drops. The receiver's `zfs recv -s` exits with the standard "checksum mismatch or incomplete stream" error and leaves a `receive_resume_token` on the destination dataset. Next cycle, the sender's LIST request to the receiver returns both the snapshot list (still empty in this case — the partial isn't a real snapshot yet) AND the resume token. The planner inspects the token, validates the encoded `to_guid` (and `from_guid` if incremental) is still on the sender, and emits `ResumeSend { token }`. The executor runs `palimpsest::send::send` with `SendArgs::resume_token(token)` instead of building a fresh full/incremental, pipes the resulting stream into the same SEND wire format, and the sink's recv side completes the dataset.

**Why this priority**: This is the slice. Without it, the resumable-recv plumbing on the palimpsest side is just shelfware.

**Independent Test**: a single integration test in `daemon/tests/integration_quic_resume.rs`. Two `LoopbackPool`s. Pre-create source dataset with ~16 MiB of urandom content. Use Strategy C from D7: a `tokio::io::copy_buf` wrapper that errors after the first N KiB. First cycle: sender opens a SEND stream wrapped in the truncating IO, sink writes the partial. Assertions: (1) `palimpsest::recv::receive_resume_token(receiver_dataset)` returns `Some`. Second cycle: sender uses the real (un-truncated) IO. Assertions: (2) cycle exits Ok; (3) full dataset is present on receiver; (4) snapshot GUID matches sender's source snapshot.

**Acceptance Scenarios**:

1. **Given** a sender mid-bootstrap whose recv was interrupted with the partial preserved, **When** the next cycle's LIST request hits the sink, **Then** the sink returns `{status: "ok", snapshots: [], receive_resume_token: Some("1-...")}`.
2. **Given** a planner that received `receive_resume_token = Some` AND the encoded `to_guid` (and `from_guid` if present) are still on the sender, **When** it picks a plan, **Then** the plan is `ResumeSend { token }` rather than Full or Incremental.
3. **Given** an executor running a `ResumeSend { token }` plan, **When** it spawns `palimpsest::send::send`, **Then** the SendArgs uses `.resume_token(token)` (which becomes `zfs send -t <token>` in the argv) and the SEND wire header carries `send_kind = "resume"`.
4. **Given** a successful resume cycle, **When** it completes, **Then** the receiver's destination dataset has the full snapshot, `palimpsest::recv::receive_resume_token` returns `None`, and the receiver's snapshot GUID matches the sender's source snapshot exactly.
5. **Given** a stale resume token (token's `to_guid` no longer matches any sender snapshot — e.g. operator destroyed the source snap), **When** the planner inspects it, **Then** the planner falls back to a fresh Full or Incremental plan AND marks the SEND header's `discard_partial_recv = true` so the sink calls `zfs recv -A` before invoking the new recv.
6. **Given** a sink running pre-006 firmware (no `receive_resume_token` field in its LIST response) talking to a post-006 sender, **When** the planner reads the response, **Then** the field deserializes as `None` (per `#[serde(default)]`) and the planner falls through to slice-005 Full/Incremental — there is no resume regression.
7. **Given** a sink running post-006 firmware talking to a pre-006 sender, **When** the sender sends a slice-005 SEND header (no `discard_partial_recv` field), **Then** the sink deserializes it with the default `false` and behaves exactly as in slice 005 — incoming Full/Incremental into a dataset with a partial fails as ZFS requires; that's the slice-005 behaviour and is unchanged.

### User Story 2 — Stale resume tokens are cleared automatically without operator intervention (Priority: P1)

An operator's snap job pruned the source snapshot that an in-flight bootstrap was tied to (the keep policy was `keep_last = 1` and a newer snapshot rolled). The receiver still carries the partial recv and its token. Next push cycle: the planner sees the token but verifies `to_guid` is no longer on the sender; falls back to Full of the latest current snapshot; the SEND header carries `discard_partial_recv = true`; the sink calls `zfs recv -A <target_dataset>` to clear the partial state, then runs `zfs recv -s` for the fresh stream. The operator does NOT have to intervene.

**Why this priority**: D6 verification proved this is required — without it, a stale token wedges the dataset until manual intervention. Slice 006 cannot ship without solving it.

**Independent Test**: covered as a sub-assertion of the integration test in User Story 1 — make the second cycle send a DIFFERENT snapshot (rather than the resume), and assert the recv succeeds because the sink aborted the partial.

**Acceptance Scenarios**:

1. **Given** a stale resume token on the receiver, **When** the planner emits a Full plan with `discard_partial_recv = true`, **Then** the sink invokes `palimpsest::recv::abort_partial(target_dataset)` before spawning the new `zfs recv -s` and the recv succeeds.
2. **Given** the sink's `abort_partial` call returns `Ok` (the partial was cleared) **OR** `Ok` (no partial existed because the planner was being defensive), **When** recv proceeds, **Then** there is no observable difference — the abort is idempotent at the level of "dataset has no partial state when this returns Ok".
3. **Given** an unexpected error from `abort_partial` (e.g. permission denied on the sink), **When** recv would have proceeded, **Then** the sink fails the cycle with `ReceiveResponse::Error { message }` carrying the abort error, the cycle's `last_error` reflects it, and the next cycle re-attempts (the partial state is still there to retry the abort).

### User Story 3 — Cycle status mentions when a resume happened (Priority: P2)

The dashboard polls `GET /api/v1/jobs` and gets back per-cycle status. A successful resumed cycle should be visually distinguishable from a successful fresh cycle in the daemon logs (so an operator scanning a journal can confirm the bootstrap is making forward progress). For this slice: a `tracing::info!("push: resuming from token", target = %target, bytes = %bytes_received)` log line at execute time. The HTTP status field stays at the cycle level — `last_error: None` on success regardless of resume vs fresh. Per-fs sub-status remains a slice-008 concern.

**Why this priority**: An operator who can't tell whether the bootstrap is resuming or restarting from zero on every cycle will not trust the system. Logs are the cheapest path to that confidence this slice; structured per-fs status comes later.

**Acceptance Scenarios**:

1. **Given** a cycle that resumed an in-flight stream, **When** the executor invokes the send, **Then** an info log line records `target`, `bytes_received` (decoded from the token via `palimpsest::resume_token::decode`).
2. **Given** a cycle that emitted a fresh Full or Incremental plan, **When** the executor invokes the send, **Then** the existing slice-005 `push: full send` / `push: incremental send` log line fires (no resume mention).
3. **Given** a cycle that detected a stale token and discarded the partial, **When** the executor invokes the send, **Then** an info log line records `target`, `discard_partial_recv = true` (so the operator can correlate `zfs recv -A` invocations with the planner decision).

## Functional Requirements *(mandatory)*

- **FR-001 — Resumable recv on the sink**: every `zfs recv` invocation in `daemon/src/jobs/sink.rs::handle_send` uses `RecvArgs::resumable()`. Without this, no token is ever populated and the slice is non-functional.
- **FR-002 — LIST response carries the token**: `ListResponse::Ok` gains an optional `receive_resume_token: Option<String>` field. The sink's `handle_list` calls `palimpsest::recv::receive_resume_token(target_dataset)` and includes the result. `DatasetNotFound` maps to `None` (same as snapshot-list behaviour). Wire-typed with `#[serde(default, skip_serializing_if = "Option::is_none")]` for backward compat with pre-006 sinks.
- **FR-003 — Planner consumes the token**: `plan_one_filesystem` returns a richer `SnapshotPlan` that adds `Resume { token: String, decoded: ResumeToken }` and `FullDiscardingPartial { to: SnapshotRef }` / `IncrementalDiscardingPartial { from, to }` variants.
- **FR-004 — Token validation**: when the LIST response carries a token, the planner calls `palimpsest::resume_token::decode(runner, token)` to extract `to_guid` and `from_guid: Option<u64>`. The token is "live" iff `to_guid` matches a sender snapshot AND (if `from_guid.is_some()`) `from_guid` matches a sender snapshot. Live → `Resume`. Stale → emit Full or Incremental with `discard_partial_recv = true`.
- **FR-005 — Executor with `-t <token>`**: when the plan is `Resume { token, .. }`, executor builds `palimpsest::send::SendArgs::new("ignored").resume_token(token)` plus the four wire flags from `[jobs.send]` (raw / embedded / compressed / large_blocks — same as slice 005). The resulting `zfs send -t <token>` argv carries those flags through.
- **FR-006 — SEND header `send_kind = "resume"`**: extend `SendKind` with a `Resume` variant. Wire payload: `from_snap = None`, `to_snap` carries the decoded token's `to_name` + `to_guid` (informational; sink doesn't validate). `flags` carries the four bools. `discard_partial_recv = false` (resume MUST not discard the partial — that's the whole point).
- **FR-007 — SEND header `discard_partial_recv` flag**: extend `SendHeader` with `#[serde(default)] pub discard_partial_recv: bool`. Sink's `handle_send` honours it before spawning recv: when `true`, call `palimpsest::recv::abort_partial(target_dataset).await?`. Failure of the abort returns `ReceiveResponse::Error` and the cycle's per-fs error reflects it.
- **FR-008 — Backwards compat**: pre-006 senders sending a slice-005 SEND header (no `discard_partial_recv`) deserialize with `false`, behaviour matches slice 005. Pre-006 sinks responding without `receive_resume_token` deserialize with `None`, planner falls through to Full/Incremental as in slice 005. No `PROTOCOL_VERSION` bump.
- **FR-009 — Backward-compatible job restart**: a push job restart between cycles does NOT lose resume capability. Receiver state IS the source of truth: the next cycle's LIST request still returns the token, the planner runs the same algorithm. No daemon-side persistence required.
- **FR-010 — Status reporting**: cycle-level `last_error` stays at slice-005 semantics (None on success, joined per-fs error string on failure). No new status field this slice. Resume detection is logged at info level only.
- **FR-011 — Constitution compliance**: all `zfs send` / `zfs recv` / `zfs recv -A` invocations route through palimpsest (FR-001 uses `RecvArgs::resumable`; abort uses `palimpsest::recv::abort_partial`; resume uses `SendArgs::resume_token`). No new direct `tokio::process::Command` in arctern source.

## Non-Functional Requirements *(mandatory)*

- **NFR-001 — Constitution compliance**:
  - I (QUIC + HTTP semantics): unchanged. Same two stream shapes (`op = "list"` and `op = "send"`).
  - II (One API): `ListResponse` and `SendHeader` extensions stay in `crates/transport`; no new `crates/api` surface.
  - III (Web UI replaces CLI): no new CLI verbs.
  - IV (ZFS through palimpsest): all new ZFS interactions (`zfs recv -s`, `zfs send -t`, `zfs recv -A`, `receive_resume_token` query, token decode) go through palimpsest. Two prep commits land on palimpsest master (recv `-s` + `abort_partial`; resume_token `from_guid`).
  - V (Local-only by default): no new bind, no new transport surface.
  - VI (SSE): not applicable.
  - VII (ZFS metadata compat): no new bookmarks. Receiver's `receive_resume_token` user property is a stock OpenZFS surface, not an arctern artifact.
- **NFR-002 — No new tokio::process::Command in arctern**: verified by the existing slice-005 grep gate, unchanged this slice.
- **NFR-003 — No new regex usage in arctern**: this slice does not parse new user-controlled strings; the resume token is opaque to arctern (palimpsest decodes it).
- **NFR-004 — Errors via thiserror in libraries**: no `anyhow`/`eyre` outside `daemon/src/main.rs`.

## Out of scope (deferred to slice 007+)

- **Per-fs sub-status** (last_pushed_snapshot, per-fs last_error, resume-vs-fresh marker as a structured field): slice 008.
- **Retry / backoff inside a cycle**: still log + continue on per-fs failure; next cycle replans. Slice 007.
- **Parallel multi-stream sends**: per-cycle concurrency. Slice 007.
- **Active `op = "abort_resume"` request**: the sink-side discard is wired into the SEND header as a flag (FR-007); a standalone abort op is not required this slice. If a future operator wants to clear a partial without sending, they can run `zfs recv -A` on the sink directly. Slice 008+ if there's demand.
- **Resume across DIFFERENT push job restarts**: works for free — receiver state is the source of truth, the planner's algorithm makes no assumptions about which sender daemon process started the partial. Confirmed in plan.md.
- **Token validation against more than from/to GUIDs**: the token also encodes `object`, `offset`, `bytes`. We do NOT cross-check those against sender state; if the token decodes and the GUIDs are still on the sender, we trust ZFS to either accept or reject the resumed stream. ZFS rejects with a clear stderr if the sender's data drifted under the partial — we surface that as the per-fs cycle error and move on.
- **Cleanup of long-stale tokens**: if a token sits on the receiver for weeks because every sender retry hits a stale-token discard, we accept that. Slice 008 may add a stale-token cleanup heuristic on the planner side ("if `discard_partial_recv` is true twice in a row, log a warning"); this slice is just "make resume work and don't wedge on stale".

## Wire protocol additions

Slice 005's wire vocabulary stays. Two additive fields:

1. **`ListResponse::Ok` gains `receive_resume_token: Option<String>`**. `#[serde(default, skip_serializing_if = "Option::is_none")]`. None means "no partial recv on this dataset, or the dataset doesn't exist".
2. **`SendHeader` gains `discard_partial_recv: bool`**. `#[serde(default)]`. False means "slice-005 behaviour"; true means "sink runs `zfs recv -A` on `target_dataset` before spawning the new recv".
3. **`SendKind` gains a `Resume` variant**. Encoded as `"resume"` per the existing snake_case rename. When `send_kind = "resume"`, the sink does NOT use the wire `from_snap` / `to_snap` for any decision (recv reads them from the actual zfs send byte stream); they are informational. `discard_partial_recv` MUST be `false` on a Resume header — the resume IS the partial.

`PROTOCOL_VERSION` stays at `1`. All three changes are additive inside the existing envelope.

## TOML schema additions

None. Slice 006 is wire + planner + executor + sink behaviour. The TOML knobs from slice 005 (`[jobs.send]` flags, `[jobs.snapshot_filter]`) carry through unchanged into resume sends.

## Edge cases (mandatory)

- **Receiver advertises a token, planner ignores it (bug case)**: the next SEND-stream Full or Incremental into the dataset will fail at `zfs recv` with the `partially-complete state` error. Cycle's `last_error` carries the message. Operator can either run `zfs recv -A` manually or wait for the planner to do so on a future cycle (FR-007 ensures it does). No silent breakage; clear failure mode.
- **Token decodes but `to_guid` is not on the sender (operator pruned)**: planner emits Full of the latest sender snap with `discard_partial_recv = true`. Sink aborts, then accepts the fresh full. Logged as info.
- **Token decodes, `to_guid` is on the sender, but `from_guid` (incremental case) is NOT on the sender**: same as above — token is stale; emit Incremental with `discard_partial_recv = true` against the highest common GUID + the latest snap. (If no common GUID exists, fall back to Full with `discard_partial_recv = true`.)
- **Token decode itself fails** (`receive_resume_token` returned a string that's not parseable — should not happen with a real ZFS-emitted token, but we treat it as defensively as we'd treat any other planner error): the per-fs cycle errors with `PlanError::ResumeTokenDecode`, the planner does NOT fall through to fresh send (because that would fail at recv with the stale-token error and not get cleaned up). Operator-driven recovery: run `zfs recv -A` manually. Cycle continues with the next filesystem.
- **Sender no longer has the dataset at all** (operator destroyed it): `palimpsest::dataset::list` rooted at the missing path returns `ZfsError::DatasetNotFound`; the per-fs cycle errors as in slice 005. The receiver's partial sits there until the operator restores the dataset or runs `zfs recv -A` manually.
- **Receiver advertises a token, sender has BOTH guids, and the network drops AGAIN during the resume**: a new (smaller) partial replaces the old one. Token offset advances. Next cycle the same algorithm runs — Resume becomes the plan again. Forward progress is monotonic.
- **A new snapshot lands on the sender between LIST and SEND such that `to_snap` is no longer the latest**: the resume targets the snapshot the partial was tied to, not the latest. After the resume completes, the next cycle's planner picks up an Incremental from the just-completed snap to the new latest. **Verified semantics**: this is the right behaviour — the operator wanted the bootstrap to finish, not to re-aim it mid-flight.
- **Concurrent partial recvs on the same target_dataset** (two arctern senders, or one arctern sender plus a manual `zfs send` someone ran for testing): ZFS itself enforces that a dataset has at most one partial recv. The second `zfs recv -s` would fail with the existing-partial error. arctern doesn't try to defend against this; the per-fs cycle reports it.
- **Token transport on the wire**: tokens from the test VM run ~150 bytes; a real production token can be larger but is bounded by ZFS to a few hundred bytes. The 1 MiB `MAX_HEADER_LEN` cap on the LIST-response side has plenty of headroom (tokens are inside the response body, not the header — but same envelope cap applies via the existing `read_to_end` then `from_slice` pattern).

## Acceptance verification (single integration test)

One test in `daemon/tests/integration_quic_resume.rs` exercises:

1. Boot two `LoopbackPool`s (`sender_pool`, `receiver_pool`).
2. Pre-create source dataset with ~16 MiB of urandom content (gives a stream large enough to truncate meaningfully) plus snapshot `test_001`. `sink_root` is `<receiver_pool>/sink` like in slice 005.
3. Spawn sink + sender daemons exactly as in slice 005's integration test.
4. **Phase 1 — interrupted cycle**: trigger the push (wakeup endpoint). Strategy C (D7): the test does NOT modify the daemon; it relies on the daemon reaching the receiver's `zfs recv -s`, then drops the QUIC connection out from under it by killing the sender daemon mid-stream. Concretely: invoke wakeup, sleep 50 ms, send the sender daemon SIGKILL (NOT SIGTERM — we want an abrupt drop, not a clean shutdown). On the receiver pool, assert `palimpsest::recv::receive_resume_token(target_dataset)` returns `Some(_)`.
5. **Phase 2 — resume cycle**: respawn the sender daemon with the same config. Trigger wakeup. Wait for the receiver to gain the snapshot. Assert: (a) `receive_resume_token` is now `None`, (b) the receiver has snapshot `test_001` with the same GUID as the sender's source.
6. **Phase 3 — stale-token discard** (sub-test in same file or separate test, see plan): destroy + recreate the source dataset so the GUIDs change (or destroy the snap and re-snapshot under a new name) such that the planner sees a token whose to_guid is no longer on the sender. Assert: the cycle succeeds, the receiver's old partial is gone (`receive_resume_token` is None mid-cycle and at end), the new full has landed.
7. Tear down both daemons; destroy both pools.

The strategy choice (D7) is C (kill the daemon mid-stream) because it requires no daemon-side test hooks and exercises the full code path including ChildHandle drop-on-cancel. Strategy A (`Connection::close` from inside the test) is rejected because the sender is in a separate process; we'd need IPC for the test to reach into the daemon. Strategy B (kill the recv child specifically) is rejected because killing only the recv child doesn't observe the same drop-the-stream-mid-flight behaviour the SIGKILL path produces.

## Reused from slice 005

- All wire types from `crates/transport`: `Op`, `ReceiveHeader`, `ReceiveResponse`, `ListResponse`, `SnapshotEntry`, `SnapshotRef`, `SendHeader`, `SendKind`, `SendFlagsWire`. Each gains additive fields/variants.
- `daemon/src/jobs/push.rs`: the `pick_plan` algorithm, `CompiledFilter`, `list_sender_snaps`, the executor's stream-pump skeleton. The plan enum gains variants; the executor learns one new branch (`Resume`).
- `daemon/src/jobs/sink.rs`: the dispatch on `Op`, the per-stream task framework, the `handle_list` handler. `handle_send` gets a leading `abort_partial` call when the header asks; `handle_list` gets a token field appended to its OK response. All recv invocations switch to `RecvArgs::resumable()`.
- `palimpsest::recv::receive_resume_token` (already present per slice 002) — no palimpsest change for this query.
- `palimpsest::resume_token::decode` (already present, now extended with `from_guid`) — palimpsest prep commit.
- `palimpsest::send::SendArgs::resume_token` (already present per slice 002) — no change.
- `palimpsest::recv::RecvArgs::resumable` and `palimpsest::recv::abort_partial` — palimpsest prep commit on master, pushed before slice 006 starts.
