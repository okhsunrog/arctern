# Tasks: Resume tokens — pick up interrupted pushes

**Feature**: `006-resume-tokens`
**Input**: [spec.md](./spec.md), [plan.md](./plan.md)

Each task = one logical commit. Per-task verification commands listed.

## T001 — `feat(transport)`: ListResponse token + SendHeader discard flag + SendKind::Resume

**Why first**: every later task imports the new types.

**Changes**:

- `crates/transport/src/protocol.rs`:
  - Extend `ListResponse::Ok` with `#[serde(default, skip_serializing_if = "Option::is_none")] receive_resume_token: Option<String>`.
  - Extend `SendHeader` with `#[serde(default)] pub discard_partial_recv: bool`.
  - Extend `SendKind` with `Resume` variant (serde rename `"resume"`).
- Unit tests:
  - `list_response_default_token_is_none`: deserialize a slice-005-shape `{status: "ok", snapshots: []}` and assert `receive_resume_token == None`.
  - `list_response_with_token_roundtrip`: write + read back `{status: "ok", snapshots: [...], receive_resume_token: Some("1-...")}` losslessly.
  - `send_header_default_discard_is_false`: deserialize a slice-005-shape `SendHeader` (no `discard_partial_recv`) and assert `false`.
  - `send_header_with_discard_roundtrip`: write + read back a header with `discard_partial_recv: true`.
  - `send_header_resume_kind_roundtrip`: a Resume header (kind: "resume", from_snap: None, to_snap: ..., discard_partial_recv: false) round-trips.
  - `send_header_serializes_resume_kind_as_lowercase`: assert the wire string is `"resume"`.

**Verify**:

```
cargo test -p arctern-transport
cargo clippy -p arctern-transport --all-targets -- -D warnings
```

**Commit**: `feat(transport): receive_resume_token + discard_partial_recv + SendKind::Resume (T001)`

## T002 — `feat(daemon)`: sink advertises tokens, uses recv -s, honours discard_partial_recv

**Changes**:

- `daemon/src/jobs/sink.rs`:
  - Every `RecvArgs::new(...)` call gains `.resumable()`. There's currently one in `handle_send`; flip it.
  - `handle_send`: before constructing `RecvArgs`, check `header.send.as_ref().map(|s| s.discard_partial_recv).unwrap_or(false)`. If true: `tracing::info!(target = %header.target_dataset, "sink: discarding partial recv per sender request"); palimpsest::recv::abort_partial(runner, &header.target_dataset).await.map_err(|e| format!("abort_partial {}: {e}", header.target_dataset))?;`. Place this AFTER the parent-create logic and BEFORE the `RecvArgs` build.
  - `handle_list`: after the snapshot enumeration succeeds (the `snapshots` `Vec` is built), call `palimpsest::recv::receive_resume_token(runner, &header.target_dataset).await`. On `Ok(opt)`: include in the response. On `Err(ZfsError::DatasetNotFound { .. })`: `None` (matches the snapshot path's behaviour). On any other `Err`: `tracing::warn!(error = %e, target = %header.target_dataset, "sink: token query failed; LIST returning None"); None` (D19 — soft failure, planner falls through to slice-005 logic).
  - Update the existing `recv_properties_propagate_to_palimpsest_args` test fixture: the expected argv now includes `-s` (the slice-005 fixture's exact arglist becomes `["recv", "-u", "-s", "-o", "canmount=off", ...]`). The order is determined by `RecvArgs::build_args` — `-u` before `-s` per palimpsest's order.
- New unit tests:
  - `handle_send_with_discard_calls_abort_first`: build a `RecordingRunner` whose first command is `["recv", "-A", "tank/sink/data"]` returning success and the second is `["recv", "-u", "-s", ..., "tank/sink/data"]` returning success; drive `handle_send` with a `SendHeader { discard_partial_recv: true, ... }`. Assert both commands ran in order. (May require a refactor to thread the runner through; if the existing function already takes `&dyn CommandRunner`, this is direct.)
  - `handle_list_includes_token_when_present`: `RecordingRunner` returns a `zfs list -j ...` for the snapshot enumeration (one entry) and a `zfs get -j -p receive_resume_token ...` returning `"1-abc123"`. Drive `handle_list`. Assert `ListResponse::Ok { snapshots, receive_resume_token: Some("1-abc123") }`.
  - `handle_list_token_none_when_dataset_missing`: token query errors with `DatasetNotFound`; assert `receive_resume_token: None`.

**Verify**:

```
cargo test -p arctern-daemon sink
cargo clippy -p arctern-daemon --all-targets -- -D warnings
```

**Commit**: `feat(daemon): sink uses recv -s, advertises token, honours discard_partial_recv (T002)`

## T003 — `feat(daemon)`: planner consumes the token, validates GUIDs, dispatches Resume / discard

**Changes**:

- `daemon/src/jobs/push.rs`:
  - Extend `SnapshotPlan` with `Full { to, discard_partial_recv: bool }` and `Incremental { from, to, discard_partial_recv: bool }` (existing variants gain the field) and a NEW `Resume { token: String, decoded: palimpsest::resume_token::ResumeToken }` variant.
  - Extend `PlanError` with `ResumeTokenDecode { source: palimpsest::resume_token::ResumeTokenError }` (use `#[from]`).
  - `pick_plan` keeps its slice-005 signature (takes only sender + receiver lists, returns Full / Incremental / Nothing without the discard flag) — call sites wrap the result. This keeps the existing slice-005 unit tests valid as-is.
  - Add `pub fn pick_plan_with_token(sender: &[SnapshotRef], receiver: &[SnapshotEntry], token: Option<&str>, decoded: Option<&ResumeToken>) -> SnapshotPlan` — pure function for testability:
    - If `token.is_none()` or `decoded.is_none()`: delegate to `pick_plan`, wrap with `discard_partial_recv: false`.
    - If both `Some`: compute `to_live` + `from_live` from the decoded GUIDs against `sender`. If both live: `Resume { token, decoded.clone() }`. Else: delegate to `pick_plan`, wrap with `discard_partial_recv: true`.
  - `plan_one_filesystem`: after fetching `(snapshots, receive_resume_token)` from the LIST, if a token is present call `palimpsest::resume_token::decode(runner, &token).await`, then `pick_plan_with_token`. Otherwise existing path.
  - `fetch_receiver_snaps` returns shape changes: now returns `Result<(Vec<SnapshotEntry>, Option<String>), PlanError>` (the second tuple element is the token from `ListResponse::Ok`). All existing callers need to adapt.
- New unit tests (pure-function — no QUIC, no runner):
  - `pick_plan_with_token_none_falls_through_to_full`: empty receiver, no token, sender has snaps → wrapped Full with discard=false.
  - `pick_plan_with_token_live_emits_resume`: sender has GUID 11587258101628135412, decoded token has `to_guid = 11587258101628135412`, `from_guid = None` → `Resume { token, decoded }`.
  - `pick_plan_with_token_live_incremental_emits_resume`: both `to_guid` and `from_guid` live on sender → Resume.
  - `pick_plan_with_token_to_guid_dead_emits_full_with_discard`: sender doesn't have `to_guid` → wrapped Full with `discard_partial_recv = true`.
  - `pick_plan_with_token_from_guid_dead_emits_full_with_discard`: incremental token, sender has `to_guid` but not `from_guid` → wrapped Full (or Incremental against another common GUID if any) with `discard_partial_recv = true`.
  - `pick_plan_with_token_dead_but_common_snap_exists_emits_incremental_with_discard`: sender + receiver share a different GUID; token's GUIDs are dead → Incremental (from common, to latest) with `discard_partial_recv = true`.
- Existing slice-005 unit tests for `pick_plan` keep passing because the function shape is unchanged. Add new tests for `pick_plan_with_token` rather than retrofitting.

**Verify**:

```
cargo test -p arctern-daemon push
cargo clippy -p arctern-daemon --all-targets -- -D warnings
```

**Commit**: `feat(daemon): planner emits Resume/Discard variants from receiver token (T003)`

## T004 — `feat(daemon)`: executor wires Resume + discard_partial_recv through the SEND header and SendArgs

**Changes**:

- `daemon/src/jobs/push.rs`:
  - `build_send_header(plan, flags)`:
    - `SnapshotPlan::Full { to, discard_partial_recv }` → SendHeader with `send_kind: Full`, `from_snap: None`, `to_snap: to.clone()`, `flags: wire_flags(flags)`, `discard_partial_recv: *discard_partial_recv`.
    - `SnapshotPlan::Incremental { from, to, discard_partial_recv }` → analogous.
    - `SnapshotPlan::Resume { decoded, .. }` → SendHeader with `send_kind: Resume`, `from_snap: None`, `to_snap: SnapshotRef { name: decoded.to_name.clone(), guid: decoded.to_guid }`, `flags: wire_flags(flags)`, `discard_partial_recv: false`.
    - `Nothing` → `None` (unchanged).
    - `debug_assert!(!(matches!(plan, Resume{..}) && resulting.discard_partial_recv))` (D18 belt-and-suspenders).
  - `build_send_args(plan, sender_dataset, flags)`:
    - Full / Incremental: unchanged (the new `discard_partial_recv` field doesn't affect SendArgs).
    - `Resume { token, .. }`: `let mut args = palimpsest::send::SendArgs::new("ignored").resume_token(token);` then apply the four wire flags from `flags`.
  - `execute_one_plan`: in the existing per-plan log block, add:
    - On `Full { discard_partial_recv: true, .. }`: `tracing::info!(sender = %sender_path, to = %to.name, "push: full send (discarding stale partial)")`.
    - On `Incremental { discard_partial_recv: true, .. }`: analogous.
    - On `Resume { decoded, .. }`: `tracing::info!(sender = %sender_path, to = %decoded.to_name, bytes = decoded.bytes_received, "push: resuming from token")`.
- Update existing slice-005 tests for `build_send_header_full_uses_all_default_flags`, `build_send_header_incremental_carries_from_and_to`, `build_send_header_nothing_yields_none`, `build_send_args_full_with_all_flags`, `build_send_args_incremental_uses_dash_i`: the `SnapshotPlan` constructors now require `discard_partial_recv: false`. Add the field to each test's plan literal. Assert `header.discard_partial_recv == false`.
- New unit tests:
  - `build_send_header_full_with_discard_sets_flag`: plan = Full { discard_partial_recv: true }; assert header `discard_partial_recv == true`.
  - `build_send_header_resume_uses_decoded_to_snap`: plan = Resume with a decoded token whose `to_name = "tank/data@snap1"`, `to_guid = 42`, `bytes_received = 1024`; assert header `send_kind = Resume`, `to_snap = SnapshotRef { name: "tank/data@snap1", guid: 42 }`, `discard_partial_recv == false`.
  - `build_send_args_resume_uses_dash_t`: plan = Resume with token `"1-abc"`; assert built argv contains `["send", ..., "-t", "1-abc"]` with no snapshot positional.

**Verify**:

```
cargo test -p arctern-daemon push
cargo clippy -p arctern-daemon --all-targets -- -D warnings
cargo test --workspace
```

**Commit**: `feat(daemon): executor — Resume branch + discard_partial_recv on Full/Incremental (T004)`

## T005 — `test(integration)`: interrupt + resume sequence (Strategy C — SIGKILL the sender mid-stream)

**Changes**:

- `daemon/tests/integration_quic_resume.rs` (NEW). Boot two `LoopbackPool`s; spawn a sink daemon (state_dir, sock, root_fs over receiver pool); pre-create source dataset on sender pool with ~16 MiB urandom + snapshot `test_001`.
- Spawn a sender daemon with a push job pointing at the sink. `interval = "1h"` — drive cycles by wakeup.
- **Phase 1 (interrupted)**:
  1. Wakeup the push job.
  2. Sleep 50 ms (D21).
  3. SIGKILL the sender child (`sender_child.kill()` if `Child::kill` sends SIGKILL on Linux — verify; otherwise use `nix::sys::signal` or a `kill -9`).
  4. Wait up to 10 s for the receiver dataset to advertise a non-`-` token. Poll via `palimpsest::recv::receive_resume_token`.
  5. Assert the token is `Some(_)` AND no snapshot is yet present on the receiver (the partial isn't a real snapshot).
- **Phase 2 (resume)**:
  1. Respawn the sender daemon with the same config.
  2. Wakeup the push job.
  3. Wait up to 30 s for the receiver to gain `test_001` snapshot.
  4. Assert `palimpsest::recv::receive_resume_token` is now `None` (the partial was consumed by the resume).
  5. Assert the receiver snapshot's GUID matches the sender's `test_001` GUID.
- Tear down both daemons; destroy both pools; remove cfg files + sock files + state dirs.
- Document Strategy C in a top-of-file comment with a reference to `specs/006-resume-tokens/spec.md` D7 + `plan.md` D20.
- If Phase 1 fails to produce a token (the SIGKILL was too fast — daemon hadn't started piping yet), the test retries Phase 1 with the delay doubled, up to 3 attempts. Print which attempt succeeded so the failure mode is debuggable.

**Verify**:

```
just vm-up
just test-integration                    # the new test runs alongside the slice-005 push test
just vm-down
```

**Commit**: `test(integration): interrupt mid-stream + resume from token (T005)`

## T006 — `test(integration)`: stale-token discard sub-test

**Changes**:

- Append a second `#[tokio::test]` to `daemon/tests/integration_quic_resume.rs` (or a separate file `integration_quic_resume_discard.rs` — pick whichever keeps the harness reuse simple; if the boot/spawn helper is already factored in T005, append in the same file).
- The flow:
  1. Boot two pools, spawn sink + sender as in T005.
  2. Pre-create source `test_001` and trigger a partial recv via the same Strategy C path. Confirm the receiver advertises a token.
  3. **Invalidate the token**: destroy `<sender_pool>/data@test_001`, snapshot a NEW `test_002`. The old `to_guid` is no longer on the sender.
  4. Wakeup. Wait for the receiver to gain `test_002`.
  5. Assert: (a) `receive_resume_token` is now `None` on the receiver; (b) `test_002` is on the receiver with the matching GUID; (c) `test_001` is NOT on the receiver (the partial was discarded, never became a snapshot).
- This test exercises the `discard_partial_recv` planner branch + the sink's `abort_partial` invocation that T002 wires up.

**Verify**:

```
just vm-up
just test-integration
just vm-down
```

**Commit**: `test(integration): stale token triggers discard + fresh send (T006)`

## Final verification

After T006, the slice meets every gate:

```
cd /home/okhsunrog/code/palimpsest
cargo test --lib && cargo clippy --all-targets --features integration -- -D warnings

cd /home/okhsunrog/code/arctern
cargo check --workspace
cargo clippy --workspace --all-targets --features integration -- -D warnings
cargo test --workspace
! grep -RnE 'tokio::process::Command' --include='*.rs' crates/api crates/client daemon/src/
! grep -RnE '^use regex' --include='*.rs' crates/api crates/client daemon/src/

just vm-up && just test-integration && just vm-down
```
