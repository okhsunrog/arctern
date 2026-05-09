# Implementation Plan: Resume tokens — pick up interrupted pushes

**Branch**: `006-resume-tokens` | **Date**: 2026-05-09 | **Spec**: [spec.md](./spec.md)
**Input**: `specs/006-resume-tokens/spec.md`

## Summary

Wire ZFS resumable receive end-to-end through arctern. Sink switches every `zfs recv` to `zfs recv -s` and surfaces the resulting `receive_resume_token` user property in the LIST response. Planner consumes the token, validates it via `palimpsest::resume_token::decode` against the sender's snapshot list, and either emits a new `SnapshotPlan::Resume { token }` (token live) or falls back to Full/Incremental with a `discard_partial_recv` flag set on the SEND header (token stale). Executor learns one new branch — Resume — that builds `SendArgs::resume_token(token)` and pipes the resulting `zfs send -t <token>` stream over the existing SEND stream. Sink learns one new branch on `handle_send` — when `discard_partial_recv == true`, call `palimpsest::recv::abort_partial(target_dataset)` before spawning the new recv. Two palimpsest prep commits land on master first: `RecvArgs::resumable` + `palimpsest::recv::abort_partial` (D6 confirmed `-A` is the only path back from a partial when the new stream's GUIDs differ), and `ResumeToken::from_guid` for incremental-resume validation. No new TOML knobs; no new HTTP routes; no `PROTOCOL_VERSION` bump.

## Technical Context

**Language/Version**: Rust 1.95, edition 2024.
**Primary Dependencies**: existing crates from slice 005. No new top-level deps. `cargo add` is not expected to fire this slice.
**Storage**: TOML config on disk (unchanged). No new persistence — receiver's `receive_resume_token` user property IS the source of truth across cycles and across daemon restarts.
**Testing**: `cargo test --workspace` for unit tests (planner Resume/Discard variant logic, wire round-trip for the new fields, sink dispatch on `discard_partial_recv`). `cargo test -p arctern-daemon --features integration -- --test-threads=1` for the end-to-end interrupt+resume test against the palimpsest VM.
**Target Platform**: Linux x86_64.
**Project Type**: Cargo workspace; no new members.
**Performance Goals**: a resumed cycle is dominated by ZFS work (the actual remaining bytes to send + recv); the planner adds one extra `palimpsest::resume_token::decode` round-trip per filesystem-with-token, ~10 ms.
**Constraints**: Constitution principles I-V apply — see Constitution Check. Async-only. No new `tokio::process::Command` in arctern source.
**Scale/Scope**: ~600-900 LoC arctern source + tests across 6 commits. Two palimpsest prep commits on master (already pushed before this slice's branch was opened).

## Constitution Check

*GATE: passes before implementation.*

| Principle | Compliance |
|---|---|
| I. QUIC With HTTP Semantics | Unchanged. Same two QUIC stream shapes from slice 005 (`op = "list"` + `op = "send"`). The new `discard_partial_recv` SEND-header field and the `receive_resume_token` LIST-response field are additive on existing payloads. |
| II. One API for Browser and Daemons | New wire fields land in `crates/transport`, daemon-internal. Nothing browser-facing this slice. `JOB_KIND_PUSH` is unchanged. |
| III. Web UI Replaces the CLI | No new CLI verbs. The wakeup endpoint from slice 005 is reused unchanged; resume happens automatically per cycle. |
| IV. ZFS Through palimpsest | Every new ZFS interaction (`zfs recv -s` via `RecvArgs::resumable()`, `zfs recv -A` via `palimpsest::recv::abort_partial`, `zfs send -t <token>` via `SendArgs::resume_token`, `receive_resume_token` query via `palimpsest::recv::receive_resume_token`, token decode via `palimpsest::resume_token::decode`) routes through palimpsest. Two prep commits (recv `-s`+`abort_partial`; resume_token `from_guid`) landed on palimpsest master before this slice opened. |
| V. Local-Only by Default, Auth Opt-In | No new bind. No new transport endpoint. Reuses slice 004's accept-any verifier. |
| VI. Live Data Over SSE | Not applicable this slice. |
| VII. ZFS Metadata Compatibility | The `receive_resume_token` user property is set by `zfs recv -s` itself — not an arctern artifact. Tokens have no operator-visible name; they aren't bookmarks; they don't survive a `zfs recv -A`. The receiver pool stays bit-identical to a zrepl-managed pool when arctern takes over. |

All applicable principles pass. Deferred work tracked in spec's "Out of scope".

## Project Structure

### Documentation (this feature)

```text
specs/006-resume-tokens/
├── spec.md     # done
├── plan.md     # this file
└── tasks.md    # next, hand-written to match slice 005's format
```

### Source code (repository root)

```text
arctern/
├── crates/
│   └── transport/
│       └── src/protocol.rs       # +ListResponse.receive_resume_token,
│                                 #  +SendKind::Resume, +SendHeader.discard_partial_recv
└── daemon/
    └── src/
        └── jobs/
            ├── sink.rs           # recv switches to .resumable(); +abort_partial dispatch;
            #                       +token in handle_list response
            └── push.rs           # +SnapshotPlan::Resume; +token validation in plan_one_filesystem;
            #                       +Resume branch in execute_one_plan; +discard_partial_recv plumbed
└── daemon/tests/
    └── integration_quic_resume.rs   # NEW: interrupt + resume + stale-token discard
```

**Structure Decision**:

- All wire-protocol additions live in the existing `crates/transport/src/protocol.rs`. No new module; the slice doesn't introduce new vocabulary, just extends slice-005 types additively.
- All planner + executor changes live in the existing `daemon/src/jobs/push.rs`. Same rationale as slice 005's "no new module" — the surface stays inside the file with its tests.
- All sink-side changes live in `daemon/src/jobs/sink.rs`. Two surgical edits: `handle_send` learns to honour `discard_partial_recv`; `handle_list` learns to populate `receive_resume_token`. Every `RecvArgs::new(...)` call gains `.resumable()`.

## Phase 0: Research

Spot-checks done at planning time:

- **Palimpsest `RecvArgs::resumable()` and `palimpsest::recv::abort_partial`** — landed in palimpsest master prep commit `feat(recv): -s (resumable) flag + abort_partial helper`. `-s` appears in `build_args` after the `-e`/`-d` block. `abort_partial` runs `zfs recv -A <ds>`, returns `Ok` for a successful abort AND for the "no partial state to abort" exit (idempotent).
- **Palimpsest `ResumeToken::from_guid: Option<u64>`** — landed in palimpsest master prep commit `feat(resume_token): expose fromguid for incremental-resume validation`. Parsed from the nvlist's `fromguid` line; `None` for full-send tokens (the nvlist omits the field).
- **Palimpsest `SendArgs::resume_token(...)`** — already present per slice 002; produces `zfs send -t <token>` and ignores the snapshot field. Verified by `palimpsest::send::tests::send_resume_token_spawns_correct_args` (passes).
- **VM verification (D6) — what `zfs recv -s` does on this OpenZFS version**:
  - `zfs recv -s <ds>` succeeds; on partial-stream input, the dataset gains `receive_resume_token = <hex string>`.
  - `zfs send -t <token>` produces a stream that completes the partial; receiver dataset reaches full size; GUID matches sender's source snapshot.
  - **CRITICAL**: a fresh `zfs send <full-snap> | zfs recv -s <ds>` into a dataset with a non-empty `receive_resume_token` is REJECTED, and `-F` does NOT override it. The error is `destination ... contains partially-complete state from "zfs receive -s"`. This is the empirical basis for FR-007 + the new `discard_partial_recv` wire flag — the planner cannot assume "fresh send into partial == auto-replace".
  - `zfs recv -A <ds>` clears the partial cleanly; subsequent `zfs list` shows the dataset does not exist (because the partial was for a fresh dataset that was never instantiated).
- **Token nvlist format** — full-send token has fields `object`, `offset`, `bytes`, `toguid`, `toname`. Incremental-resume token also has `fromguid`. Verified in the VM with a synthetic incremental partial. The palimpsest fixture covers the full case; the new test in palimpsest covers the incremental case as a parser unit test against a hand-rolled nvlist string (we don't need a captured fixture for it because the format is stable).
- **Wire compat**: `ListResponse::Ok { snapshots, receive_resume_token }` — the field is `#[serde(default, skip_serializing_if = "Option::is_none")]` so a slice-005 sink (no field) emits a JSON that deserializes cleanly with `receive_resume_token: None`. `SendHeader.discard_partial_recv` is `#[serde(default)]` so a slice-005 sender (no field) deserializes as `false`. `SendKind::Resume` is a NEW serde variant — a slice-005 sink receiving `send_kind = "resume"` would fail deserialization. Acceptable: slice 005 never shipped externally; we update both ends in lockstep.
- **Existing receive_resume_token test infra** — `palimpsest::recv::receive_resume_token` is fully tested in palimpsest with both `-` (no token) and a real token fixture. The arctern-side use is a one-line call; no need for a parallel arctern unit test.

## Phase 1: Design artifacts

### Wire protocol changes (additive on slice 005)

```rust
// crates/transport/src/protocol.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ListResponse {
    Ok {
        snapshots: Vec<SnapshotEntry>,
        /// Receiver-side `receive_resume_token` for this dataset, if a
        /// partial recv from a prior `zfs recv -s` is still in flight.
        /// None means no partial OR the dataset doesn't exist.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receive_resume_token: Option<String>,
    },
    Error { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendKind {
    Full,
    Incremental,
    /// `zfs send -t <token>` resume of a prior partial recv. The wire
    /// `from_snap` and `to_snap` are informational; the actual stream
    /// content is determined by the token. Resume headers MUST have
    /// `discard_partial_recv = false` — the resume IS the partial.
    Resume,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendHeader {
    pub send_kind: SendKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_snap: Option<SnapshotRef>,
    pub to_snap: SnapshotRef,
    pub flags: SendFlagsWire,
    /// When true, the sink calls `palimpsest::recv::abort_partial` on
    /// `target_dataset` before spawning the new `zfs recv`. Used when
    /// the planner detected a stale resume token on the receiver and
    /// chose to send a fresh full or incremental rather than continue.
    /// `false` for slice-005-shape Full / Incremental and for Resume.
    #[serde(default)]
    pub discard_partial_recv: bool,
}
```

### Planner extension

```rust
// daemon/src/jobs/push.rs

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPlan {
    Nothing,
    Full {
        to: SnapshotRef,
        /// Receiver had a stale partial recv; sink must clear it first.
        discard_partial_recv: bool,
    },
    Incremental {
        from: SnapshotRef,
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    /// Resume an in-flight partial recv on the receiver. The token
    /// targets a specific (from, to) snapshot pair encoded inside it.
    Resume {
        token: String,
        /// Decoded for logging + planner-side validation. The wire
        /// payload only carries the raw token; this is local-only.
        decoded: palimpsest::resume_token::ResumeToken,
    },
}
```

`plan_one_filesystem` algorithm:

1. List sender snapshots (unchanged from slice 005).
2. Run LIST request. Capture `(snapshots, receive_resume_token)`.
3. If `receive_resume_token.is_none()`: classic slice-005 `pick_plan` → wrap in `Full { discard_partial_recv: false, ... }` / `Incremental { discard_partial_recv: false, ... }` / `Nothing`.
4. If `receive_resume_token.is_some()`:
   - `decode = palimpsest::resume_token::decode(runner, &token).await?`
   - Build the sender's GUID set.
   - `to_live = sender_guids.contains(decode.to_guid)`
   - `from_live = decode.from_guid.is_none() || sender_guids.contains(decode.from_guid.unwrap())`
   - If `to_live && from_live`: emit `SnapshotPlan::Resume { token, decoded }`.
   - Else: token is stale. Run slice-005 `pick_plan` → wrap with `discard_partial_recv: true`.

### Executor extension

```rust
// daemon/src/jobs/push.rs

pub fn build_send_header(plan: &SnapshotPlan, flags: &SendFlagsConfig) -> Option<SendHeader> {
    match plan {
        SnapshotPlan::Nothing => None,
        SnapshotPlan::Full { to, discard_partial_recv } => Some(SendHeader {
            send_kind: SendKind::Full,
            from_snap: None,
            to_snap: to.clone(),
            flags: wire_flags(flags),
            discard_partial_recv: *discard_partial_recv,
        }),
        SnapshotPlan::Incremental { from, to, discard_partial_recv } => Some(SendHeader {
            send_kind: SendKind::Incremental,
            from_snap: Some(from.clone()),
            to_snap: to.clone(),
            flags: wire_flags(flags),
            discard_partial_recv: *discard_partial_recv,
        }),
        SnapshotPlan::Resume { decoded, .. } => Some(SendHeader {
            send_kind: SendKind::Resume,
            from_snap: None,
            to_snap: SnapshotRef {
                name: decoded.to_name.clone(),
                guid: decoded.to_guid,
            },
            flags: wire_flags(flags),
            discard_partial_recv: false,
        }),
    }
}

pub fn build_send_args(plan: &SnapshotPlan, sender_dataset: &str, flags: &SendFlagsConfig) -> Option<SendArgs> {
    match plan {
        SnapshotPlan::Nothing => None,
        SnapshotPlan::Full { to, .. } => /* slice-005 builder, unchanged */,
        SnapshotPlan::Incremental { from, to, .. } => /* slice-005 builder, unchanged */,
        SnapshotPlan::Resume { token, .. } => {
            // Snapshot field is ignored when SendArgs.from = ResumeToken;
            // pass a placeholder so the type is satisfied.
            let mut args = SendArgs::new("ignored").resume_token(token);
            // Apply the user's wire flags. zfs send -t <token> respects
            // -w/-c/-L/-e on the resumed stream just like a fresh send.
            if flags.encrypted { args = args.raw(); }
            if flags.embedded_data { args = args.embedded(); }
            if flags.compressed { args = args.compressed(); }
            if flags.large_blocks { args = args.large_blocks(); }
            Some(args)
        }
    }
}
```

`execute_one_plan` adds an info log line for Resume (carrying `bytes_received` from `decoded.bytes_received`) and for stale-discard (carrying `discard_partial_recv = true`). Otherwise the stream-pump skeleton is identical to slice 005.

### Sink extension

```rust
// daemon/src/jobs/sink.rs

async fn handle_send(...) -> Result<(), String> {
    // ...existing parent-create logic...

    if let Some(send) = &header.send
        && send.discard_partial_recv
    {
        tracing::info!(target = %header.target_dataset, "sink: discarding partial recv");
        if let Err(e) = palimpsest::recv::abort_partial(runner, &header.target_dataset).await {
            return Err(format!("abort_partial {}: {e}", header.target_dataset));
        }
    }

    let mut args = RecvArgs::new(header.target_dataset.clone()).unmounted().resumable();
    // ... existing properties_override + properties_inherit + recv plumbing ...
}

async fn handle_list(...) -> ListResponse {
    // ... existing snapshot enumeration ...
    let receive_resume_token = match palimpsest::recv::receive_resume_token(
        runner, &header.target_dataset
    ).await {
        Ok(opt) => opt,
        Err(palimpsest::ZfsError::DatasetNotFound { .. }) => None,
        Err(e) => {
            // Snapshot list already succeeded; the token query failing
            // is a soft failure — log + return None.
            tracing::warn!(error = %e, target = %header.target_dataset,
                "sink: receive_resume_token query failed; LIST returning None");
            None
        }
    };
    ListResponse::Ok { snapshots, receive_resume_token }
}
```

### Quickstart (developer)

Same as slice 005's quickstart — no new TOML, no new endpoint. To exercise resume manually:

```bash
# Inside the VM, after the sender has streamed enough bytes:
ssh root@vm 'pkill -KILL arctern-daemon-sender'  # abrupt drop
ssh root@vm 'zfs get -H -o value receive_resume_token tank/sink/tank/data'
# expected: 1-bada404f7-...  (a token, not "-")

# Restart the sender daemon. The next cycle resumes automatically.
```

CI:

```bash
cd ~/code/arctern && just test-vm
```

## Phase 2: Tasks

Generated into `specs/006-resume-tokens/tasks.md`. Six tasks (one logical commit each):

1. **T001 — feat(transport)**: `ListResponse::Ok` gains `receive_resume_token: Option<String>`; `SendHeader` gains `discard_partial_recv: bool`; `SendKind` gains `Resume`. All additive with `#[serde(default)]`/`skip_serializing_if`. Round-trip + default-deserialization tests.
2. **T002 — feat(daemon)**: `sink.rs::handle_list` populates `receive_resume_token` via `palimpsest::recv::receive_resume_token`. `handle_send` switches to `RecvArgs::resumable()` and honours `discard_partial_recv` via `palimpsest::recv::abort_partial`.
3. **T003 — feat(daemon)**: `push.rs` planner — extend `SnapshotPlan` with `Resume` and the `discard_partial_recv` flag on Full/Incremental; teach `plan_one_filesystem` to read the token, decode it, validate against sender GUIDs, and dispatch.
4. **T004 — feat(daemon)**: `push.rs` executor — wire `Resume` and `discard_partial_recv` through `build_send_header` + `build_send_args` + `execute_one_plan` (with the new info log lines).
5. **T005 — test(integration)**: `daemon/tests/integration_quic_resume.rs` — the interrupt+resume sequence from spec's "Acceptance verification".
6. **T006 — test(integration)**: stale-token discard sub-test — append to T005 file or add a second `#[tokio::test]` in the same file. Asserts the planner's stale-token path actually clears the partial via the wire flag.

## Decisions made beyond the slice ticket's D1-D10

- **D11 — `discard_partial_recv` lives on `SendHeader`, not as a standalone op**: D6 verification proved that `zfs recv -F` does NOT override a partial — only `zfs recv -A` does. The slice-005 path "fresh full into a dataset with a partial" is therefore broken under slice 006's own resumable-recv-by-default behaviour. We need a way to clear partials. Options were:
  - (a) New `op = "abort_resume"` request — D10 explicitly defers this. Its weakness: requires the planner to do a 3-stream cycle (LIST → ABORT → SEND) per filesystem when a stale token is present, doubling round-trip cost on the unhappy path.
  - (b) `discard_partial_recv: bool` flag on SEND header — single round-trip, sink does the abort transactionally with the recv it's about to spawn. Adds one bool to the wire, one branch to `handle_send`.
  - Chose (b). The added wire surface is one nullable bool; the planner code is one match arm; the unhappy-path cost is a single `zfs recv -A` invocation on the sink, microseconds.
- **D12 — `SnapshotPlan::Resume` does NOT carry a separate `flags` field**: the `SendFlagsConfig` from `[jobs.send]` flows from the executor at apply time, not from the plan. Plans are descriptions of "what to do"; flag application is "how to invoke the tool". Same separation slice 005 already uses for Full/Incremental.
- **D13 — Stale-token detection requires BOTH guids to be live, not just `to_guid`**: full-send tokens (no `fromguid`) only need `to_guid` live. Incremental-resume tokens need both. Reason: if the sender's `from` snapshot was pruned between the original send's start and now, ZFS cannot generate the incremental delta — `zfs send -t <token>` would fail with "no such snapshot" anyway. Better to detect this in the planner and fall through to a fresh send than to spawn a doomed `zfs send`.
- **D14 — Stale-token fallback emits Full or Incremental, NOT just Full**: the existing `pick_plan` algorithm still applies. If the receiver has shared GUIDs with the sender (via earlier completed snapshots), an Incremental + discard is cheaper than a Full + discard. The discard part doesn't care which.
- **D15 — `palimpsest::resume_token::decode` on the planner-side, NOT on the sink-side**: the sink only needs the raw token (to advertise) and `target_dataset` (to abort). Decoding adds a `zfs send -nvt <token>` round-trip — would force a sink-side palimpsest call per LIST request even for senders that don't care about the token. Cheaper to decode once on the planner, where the decision is made.
- **D16 — `ResumeToken::from_guid` is `Option<u64>` not `u64`**: the nvlist field `fromguid` is absent for full-send tokens. Modeling it as `Option` lets `decode` succeed for both cases without inventing a sentinel value. Validates cleanly in D13's "both guids live" check.
- **D17 — `SendKind::Resume` is a NEW serde variant, not a flag on existing variants**: making it an enum variant keeps the protocol-shape tests honest — a Resume header without a `from_snap` is correct (resume doesn't need it); a Full header without a `from_snap` is also correct; an Incremental header WITHOUT a `from_snap` is wrong. Encoding Resume as `SendKind::Full { resume: bool }` would muddy that invariant.
- **D18 — `discard_partial_recv = true` on a Resume header is a planner bug**: the SEND header type permits the combination at the type level (both fields exist on `SendHeader`), but the planner never emits it. We add a debug-assert in `build_send_header`'s Resume arm.
- **D19 — Sink-side `receive_resume_token` query is best-effort**: if the snapshot enumeration succeeded but the token query fails (race against a `zfs recv -A` from another arctern instance, transient ZFS busy, etc.), return `None` rather than failing the whole LIST. The planner will then fall through to slice-005 behaviour and the next cycle picks up whatever the actual state is. The risk is one cycle that doesn't resume when it could have; ZFS recovers on the next.
- **D20 — Integration test uses Strategy C (kill the daemon mid-stream)**: see spec "Acceptance verification" — Strategy A requires in-process control over the daemon's QUIC connection (the daemon is a subprocess in the test); Strategy B requires SSH'ing into the VM and `pkill`-ing the recv child specifically (fragile across timing). Strategy C is brutal but observable: SIGKILL the sender daemon while it has bytes in flight, then assert the receiver advertises a token on the next LIST.
- **D21 — Test sleep before SIGKILL**: 50 ms after wakeup. The wakeup endpoint returns 204 immediately; the daemon then opens the QUIC connection, runs the LIST, gets back empty snapshots, opens the SEND stream, spawns `zfs send`, and starts piping bytes. With ~16 MiB of urandom in the source dataset, all of that takes well under 50 ms in a hot VM. We pick 50 ms because it's the smallest delay that reliably gets us into the `tokio::io::copy` loop on a cold VM (verified locally with a `tracing::info!` instrumented build during planning).
- **D22 — Stale-token sub-test reuses the T005 fixture**: rather than a brand-new dataset, the T006 sub-test takes the partially-recv'd state from T005-phase-1 and destroys the SOURCE snapshot to invalidate `to_guid`. Then trigger a wakeup and assert: (a) the sink ran `recv -A` (the partial is gone), (b) a fresh full landed against the new latest sender snapshot. This reuses the wire/sink/planner machinery of T005 and exercises a different planner branch.
- **D23 — No `PROTOCOL_VERSION` bump**: every wire change is additive with `#[serde(default)]`/`skip_serializing_if`. A pre-006 sink talking to a post-006 sender works (sender's `discard_partial_recv: false` is the default; sender ignores the missing `receive_resume_token` field as `None`). A post-006 sink talking to a pre-006 sender works (sink's `discard_partial_recv = false` matches slice-005 behaviour exactly; sink's added `receive_resume_token` field is harmlessly serialized into the response — pre-006 sender ignores it). The one breaking-shape change is `SendKind::Resume`, which a pre-006 sink would reject — but a pre-006 sender would never emit it.
- **D24 — Resume across job restart works for free**: planner reads receiver state at the start of every cycle; daemon restart doesn't lose that. Tested implicitly by T005 (the sender daemon IS killed and restarted between phase 1 and phase 2). Documented here for completeness per the slice ticket's D10 closing bullet.

## Verification

```bash
# Inside arctern repo
cargo check --workspace
cargo clippy --workspace --all-targets --features integration -- -D warnings
cargo test --workspace                          # unit tests

# Constitution principle IV gates (unchanged from slice 005)
! grep -RnE 'tokio::process::Command' --include='*.rs' crates/api crates/client crates/transport daemon/src/
! grep -RnE '^use regex' --include='*.rs' crates/api crates/client daemon/src/

# Integration (requires VM)
just vm-up
just test-integration
just vm-down
```

## Risks

- **Token decode round-trip cost on every cycle when a token is present**: one extra `zfs send -nvt <token>` per filesystem-with-token per cycle. Bounded by ZFS's own decode cost (microseconds in user space) plus an SSH round-trip in tests. Negligible for production WG-only deployments.
- **Stale-token detection false positives**: the planner only checks `to_guid`/`from_guid` against currently-listed sender snapshots. If a snapshot was destroyed and recreated under the same name with a new GUID between the partial-recv-start and now, the planner sees the GUID as gone — correctly emits stale-discard. There is no false-positive risk because GUIDs are not reused.
- **Stale-token detection false negatives**: theoretically possible if the sender's snapshot list call is racing a destroy, but `palimpsest::dataset::list` is one synchronous `zfs list` invocation — its output is consistent at the moment it ran. Worst case: the planner picks Resume; `zfs send -t <token>` fails with "no such snapshot"; per-fs cycle reports the error; next cycle replans (and now the destroy is committed everywhere, planner sees the token as stale, emits discard). Recovers in two cycles.
- **`zfs recv -A` failing while the partial exists**: would happen if a competing arctern instance or a manual operator command is also messing with the dataset. We surface the abort failure as the per-fs error and don't try to recv. Operator can clear by hand. Same recovery story as any other ZFS command failure in a per-fs cycle.
- **Resume token wire size**: ZFS-emitted tokens for our test workload are ~150 bytes. We've never seen one larger than ~512 bytes in the wild. The `MAX_HEADER_LEN` of 1 MiB applies to the LIST request header, not the response — but the response is read with `read_to_end` and parsed via `serde_json::from_slice`, no per-field cap. A pathologically large token (multi-MB) would still fit in process memory; we accept that.
- **SIGKILL timing in T005**: see D21. If the test environment is slower than expected and 50 ms isn't enough to get into the IO copy, the sender daemon dies before opening any stream and the receiver shows no partial. T005 retries with a longer delay if the first phase didn't produce a token; the test reports it explicitly so the failure mode is debuggable rather than mysterious.
- **Sink's `abort_partial` on a dataset whose parent doesn't exist**: shouldn't happen in practice (parent creation already happened on the original recv that produced the partial), but if it does, `zfs recv -A` returns "dataset does not exist" — `abort_partial` matches that as a soft success. The subsequent recv then errors normally on the missing parent and the per-fs cycle reports it.
- **Wire compat with future SendKind variants**: a slice-007+ sink seeing a `SendKind::Resume` from a slice-006 sender works fine (recognized variant). A slice-006 sink seeing a hypothetical future `SendKind::Bookmark` from a slice-007+ sender would fail deserialization. We accept that — it's the same compat story as adding a new HTTP route. Bumping `PROTOCOL_VERSION` would be appropriate for any change that requires a slice-006 sink to refuse the request rather than misinterpret it.
