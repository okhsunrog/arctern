# Feature Specification: QUIC sink job — passive receiver of `zfs send` streams

**Feature Branch**: `004-quic-sink`
**Created**: 2026-05-09
**Status**: Draft
**Input**: Slice 004 of arctern. Add a `sink` job type that binds a QUIC listener (default port 8888, configurable) on the receiver, accepts inbound bidirectional streams, reads a small framed JSON header naming the target dataset, then pipes the rest of the stream into `palimpsest::recv::recv` (streaming form). Replaces zrepl's `internal/transport/tcp/`, `internal/daemon/job/sinkjob.go`, and the receive side of `internal/endpoint/endpoint.go`.

## Why this slice

Slice 003 made the daemon do *local* periodic work (snap jobs). To get arctern past "single-host snapshotter" and into actual replication, exactly two things have to land: a transport (this slice — the passive sink) and a planner+executor (slice 005 — the active push). Sink-only first means we can drive end-to-end receives with raw `quinn` from a test harness, exercising the wire protocol without also writing a full push pipeline; slice 005 then layers planning + cursor management on top of a known-working receiver.

zrepl uses a custom RPC over TCP+TLS (`internal/transport/tcp/`) plus a length-prefixed dataversion stream multiplexed onto the same connection. arctern collapses that to QUIC: each `zfs recv` is one bidirectional stream, framed with a tiny JSON header, and bulk send bytes flow as raw stream payload until FIN. No HTTP, no length prefixing of the bulk stream — the QUIC FIN is the framing. Authentication is deferred (constitution V): WireGuard is the security perimeter, the cert is self-signed and the verifier accepts anything. This is documented and revisited when arctern leaves loopback/wg-only deployments.

## User Scenarios & Testing *(mandatory)*

The "user" is the operator who runs an arctern daemon on a backup box reachable from one or more push peers over WireGuard. They drop a `[[jobs]] type = "sink"` block into `/etc/arctern/arctern.toml`, restart `arctern daemon`, and the daemon binds a QUIC port on `0.0.0.0:8888` (or whatever they configured) and accepts incoming receives.

### User Story 1 — Operator declares a sink job and inbound `zfs send` streams land as datasets (Priority: P1)

An operator adds a sink job to their TOML config, restarts the daemon, and a peer (real or test) opens a QUIC stream, sends a header naming a target dataset under the sink's `root_fs`, and writes a `zfs send` byte stream. The receiver creates the target dataset (full receive) or extends it (incremental), emits a JSON `{"status":"ok"}` response on the stream, and closes.

**Why this priority**: This *is* the slice. Push, planner, cursor management all build on top.

**Independent Test**: Boot a loopback receiver pool. Pre-create a source dataset in the SAME VM with a snapshot; capture `zfs send` bytes via SSH into a buffer. Spawn the daemon with a sink-job config. Build a `quinn` client, open a bidirectional stream, write the framed header naming the receiver target, write the captured bytes, finish the stream, await the JSON response. Assert response status = ok and that the target dataset + snapshot now exist on the receiver pool.

**Acceptance Scenarios**:

1. **Given** a sink job listening on `127.0.0.1:0` and a captured `zfs send` byte stream, **When** a QUIC client opens a stream, writes the header `{"version":1,"target_dataset":"<sink_root>/recv_target"}` followed by the bytes and finishes, **Then** the receiver replies `{"status":"ok"}`, the dataset exists on the receiver pool, and at least one snapshot is present on it.
2. **Given** a sink job and a malformed header (truncated, invalid JSON, wrong version), **When** the client writes it, **Then** the server replies `{"status":"error","message":"<reason>"}` and closes the stream; the receiver creates no dataset.
3. **Given** a sink job and a header whose `target_dataset` is outside the configured `root_fs`, **When** the client writes it, **Then** the server replies with an error (`message` names the violation) and closes the stream without invoking `zfs recv`.
4. **Given** a sink job and a `zfs recv` failure (e.g., bytes are not a valid send stream), **When** the receiver runs `zfs recv`, **Then** the server replies `{"status":"error","message":"<recv stderr summary>"}` and the connection closes; the daemon stays up.
5. **Given** the daemon receives `SIGTERM` while a receive is in flight, **When** it shuts down, **Then** the QUIC endpoint stops accepting new connections, in-flight per-stream tasks are cancelled, the job-manager shutdown deadline is honoured, and the process exits cleanly.
6. **Given** two streams arrive concurrently on the same connection (or on different connections), **When** they each name a different target, **Then** both receives proceed in parallel and both replies are correct.

### User Story 2 — `arctern configcheck` validates sink jobs (Priority: P1)

A CI / pre-deploy script invokes `arctern configcheck /etc/arctern/arctern.toml` against a config containing a sink job. The command parses + validates the file (including the sink-specific fields: `listen` parses as `SocketAddr`, `root_fs` is non-empty), prints `ok` and exits 0; on failure it exits non-zero with a stderr message naming the offending field.

**Why this priority**: Same rationale as slice 003's User Story 2 — a config that ships to production must be validatable without standing the daemon up.

**Independent Test**: `cargo run -p arctern-daemon -- configcheck` against a valid sink config exits 0; against `listen = "not-a-socket-addr"` it exits non-zero naming the field.

**Acceptance Scenarios**:

1. **Given** a TOML config with a syntactically valid sink job, **When** `configcheck` runs, **Then** it exits 0 with `ok`.
2. **Given** a sink job with `listen = "not-an-addr"`, **When** validated, **Then** the command exits non-zero with a stderr message naming `jobs[N].listen` and the parse error.
3. **Given** a sink job with `root_fs = ""`, **When** validated, **Then** the command exits non-zero with a stderr message naming `jobs[N].root_fs`.

### User Story 3 — `GET /api/v1/jobs` reports sink jobs (Priority: P2)

Operators use the existing `GET /api/v1/jobs` endpoint to confirm a sink is bound and observe `last_run` / `last_error`. For a sink, `last_run` reflects the most recent accepted stream (or connection), and `last_error` reflects the last per-stream failure summary. `next_run` is `null` (event-driven, no schedule).

**Why this priority**: Constitution III makes job state a UI concern. Operators need to see "yes, the sink received a stream at T from peer P" without grepping logs.

**Independent Test**: With the integration-test sink running, fire one stream, then GET `/api/v1/jobs` and assert the returned `JobStatus` for the sink has `kind = "sink"`, `last_run` non-null, `last_error` null.

**Acceptance Scenarios**:

1. **Given** the daemon is running with one sink job, **When** the client GETs `/api/v1/jobs`, **Then** the response contains an entry with `kind = "sink"` and `next_run = null`.
2. **Given** the daemon has just accepted a successful stream, **When** the client GETs `/api/v1/jobs`, **Then** `last_run` is the stream-completion time and `last_error` is null.
3. **Given** the most recent stream failed (recv or framing), **When** the client GETs `/api/v1/jobs`, **Then** `last_error` is a short Display string for the failure.

### Edge Cases

- **Cert + key files do not exist on first start**: the daemon generates a self-signed cert + key with subject CN `arctern` and writes them to `<state_dir>/cert.pem` (mode 0o644) and `<state_dir>/key.pem` (mode 0o600). On subsequent starts, the existing files load.
- **`<state_dir>` does not exist**: the daemon `mkdir -p`s it on startup. Default is `/var/lib/arctern`. Override via top-level `state_dir = "..."` in the TOML.
- **`<state_dir>/key.pem` is world-readable**: the daemon does NOT auto-fix permissions on existing files (they were written by something the operator may rely on). It logs a warn but does not refuse to start. Lazy enforcement; revisit when auth lands.
- **Listener bind fails** (port in use, EACCES on a privileged port, address unparseable at runtime): the daemon's startup fails the same way as a UDS bind failure: log + non-zero exit. No partial-up state.
- **Two sink jobs declared with overlapping `listen` addresses**: validated at config-load time. Two sinks on `0.0.0.0:8888` and `0.0.0.0:8888` is rejected; `0.0.0.0:8888` and `127.0.0.1:8888` is also rejected (the wildcard subsumes the loopback). `0.0.0.0:8888` and `0.0.0.0:8889` is allowed.
- **Header length prefix declares > 1 MiB**: rejected before reading the body. The header is a small JSON object; > 1 MiB is a malformed sender or an attacker. Reply with the standard error response, close the stream.
- **`target_dataset` is exactly equal to `root_fs`**: rejected. Sink writes go to children of `root_fs`, not `root_fs` itself. (`root_fs` is created by the operator; the sink is a tenant inside it.)
- **`target_dataset` contains `..` or starts with `/`**: rejected by the same `starts_with("<root_fs>/")` check. ZFS dataset names cannot contain `..` legally, but defence-in-depth.
- **Receiver stream FIN before the header is fully written**: framing layer returns `UnexpectedEof`; reply error if possible (the response stream may already be closed by the peer; ignore the response-write error).
- **Concurrent receives to the same target dataset**: not prevented by arctern. ZFS itself will return an error from one of the two `zfs recv` invocations; the loser sees an `error` response. This matches zrepl's behaviour (no per-target lock at the daemon layer).
- **State-dir on a noexec/nosuid fs**: irrelevant — we only write data files (cert + key), not executables. No special handling.
- **Existing `key.pem` exists but `cert.pem` does not (or vice versa)**: refuse to start with a clear error. The two are a pair; never regenerate one without the other.

## Requirements *(mandatory)*

### Functional Requirements

#### Config schema

- **FR-001**: `Config` MUST gain an optional top-level field `state_dir: PathBuf` (default `/var/lib/arctern`). The daemon MUST `mkdir -p` it on startup.
- **FR-002**: `JobConfig` MUST gain a `Sink(SinkJobConfig)` variant alongside `Snap(...)`. `#[serde(deny_unknown_fields)]` continues to apply.
- **FR-003**: `SinkJobConfig` MUST contain at minimum: `name: String`, `listen: SocketAddr` (parses `"0.0.0.0:8888"`), `root_fs: String` (non-empty, no trailing slash), `recv: RecvConfig` (optional, defaulted).
- **FR-004**: `RecvConfig` MUST contain `properties: RecvProperties` (optional, defaulted). `RecvProperties` MUST contain `override: BTreeMap<String, String>` and `inherit: Vec<String>`. Both default empty. Mirrors zrepl's `recv.properties` block.
- **FR-005**: `arctern configcheck` MUST validate sink-job specifics: `listen` parses (already serde-side), `root_fs` non-empty + no trailing `/`, no two sink jobs share an effective `listen` address (per Edge Cases — wildcard 0.0.0.0 subsumes loopback). Job-name uniqueness across mixed `snap`/`sink` types continues to apply.

#### Wire protocol

- **FR-006**: Each accepted QUIC bidirectional stream represents exactly one receive operation. Framing on the stream is:
  ```
  [ u32 BE: header_length ]            // 4 bytes; rejected if > 1 MiB
  [ JSON header: ReceiveHeader ]        // exactly header_length bytes
  [ raw zfs send bytes ]                // up to stream FIN
  // server then writes:
  [ JSON response: ReceiveResponse ]    // single line, no length prefix
  // server finishes its send-side
  ```
- **FR-007**: `ReceiveHeader` MUST be `{ version: u32, target_dataset: String, send_flags: SendFlags? }`. `version` MUST be exactly `1` for slice 004; any other value is rejected. `target_dataset` is the FULL receiver-side dataset path (e.g., `okdata/backups/laptop/okdata/data/home`) — the sender computes it from its source dataset + the receiver's `root_fs`. `send_flags` is reserved for slice 005's resume + raw + properties handling and is ignored this slice.
- **FR-008**: `ReceiveResponse` MUST be `{ "status": "ok" }` on success or `{ "status": "error", "message": "<reason>" }` on failure. `message` MUST NOT contain newlines (single-line JSON is easier for telemetry).
- **FR-009**: The server MUST validate `target_dataset.starts_with(&format!("{}/", root_fs))` before invoking `zfs recv`. A target equal to `root_fs` itself is rejected (sink datasets are tenants under the root, not the root).

#### TLS identity

- **FR-010**: On first sink-job startup, if `<state_dir>/cert.pem` and `<state_dir>/key.pem` are both absent, the daemon MUST generate a self-signed certificate (subject CN `arctern`) via `rcgen`, write `cert.pem` with mode 0o644 and `key.pem` with mode 0o600. If both files exist, load them. If exactly one exists, refuse to start with a clear error.
- **FR-011**: The QUIC client (slice 005, but the verifier landing this slice) MUST install a `rustls::client::danger::ServerCertVerifier` that returns `Ok(ServerCertVerified::assertion())` for every cert. WireGuard is the security perimeter (constitution V deferral, documented in the plan).

#### Sink job runtime

- **FR-012**: `SinkJob` MUST implement `Job` (the trait introduced in slice 003). On `run`, it MUST:
  1. Bind a `quinn::Endpoint` server to `listen` using the loaded TLS identity. On bind failure, set `last_error` and exit cleanly (the daemon should already have failed startup if bind fails synchronously; bind happens at job-spawn time).
  2. Print a `LISTEN_QUIC <bound_addr>` line to stdout (after the existing `LISTEN unix:<path>` line) so the integration test can discover the actual bound port (jobs may use `:0` to request an OS-assigned port).
  3. Loop: `tokio::select! { _ = cancel.cancelled() => break, conn = endpoint.accept() => spawn_per_connection(conn) }`.
- **FR-013**: Per-connection task: accepts bidirectional streams in a loop with the same select-on-cancel pattern. Per-stream task: reads the framed header, validates, invokes `palimpsest::recv::recv`, copies the QUIC RecvStream into the recv child's stdin, finishes recv, drains stderr, writes the JSON response on the stream's send half, finishes the send half. Updates the `JobStatusInner` after every stream completion (success or error).
- **FR-014**: On cancellation, the sink MUST stop accepting new connections, abort in-flight per-stream tasks (best effort — quinn's RecvStream/SendStream implement cancel-on-drop), and return from `run` within the JobManager's shutdown deadline (5s, set in slice 003).

#### Daemon wiring

- **FR-015**: The daemon's `run_daemon` MUST: (1) ensure `state_dir` exists; (2) for each `JobConfig::Sink(...)`, instantiate a `SinkJob` (loading or generating the cert + key on first sink construction, cached for subsequent sinks); (3) hand the job to the existing `JobManager`. The slice-003 `Snap` path is unchanged.
- **FR-016**: The daemon MUST print `LISTEN_QUIC <addr>` (one line per sink job) to stdout AFTER the existing `LISTEN unix:<path>` line and BEFORE the loop's main idle (so an integration test reading stdout line-by-line can wait for both). Lines are flushed.
- **FR-017**: HTTP API: `JobStatus.kind` for a sink job is `"sink"` (constant `JOB_KIND_SINK` in `crates/api`). `next_run` is always `None`; `last_run` and `last_error` are populated per FR-013.

#### Constitution-IV compliance

- **FR-018**: The new `crates/transport` crate (or whatever holds the cert + verifier + protocol code) MUST NOT import `tokio::process::Command` or `regex::*`. Same gate as slices 002 + 003. The constitution-IV grep MUST be extended to include `crates/transport`.

### Non-Functional Requirements

- **NFR-001**: Total slice size: ~1200-1800 LoC of Rust + spec-kit artifacts. The transport crate (cert + verifier + framing) is ~300; SinkJob is ~250; config additions ~150; the rest is tests + wiring.
- **NFR-002**: NO `tokio::process::Command` calls in `daemon/`, `crates/{api,client,transport}/`. All ZFS interaction goes through `palimpsest::recv::recv`.
- **NFR-003**: NO `anyhow`/`eyre` in `crates/{api,client,config,transport}`. Daemon binary keeps `eyre` for top-level reporting.
- **NFR-004**: Per-stream tasks MUST NOT block the runtime. The hot loop is `tokio::io::copy(&mut quic_recv, &mut child_stdin)` — both sides are fully async. Stderr drain runs on the same task or a sibling.
- **NFR-005**: The integration test MUST take ≤30 seconds wall-clock. Boot a pool, create a small source dataset, snapshot it, capture send bytes (~10 KiB), spawn the daemon, fire one stream, assert, tear down. The QUIC endpoint accept-and-recv loop completes in <1 second for a small stream.

### Key Entities

- **`SinkJobConfig`** (in `crates/config`): `{ name, listen: SocketAddr, root_fs: String, recv: RecvConfig }`.
- **`RecvConfig` + `RecvProperties`** (in `crates/config`): mirrors zrepl `recv.properties.{override, inherit}`.
- **`Config.state_dir`** (in `crates/config`): top-level `Option<PathBuf>` with default `/var/lib/arctern` resolved at the daemon.
- **`crates/transport`** (new workspace member): cert + verifier + framing types. Public API:
  - `pub fn load_or_generate_identity(state_dir: &Path) -> Result<TransportIdentity, TransportError>` — reads or generates `cert.pem` + `key.pem`.
  - `pub fn server_config(identity: &TransportIdentity) -> Result<quinn::ServerConfig, TransportError>`.
  - `pub fn client_config_accept_any() -> quinn::ClientConfig` — installs the accept-any verifier.
  - `pub struct ReceiveHeader { version: u32, target_dataset: String, send_flags: Option<SendFlags> }` (serde).
  - `pub enum ReceiveResponse { Ok, Error { message: String } }` (serde, `#[serde(tag = "status", rename_all = "snake_case")]`).
  - `pub async fn read_header<R: AsyncRead + Unpin>(r: &mut R) -> Result<ReceiveHeader, ProtocolError>`.
  - `pub async fn write_response<W: AsyncWrite + Unpin>(w: &mut W, resp: &ReceiveResponse) -> Result<(), ProtocolError>`.
- **`SinkJob`** (in `daemon/src/jobs/sink.rs`): concrete `Job` impl owning a `quinn::Endpoint` + `Arc<dyn CommandRunner>` + the sink config.
- **`JOB_KIND_SINK`** (in `crates/api`): `pub const JOB_KIND_SINK: &str = "sink";`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `cargo check --workspace`, `cargo clippy --workspace --all-targets --features integration -- -D warnings`, and `cargo test --workspace` all exit 0 on the resulting branch.
- **SC-002**: With `just vm-up` running, `just test-integration` exits 0 across 3 consecutive runs without flake. The new sink integration test plus the slice-003 snap-loop test both pass.
- **SC-003**: A QUIC client built with the sibling accept-any verifier connects to a sink listening on `127.0.0.1:<bound_port>`, opens a bidi stream, writes the framed header + captured `zfs send` bytes, finishes, and receives `{"status":"ok"}`. The receiver pool then contains the named dataset and at least one snapshot.
- **SC-004**: `arctern configcheck` against a valid sink config exits 0; against `listen = "not-an-addr"` exits non-zero with a stderr message naming the field; against `root_fs = ""` exits non-zero similarly.
- **SC-005**: Constitution-IV grep returns zero matches across `crates/{api,client,transport}` and `daemon/src/`:
  ```
  ! grep -RnE 'tokio::process::Command' --include='*.rs' crates/api crates/client crates/transport daemon/src/
  ! grep -RnE '^use regex' --include='*.rs' crates/api crates/client crates/transport daemon/src/
  ```
- **SC-006**: `GET /api/v1/jobs` returns an entry with `kind = "sink"` and a populated `last_run` after the integration test fires a stream.
- **SC-007**: Cert + key files appear at `<state_dir>/cert.pem` (0o644) and `<state_dir>/key.pem` (0o600) after first sink-job startup; subsequent starts load them without regenerating.

## Assumptions

- WireGuard is the security perimeter for the QUIC link in this slice. The accept-any TLS verifier is documented in plan + spec; fixing it requires a credential exchange mechanism that does not exist yet (slice 006 or later).
- `quinn` 0.11 (or whatever `cargo add` resolves to today) is compatible with `rustls` 0.23 and the workspace's existing `tokio` 1.x. The plan codifies the version pinning if any peer-dep mismatch surfaces during T001.
- `palimpsest::recv::recv` returns a `ChildHandle` whose `stdin` is an `AsyncWrite`; this is the streaming form per the prior summary and is verified by reading `palimpsest/src/recv/mod.rs` during T002. If the API is missing a flag the sink needs (`-F`, `-u`, `-o property=...`), it is added to palimpsest first as a prep commit on master.
- Integration-test VM (port 2226) is up to date with palimpsest's expectations from slices 001–003. No new VM-side requirements.
- The receiver pool's `root_fs` is pre-created by the test (and in production by the operator); the sink does not auto-create it.

## Out of scope (Non-Goals)

- The `push` job (planner, executor, cursor bookmarks, resume token handling, dry-run sizing). Lands in slice 005.
- An `arctern client send ...` CLI verb. Push happens in slice 005; the integration test for slice 004 uses raw `quinn` directly.
- `pull` and `source` job types. Land later.
- TLS authentication beyond accept-any. WireGuard is the perimeter for now.
- Per-peer authorisation: which client may write to which `root_fs`. Trivially adds when auth lands.
- `recv -o canmount=off` etc. as recv-time flags on the wire. The `RecvProperties` config field exists this slice but is NOT wired into the recv invocation; that wiring lands in slice 005 alongside the matching send-side resume/raw/properties story so they can be tested as one piece.
- Bandwidth shaping, multiplexing limits, fairness across peers. Single-threaded-fair via quinn's congestion control; no arctern-side limits.
- Hot-reload of the sink config (rebind on TOML change). Restart the daemon.
- macOS / BSD support (carried over).
