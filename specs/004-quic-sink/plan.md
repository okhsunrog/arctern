# Implementation Plan: QUIC sink job — passive receiver of `zfs send` streams

**Branch**: `004-quic-sink` | **Date**: 2026-05-09 | **Spec**: [spec.md](./spec.md)
**Input**: `specs/004-quic-sink/spec.md`

## Summary

Add a passive `sink` job that binds a QUIC listener via `quinn`, accepts inbound bidirectional streams, reads a length-prefixed JSON header naming the receiver dataset, then pipes the raw stream payload into `palimpsest::recv::recv`. TLS identity is a self-signed cert with an accept-any verifier on both ends — WireGuard is the security perimeter and proper auth is deferred. Cert + key are generated lazily on first sink-job startup at `<state_dir>/cert.pem` + `key.pem`. Config gains a top-level `state_dir` field and a `Sink(SinkJobConfig)` variant. Daemon prints a `LISTEN_QUIC <addr>` handshake line per sink so integration tests can discover the bound port. `JOB_KIND_SINK` lands in `crates/api`; `GET /api/v1/jobs` reports sink jobs alongside snap jobs.

## Technical Context

**Language/Version**: Rust 1.95, edition 2024.
**Primary Dependencies**: existing `axum` 0.8, `clap`, `tokio`, `tracing`, `serde`, `palimpsest`, `tokio-util`, `time`, `arctern-config`. New (added via `cargo add` per CLAUDE.md): `quinn` (server + client endpoints), `rustls` (matched to quinn's peer-dep version — quinn 0.11 wants rustls 0.23, verified at T001), `rustls-pemfile` (PEM load), `rcgen` (self-signed cert generation). `tokio-rustls` is pulled transitively by quinn's TLS shim; only added explicitly if compile errors require it.
**Storage**: TOML config on disk (slice 003 carries forward), `<state_dir>/cert.pem` + `key.pem` for the QUIC TLS identity.
**Testing**: `cargo test --workspace` for unit tests (framing round-trip, header validation, root-fs prefix check, cert load/regen logic). `cargo test -p arctern-daemon --features integration -- --test-threads=1` for the end-to-end sink test against the palimpsest VM.
**Target Platform**: Linux x86_64 (carried over).
**Project Type**: Cargo workspace; gains `crates/transport`.
**Performance Goals**: A single small-stream receive (<1 MB) completes in <1 second wall-clock on the test VM. Per-stream overhead is dominated by `zfs recv` startup; the framing layer adds microseconds.
**Constraints**: Constitution principles I-V apply — see Constitution Check. Async-only. No `tokio::process::Command` in arctern source. The new `crates/transport` joins the constitution-IV grep gate.
**Scale/Scope**: ~1200-1800 LoC arctern source + tests + 8 commits.

## Constitution Check

*GATE: passes before implementation.*

| Principle | Compliance |
|---|---|
| I. QUIC With HTTP Semantics | This slice **is** the QUIC plane. Bulk send bytes flow on raw QUIC bidirectional streams (one stream = one receive); HTTP framing is reserved for the future control RPC (slice 005+). The constitution explicitly anticipates this split — bulk = raw QUIC, control = HTTP semantics where helpful. |
| II. One API for Browser and Daemons | `JobStatus` (slice 003) gains `kind = "sink"` via the existing `String` field — no schema change. `JOB_KIND_SINK` is a constant in `crates/api`. The wire protocol is QUIC-stream-internal and intentionally NOT in `crates/api` (it is not browser-facing). |
| III. Web UI Replaces the CLI | No new CLI verbs. The integration test uses raw `quinn` directly; `arctern client send ...` is deferred to slice 005. The existing `configcheck` is extended to validate sink-specific fields. |
| IV. ZFS Through palimpsest | Sink invokes `palimpsest::recv::recv` (streaming form) — NO direct `zfs recv` spawn in arctern source. NO `tokio::process::Command` in `daemon/`, `crates/{api,client,transport}`. The constitution-IV grep extends to `crates/transport`. If `palimpsest::recv::recv` lacks a flag the sink needs (`-F`, `-u`, `-o property=...`), the change goes into palimpsest first as a separate prep commit on master. |
| V. Local-Only by Default, Auth Opt-In | The sink binds a network port — this is the first arctern feature that does. The TLS layer uses a self-signed cert and accept-any verifier on both ends; WireGuard is the security perimeter. The spec + this plan + the cert generation code all document the deferral. The web UI's "network-exposed without auth" indicator is future work. |
| VI. Live Data Over SSE | Not applicable this slice. A future slice may push per-receive progress via SSE; the polling `GET /api/v1/jobs` covers v0 visibility for sinks too. |
| VII. ZFS Metadata Compatibility | The wire protocol is intentionally arctern-native (constitution VII says explicitly "wire protocol is greenfield"). zrepl's `internal/transport/tcp/` framing is NOT honoured. The metadata that ends up in the receiver pool (the snapshot itself) is whatever the sender's `zfs send` produced — full metadata compatibility carries over for free. |

All applicable principles pass. Deferred work tracked in spec's Non-Goals.

## Project Structure

### Documentation (this feature)

```text
specs/004-quic-sink/
├── spec.md     # done
├── plan.md     # this file
└── tasks.md    # next, via speckit-tasks
```

### Source code (repository root)

```text
arctern/
├── crates/
│   ├── api/src/lib.rs          # +pub const JOB_KIND_SINK
│   ├── client/                 # unchanged this slice
│   ├── config/src/schema.rs    # +Config.state_dir, +JobConfig::Sink, +SinkJobConfig, +RecvConfig
│   ├── config/src/lib.rs       # extend validate() for sink jobs
│   └── transport/              # NEW workspace member
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs          # public re-exports
│           ├── identity.rs     # load_or_generate_identity + TransportIdentity
│           ├── tls.rs          # server_config + client_config_accept_any + AcceptAnyVerifier
│           └── protocol.rs     # ReceiveHeader / ReceiveResponse / read_header / write_response
├── daemon/
│   ├── Cargo.toml              # +arctern-transport, +quinn
│   └── src/
│       ├── main.rs             # ensure_state_dir; build sink jobs; print LISTEN_QUIC line per sink
│       ├── jobs/
│       │   ├── mod.rs          # unchanged (Job trait covers sink)
│       │   ├── snap.rs         # unchanged
│       │   └── sink.rs         # NEW: SinkJob impl
│       └── handlers/jobs.rs    # unchanged (kind passthrough already works)
└── daemon/tests/
    └── integration_quic_sink.rs   # NEW: end-to-end QUIC sink test
```

**Structure Decision**:

- The `crates/transport` crate exists because slice 005's push job will reuse the cert + verifier + framing code. Putting it in `daemon/src/quic.rs` would force slice 005 to either depend on the daemon binary (no) or duplicate the code. A leaf `crates/transport` is the cheapest correct choice today.
- `crates/transport` is a leaf crate — no `palimpsest`, no `axum`, no `arctern-config`. It owns wire types and TLS-identity types only. The sink wires it together with `palimpsest::recv` inside `daemon/src/jobs/sink.rs`.
- The sink handler lives in `daemon/src/jobs/sink.rs` (not in `crates/transport`) because it composes transport + palimpsest + the daemon's `Job` trait — three coupling points that cannot all live in a leaf crate.

## Phase 0: Research

Spot-checks done at planning time:

- **`quinn` 0.11 + `rustls` 0.23**: `quinn::Endpoint::server(server_config, addr)?` binds and returns the endpoint. `endpoint.local_addr()?` reports the OS-assigned port when bound to `:0` (needed for the `LISTEN_QUIC` handshake). `endpoint.accept().await` yields `Connecting`; `connecting.await` yields `Connection`; `connection.accept_bi().await` yields `(SendStream, RecvStream)`. RecvStream implements `tokio::io::AsyncRead`; SendStream implements `AsyncWrite`. Cert + key go through `quinn::ServerConfig::with_single_cert(vec![cert_der], key_der)` (or `with_crypto(rustls::ServerConfig)` for full control).
- **`rcgen`**: `rcgen::generate_simple_self_signed(vec!["arctern".to_string()])?` returns a `CertifiedKey` (or older `Certificate`) with `cert.pem()` and `key_pair.serialize_pem()` accessors. Mapping its rustls types into quinn 0.11's expected types is one match arm — verified at T002.
- **Accept-any verifier**: `rustls::client::danger::ServerCertVerifier` trait — implement `verify_server_cert`, `verify_tls12_signature`, `verify_tls13_signature`, `supported_verify_schemes`. Returning `Ok(ServerCertVerified::assertion())` from `verify_server_cert` is the documented escape hatch for "trust everything"; the signature methods return `HandshakeSignatureValid::assertion()`. The `dangerous()` config builder hands back a `DangerousClientConfig` for installing the verifier.
- **palimpsest streaming recv API** (`palimpsest/src/recv/mod.rs`): `pub async fn recv(runner: &dyn CommandRunner, args: &RecvArgs) -> Result<ChildHandle, RecvError>`. ChildHandle's `stdin: Option<Box<dyn AsyncWrite + Unpin + Send>>`, `stderr: Option<Box<dyn AsyncRead + Unpin + Send>>`. After streaming completes: drain stderr, call `child.wait()`, then `check_recv_stderr(&stderr_text)` to detect resume tokens. The sink's hot loop is `tokio::io::copy(&mut quic_recv, child.stdin.as_mut().unwrap()).await`, then drop stdin (close it), then drain stderr concurrently with `wait()`. Slice 004 does not need `-F`, `-u`, or `-o property=...` (only the default RecvArgs); future slices will. **No palimpsest prep commit required.**
- **Length-prefixed framing**: `tokio::io::AsyncReadExt::read_exact` for the 4-byte header length and the JSON body. Write side: `tokio::io::AsyncWriteExt::write_all`. Cap header_length at 1 MiB before reading the body.
- **JSON response on the same stream**: write the response bytes after copying recv stdout (note: recv's stdout is unused; it's stderr we drain) and after `child.wait()` returns — that way the response reflects the actual recv outcome. Then `send_stream.finish().await?`.
- **Per-stream concurrency**: `tokio::spawn` per accepted bi stream. The connection task awaits `connection.closed()` after spawning the stream tasks; cancellation cascades by dropping the connection. quinn handles connection close gracefully.
- **Endpoint shutdown**: `endpoint.close(0u32.into(), b"shutdown")` then `endpoint.wait_idle().await`. Inside `select!` with `cancel.cancelled()`.

## Phase 1: Design artifacts

### TOML schema additions

```toml
# Top-level (new)
state_dir = "/var/lib/arctern"   # default; absent is fine

[[jobs]]
name = "from_remote"
type = "sink"
listen = "0.0.0.0:8888"
root_fs = "okdata/backups"

# Optional. Defaults to empty override + empty inherit.
[jobs.recv.properties]
override = { canmount = "off", "org.openzfs.systemd:ignore" = "on" }
inherit = ["mountpoint"]
```

### Rust types (excerpt)

```rust
// crates/config/src/schema.rs
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub state_dir: Option<PathBuf>,   // NEW
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobConfig {
    Snap(SnapJobConfig),
    Sink(SinkJobConfig),               // NEW
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkJobConfig {
    pub name: String,
    pub listen: SocketAddr,
    pub root_fs: String,
    #[serde(default)]
    pub recv: RecvConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecvConfig {
    #[serde(default)]
    pub properties: RecvProperties,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecvProperties {
    #[serde(default, rename = "override")]
    pub overrides: BTreeMap<String, String>,
    #[serde(default)]
    pub inherit: Vec<String>,
}
```

### Wire protocol types

```rust
// crates/transport/src/protocol.rs
#[derive(Debug, Serialize, Deserialize)]
pub struct ReceiveHeader {
    pub version: u32,
    pub target_dataset: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_flags: Option<SendFlags>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SendFlags { /* reserved for slice 005 */ }

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReceiveResponse {
    Ok,
    Error { message: String },
}
```

### Quickstart (developer)

```bash
cd ~/code/palimpsest && just vm-up
cd ~/code/arctern

cat > /tmp/arctern-sink.toml <<'EOF'
state_dir = "/tmp/arctern-state"
[[jobs]]
type = "sink"
name = "smoke_sink"
listen = "127.0.0.1:8888"
root_fs = "tank/backups"
EOF

PALIMPSEST_SSH_TARGET=root@localhost:2226 PALIMPSEST_SSH_PASSWORD="" \
  cargo run -p arctern-daemon -- daemon --config /tmp/arctern-sink.toml \
    --socket /tmp/arctern.sock &
# stdout shows: LISTEN unix:/tmp/arctern.sock
#               LISTEN_QUIC 127.0.0.1:8888
sleep 2
curl --unix-socket /tmp/arctern.sock http://_/api/v1/jobs | jq .
kill %1
cd ~/code/palimpsest && just vm-down
```

CI:

```bash
cd ~/code/arctern && just test-vm
```

## Phase 2: Tasks

Generated by `speckit-tasks` into `specs/004-quic-sink/tasks.md`. Expected ordering (8 tasks):

1. T001 — `chore(workspace)`: add `crates/transport` + quinn/rustls/rustls-pemfile/rcgen deps.
2. T002 — `feat(transport)`: self-signed cert generation + load + accept-any verifier.
3. T003 — `feat(transport)`: wire protocol — header struct, length-prefixed framing, encode/decode.
4. T004 — `feat(config)`: sink job schema + validation (state_dir, listen overlap, root_fs).
5. T005 — `feat(daemon)`: `SinkJob` — bind endpoint, accept loop, per-stream task; cert dir bootstrap.
6. T006 — `feat(daemon)`: wire `SinkJob` into JobManager; add `LISTEN_QUIC` handshake; sink-aware configcheck.
7. T007 — `feat(api)`: `JOB_KIND_SINK` constant.
8. T008 — `test(integration)`: end-to-end sink test (capture send → QUIC client → recv).

## Decisions made beyond the slice ticket's D1-D12

- **D13** (formalised at planning, see also slice 003 D13): `crates/transport` joins the constitution-IV grep allowlist of crates checked. The verifier check is `crates/api crates/client crates/transport daemon/src/`. Reason: `crates/transport` is a leaf crate that owns network surface; it has every reason NOT to spawn `zfs` and every opportunity to.
- **D14**: the `state_dir` default `/var/lib/arctern` is hard-coded in the daemon binary, not in `arctern-config`. Reason: defaults that depend on the deployment context (root-only path) belong with the binary's CLI surface, not the leaf config crate. `Config.state_dir` is `Option<PathBuf>` and the daemon resolves the `None` case.
- **D15**: cert + key are loaded once at daemon startup (the first time a sink job is constructed), then handed to every sink via an `Arc<TransportIdentity>`. Reason: a single keypair across all sinks on the same host is fine for slice 004's "one daemon per box" deployment; per-job keypairs would just confuse operators.
- **D16**: the `LISTEN_QUIC <addr>` handshake line uses the same shape as `LISTEN unix:<path>` — a single space-separated token line on stdout, flushed. The integration test's `spawn_daemon_uds_with_config` extends to read additional lines until EOF or a configurable predicate; reuse is the goal, not a new helper. (See T008.)
- **D17**: per-stream tasks update `JobStatusInner` via a shared `Arc<Mutex<JobStatusInner>>` owned by `SinkJob`. The same pattern as `SnapJob` from slice 003. No per-stream metrics fan-out yet (no SSE topic).
- **D18**: the wire protocol's `version` field is `u32` (not a bitfield enum) so that future-slice clients can negotiate by sending `version: 2` and getting a clear error from a `version: 1` server. The error response carries `message: "unsupported version: 2"`.
- **D19**: header-length cap is 1 MiB. A real header today is ~150 bytes; 1 MiB is comfortably above any conceivable expansion. A 4 GiB cap (the u32 max) would let an attacker burn 4 GiB of receiver memory before a single byte of validation; 1 MiB stays well within receiver budget.
- **D20**: response is written AFTER `child.wait()` so the wire status reflects the recv outcome. A naive design would write `Ok` as soon as `tokio::io::copy` completes, but `zfs recv` can fail post-stream during the final commit; the test would race. Synchronous wait keeps the protocol simple.
- **D21**: per-sink concurrency is unbounded (one task per stream, one task per connection). zrepl bounds receive concurrency per peer; arctern defers that knob until an operator hits a real backpressure problem. quinn's flow control caps memory per connection.
- **D22**: `RecvConfig.properties.{override, inherit}` is parsed and validated this slice but NOT wired into `palimpsest::recv::recv` invocations. Reason: `palimpsest::recv::RecvArgs` does not currently accept `-o property=...` or `-x property` flags; adding them is a palimpsest change that pairs naturally with slice 005 (where the matching send-side `-o`/`-x`/raw/properties story also lands and can be tested as one piece). The config field exists now so operators can declare intent; honouring it is a one-commit follow-up in slice 005 that touches palimpsest plus the sink invocation.

## Verification

```bash
# Inside arctern repo
cargo check --workspace
cargo clippy --workspace --all-targets --features integration -- -D warnings
cargo test --workspace                          # unit tests

# Constitution principle IV gates (D13 — extended to crates/transport)
! grep -RnE 'tokio::process::Command' --include='*.rs' crates/api crates/client crates/transport daemon/src/
! grep -RnE '^use regex' --include='*.rs' crates/api crates/client crates/transport daemon/src/

# Integration (requires VM)
just vm-up
just test-integration
just vm-down
```

## Risks

- **quinn / rustls peer-dep mismatch**: `cargo add quinn` may resolve a quinn version whose rustls peer-dep does not match the rustls `cargo add` resolves separately. Mitigation: T001 runs `cargo tree -i rustls` and verifies a single version; if two versions appear, pin them in T001's commit.
- **`rcgen` API churn**: rcgen's API moved between 0.12 and 0.13 (CertifiedKey vs Certificate). Mitigation: T002 codes against whatever `cargo add` resolves and writes a tiny test confirming PEM round-trip; if the test fails, pin to a known version.
- **Accept-any verifier compile errors**: rustls 0.23 made the danger trait stricter (more methods to implement). Mitigation: T002 implements every method explicitly with documented "trust-all" behaviour.
- **Integration-test flakiness from QUIC port collision**: the test asks for `:0`; the OS assigns a free port; the test reads it from the LISTEN_QUIC line. No collision possible.
- **`palimpsest::recv::recv` deadlock from unread stderr**: documented in palimpsest's recv module ("Callers must consume `stdout`/`stderr` before (or concurrently with) calling `wait()`"). The sink's per-stream task drains stderr concurrently with the `tokio::io::copy` of the QUIC payload into stdin; both await before `child.wait()`.
- **State-dir race**: two sinks starting simultaneously could both attempt to generate cert files. Mitigation: cert load + (if absent) generation happens once at daemon startup, before sinks spawn; `Arc<TransportIdentity>` is shared. No race.
