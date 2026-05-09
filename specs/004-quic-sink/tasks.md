# Tasks: QUIC sink job — passive receiver of `zfs send` streams

**Feature**: `004-quic-sink`
**Input**: [spec.md](./spec.md), [plan.md](./plan.md)

Each task = one logical commit. Per-task verification commands listed.

## T001 — `chore(workspace)`: add `crates/transport` + quinn/rustls deps

**Why first**: every later transport task lives in this crate or imports it.

**Changes**:
- Hand-create `crates/transport/{Cargo.toml, src/lib.rs}` (cargo new is overkill).
- `crates/transport/Cargo.toml`: `name = "arctern-transport"`, `version.workspace`, `edition.workspace`, `publish.workspace`. From inside `crates/transport/`, run `cargo add quinn rustls rustls-pemfile rcgen serde tokio thiserror`. Verify `cargo tree -i rustls` shows a single version (D-risk-1 in plan).
- Add `serde` features `derive`; add `tokio` features `io-util` (for AsyncReadExt/AsyncWriteExt).
- `crates/transport/src/lib.rs`: empty `pub` skeleton.
- Workspace `Cargo.toml`: add `"crates/transport"` to `members`.

**Verify**:
```
cargo check -p arctern-transport
cargo tree -i rustls   # exactly one version
```

**Commit**: `chore(workspace): add crates/transport + quinn/rustls deps (T001)`

## T002 — `feat(transport)`: TLS identity — load + lazy generate + accept-any verifier

**Changes**:
- `crates/transport/src/identity.rs`:
  - `pub struct TransportIdentity { pub cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>, pub key: rustls::pki_types::PrivateKeyDer<'static> }`.
  - `pub fn load_or_generate_identity(state_dir: &Path) -> Result<TransportIdentity, TransportError>`. If both `cert.pem` + `key.pem` exist: parse via `rustls_pemfile`. If neither exists: `rcgen::generate_simple_self_signed(vec!["arctern".into()])`, write `cert.pem` (0o644) and `key.pem` (0o600). If exactly one exists: error.
- `crates/transport/src/tls.rs`:
  - `pub fn server_config(identity: &TransportIdentity) -> Result<quinn::ServerConfig, TransportError>` — wraps a `rustls::ServerConfig` (built via `rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(...)`) in quinn's `QuicServerConfig::try_from(...)`.
  - `pub fn client_config_accept_any() -> quinn::ClientConfig` — builds `rustls::ClientConfig` via `dangerous().set_certificate_verifier(Arc::new(AcceptAnyVerifier))`.
  - `struct AcceptAnyVerifier;` impl `rustls::client::danger::ServerCertVerifier` returning OK from every method. Document why in module-level comment (WireGuard perimeter; constitution V deferral).
- `crates/transport/src/lib.rs`: re-exports + `pub enum TransportError` (thiserror).
- Unit tests: round-trip — generate to a tempdir, then load from same tempdir, assert identity bytes equal. Two-of-one error case (only key.pem present) returns the expected error.

**Verify**:
```
cargo test -p arctern-transport identity
cargo test -p arctern-transport tls
```

**Commit**: `feat(transport): TLS identity load/generate + accept-any verifier (T002)`

## T003 — `feat(transport)`: wire protocol — header + response + framing

**Changes**:
- `crates/transport/src/protocol.rs`:
  - `pub struct ReceiveHeader { pub version: u32, pub target_dataset: String, #[serde(default, skip_serializing_if = "Option::is_none")] pub send_flags: Option<SendFlags> }`.
  - `pub struct SendFlags;` (empty placeholder for slice 005).
  - `pub enum ReceiveResponse { Ok, Error { message: String } }` with `#[serde(tag = "status", rename_all = "snake_case")]`.
  - `pub const PROTOCOL_VERSION: u32 = 1;`.
  - `pub const MAX_HEADER_LEN: usize = 1 << 20;` // 1 MiB cap (D19).
  - `pub async fn read_header<R: AsyncRead + Unpin>(r: &mut R) -> Result<ReceiveHeader, ProtocolError>` — read 4-byte BE u32 length, validate <= MAX_HEADER_LEN, read body, `serde_json::from_slice`, validate `version == PROTOCOL_VERSION`.
  - `pub async fn write_header<W: AsyncWrite + Unpin>(w: &mut W, h: &ReceiveHeader) -> Result<(), ProtocolError>` — symmetric for the future client side.
  - `pub async fn write_response<W: AsyncWrite + Unpin>(w: &mut W, resp: &ReceiveResponse) -> Result<(), ProtocolError>` — JSON serialize + `write_all`. No length prefix on the response (single line until stream close suffices for slice 004; future framing change is wire-incompatible and bumps the version).
  - `pub async fn read_response<R: AsyncRead + Unpin>(r: &mut R) -> Result<ReceiveResponse, ProtocolError>` — `read_to_end` then JSON parse. (Used by slice-005 client side and by integration test.)
  - `pub enum ProtocolError` (thiserror) variants: Io, Json, HeaderTooLarge, UnsupportedVersion.
- `cargo add serde_json` in `crates/transport`.
- Unit tests: header round-trip; response round-trip (Ok + Error); HeaderTooLarge for length > 1 MiB; UnsupportedVersion for `version: 2`.

**Verify**:
```
cargo test -p arctern-transport protocol
```

**Commit**: `feat(transport): receive-header + response wire types + framing (T003)`

## T004 — `feat(config)`: sink job schema + state_dir + validation

**Changes**:
- `crates/config/src/schema.rs`:
  - `Config` gains `pub state_dir: Option<PathBuf>`.
  - `JobConfig::Sink(SinkJobConfig)` variant.
  - `SinkJobConfig { name, listen: SocketAddr, root_fs: String, recv: RecvConfig (default) }`.
  - `RecvConfig { properties: RecvProperties (default) }`.
  - `RecvProperties { #[serde(rename = "override")] overrides: BTreeMap<String, String>, inherit: Vec<String> }`.
  - Update `JobConfig::name()` to cover the Sink arm.
- `crates/config/src/lib.rs`:
  - Extend `validate(...)`: dispatch on JobConfig variants. New `validate_sink(idx, &SinkJobConfig)` checks: `root_fs` non-empty + no leading/trailing `/`. Cross-job check: collect all sink `listen` addrs and reject if two effective binds overlap (wildcard 0.0.0.0:N subsumes any IP:N).
  - Re-export `SinkJobConfig`, `RecvConfig`, `RecvProperties` from the crate root.
- Unit tests: a sink config parses; `listen = "not-an-addr"` rejected; `root_fs = ""` rejected; `root_fs = "tank/"` rejected; two sinks on `0.0.0.0:8888` rejected; two sinks on `0.0.0.0:8888` + `127.0.0.1:8888` rejected; sinks on `0.0.0.0:8888` + `0.0.0.0:8889` accepted.

**Verify**:
```
cargo test -p arctern-config sink
cargo test -p arctern-config       # all
```

**Commit**: `feat(config): sink job schema + state_dir + listen-overlap validation (T004)`

## T005 — `feat(daemon)`: SinkJob — bind + accept loop + per-stream recv pipeline

**Changes**:
- `daemon/Cargo.toml`: `cargo add quinn arctern-transport` (path). `cargo add serde_json` if not already present.
- `daemon/src/jobs/sink.rs` (new):
  - `pub const KIND: &str = "sink";`
  - `pub struct SinkJob { config: SinkJobConfig, identity: Arc<TransportIdentity>, status: Mutex<JobStatusInner>, bound_addr: Mutex<Option<SocketAddr>> }`.
  - `impl SinkJob { pub fn new(config: SinkJobConfig, identity: Arc<TransportIdentity>) -> Self }`.
  - `pub fn bound_addr(&self) -> Option<SocketAddr>` — for the daemon to print `LISTEN_QUIC`.
  - `impl Job for SinkJob`:
    - `name()`, `kind() -> KIND`, `status()` mirror SnapJob.
    - `run`: build server config from identity + bind `quinn::Endpoint::server`, store `local_addr` in `bound_addr`. Loop with select-on-cancel; `accept_connection` → spawn per-connection task. On cancel: `endpoint.close(0u32.into(), b"shutdown"); endpoint.wait_idle().await`.
  - per-connection task: loop `connection.accept_bi()` with select-on-cancel; spawn per-stream task. Connection task ends when peer closes or cancel fires.
  - per-stream task:
    1. `read_header(&mut recv)` — if Err, write Error response, finish.
    2. Validate `target_dataset.starts_with(&format!("{}/", root_fs))` and `target_dataset != root_fs`. If invalid, write Error response, finish.
    3. `palimpsest::recv::recv(runner, &RecvArgs::new(target_dataset))` — get ChildHandle.
    4. Spawn a stderr drain into `Vec<u8>`. Concurrently `tokio::io::copy(&mut quic_recv, child.stdin.as_mut().unwrap()).await`. Drop stdin (close). Await stderr drain.
    5. `child.wait().await`. If exit non-success: build Error response from stderr summary; if success: Ok.
    6. `write_response(&mut send, &resp)`; `send.finish()`.
    7. Update `JobStatusInner.last_run` + `last_error`.
- Bootstrap: `pub fn build_sink(config: SinkJobConfig, identity: Arc<TransportIdentity>, runner: Arc<dyn CommandRunner>) -> SinkJob` (the runner lives in JobContext; passed via the Job trait's `run` arg, matching SnapJob).
- Module wiring: `daemon/src/jobs/mod.rs` adds `pub mod sink;`.

**Verify**:
```
cargo check -p arctern-daemon
cargo clippy -p arctern-daemon -- -D warnings
```

**Commit**: `feat(daemon): SinkJob — quinn endpoint + per-stream recv pipeline (T005)`

## T006 — `feat(daemon)`: wire SinkJob in main.rs + LISTEN_QUIC handshake + configcheck path

**Changes**:
- `daemon/src/main.rs`:
  - Resolve `state_dir` from config (default `/var/lib/arctern`). `std::fs::create_dir_all`.
  - If any sink jobs exist, call `arctern_transport::load_or_generate_identity(&state_dir)?` once and wrap in `Arc`.
  - In the job-construction loop, add `JobConfig::Sink(s) => { let job = Arc::new(SinkJob::new(s, identity.clone())); manager.spawn(job.clone(), ctx.clone()); sinks.push(job); }`. After spawning, give each sink a brief moment to bind (poll `job.bound_addr()` for up to 5s) and then print one `LISTEN_QUIC <addr>` per sink to stdout.
  - Print `LISTEN_QUIC` lines AFTER the existing `LISTEN unix:` line. Flush.
- `daemon/src/configcheck.rs`: unchanged (validation is in `arctern_config`; T004 covers it). Verify by smoke test.

**Verify**:
```
cargo build -p arctern-daemon
cat > /tmp/cc-sink.toml <<'EOF'
[[jobs]]
type = "sink"
name = "x"
listen = "127.0.0.1:0"
root_fs = "tank/backups"
EOF
cargo run -p arctern-daemon -- configcheck /tmp/cc-sink.toml   # expect: ok
```

**Commit**: `feat(daemon): wire SinkJob, state_dir bootstrap, LISTEN_QUIC handshake (T006)`

## T007 — `feat(api)`: JOB_KIND_SINK constant

**Changes**:
- `crates/api/src/lib.rs`: `pub const JOB_KIND_SINK: &str = "sink";`.
- `daemon/src/jobs/sink.rs`: `KIND` becomes `arctern_api::JOB_KIND_SINK` (via re-export or direct constant).

**Verify**:
```
cargo check -p arctern-api -p arctern-daemon
```

**Commit**: `feat(api): JOB_KIND_SINK constant (T007)`

## T008 — `test(integration)`: end-to-end QUIC sink

**Changes**:
- `daemon/tests/common/mod.rs`: extend `spawn_daemon_uds_with_config` (or add `spawn_daemon_uds_with_config_and_quic`) to also collect `LISTEN_QUIC <addr>` lines from stdout. Returns the bound QUIC port(s) alongside the existing `(child, socket_path)`.
  - Implementation: keep reading stdout lines for up to 10 seconds OR until N expected QUIC lines have been seen (caller passes `expected_quic: usize`).
- `daemon/Cargo.toml` dev-deps: `cargo add --dev quinn arctern-transport` (path).
- `daemon/tests/integration_quic_sink.rs` (new):
  1. Boot a `LoopbackPool` as the receiver. Create child dataset `<pool>/sink_root` (`zfs create -o mountpoint=none`).
  2. Create a separate source dataset in the SAME VM via SSH: `<pool>/source_data`. Write a small file via `zfs set` of a property is not enough — instead, the test SSH-execs `dd if=/dev/urandom of=/tmp/sd-data bs=1k count=4` then `zfs snapshot <pool>/source_data@s1`. (Since the source dataset must be mounted to receive writes, set `mountpoint=/tmp/sd-mp` and rely on altroot for safety.)
     - Simpler alternative used here: create the source dataset, snapshot it without writing data (an empty snapshot is still a valid send stream). The test exercises framing + recv plumbing, not a particular payload.
  3. Capture `zfs send <pool>/source_data@s1` bytes via SSH into a `Vec<u8>` (use `runner.run` with stdout capture; an empty-dataset snapshot is a few hundred bytes).
  4. Write a TOML config with `state_dir = "/tmp/arctern_test_<nanos>"` and one sink job: `name = "sink_test"`, `listen = "127.0.0.1:0"`, `root_fs = "<pool>/sink_root"`.
  5. Spawn the daemon; wait for `LISTEN unix:` + one `LISTEN_QUIC` line.
  6. Build a `quinn::Endpoint::client((Ipv4Addr::LOCALHOST, 0).into())?`, install accept-any client config, `endpoint.connect(quic_addr, "arctern")?.await?`.
  7. Open a bidi stream. `write_header(&mut send, &ReceiveHeader { version: 1, target_dataset: format!("{}/sink_root/recv_target", pool.name()), send_flags: None })`. Then `send.write_all(&captured_bytes).await?; send.finish()?;`.
  8. `read_response(&mut recv).await?` — assert `ReceiveResponse::Ok`.
  9. List snapshots on receiver via `palimpsest::dataset::list` rooted at `<pool>/sink_root/recv_target`; assert ≥1 snapshot present.
  10. Tear down: kill daemon, destroy pool.
- Test attribute: `#[tokio::test(flavor = "multi_thread")]`.

**Verify**:
```
just vm-up
just test-integration
just vm-down
```

**Commit**: `test(daemon): integration test for QUIC sink end-to-end (T008)`

## Dependency graph

```
T001 (workspace) ──> T002 (identity+TLS) ──┐
                ──> T003 (protocol) ───────┤
                                            ├─> T005 (SinkJob)
T004 (config schema) ──────────────────────┘
T005 ──> T006 (daemon wiring + LISTEN_QUIC) ──> T007 (api const)
                                                │
                                                v
                                            T008 (integration)
```

T001 strictly first. T002 + T003 + T004 are independent (different crates) and can land in any order after T001. T005 needs T002, T003, T004. T006 needs T005. T007 is a one-line tweak. T008 verifies the whole stack.

## Done when

All of: `cargo test --workspace` green, `cargo clippy --workspace --all-targets --features integration -- -D warnings` clean, `just test-integration` exits 0, all 8 commits land on the slice branch, the constitution-IV grep returns no matches in `crates/{api,client,transport} daemon/src/`.
