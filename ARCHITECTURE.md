# arctern: architecture pivot

This document supersedes the QUIC-based design in `.specify/memory/constitution.md` and the slice docs under `specs/`. Read it end-to-end before changing code.

## What changes

The QUIC transport, the dual HTTP/2-and-HTTP/3 axum router, the self-signed TLS identity, and the accept-any verifier are all gone. Replication runs over SSH using a multi-channel stdinserver pattern. Each daemon serves its own UI on loopback HTTP. The active daemon proxies a subset of API endpoints to its peer over a persistent SSH control channel, so the user gets a single dashboard that sees both hosts without the passive host exposing a network listener.

The push direction stays the same: the laptop has the data, the home server stores backups, the laptop is the active sender, the home server is the passive receiver. Everything else about replication semantics — GUID-based common-snapshot detection, resume token logic, `discard_partial_recv` + `recv -A`, send flags, retention rules — is preserved.

The spec-kit workflow is dropped. `.specify/` and `specs/` are deleted. Comments referencing slice numbers (D6, T002, etc.) are removed opportunistically as files are touched.

## Topology

```
                        ┌─────────────────────────────────┐
                        │              Laptop             │
                        │                                 │
              browser ──┼──→ axum on 127.0.0.1:7878       │
                        │     │                            │
                        │     │ /api/v1/...   (local)     │
                        │     │ /api/v1/peers/home/...    │
                        │     │   (proxied over SSH)      │
                        │     ↓                            │
                        │   arctern daemon                │
                        │     ├─ scheduler (snap, push)   │
                        │     ├─ PeerLink to home server  │
                        │     ├─ SQLite at state.db       │
                        │     └─ openssh::Session         │
                        └─────────────┬───────────────────┘
                                      │
                                      │  one TCP+SSH session
                                      │  (ControlMaster, multi-channel)
                                      │
                        ┌─────────────┴───────────────────┐
                        │           Home server           │
                        │                                 │
                        │   sshd                          │
                        │     │ ForcedCommand on          │
                        │     │ authorized_keys           │
                        │     ↓                            │
                        │   arctern stdinserver-dispatch  │
                        │     ├─ control channel (long)   │
                        │     ├─ recv channel (per send)  │
                        │     └─ ... (parallel)           │
                        │                                 │
                        │   arctern daemon (optional)     │
                        │     for own snap-jobs and       │
                        │     own UI on 127.0.0.1:7878    │
                        └─────────────────────────────────┘
```

The home server runs sshd, the arctern binary in PATH, and an `authorized_keys` entry with `ForcedCommand`. That alone is enough for replication and for the laptop's UI to view/manage server state. The home server's own `arctern daemon` is optional — only needed if you want the home server to run its own snap-jobs and serve its own loopback UI for local browsing.

## Transport: SSH with multiple channels

Use the `openssh` crate. Not `russh`. The system `ssh(1)` brings `~/.ssh/config`, agent, hardware tokens, ProxyJump, and ControlMaster — none of which we want to reimplement.

The laptop's daemon holds one `openssh::Session` per peer. ControlMaster keeps the underlying TCP and crypto state alive across all channels in that session. Channels are opened on demand via `session.command(...)`. Each channel maps to one `arctern stdinserver-dispatch` process on the home server, spawned by sshd, with a role determined by `SSH_ORIGINAL_COMMAND`.

Channel kinds:

- **`control`** — long-lived, one per session. Carries framed RPC requests and responses. Used for LIST, status queries, snapshot inventory, destroy operations, anything not bulk. The laptop's daemon opens this when establishing the peer link and reuses it indefinitely.
- **`recv`** — short-lived, one per replication step. Spawned for the duration of a single `zfs send → zfs recv` pipe. Closed after the recv completes (success or failure).

Multiple `recv` channels can be open concurrently for parallel replication of different filesystems, alongside the control channel which keeps serving UI queries during transfers. SSH multiplexes them transparently over the one TCP connection.

### authorized_keys entry

```
command="/usr/local/bin/arctern stdinserver-dispatch laptop_nova",restrict ssh-ed25519 AAAA...laptop-key
```

The identity name (`laptop_nova`) is hardcoded per key. OpenSSH's `command=` directive does not have a portable substitution for the authenticating key's fingerprint — `%k` only exists in `AuthorizedKeysCommand` (server-side dynamic auth), not in `authorized_keys`. Valid substitutions in `command=` are `%h` (home), `%u` (user), `%i` (key id from `authorized_keys` options), and `%%`.

`restrict` (OpenSSH ≥ 7.2) disables every channel feature except command exec. The full requested command is in `SSH_ORIGINAL_COMMAND`. The matched `authorized_keys` line is exposed via `SSH_AUTH_INFO_0` (OpenSSH ≥ 7.4) for an optional defense-in-depth fingerprint pin — see ACL config below.

`stdinserver-dispatch` reads `SSH_ORIGINAL_COMMAND`, parses `arctern stdinserver <job> <op> [args...]`, validates that the identity is allowed for the requested `(job, op)` pair via arctern config, then exec's into the appropriate handler.

### Wire protocol on a control channel

The control channel uses framed enum messages. `tokio_util::codec::LengthDelimitedCodec` for framing, `serde_json` for payload (readable in logs; switch to `postcard` later if size matters).

```rust
// crates/transport/src/protocol.rs

#[derive(Serialize, Deserialize)]
pub struct RequestFrame {
    pub id: u64,                             // monotonic per-session, client-assigned
    #[serde(flatten)]
    pub body: Request,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    ListSnapshots {
        dataset: String,
        prefix_regex: Option<String>,
    },
    GetReceiveResumeToken {
        dataset: String,
    },
    DestroySnapshot {
        name: String,
    },
    ListJobs,
    GetJobStatus { name: String },
    WakeupJob { name: String },
    SubscribeEvents { since: Option<u64> },  // for SSE proxying
    GetLogCursor,                            // returns current max log_events.id
    Shutdown,                                // graceful
}

#[derive(Serialize, Deserialize)]
pub struct ResponseFrame {
    pub request_id: Option<u64>,             // None for pushed Event frames
    #[serde(flatten)]
    pub body: Response,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Response {
    ListSnapshotsOk { snapshots: Vec<SnapshotEntry> },
    GetReceiveResumeTokenOk { token: Option<String> },
    DestroySnapshotOk,
    ListJobsOk { jobs: Vec<JobStatusWire> },
    GetJobStatusOk(JobStatusWire),
    WakeupJobOk,
    GetLogCursorOk { id: u64 },
    Event(EventWire),                        // pushed; ResponseFrame.request_id = None
    Error { code: ErrorCode, message: String },
}
```

Requests and responses are correlated by `request_id`. Client maintains a `HashMap<u64, oneshot::Sender<Response>>` keyed by id; server may process requests concurrently and emit responses in any order. `Event` frames have `request_id = None` and are routed to the broadcast subscribers. This avoids head-of-line blocking when one slow query (e.g. `ListSnapshots` over a 10k-snapshot dataset) would otherwise stall the UI.

### Wire protocol on a recv channel

A recv channel writes one header frame followed by the raw `zfs send` byte stream, then half-closes. The server reads the header, spawns `zfs recv -s -u`, pipes the channel's stdin into recv's stdin, waits for the channel EOF and recv's exit, then writes a single response frame and exits.

```rust
#[derive(Serialize, Deserialize)]
pub struct RecvHeader {
    pub version: u32,
    pub target_dataset: String,
    pub send: SendHeader,                    // existing struct, unchanged
}
```

`SendHeader`, `SendKind`, `SnapshotRef`, `SendFlagsWire`, the `discard_partial_recv` flag — all preserved from the current `transport::protocol`. They get reused inside `RecvHeader` and inside `Request::ListSnapshots` responses.

### The dispatch entry point

```rust
// daemon/src/stdinserver/dispatch.rs

#[tokio::main]
async fn main() -> ExitCode {
    let fp = env::args().nth(1).unwrap_or_default();
    let original = env::var("SSH_ORIGINAL_COMMAND").unwrap_or_default();
    // SSH_ORIGINAL_COMMAND parses to: arctern stdinserver <job> <op> [args...]
    
    let parts: Vec<&str> = original.split_whitespace().collect();
    let (job, op, args) = match parts.as_slice() {
        ["arctern", "stdinserver", job, op, rest @ ..] => (job, op, rest),
        _ => return exit_with_error("malformed SSH_ORIGINAL_COMMAND"),
    };
    
    let cfg = arctern_config::load_from_path(&default_config_path())?;
    let acl = cfg.peer_acl(job, &fp).ok_or("unauthorized")?;
    
    match *op {
        "control" if acl.allows_control() => 
            stdinserver::control::run(cfg, job).await,
        "recv" if acl.allows_recv() => 
            stdinserver::recv::run(cfg, job).await,
        _ => exit_with_error("operation not permitted"),
    }
}
```

The control handler holds open framed stdio and serves Requests until EOF. The recv handler reads one RecvHeader, runs recv, writes one Response, exits.

## Replication flow

For a push job firing on the laptop:

1. Daemon ensures the `PeerLink` to the home server is alive (open `Session` if not, send a `Ping` request on control if it is).
2. For each filesystem in the job:
   - `Request::ListSnapshots { dataset = target, prefix_regex }` over the control channel. Receive `Response::ListSnapshotsOk { snapshots }`.
   - `Request::GetReceiveResumeToken { dataset = target }`. Receive token if present.
   - List local sender snapshots via palimpsest.
   - Compute plan via existing `pick_plan_with_token` logic.
   - If plan is `Resume { decoded }`, validate the local-side prerequisites (the snapshots referenced by the token still exist on the sender). Adjust to fall through to `Full+discard` or `Incremental+discard` if invalid.
   - If plan is `Nothing`, skip.
   - Otherwise:
     - Place a step hold on the `to` snapshot locally before sending. `palimpsest::hold::hold(runner, &full_to, "arctern_step_J_<job>")`.
     - If `discard_partial_recv`, send `Request::DiscardPartialRecv { dataset }` over control first.
     - Open a fresh recv channel: `session.command("arctern").arg("stdinserver").arg(job).arg("recv").spawn()`.
     - Write `RecvHeader` to the channel's stdin.
     - Spawn `zfs send` locally, pipe its stdout into the recv channel's stdin via `tokio::io::copy`, drain stderr on a separate task, wait for both.
     - Half-close stdin, read the recv channel's stdout for the single Response frame.
     - On Ok: advance the cursor — `palimpsest::bookmark::create(runner, &full_to, &cursor_bookmark_name)`, then release the previous step hold.
     - On Error: log, leave step hold in place (so a retry can find the snapshot), record the error.
3. After all filesystems processed, drop the recv channels (control stays open). ControlMaster keeps TCP alive.

## Holds and replication cursor (must be added — currently missing)

The hold/bookmark choreography I described in the survey is the protection against the snap-job's prune racing the push-job's send. palimpsest exposes `hold`, `release`, `list_holds`, `bookmark::create`, `bookmark::destroy` already. Wire them.

Naming conventions, pinned for compatibility:

- Step hold: `arctern_step_J_<jobname>` on snapshots that are currently the `from` or `to` of an in-flight send.
- Replication cursor bookmark: `<dataset>#arctern_cursor_J_<jobname>`. Single bookmark per (job, dataset). On success, create a bookmark from the new `to` snapshot, then destroy the previous cursor bookmark by name. ZFS bookmarks are GUID-anchored so the new one survives even if the underlying snapshot is later destroyed.
- Last-received hold (on receiver side): `arctern_last_J_<jobname>` on the most recent successfully received snapshot. Set by stdinserver after recv exits cleanly. Released when the next recv on the same dataset succeeds.

The snap-job's pruner already skips snapshots that return `ZfsError::SnapshotHeld`. Verify this still holds for the new tag names.

If a step or last-received hold is found at startup with no in-flight job (orphan), log a warning and leave it. Adding a recovery sweep is a later concern.

## UI federation: peer-aware axum routes

The laptop's daemon mounts these routes:

```
GET    /api/v1/jobs                                       (local)
GET    /api/v1/jobs/{name}                                (local)
POST   /api/v1/jobs/{name}/wakeup                         (local)
GET    /api/v1/datasets                                   (local)
GET    /api/v1/snapshots?dataset=...                      (local)
GET    /api/v1/events                                     (local SSE)

GET    /api/v1/peers                                      (lists configured peers + reachability)
GET    /api/v1/peers/{peer}/jobs                          (proxied)
GET    /api/v1/peers/{peer}/snapshots?dataset=...         (proxied)
POST   /api/v1/peers/{peer}/snapshots/{name}/destroy      (proxied, if ACL allows)
GET    /api/v1/peers/{peer}/events                        (proxied SSE)
```

Proxied handlers go through the daemon's `PeerLink`, send a Request on the control channel, await Response, translate to the HTTP type. There's no second axum router on the home server reachable from the laptop's browser; the laptop's daemon is the single point of contact. The home server's optional own daemon serves its own loopback UI for local-on-server browsing only.

The browser sees one API. The daemon hides the SSH channel.

### PeerLink shape

```rust
// daemon/src/peer/mod.rs

pub struct PeerLink {
    name: String,
    session: Arc<openssh::Session>,
    control: ControlClient,
    // recv_channels are owned by individual replication tasks, not stored here
}

pub struct ControlClient {
    tx: mpsc::Sender<(Request, oneshot::Sender<Result<Response, RpcError>>)>,
    events: broadcast::Sender<EventWire>,
    // background task owns the channel's stdio and demuxes Responses by request order
    // plus pushes any Event frames into `events`.
}

impl PeerLink {
    pub async fn connect(name: String, target: SshTarget, acl_role: Role) -> Result<Self> { ... }
    pub async fn list_snapshots(&self, dataset: &str) -> Result<Vec<SnapshotEntry>> { ... }
    pub async fn destroy_snapshot(&self, name: &str) -> Result<()> { ... }
    pub async fn open_recv(&self, header: RecvHeader) -> Result<RecvChannel> { ... }
    pub fn subscribe_events(&self) -> broadcast::Receiver<EventWire> { ... }
}
```

A background task owns the control channel's read half, demuxes `ResponseFrame`s by `request_id` into the matching oneshot from the in-flight map, and routes Event frames (request_id = None) into the broadcast.

Reconnect runs **eagerly in a background task** per peer, not lazily on next call. On session loss the task tears down the PeerLink, marks the peer unreachable in `/api/v1/peers`, and attempts reconnect with exponential backoff (1s, 2s, 4s, … capped at 60s). UI calls during a backoff window return HTTP 503 immediately with `Retry-After`, instead of blocking on the backoff. Once reconnected, the task installs the new `ConnectedSession` into a shared `RwLock<Option<…>>` that handlers read.

### ACL config

```toml
[[peers]]
name = "home"
ssh_target = "arctern-replicator@homeserver.lan"

[[jobs]]
type = "push"
name = "backup"
peer = "home"                          # references [[peers]] entry
interval = "15m"
# ... existing push fields, minus connect/server_name
target = { root_fs = "tank/backups/laptop" }

[jobs.snapshot_filter]
prefix = "zrepl_"

# On the HOME SERVER side, the peer's authorized_keys + arctern config
# define what the laptop is allowed to do.

[[allowed_clients]]
identity = "laptop_nova"               # matches the argv to stdinserver-dispatch
fingerprint = "SHA256:abc123..."       # optional defense-in-depth pin; verified
                                       # against SSH_AUTH_INFO_0 if set
jobs = ["backup", "test_backup"]       # one identity may serve multiple jobs
operations = ["control", "recv"]       # control: RPC, recv: bulk receive
root_fs = "tank/backups/laptop"        # recv operations restricted to this subtree
```

`stdinserver-dispatch` enforces that:
- the identity matches an `[[allowed_clients]]` entry,
- if `fingerprint` is set, it matches the key in `SSH_AUTH_INFO_0`,
- the parsed `<job>` from `SSH_ORIGINAL_COMMAND` is in `jobs`,
- the requested `<op>` is in `operations`,
- recv operations target a dataset under the configured `root_fs`,
- destroy operations on snapshots are restricted to that subtree.

## State storage

All replication state lives in ZFS (holds, bookmarks, `receive_resume_token`). The daemon is a stateless scheduler that re-derives plans from ZFS state every cycle. **Do not introduce etcd or any external coordination store.**

Per-daemon SQLite for observability only. Path: `<state_dir>/state.db`. Use `sqlx` with the `sqlite` and `runtime-tokio` features. `journal_mode=WAL`, `synchronous=NORMAL`. Schema:

```sql
CREATE TABLE IF NOT EXISTS job_runs (
    job_name      TEXT NOT NULL,
    started_at    INTEGER NOT NULL,             -- unix seconds
    finished_at   INTEGER,
    status        TEXT NOT NULL,                -- 'ok' | 'error' | 'cancelled' | 'running'
    error_message TEXT,
    bytes_sent    INTEGER,
    PRIMARY KEY (job_name, started_at)
);

CREATE TABLE IF NOT EXISTS log_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    level     TEXT NOT NULL,
    job_name  TEXT,
    message   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_log_recent ON log_events(timestamp DESC);
```

`stdinserver` processes also open this DB to log their operations. SQLite WAL mode handles multiple writers; brief lock contention is fine. Trim policy: keep last 30 days of `job_runs`, last 24 h of `log_events`. Run trim every 6 hours from the daemon's scheduler.

The SQLite tracing layer filters at `INFO` and above. `DEBUG`/`TRACE` events go only to stdout/journald via the standard `tracing-subscriber` fmt layer. This is a hard rule — without it, a debug-level run inside tokio internals will produce kHz-rate events and explode the DB.

## Cancellation and backpressure

Existing patterns in `push.rs` are correct and stay. Specifically:

- Wrap `tokio::io::copy(&mut zfs_send_stdout, &mut recv_channel_stdin)` in `tokio::select!` against the job's `CancellationToken`, with `biased;` so cancel wins races.
- On cancel: drop the copy future, drop the recv channel (which closes the SSH child's stdin and propagates SIGPIPE to remote `zfs recv`), call `start_kill` on the local `zfs send` child, then `wait` to reap.
- Drain `zfs send` stderr on a separate `tokio::spawn` to avoid pipe deadlock.
- Always use `recv -s` so partial state survives. The next cycle picks up via resume token logic.
- After copy completes (success or error), call `child.stdin.shutdown().await` on the SSH channel before reading the response frame, so the remote `zfs recv` sees EOF and finalises.

## What to delete

- `crates/transport/src/tls.rs`
- `crates/transport/src/identity.rs`
- All `quinn`, `rustls`, `rcgen`, `rustls-pemfile`, `rustls-pki-types` dependencies in `crates/transport/Cargo.toml`, `daemon/Cargo.toml`.
- The `LISTEN_QUIC` handshake line in `daemon/src/main.rs` and the `bound_addr` polling loop.
- The `parking_lot_or_std` cosmetic re-export module in `daemon/src/jobs/mod.rs`.
- `_join_state_dir` and `_io_kind` placeholder dead code in `transport/src/identity.rs` (gone with the file anyway).
- `.specify/` directory entirely.
- `specs/` directory entirely.
- The `<!-- SPECKIT START -->` ... `<!-- SPECKIT END -->` markers in `CLAUDE.md`.
- All inline doc comments referencing slice numbers (D6, D16, D18, D22, T002, T008, etc.). Remove opportunistically as files are touched, not in a dedicated sweep.

## What to keep verbatim

- `palimpsest` entirely. Do not modify.
- The `arctern-api` crate's wire types for the local API. They keep working unchanged for `/api/v1/...` endpoints.
- The `arctern-config` crate's `KeepRule`, grid retention algorithm, prune evaluator, filter resolver. The `SnapJobConfig`, `PruningConfig`, `SnapshottingConfig`, `FilesystemFilter`, `SnapshotFilterConfig`, `SendFlagsConfig` shapes. Add `peer` field to `PushJobConfig` (replaces `connect`, `server_name`); add new top-level `[[peers]]` and `[[allowed_clients]]` sections.
- `SnapJob` entirely.
- `JobManager`, `Job` trait, status reporting, wakeup mechanism.
- The planner: `pick_plan`, `pick_plan_with_token`, `pick_plan_with_discard`, `CompiledFilter`, `list_sender_snaps`, `build_send_header`, `build_send_args`. These are pure functions over the plan; they don't care about the transport.
- The resume token decode and validation logic.
- The `discard_partial_recv` plumbing in `SendHeader`.
- The `RecvProperties` (overrides + inherit) wiring on the receive side.
- `palimpsest::recv::abort_partial` integration on the receive side.
- The CLI subcommand surface (`daemon`, `stdinserver`, `configcheck`). Replace `stdinserver` with the dispatch shape above; the others are unchanged.

## Crate layout after pivot

```
crates/
  api/        request/response types for HTTP API (utoipa::ToSchema). Add peer-aware
              variants and the EventWire type for SSE.
  config/     unchanged structurally; add Peer, AllowedClient, peer-reference in PushJobConfig.
  transport/  framed protocol enums (Request, Response, RecvHeader, SendHeader, etc.),
              the LengthDelimitedCodec wrapper, error types. Pure types; no I/O. No more
              tls/identity modules.
daemon/
  src/
    main.rs                  daemon and dispatch entry points (split via subcommand)
    auth.rs                  PeerCredentials connect-info for UDS
    handlers/                axum handlers
      jobs.rs                local job endpoints
      snapshots.rs           local snapshot endpoints
      datasets.rs            local dataset endpoints
      peers.rs               new — proxied peer endpoints
      events.rs              new — SSE for live logs/events
    jobs/
      mod.rs                 JobManager, Job trait
      snap.rs                unchanged
      push.rs                rewritten executor — uses PeerLink::open_recv,
                             palimpsest::hold + bookmark for cursor choreography
    peer/
      mod.rs                 PeerLink, ControlClient, RecvChannel
      reconnect.rs           backoff + reachability state
    stdinserver/
      dispatch.rs            entry point for `arctern stdinserver-dispatch %k`
      control.rs             control channel handler
      recv.rs                recv channel handler
    state/
      mod.rs                 SQLite pool, migrations
      job_runs.rs            queries
      log_events.rs          queries + tracing layer
    router.rs                axum router wiring
    error.rs                 ApiError → HTTP response mapping
admin-ui/                    Vue + Nuxt UI as before; add Peers tab to AdminView
```

## Conventions (preserved from CLAUDE.md)

- Rust edition 2024.
- Async-only.
- `cargo add` for deps, no hand-edits to Cargo.toml versions.
- Errors via `thiserror` in libraries, `eyre` only in `main.rs`.
- Comment WHY, never WHAT.
- No emojis in code, comments, or commit messages.
- TS client auto-generated from OpenAPI; never hand-edit `admin-ui/src/client/`.

## Order of work

Suggested commit sequence. Each commit should compile and pass tests. Don't try to do this in one PR.

1. Delete `.specify/` and `specs/`. Update `CLAUDE.md` to reflect the new design (remove SPECKIT markers, rewrite Stack and Status sections, point at this `ARCHITECTURE.md`).
2. Delete `crates/transport/src/{tls.rs, identity.rs}` and the QUIC-only fields in `Cargo.toml`. Stub out the QUIC code paths in jobs (compile-time disabled) so the rest of the tree still builds while transport is being rewritten.
3. Add the framed protocol module: `Request`, `Response`, `RecvHeader`, `LengthDelimitedCodec` wrapper, codec helpers. Unit tests for round-trip serde of every variant. No I/O yet.
4. Add `daemon/src/state/` — SQLite pool, schema migrations, basic queries. Add a tracing layer that writes to `log_events`.
5. Replace `daemon/src/main.rs`'s subcommand dispatch: keep `daemon`, `configcheck`; replace `stdinserver` with `stdinserver-dispatch`. Add `stdinserver/dispatch.rs` that parses `SSH_ORIGINAL_COMMAND` and exits with not-implemented for now.
6. Add `daemon/src/peer/` — `PeerLink`, `ControlClient`, the openssh integration. Smoke test against a local sshd.
7. Add `stdinserver/control.rs` — handle `Request::ListSnapshots`, `GetReceiveResumeToken`, `DestroySnapshot`, basic plumbing. Unit-test handlers with `RecordingRunner` from palimpsest.
8. Add `stdinserver/recv.rs` — read RecvHeader, run `zfs recv -s -u`, copy stdin, return Response. Reuse the existing sink logic.
9. Rewrite `daemon/src/jobs/push.rs` executor to use `PeerLink::open_recv` instead of QUIC. Wire holds and bookmark cursor choreography.
10. Add `handlers/peers.rs` — proxy local axum routes to PeerLink. Add `/api/v1/peers/{peer}/...` to router.
11. Add SSE infrastructure for `/api/v1/events` and `/api/v1/peers/{peer}/events`. Hook it to the SQLite-backed log layer.
12. Update `admin-ui/` — add a Peers tab to `AdminView.vue`, add peer-namespaced views for jobs and snapshots, update the OpenAPI codegen.
13. Integration test: real ssh between two VM instances, full push cycle with hold + cursor + resume token paths.
14. Sweep: remove orphaned slice references in comments, retire any leftover QUIC scaffolding, update the integration test fixtures.

## Out of scope for v1

- Multi-peer fan-out per push job (one peer per job is fine).
- Pull jobs. The data direction is laptop → home server only.
- HTTP/3. Browsers reach the daemon over HTTP/1.1 or HTTP/2 over TLS as axum handles natively. No h3 integration anywhere.
- Persistent state for the scheduler beyond what SQLite covers.
- Federation across more than two hosts. The PeerLink design extends naturally but don't generalise speculatively.
- Hooks (pre/post-replication scripts).
- Auth on the local UI's loopback bind. UNIX socket permissions and loopback are the perimeter for now.

## Out of scope for the daemon binary entirely

- `arctern status`, `arctern signal`, `arctern wakeup`, `arctern list` subcommands. The web UI replaces them. Only `daemon`, `stdinserver-dispatch`, `configcheck` are CLI surface.
