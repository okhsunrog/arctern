# arctern: architecture

The durable design: transport, protocol, ACL model, scheduling, state storage.
Read it end-to-end before changing code. (This document started life as the
QUIC→SSH pivot plan; the pivot is complete and everything below describes the
tree as built.)

The push direction is fixed: the laptop has the data, the home server stores
backups, the laptop is the active sender, the home server is the passive
receiver. Replication semantics — GUID-based common-snapshot detection, resume
token logic, `discard_partial_recv` + `recv -A`, send flags, retention rules —
are shared with zrepl in spirit, not in wire format.

## Topology

```
                        ┌─────────────────────────────────┐
                        │              Laptop             │
                        │                                 │
              browser ──┼──→ axum on 127.0.0.1:7878       │
                        │     │                           │
                        │     │ /api/v1/...   (local)     │
                        │     │ /api/v1/peers/mira/...    │
                        │     │   (proxied over SSH)      │
                        │     ↓                           │
                        │   arctern daemon                │
                        │     ├─ scheduler (snap, push)   │
                        │     ├─ PeerLink per [[peers]]   │
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
                        │     ↓                           │
                        │   arctern stdinserver-dispatch  │
                        │     ├─ control channel (long)   │
                        │     ├─ recv channels (parallel) │
                        │     └─ events channel (stream)  │
                        │                                 │
                        │   arctern daemon (optional)     │
                        │     own snap/prune jobs, own    │
                        │     loopback UI, proxy target   │
                        └─────────────────────────────────┘
```

The home server runs sshd, the arctern binary in PATH, and an
`authorized_keys` entry with `ForcedCommand`. That alone is enough for
replication. Its own `arctern daemon` is optional — run it for local snap and
prune jobs, and to make the host manageable through the sender's console (the
control channel's `proxy` RPC forwards into the receiver daemon's local API
over its UNIX socket).

## Transport: SSH with multiple channels

Use the `openssh` crate. Not `russh`. The system `ssh(1)` brings
`~/.ssh/config`, agent, hardware tokens, ProxyJump, and ControlMaster — none
of which we want to reimplement.

The sender's daemon holds one `openssh::Session` per peer. ControlMaster keeps
the underlying TCP and crypto state alive across all channels in that session.
Channels are opened on demand via `session.command(...)`. Each channel maps to
one `arctern stdinserver-dispatch` process on the receiver, spawned by sshd,
with a role determined by `SSH_ORIGINAL_COMMAND`.

Channel kinds:

- **`control`** — long-lived, one per session. Carries tarpc RPC (see below).
  Opened when the peer link is established, reused indefinitely.
- **`recv`** — short-lived, one per replication step. Spawned for the duration
  of a single `zfs send → zfs recv` pipe. Closed after the recv completes
  (success or failure). A push job with `parallel = N` keeps up to N of these
  open concurrently, alongside the control channel which keeps serving UI
  queries during transfers.
- **`events`** — long-lived, one-way. Streams the receiver host's structured
  event log as NDJSON lines; the sender fans it out to its SSE subscribers.

### authorized_keys entry

```
command="/usr/local/bin/arctern stdinserver-dispatch laptop_nova",restrict ssh-ed25519 AAAA...laptop-key
```

The identity name (`laptop_nova`) is hardcoded per key. OpenSSH's `command=`
directive does not have a portable substitution for the authenticating key's
fingerprint — `%k` only exists in `AuthorizedKeysCommand` (server-side dynamic
auth), not in `authorized_keys`. Valid substitutions in `command=` are `%h`
(home), `%u` (user), `%i` (key id from `authorized_keys` options), and `%%`.

`restrict` (OpenSSH ≥ 7.2) disables every channel feature except command exec.
The full requested command is in `SSH_ORIGINAL_COMMAND`. The matched
`authorized_keys` line is exposed via `SSH_AUTH_INFO_0` (OpenSSH ≥ 7.4) for an
optional defense-in-depth fingerprint pin — see ACL config below. (Pinning
requires `ExposeAuthInfo yes` in sshd_config; the default is no.)

`stdinserver-dispatch` reads `SSH_ORIGINAL_COMMAND`, parses
`arctern stdinserver <job> <op>`, validates the identity/job/op against the
`[[allowed_clients]]` ACL (plus the optional fingerprint pin), and runs the
matching handler: `control` (job-unscoped — one channel serves every job),
`recv` (job-scoped), or `events` (read-only, implicitly granted with the
`control` scope). The dispatch process opens the same SQLite as the daemon, so
its tracing events land in the shared host event log.

### Wire protocol on a control channel

The control channel is a tarpc RPC service (`ArcternControl` in
`crates/transport/src/control.rs`) over `tokio_util::codec::LengthDelimitedCodec`
framing with `serde_json` payloads (readable in logs; switch to `postcard`
later if size matters). tarpc generates the client and the server glue and
owns request-id correlation — the server processes requests concurrently (each
on its own task) and emits responses in any order, so one slow query over a
10k-snapshot dataset never head-of-line blocks the UI proxy.

```rust
// crates/transport/src/control.rs

#[tarpc::service]
pub trait ArcternControl {
    // Replication core — must work with no daemon on the receiver.
    async fn list_receiver_guids(
        dataset: String,
        prefix_regex: Option<String>,
    ) -> Result<GuidsReply, WireError>;
    async fn discard_partial_recv(dataset: String) -> Result<(), WireError>;
    // Cheap; doubles as the link liveness probe.
    async fn log_cursor() -> u64;
    // Management plane — passthrough into the receiver's local daemon
    // HTTP API. GET rides the control read scope; mutating methods
    // require the explicit control:proxy_admin ACL grant.
    async fn proxy(
        method: String,
        path: String,
        body: Option<String>,
    ) -> Result<ProxyReply, WireError>;
}
```

There is no shutdown RPC — transport EOF (the client dropping the channel) is
the shutdown signal on both ends.

Events are not RPC: they ride the dedicated events channel as
newline-delimited `EventWire` JSON, bridged from the receiver daemon's SSE
endpoint (or a SQLite poll on a daemon-less receiver).

### Wire protocol on a recv channel

A recv channel writes one header frame followed by the raw `zfs send` byte
stream, then half-closes. The server reads the header, spawns
`zfs recv -s -u`, pipes the channel's stdin into recv's stdin, waits for the
channel EOF and recv's exit, then writes a single response frame
(`Response::Ok` / `Response::Error`) and exits.

```rust
#[derive(Serialize, Deserialize)]
pub struct RecvHeader {
    pub version: u32,
    pub target_dataset: String,
    pub send: SendHeader,
}
```

`SendHeader`, `SendKind`, `SnapshotRef`, `SendFlagsWire` and the
`discard_partial_recv` flag live in `crates/transport/src/protocol.rs`
alongside the length-delimited framing helpers the raw channels share.

On success the recv handler also advances the last-received hold (see below)
and records the completed transfer — byte count from the copy loop, wall time,
sender identity — into the `recv_transfers` table plus a structured
`recv: transfer complete` event. That is the receiver-side accounting the
console's "Incoming" panel reads.

## Replication flow

For a push job replicating one target peer:

1. The scheduler picks the peer's live `PeerLink` from the shared peers map
   (maintained by the reconnect task; see below). No link → the target is
   blocked, retried on the next connectivity signal.
2. Filesystems replicate through a bounded-concurrency pipeline (`parallel =
   N`, default 1). For each filesystem:
   - `list_receiver_guids(target, None)` over the control channel — receiver
     GUIDs plus the `receive_resume_token`. Deliberately unfiltered: the
     common base may carry a foreign prefix (zrepl history, a travelled
     manual snapshot).
   - List local sender snapshots (filtered) and bookmarks via palimpsest.
   - Compute the plan via `pick_plan_with_token`: resume, full, incremental
     from snapshot, or incremental from bookmark (the no-common-snapshot
     fallback).
   - If the plan wants `discard_partial_recv`, call the RPC first — it's
     idempotent and makes the recv channel's first action a clean recv.
   - Place step holds (the `to` snapshot, plus the `from` snapshot for
     incrementals) BEFORE the send.
   - Open a fresh recv channel, write `RecvHeader`, spawn `zfs send` locally,
     and copy its stdout into the channel — through the job-wide token bucket
     when `bandwidth_limit` is set, publishing progress into the job's
     transfer slots as it goes.
   - Half-close stdin, read the single response frame.
   - On Ok: advance the cursor — create the new GUID-named cursor bookmark,
     destroy stale same-(job, peer) cursors, then sweep the step-hold tag from
     the dataset's filtered snapshots.
   - On Error: log, leave step holds in place (so a retry can find the
     snapshot), record the error.
3. After all filesystems, record the per-peer outcome (`push_syncs`) and the
   cycle (`job_runs`, with bytes sent). Recv channels are gone; control stays
   open.

## Scheduling

Push scheduling is event-driven, not tick-based. The job sleeps until the
earliest auto target is due (last success + the peer's `auto_interval`) and
wakes early on:

- a manual "Send now" request (per-target) or a job wakeup,
- any peer connectivity change (the reconnect tasks bump a watch channel).

A target is auto-eligible only while its ACTIVE route has `auto = true` —
route reachability is the locality signal, so "auto at home over LAN, manual
on the road over WireGuard" needs no network-detection config. A due-but-
blocked target retries every 5 minutes; `interval` on the job is only an
optional safety-net bound on the blind sleep (default 15m), not a poll rate.

## Holds and replication cursor

The hold/bookmark choreography is the protection against the snap-job's prune
racing the push-job's send. palimpsest exposes `hold`, `release`,
`list_holds`, `list_holds_many`, `bookmark::create`, `bookmark::destroy`.

Naming conventions, pinned for compatibility (peer-namespaced so a
multi-target push job tracks each receiver independently):

- Step hold: `arctern_step_J_<jobname>_P_<peer>` on the `to` snapshot of an
  in-flight send. Placed before the send; on success the tag is swept from
  every filtered snapshot of the dataset (one `zfs holds` invocation via
  `list_holds_many`, then a release per actual holder) — this also cleans
  stale holds left by earlier failed cycles, which would otherwise pin their
  snapshots against prune forever. On failure the hold stays so a retry can
  find the snapshot.
- Replication cursor bookmark:
  `<dataset>#arctern_cursor_G_<guid>_J_<jobname>_P_<peer>`. GUID-suffixed
  (zrepl's scheme): on success the new cursor is created first, then stale
  cursors for the same (job, peer) are destroyed — a crash in between leaves
  at least one cursor alive. ZFS bookmarks are GUID-anchored so the cursor
  survives even if the underlying snapshot is later destroyed. When the
  planner finds no common *snapshot* between sender and receiver (offline gap
  longer than the sender's retention window), it falls back to
  `zfs send -i <bookmark>` from any sender bookmark whose GUID the receiver
  still has — matching is by GUID, not name, so zrepl's `#zrepl_CURSOR_*`
  bookmarks qualify too (this is the zrepl migration path).
- Last-received hold (on receiver side): `arctern_last_J_<jobname>` on the
  most recent successfully received snapshot. Set by stdinserver after recv
  exits cleanly; the tag is then swept from the dataset's other snapshots.
  This keeps a receiver-side prune job from destroying the last common
  snapshot between syncs (which would force a full resend).

The snap-job's pruner skips snapshots that return `ZfsError::SnapshotHeld`, so
held snapshots survive prune on both sides.

## UI federation: the host-scoped console

The guiding rule (owner's): **managing a peer must be identical to managing
the local host** — never a parallel, lesser UI.

The sender's daemon mounts, besides its local API, a generic passthrough:

```
GET    /api/v1/peers                              configured peers + routes + reachability
ANY    /api/v1/peers/{peer}/proxy/{*rest}         → the peer daemon's /api/v1/{rest}
GET    /api/v1/peers/{peer}/events                proxied SSE (events channel + backlog)
```

The proxy forwards the raw (still percent-encoded) path and query over the
control channel's `proxy` RPC; the receiver's stdinserver relays it into the
local daemon's UNIX socket. `GET` rides the control read scope; `POST` /
`DELETE` require the receiver to grant `control:proxy_admin` — that single ACL
line is the switch between "sender may watch this host" and "sender may manage
this host like its own".

The SPA renders every view under `/h/{host}/...` with the same components and
composables it uses locally, just with a per-peer base URL. The sidebar's
Hosts group switches scope; the browser talks to one endpoint (the sender's
loopback bind) regardless of which host it is looking at.

### PeerLink shape

```rust
// daemon/src/peer/mod.rs

pub struct PeerLink {
    name: String,
    session: Arc<openssh::Session>,
    control: ArcternControlClient,      // tarpc client; dispatch task owns the stdio
    // recv channels are owned by individual replication tasks, not stored here
}

impl PeerLink {
    pub async fn connect(name: String, ssh_target: &str, job: &str) -> Result<Self> { ... }
    pub async fn list_receiver_guids(&self, dataset: String, prefix_regex: Option<String>) -> Result<GuidsReply> { ... }
    pub async fn discard_partial_recv(&self, dataset: String) -> Result<()> { ... }
    pub async fn log_cursor(&self) -> Result<u64> { ... }
    pub async fn proxy(&self, method: String, path: String, body: Option<String>) -> Result<ProxyReply> { ... }
    pub async fn open_recv(&self, job: &str, header: &RecvHeader) -> Result<RecvChannel> { ... }
    pub async fn subscribe_events(&self) -> Result<broadcast::Receiver<EventWire>> { ... }
}
```

tarpc's dispatch task owns the control channel's stdio and correlates
responses (60s per-request deadline — anything longer is a dead or half-open
session). The peer's event stream is a separate NDJSON channel opened lazily
by the first subscriber and fanned out via a broadcast.

Reconnect runs **eagerly in a background task** per peer, not lazily on next
call. It tries routes in priority order (first reachable wins), probes the
live link every 15s with `log_cursor` (skipped while recv channels are
streaming — a bulk send legitimately starves the control channel), re-ranks
back to a higher-priority route when one returns, and on loss tears the entry
down and retries with exponential backoff (1s, 2s, 4s, … capped at 60s). UI
calls during a backoff window return HTTP 503 immediately with `Retry-After`.
Every state change bumps a watch channel that wakes the push schedulers.

### ACL config

```toml
# Sender side: a peer is one PHYSICAL host; multi-homed hosts list
# ordered [[peers.routes]] (highest priority first). `ssh_target` on the
# peer itself is shorthand for a single route. Cursor bookmarks, step
# holds and push_syncs are keyed by the PEER name, never the route, so
# switching networks never invalidates replication state.
[[peers]]
name = "mira"
auto_interval = "1d"            # auto-sync at most once a day
[[peers.routes]]
name = "lan"
ssh_target = "arctern-mira-lan" # ~/.ssh/config alias
[[peers.routes]]
name = "wg"
ssh_target = "arctern-mira-wg"
auto = false                    # manual "Send now" only on this route

[[jobs]]
type = "push"
name = "push_to_mira"
targets = ["mira"]              # multi-target: each peer keeps its own cursors
parallel = 2                    # concurrent filesystems per target (1..=4)
# bandwidth_limit = "10MiB"     # shared across parallel sends
filesystems = { "novafs/arch0/data/home" = true }
[jobs.target]
root_fs = "okdata/backups/nova"

# On the RECEIVER side, authorized_keys + arctern config define what the
# sender is allowed to do.
[[allowed_clients]]
identity = "laptop_nova"               # matches the argv to stdinserver-dispatch
fingerprint = "SHA256:abc123..."       # optional pin, verified against SSH_AUTH_INFO_0
jobs = ["push_to_mira"]                # one identity may serve multiple jobs
operations = [
  "control",                           # read RPC + GET proxy + events
  "control:discard_partial_recv",      # zfs recv -A over RPC
  "recv",                              # bulk receive
  "control:proxy_admin",               # mutating proxy = full host console
]
root_fs = "okdata/backups/nova"        # recv confined to this subtree
```

`stdinserver-dispatch` enforces that:
- the identity matches an `[[allowed_clients]]` entry,
- if `fingerprint` is set, it matches the key in `SSH_AUTH_INFO_0`,
- the parsed `<job>` from `SSH_ORIGINAL_COMMAND` is in `jobs` (recv only —
  the control channel is per-peer, not per-job),
- the requested `<op>` is in `operations` (fine-grained `control:*` checks
  happen per RPC in the control handler),
- recv and discard operations target a dataset under the configured `root_fs`.

## State storage

All replication state lives in ZFS (holds, bookmarks, `receive_resume_token`).
The daemon is a stateless scheduler that re-derives plans from ZFS state every
cycle. **Do not introduce etcd or any external coordination store.**

Per-daemon SQLite for observability only. Path: `<state_dir>/state.db`. `sqlx`
with the `sqlite` + `runtime-tokio` features, `journal_mode=WAL`,
`synchronous=NORMAL`. Tables:

- `job_runs` — one row per cycle (status, error, bytes sent). 30-day trim.
- `log_events` — the host event log, written by a tracing layer (INFO+ only;
  DEBUG/TRACE go to journald — kHz-rate tokio internals would explode the DB).
  24-hour trim.
- `push_syncs` — latest per-(job, peer) outcome; drives `auto_interval` and
  the per-target status UI.
- `recv_transfers` — completed inbound transfers recorded by recv channels
  (bytes, duration, sender identity). 30-day trim.
- `arcstats_history` — ARC stats time series for the dashboard.

`stdinserver` processes open the same DB (WAL handles multiple writers), so
receiver-side events and transfers land in the shared host log. Trims run
every 6 hours from the daemon.

Events flow through an in-process bus: the tracing layer sends to a writer
task (SQLite assigns the event id) which broadcasts to SSE subscribers —
there is no polling anywhere in the daemon-side pipeline. The stdinserver
events channel bridges the daemon's SSE when one is running and falls back to
tailing SQLite on a daemon-less receiver.

## Cancellation and backpressure

Patterns in `push.rs`:

- The bulk copy loop races the job/cycle `CancellationToken` inside
  `tokio::select!` with `biased;` so cancel wins races.
- On cancel: drop the recv channel (which closes the SSH child's stdin and
  propagates SIGPIPE to remote `zfs recv`), `start_kill` the local `zfs send`
  child, then `wait` to reap.
- Drain `zfs send` stderr on a separate `tokio::spawn` to avoid pipe deadlock.
- Always `recv -s`, so partial state survives; the next cycle resumes via the
  token. Pause = cancel the in-flight transfer resumably + suspend scheduling.
- After the copy completes, `shutdown()` the channel's stdin before reading
  the response frame, so the remote `zfs recv` sees EOF and finalises.
- `bandwidth_limit` is enforced by a job-wide debt-based token bucket shared
  by all parallel sends; each stream may overshoot by one 256 KiB chunk and
  then sleeps off the debt.

## Crate layout

```
crates/
  api/        request/response types for the HTTP API (utoipa::ToSchema),
              including EventWire-shaped LogEvent, TransferInfo, RecvTransfer.
  config/     TOML schema: jobs, peers + routes, retention grid, filters, ACL.
  transport/  control.rs — the tarpc ArcternControl service + transport();
              protocol.rs — recv/event framing shared by the raw channels.
              Pure types; no I/O.
  client/     UNIX-socket client helpers (raw requests, SSE streaming) used
              by the stdinserver proxy and the events bridge.
daemon/
  src/
    main.rs                  daemon and dispatch entry points (split via subcommand)
    auth.rs                  UDS peer credentials + Sec-Fetch-Site CSRF guard
    handlers/                axum handlers: jobs, snapshots, datasets, pools,
                             system (ARC), events (SSE), transfers, peers (proxy)
    jobs/                    JobManager + snap / push / prune
    peer/                    PeerLink (tarpc client), reconnect + route ranking
    stdinserver/             dispatch, control (tarpc server), recv, events
    state/                   SQLite pool, migrations, queries, tracing layer
    router.rs                axum wiring
    error.rs                 ApiError → HTTP response mapping
admin-ui/                    Vue 3 + Nuxt UI SPA; host-scoped console under
                             /h/{host}; embedded into the binary by build.rs
```

## Conventions (preserved from CLAUDE.md)

- Async-only.
- `cargo add` for deps, no hand-edits to Cargo.toml versions.
- Errors via `thiserror` in libraries, `eyre` only in `main.rs`.
- Comment WHY, never WHAT.
- No emojis in code, comments, or commit messages.
- TS client auto-generated from OpenAPI; never hand-edit `admin-ui/src/client/`.
- All ZFS work goes through palimpsest; missing primitives are added there
  first.

## Out of scope

- Pull jobs. The data direction is sender → receiver only. (A receiver that
  should push elsewhere runs its own push job.)
- HTTP/3. Browsers reach the daemon over HTTP/1.1 or HTTP/2 as axum handles
  natively.
- Persistent scheduler state beyond what SQLite covers.
- Federation beyond a handful of peers. The PeerLink design extends naturally
  but don't generalise speculatively.
- Hooks (pre/post-replication scripts).
- Auth on the local UI's loopback bind. UNIX socket permissions + loopback +
  the Sec-Fetch-Site guard are the perimeter; multi-user access is a
  different product.
- `arctern status` / `signal` / `wakeup` / `list` subcommands. The web UI
  replaces them; only `daemon`, `stdinserver-dispatch`, `configcheck`,
  `openapi` are CLI surface.

See `docs/design-process-model.md` for why the stdinserver stays a separate
process per channel (and what would justify merging it into the daemon), and
`docs/roadmap.md` for the feature direction.
