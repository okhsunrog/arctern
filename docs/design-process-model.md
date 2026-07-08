# Process model: why multiple processes, and when to merge them

Status: decided 2026-07-09 (owner + Claude design review). This documents the
alternatives we weighed so a future "merge into one process" discussion starts
from here instead of from scratch.

## Current model (the hybrid we converged on)

Per receiving host:

| process | count | lifetime | role |
|---|---|---|---|
| `arctern daemon` (systemd) | 1 | persistent | local jobs (snap/prune), UDS API, UI, event bus |
| dispatch `control` | 1 per peer link | hours–days (dies with the link) | RPC server |
| dispatch `recv` | 1 per transfer | seconds–hours | one `zfs recv` stream |
| dispatch `events` | 1 per peer link | as control | one-way event stream |

Every dispatch process is spawned by **sshd** via the forced command in
`authorized_keys` and is a pure function of (stdin, config, zfs). The sender
side mirrors this: daemon + one ssh ControlMaster per route + one ssh client
process per channel. Upper bound per peer: 2 long-lived + `parallelism`
transient processes.

### The plane split (the actual design rule)

- **Data plane** — replication-critical, zfs-only, works with NO daemon on the
  receiver: the recv stream and the planner RPCs (`ListReceiverGuids`,
  `GetReceiveResumeToken`, `DiscardPartialRecv`). These are served by the
  dispatch process directly shelling out to `zfs`.
- **Control plane** — anything touching daemon state (job status, wakeups, the
  generic `Proxy`, host management, the event feed): dispatch **bridges** to
  the local daemon over its UNIX socket (`socket` key in arctern.toml). No
  daemon → control plane degrades, replication keeps working.

### Coordination points and their owners

1. **ZFS** — the only replication truth. Holds/bookmarks/resume tokens are
   atomic at the zfs level; concurrent `zfs recv` into one dataset is arbitrated
   by ZFS itself. Prefer dataset user properties (`org.arctern:*`) if per-object
   state is ever needed.
2. **The daemon via UDS** — owner of host runtime state. Processes that need it
   *ask the owner*; nothing coordinates through shared storage.
3. **SQLite** — history/observability ONLY (job_runs, log_events, push_syncs).
   It is a journal, not an IPC mechanism: no notifications, no pub/sub. Using it
   as a bus is the anti-pattern the 2026-07 event-bus refactor removes.

## Alternatives considered (and why not, for now)

### A. Full merge: daemon owns every channel (fd passing)

sshd still spawns the forced command (unavoidable), but dispatch immediately
passes its stdin/stdout to the daemon via `SCM_RIGHTS` and exits; all channels
run as tokio tasks in one process.

Real gains, stated honestly:
- events = in-process broadcast subscription (no UDS bridge at all);
- jobs federation = a function call;
- **central transfer scheduling**: global bandwidth budgets across channels,
  admission control (max N concurrent recvs), and receiver-side visibility of
  in-flight incoming transfers in its own UI — all become a HashMap instead of
  a protocol;
- one config parse, one owner, lower cognitive load ("how many processes run?"
  is a real cost of the current model);
- transfers inherit the daemon's systemd sandboxing.

Costs:
- the daemon becomes **mandatory for replication** (daemon down = no backups at
  all, not "backups without UI"). Softened by resume tokens: a daemon restart
  costs one reconnect + resume, not data — this weakens the strongest argument
  *against* A, and is worth remembering;
- daemon restart kills in-flight recvs (resumable, but still);
- a panic in one transfer risks the whole process;
- fd-passing is platform code; migration effort is days.

### B. zrepl-style bridge (no fd passing)

Dispatch pipes stdin/stdout ↔ daemon UDS byte-for-byte. This is literally what
`zrepl stdinserver` does. Simplest possible merge, but the bulk recv stream
pays a permanent double userspace copy. Fine for control/events (we DO this —
that's the plane split); wrong for the data plane at 100s-of-GiB scale.

### D. Self-daemonizing dispatch (first process becomes the daemon)

Leader election via lock/socket race, later dispatches forward to the leader.
Rejected outright: leader lifecycle and upgrade races are a historical bug farm
(ssh-agent/gpg-agent lineage).

## Decision and the revisit trigger

Stay on the hybrid. The design space, honestly explored, converges back to it:
data plane in self-contained processes (zero-copy, survives daemon death,
per-channel crash isolation, binary upgrades picked up by the next channel),
control plane bridged to the daemon.

**Revisit A when** arctern needs parallel multi-peer transfer scheduling with
shared budgets, receiver-side admission control, or first-class visibility of
incoming transfers beyond reporting. The migration boundary is already clean:
dispatch shrinks to an fd-passer; everything else is behind the UDS line today.

Cheap step toward A taken instead: the recv process **reports finished
transfers to the daemon over UDS** (dataset, bytes, duration), so the receiver
sees its incoming traffic in its own UI/history without changing the process
model.

## Companion decisions (same review)

- **Recv channel framing stays in-band** (`[JSON header][raw bytes][JSON
  trailer]` on one pipe). A transfer is one process with one fd: rendezvous is
  atomic, failure = channel death, nothing to clean up. Splitting header/status
  onto the control channel would buy aesthetics and pay with a transfer-id
  rendezvous protocol and orphan-cleanup states. Length-prefixed framing already
  removes boundary ambiguity.
- **Events are a stream, not RPC**: dedicated one-way channel (`stdinserver
  <identity> events`, NDJSON), bridged from the daemon's in-process event bus;
  falls back to polling SQLite on daemon-less receivers. This unmixes server
  push from the control channel, which is the precondition for
- **tarpc on the control channel**: after protocol slimming (~6 methods), the
  hand-rolled request-id demux/pending-map/timeout code is replaced by a
  `#[tarpc::service]` trait over the same LengthDelimited+JSON transport. Trade
  noted: tarpc's envelope is library-defined, less hand-versionable than our
  bare serde enums — acceptable since both ends deploy in lockstep.
- **Event pipeline**: tracing layer → mpsc → single writer task (SQLite insert
  assigns the id) → in-process broadcast. SSE and the events channel read the
  bus; the 500ms pollers die. SQLite becomes a subscriber, never a bus.
- **Push scheduling goes event-driven**: sleep until `min(next auto due)`,
  wake on manual request / peer-connected; the 15m tick disappears (interval
  becomes an optional safety-net poll).
