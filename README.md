<h1 align="center">arctern</h1>

<p align="center">Push-based ZFS replication over SSH — with a web console for <em>both</em> ends of the link.</p>

<p align="center">
  <a href="LICENSE"><img alt="MIT license" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
  <img alt="Rust" src="https://img.shields.io/badge/rust-stable-orange.svg">
  <img alt="OpenZFS" src="https://img.shields.io/badge/OpenZFS-%E2%89%A5%202.2-lightgrey.svg">
</p>

![Dashboard](docs/screenshots/dashboard.png)

> **Status:** pre-1.0, but real. The design is settled (see
> [`ARCHITECTURE.md`](ARCHITECTURE.md)) and arctern runs in production on the
> author's machines, where it replaced zrepl for laptop→NAS backups. The HTTP
> API and TOML config schema may still change before 1.0.

## What it is

arctern replicates ZFS datasets from an **active sender** (a laptop or
workstation that holds the data) to one or more **passive receivers** (a home
NAS that stores backups). Replication is **push-only**: the sender runs the
scheduler, decides what to send, and drives every transfer.

It is heavily inspired by [zrepl](https://zrepl.github.io/) — same snapshot
naming idiom (`<prefix><RFC3339-utc>`), same grid retention, same hold/cursor
discipline — but built as a single Rust binary with an embedded web console
that treats the peer as a first-class host, not a footnote.

## How it works

<p align="center">
  <img src="docs/diagrams/topology.svg" alt="Topology: the sender's daemon (web UI, scheduler, PeerLink) drives one multi-channel SSH session — control (tarpc RPC), parallel recv streams, and an events stream — into the receiver's sshd ForcedCommand; the receiver runs no network listener of its own." width="720">
</p>

- **Transport is plain SSH.** arctern drives the system `ssh(1)` via the
  [`openssh`](https://docs.rs/openssh) crate, so it inherits `~/.ssh/config`,
  the agent, hardware tokens, `ProxyJump`, and `ControlMaster` for free. One
  SSH session per peer multiplexes a long-lived **control** channel (tarpc RPC:
  receiver snapshot inventory, resume tokens, liveness, API proxy), short-lived
  **recv** channels (one per `zfs send | zfs recv` pipe, up to `parallel = N`
  at once), and a one-way **events** stream (the receiver's live log).
- **The receiver exposes no service of its own.** It needs `sshd`, the
  `arctern` binary on `PATH`, and an `authorized_keys` entry whose
  `ForcedCommand` runs `arctern stdinserver-dispatch <identity>`. A
  per-identity ACL in its config decides which jobs and operations that key may
  use and confines `recv` to a dataset subtree.
- **State lives in ZFS, not in arctern.** Holds, cursor bookmarks, and
  `receive_resume_token` are the source of truth; the scheduler is stateless
  and re-derives each plan from ZFS every cycle. A per-host SQLite database is
  observability only (job history, event log, received transfers, ARC stats).
- **One console, every host.** The sender's daemon serves the UI on loopback
  and proxies the peer's local API over the SSH control channel. Switching to a
  peer in the sidebar gives you the *same* console — jobs, snapshots, pools,
  events — scoped to that host, without the receiver exposing anything to the
  network.

## The console

**A peer is the same console, scoped to that host.** Below: the receiver's
jobs viewed from the sender, including what it received and how fast
("Incoming" is recorded by the receiver's own recv channels):

![Peer host console with incoming transfers](docs/screenshots/mira-jobs.png)

**Snapshots answer "what eats my space"** — dataset tree with sizes,
per-snapshot `used`, holds, create/destroy right there:

![Snapshot browser](docs/screenshots/snapshots.png)

**Multi-path peers.** One peer = one physical host with prioritized routes;
the link picks the best reachable route and re-ranks automatically. A route
marked `auto = false` (say, metered WireGuard) still carries manual
"Send now" pushes but never auto-replicates — "auto at home, manual on the
road" without any network-detection config:

![Peer links with routes](docs/screenshots/peers.png)

**Pools and events** round it out — scrub control, vdev tree with error
counters, and a live structured event feed (both hosts' events, bridged over
SSH):

| ![Pool detail](docs/screenshots/pools.png) | ![Events](docs/screenshots/events.png) |
|---|---|

## Features

- **Job types:** `snap` (periodic snapshots), `push` (replicate to peers),
  `prune` (receiver-side retention).
- **Grid retention** (`4x15m | 24x1h | 14x1d`) with the zrepl idiom of
  protecting non-prefixed (manual) snapshots by default.
- **Robust replication:** GUID-based common-snapshot detection, resume tokens
  (`recv -s`), `discard_partial_recv`, bookmark fallback when the common
  snapshot aged out (zrepl's `#zrepl_CURSOR_*` bookmarks qualify — that is the
  migration path), and a hold + cursor-bookmark choreography that stops the
  pruner from racing an in-flight send.
- **Peer routes:** multiple prioritized paths to one host (LAN, WireGuard, …);
  cursors and holds are keyed by peer name, so switching networks never
  invalidates replication state.
- **Event-driven scheduling:** push jobs sleep until the earliest auto target
  is due and wake on "Send now" or peer connectivity changes — no blind
  polling ticks in the UI or the logs.
- **Parallel sends** (`parallel = N`, each on its own recv channel) under a
  shared `bandwidth_limit` token bucket.
- **Receiver-side accounting:** every received stream is recorded (bytes,
  duration, sender identity) and shown in the console's "Incoming" panel.
- **Encrypted raw sends by default** (`zfs send -w -e -c -L`); per-job
  override. Per-client `recv -o/-x` property overrides in the receiver's ACL.
- **Live events** end to end: structured tracing events (which snapshot was
  created / destroyed / sent, how many bytes) streamed over SSE locally and
  bridged from peers over SSH.
- **Cancellable and pausable transfers** — partial receive state keeps them
  resumable.

## Requirements

- OpenZFS ≥ 2.2 and OpenSSH on both hosts.
- Rust stable (edition 2024) to build.
- The ZFS toolkit underneath is [`zfskit`](https://crates.io/crates/zfskit)
  (same author) — pulled from crates.io like any other dependency.

## Build

The daemon **embeds the built admin UI** (`build.rs` bundles `admin-ui/dist`),
so build the UI first.

```sh
git clone https://github.com/okhsunrog/arctern
cd arctern

# 1. build the admin UI  (uses Vite+ / bun — see admin-ui/package.json)
cd admin-ui && vp install && vp build && cd ..

# 2. build the daemon (binary: target/release/arctern)
cargo build --release
```

For UI development, `vp dev` runs the SPA with its `/api` calls proxied to a
running daemon on `127.0.0.1:7878`.

## Run

```sh
# Validate a config (for CI / pre-deploy)
arctern configcheck /etc/arctern/arctern.toml

# Run the daemon (API over a UNIX socket + web UI on 127.0.0.1:7878)
arctern daemon --config /etc/arctern/arctern.toml
```

A minimal sender config with a two-route peer:

```toml
state_dir = "/var/lib/arctern"
socket = "/run/arctern/arctern.sock"

[defaults]
prefix = "arctern_"           # snapshot tag shared by snap/push/prune jobs

[[peers]]
name = "mira"
auto_interval = "1d"          # auto-sync at most once a day
[[peers.routes]]
name = "lan"
ssh_target = "arctern-mira-lan"   # a Host alias from ~/.ssh/config
[[peers.routes]]
name = "wg"
ssh_target = "arctern-mira-wg"
auto = false                  # metered: manual "Send now" only

[[jobs]]
type = "push"
name = "push_to_mira"
targets = ["mira"]
parallel = 2                  # replicate 2 filesystems concurrently
filesystems = { "novafs/arch0/data/home" = true, "novafs/arch0/data/root" = true }
[jobs.target]
root_fs = "okdata/backups/nova"
```

On the **receiver**, instead of running a daemon, add the sender's key to
`~/.ssh/authorized_keys` with a forced command:

```
command="/usr/local/bin/arctern stdinserver-dispatch laptop_nova",restrict ssh-ed25519 AAAA…
```

and authorize the identity in the receiver's config:

```toml
[[allowed_clients]]
identity = "laptop_nova"
jobs = ["push_to_mira"]
operations = ["control", "control:discard_partial_recv", "recv",
              "control:proxy_admin"]   # last one = full host console for this sender
root_fs = "okdata/backups/nova"
```

The receiver's own `arctern daemon` is optional — run it if the host should
take its own snapshots, prune what it received, or be manageable through the
sender's console.

See [`docs/example-config.toml`](docs/example-config.toml) for the annotated
full schema, and [`docs/deploy-snap-only.md`](docs/deploy-snap-only.md) /
[`docs/deploy-full-mirror.md`](docs/deploy-full-mirror.md) for staged
deployment guides.

## CLI

The web UI *is* the administration surface; the CLI stays deliberately small.

| Subcommand | Purpose |
|---|---|
| `daemon` | Run the scheduler + HTTP API (UNIX socket) + web UI (loopback TCP). |
| `stdinserver-dispatch <identity>` | SSH transport entry point, invoked by sshd via `ForcedCommand`. |
| `configcheck <path>` | Validate a config file and exit. |
| `openapi` | Print the OpenAPI spec (used to regenerate the UI's typed client). |

## Project layout

```
crates/
  api/         HTTP request/response types (OpenAPI schema)
  config/      TOML config: jobs, peers + routes, retention grid, filters, ACL
  transport/   wire types: tarpc control service, recv/event framing; no I/O
  client/      UNIX-socket client helpers (used by the stdinserver proxy)
daemon/        the arctern binary: scheduler, peer link, stdinserver, axum API
admin-ui/      Vue admin UI (embedded into the daemon at build time)
```

The replication primitives (snapshots, sends, holds, bookmarks, resume tokens)
live in the separate [`zfskit`](https://github.com/okhsunrog/zfskit)
crate.

## Scope

Push direction only (sender → receiver). A push job can target multiple peers,
each with its own cursor state; a peer can be multi-homed via routes. Pull
jobs, fan-out beyond a handful of peers, and pre/post hooks are out of scope
for now — see [`ARCHITECTURE.md`](ARCHITECTURE.md#out-of-scope) and
[`docs/roadmap.md`](docs/roadmap.md) for where this is going.

## License

[MIT](LICENSE)
