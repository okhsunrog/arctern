<h1 align="center">arctern</h1>

<p align="center">Push-based ZFS replication daemon — async Rust over SSH, with a web UI.</p>

> **Status:** early, under active development (`v0.1.0`, edition 2024). The design
> is settled (see [`ARCHITECTURE.md`](ARCHITECTURE.md)); APIs and config are not
> yet stable.

## What it is

arctern replicates ZFS datasets from an **active sender** (e.g. a laptop or
workstation that holds the data) to one or more **passive receivers** (e.g. a
home NAS that stores backups). Replication is **push-only**: the sender runs the
scheduler, decides what to send, and drives every transfer.

It is heavily inspired by [zrepl](https://zrepl.github.io/). Snapshot names use
the same `<prefix><RFC3339-utc-no-colons>` convention (e.g.
`zrepl_20260514T134500Z`), so a host migrating from zrepl keeps its existing
snapshot history.

## How it works

```
                ┌──────────────────────────────────────┐
                │            Sender (laptop)            │
   browser ──┐  │  arctern daemon                       │
             └──┼─→ axum on 127.0.0.1:7878 (web UI+API) │
                │   ├─ scheduler: snap / push / prune   │
                │   ├─ PeerLink ──┐  one SSH session    │
                │   └─ SQLite (observability)           │
                └─────────────────┼────────────────────┘
                                  │  ControlMaster, multi-channel
                                  │  · control  (framed RPC)
                                  │  · recv     (zfs send → recv)
                ┌─────────────────┴────────────────────┐
                │           Receiver (NAS)              │
                │  sshd → authorized_keys ForcedCommand │
                │       → arctern stdinserver-dispatch  │
                │  (no bespoke network listener)        │
                └───────────────────────────────────────┘
```

- **Transport is plain SSH.** arctern uses the system `ssh(1)` via the
  [`openssh`](https://docs.rs/openssh) crate, so it inherits `~/.ssh/config`, the
  agent, hardware tokens, `ProxyJump`, and `ControlMaster` for free. The sender
  holds **one** SSH session per peer and multiplexes channels over it: a
  long-lived **control** channel (framed RPC — snapshot listing, resume tokens,
  status, events) and short-lived **recv** channels (one per `zfs send | zfs
  recv` pipe, run in parallel).
- **The receiver exposes no service of its own.** It only needs `sshd`, the
  `arctern` binary on `PATH`, and an `authorized_keys` entry with a
  `ForcedCommand` that runs `arctern stdinserver-dispatch <identity>`. A
  per-identity ACL in its config decides which jobs/operations that key may use
  and confines `recv` to a dataset subtree.
- **State lives in ZFS, not in arctern.** Holds, cursor bookmarks, and
  `receive_resume_token` are the source of truth; the scheduler is stateless and
  re-derives each plan from ZFS every cycle. A per-host SQLite database is used
  for observability only (job runs, logs, ARC stats).
- **One dashboard, two hosts.** The sender's daemon serves a Vue admin UI on
  `127.0.0.1:7878` and proxies a subset of the peer's API over the SSH control
  channel — so the browser sees both hosts through a single endpoint, while the
  receiver keeps no UI or API reachable over the network.

## Features

- **Job types:** `snap` (take snapshots), `push` (replicate to a peer), `prune`
  (receiver-side retention).
- **Grid retention** (`4x15m | 24x1h | 14x1d`), with the zrepl idiom of
  protecting non-prefixed (manual) snapshots by default.
- **Robust replication:** GUID-based common-snapshot detection, resume tokens
  (`recv -s`), `discard_partial_recv`, and a hold + cursor-bookmark choreography
  that stops the snap job's pruner from racing an in-flight send.
- **Ordered failover:** a push job can list several peers; the cycle uses the
  first reachable one, and each peer keeps its own cursor bookmark so an offline
  peer catches up cleanly later.
- **Encrypted raw sends by default** (`zfs send -w -e -c -L`); override per job.
- **Per-client recv tuning:** `-o`/`-x` property overrides and inherits, set in
  the receiver's ACL.
- **Cancellable jobs** with graceful cleanup; SSE live event stream in the UI.

## Requirements

- OpenZFS on both hosts, and OpenSSH for the transport.
- A Rust toolchain with **edition 2024** support to build.
- Sibling crate [`palimpsest`](https://github.com/okhsunrog/palimpsest) — arctern
  depends on it via a relative path (`../palimpsest`), so clone it next to this
  repo.

## Build

The daemon **embeds the built admin UI** (`build.rs` bundles `admin-ui/dist`), so
build the UI first.

```sh
# 1. clone the sibling dependency next to this repo
git clone https://github.com/okhsunrog/palimpsest
git clone https://github.com/okhsunrog/arctern
cd arctern

# 2. build the admin UI  (uses Vite+ / bun — see admin-ui/package.json)
cd admin-ui && vp install && vp build && cd ..

# 3. build the daemon (binary: target/release/arctern)
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

On the **receiver**, instead of running the daemon, add the sender's key to
`~/.ssh/authorized_keys` with a forced command:

```
command="/usr/local/bin/arctern stdinserver-dispatch laptop_nova",restrict ssh-ed25519 AAAA…
```

The receiver's own `arctern daemon` is optional — only needed if it should also
take its own snapshots or serve a local UI.

See [`docs/example-config.toml`](docs/example-config.toml) for an annotated
configuration covering peers, snap/push/prune jobs, and the receiver-side ACL.

## CLI

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
  config/      TOML config: jobs, peers, retention grid, filters, ACL
  transport/   framed wire protocol (Request/Response/RecvHeader); pure types
  client/      shared client helpers
daemon/        the arctern binary: scheduler, peer link, stdinserver, axum API
admin-ui/      Vue admin UI (embedded into the daemon at build time)
```

The replication primitives (snapshots, sends, holds, bookmarks, resume tokens)
live in the separate [`palimpsest`](https://github.com/okhsunrog/palimpsest)
crate.

## Scope (v1)

Push direction only (sender → receiver), one peer in use per cycle, two-host
federation. Pull jobs, multi-host fan-out, and pre/post hooks are explicitly out
of scope for now — see [`ARCHITECTURE.md`](ARCHITECTURE.md#out-of-scope-for-v1).

## License

[MIT](LICENSE)
