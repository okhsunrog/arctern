# arctern roadmap

What's planned for arctern beyond the current snapshot+replication
feature set. arctern's direction is shifting from "zrepl-but-Rust" to
"the ZFS console you wish you had" — a single loopback web UI for
day-to-day ZFS administration on a host, with replication being one
tab among many.

This document is a working list, not a contract. Items move,
priorities shift. Each item carries enough context that picking it up
later doesn't need re-explanation.

## What's already shipped (for context)

- Snap + prune jobs (zrepl-compatible prefix tags, grid retention).
- Push replication over SSH: full + incremental, resume tokens,
  bookmark fallback, hold + cursor bookmark choreography, multi-target
  jobs, peer routes with auto re-ranking, event-driven scheduling,
  `parallel = N` sends under a shared `bandwidth_limit`.
- tarpc control channel + dedicated NDJSON events channel; receiver
  transfer accounting (`recv_transfers` + the Incoming panel).
- Host-scoped console: every view works for peers via the generic
  control-channel proxy (`/h/{host}/...`), gated by
  `control:proxy_admin` for mutations.
- Local loopback admin UI (Vue 3 + Nuxt UI v4): Dashboard (capacity,
  ARC gauge, job cards), Jobs (multi-slot transfer progress),
  Snapshots (dataset tree with sizes, holds, destroy confirm), Pools
  (vdev tree, scrub control), ARC charts, Events (SSE live tail,
  structured fields), Peer links (routes + health), Config.
- SQLite persistence: `job_runs`, `log_events`, `push_syncs`,
  `recv_transfers`, `arcstats_history`.
- CSRF guard (`Sec-Fetch-Site`) on mutating endpoints.
- `zfs allow`-friendly: daemon runs unprivileged with delegated
  permissions; no CAP_SYS_ADMIN unless mounting is required.

---

## Now — the next slice

Items 1–5 below shipped (kept for the record with a DONE marker);
the live edge starts at #6.

### 1. CSRF guard for mutating endpoints — DONE

Loopback is a perimeter against off-host attackers, not against a
malicious local browser tab issuing a cross-origin POST. Cheap to fix,
annoying to retrofit:

- Reject mutating requests (POST / DELETE / PUT) whose
  `Sec-Fetch-Site` header is not `same-origin` or `none`.
- Skip the check for `/api/v1/*` calls from `arctern-client` (it can
  set a header the browser cannot forge from cross-origin; e.g.
  `X-Arctern-Cli: 1`).
- Tests: 403 on cross-origin POST, 204 on same-origin POST.

### 2. Pool overview + scrub control — DONE

The first ZFS-console slice — replaces the current admin UI's "yeah,
it has replication" framing with "here's your pool's health, here's
its scrub state, click to start one."

**zfskit additions:**

- `pool::status_json(name) -> PoolStatus` parsing `zpool status -j
  <name>` (added in OpenZFS 2.3 — verify on host first; if absent on
  the operator's OS, parse the text format as a fallback).
- `pool::scrub(name, action)` where `action ∈ Start | Pause | Resume |
  Stop`. Calls `zpool scrub` with the appropriate flag.

**daemon additions:**

- `GET /api/v1/pools` — every imported pool with health + capacity.
- `GET /api/v1/pools/{name}` — full status: vdev tree, error counters,
  scrub timestamp + duration + progress.
- `POST /api/v1/pools/{name}/scrub` — body `{ action: "start" | ... }`.

**UI additions:**

- `Pools` nav entry with overview table.
- Pool detail page: vdev tree with health badges (green / degraded /
  faulted), capacity ring, Scrub button + live progress bar when
  active, recent scrub history (date + duration + errors).

### 3. ARC stats — `zfskit::system` module — DONE

ARC stats live in `/proc/spl/kstat/zfs/arcstats`, not in `zfs(8)`. This
breaks zfskit's "CLI-only" rule by design — add a new sibling
module that owns `/proc/spl/kstat/...` parsing:

- `zfskit::system::arc_stats() -> ArcStats { size, target_size,
  hits, misses, demand_hits, mfu_hits, mru_hits, l2_size, ... }`.
  Parse the kstat text format (`name TYPE value` per line).
- `zfskit::system::pool_io(pool) -> PoolIo { read_ops, write_ops,
  read_bytes, write_bytes }` from `/proc/spl/kstat/zfs/<pool>/io`.

The constitution note (no FFI, CLI-only) gets a "kstat files are not
FFI and not a CLI" amendment.

**daemon additions:**

- A 60s background sweep that records `arc_stats()` + per-pool
  capacity into new SQLite tables `arcstats_history`,
  `pool_capacity_history`. ~1 row/minute, ~30 days retention.
- `GET /api/v1/system/arc` — current snapshot.
- `GET /api/v1/system/arc/history?since=&limit=` — time series.

**UI additions:**

- Dashboard hero: pool capacity ring(s), ARC hit-rate gauge, last
  scrub badge per pool, "X replication jobs healthy" summary.
- ARC tab with hit-rate line chart, demand vs. prefetch breakdown,
  size vs. target-size area chart.

---

## Next — the next slice after that

### 4. Per-vdev error counters + tree view — DONE

Already comes back from `zpool status -j`. Render as a collapsible
tree with red badges on degraded vdevs. Operators look at this at
3am with one eye open; the visual hierarchy matters more than the
data density.

### 5. Hold inspection on snapshots — DONE

zfskit already has `hold::holds(snapshot)`. Wire it through:

- `GET /api/v1/datasets/{name}/snapshots/{snap}/holds`.
- In SnapshotsView, clicking a row reveals who's holding it; the
  Destroy button explains why it's disabled when held.

### 6. Property editor per dataset

The "you can change this" features most people forget exist. UI:
sortable property table per dataset, edit-in-place for the common
ones (compression, recordsize, atime, sync, quota, reservation).
Server-side: one new endpoint, gated by an allow-list of editable
properties (no `mountpoint=/foo` from the browser — that's a footgun
for another day).

### 7. Encrypted dataset key management

zfskit::encryption already wraps load-key / unload-key /
change-key / change-keylocation. Add per-encrypted-dataset rows in
the UI:

- Lock icon + state (loaded / not-loaded).
- Buttons: Load key (prompt for passphrase, file path, or "use
  configured keylocation"), Unload key.
- Change key flow: separate page, double-confirm, explain that
  unloading inflight datasets will fail.

### 8. `zpool events` tail

Kernel-side ZFS events (checksum errors, vdev state changes,
resilver progress). `zpool events -v` is the source; parse as a
streaming endpoint. Useful for early-warning. Read-only.

### 9. TRIM status + start

`zpool trim` start + status. One row per pool with sparkline of
"% trimmed last cycle." Low-effort, high-value for SSD pools.

---

## Later — when the foundation is solid

### 10. Capacity forecasting

With `pool_capacity_history` recording daily `used` values, simple
linear regression on the last 30 days: "at this rate, pool fills in
47 days." Display below the capacity ring. Cheap, useful, surprising.

### 11. Snapshot diff browser

`zfs diff snap1 snap2` rendered as a file tree, with +/-/M
indicators. Great for "what changed between backups?" Niche but
delightful. UI carries most of the weight.

### 12. Iostat live view

`zpool iostat -j 1 1` (verify -j support — text fallback otherwise).
Per-pool, per-vdev throughput sparklines. Updates every 1-2s while
the tab is open. Pause when the tab is hidden.

### 13. Pool import wizard

`zpool import` with no args lists importable pools. Pick one,
provide altroot, click import. Useful for disaster recovery; the
zfskit primitives already exist from the archinstall_zfs
side of the project.

### 14. Real `bytes_sent` from push jobs — DONE

The push copy loop counts bytes per transfer slot; cycle totals land
in `job_runs.bytes_sent` and the receiver records its own view in
`recv_transfers`.

### 15. Dataset CRUD with strong confirms

Create / destroy / rename. Each is a footgun. Worth doing
eventually; not high-leverage for the first console slice.

### 16. Multi-pool aggregate views

When you have more than one pool, the dashboard needs a layer:
total used / total free across all pools, ARC stats stay system-wide,
scrub status grouped by pool. Defer until you actually have two pools
on one host.

### 17. Write-back config editing

The current `/config` view is read-only. Future option: textarea
editor with `arctern_config::load_from_str` validation, atomic write,
`POST /api/v1/reload` that does an in-place re-exec. Hairy because
job state is in-memory; the re-exec path is the cleanest reload story
but requires care around graceful shutdown of inflight pushes.

### 18. SMART data integration

`smartctl` JSON output → per-disk health (temperature, reallocated
sectors, hours powered on, error counters). Tie disks to ZFS vdevs
via `zpool status -P` (physical paths) so the UI can show
"vdev mirror-0 → /dev/disk/by-id/ata-... → SMART: 312 reallocated".
External tool dependency; gate the feature on smartctl being
installed. Not a small piece of work.

---

## Architectural decisions waiting for code

These shape multiple roadmap items above:

### Where kstat-reading lives

Decided: new `zfskit::system` module. Sibling to the CLI wrappers,
clearly scoped to "things ZFS exposes outside of `zfs(8)`/`zpool(8)`."

### Mutation auth perimeter

Decided: `Sec-Fetch-Site` header check + CLI bypass header (#1). Not
strong enough to expose the daemon off-host, but good enough that a
local browser tab can't trigger destructive operations via CSRF.

### History retention

Open. Today `log_events` is 24h, `job_runs` is 30d (declared, not yet
swept). For arcstats + pool capacity, recording 1 row/minute = ~1.4M
rows/year/host. Probably want: 1m resolution for the last 24h,
hourly aggregation after that, daily after 30d. Standard time-series
downsampling. Not blocking the first slice, but settle before
shipping #3.

### Read-runner pooling

Today every read endpoint borrows `AppState::runner` (which is a
single `Arc<RealRunner>` — fine, the runner is internally
concurrency-safe per zfskit's design). When we add live polling
endpoints (iostat, scrub progress), per-request runner spawning is
still cheap; revisit only if `zfs(8)` exec time dominates a frame.

---

## Anti-goals

Things arctern deliberately won't do, so feature creep stays bounded:

- **Multi-host federation in the ZFS console views.** Peers stay in
  their own tab. A pool detail page shows one host's pool, not three.
  Federation lives at the replication layer.
- **GUI for the full TOML schema.** The config stays a text file the
  operator owns. Read-only display in the UI is fine; a structured
  editor would re-implement TOML semantics badly.
- **Authentication for the loopback UI itself.** Loopback bind + SSH
  tunnel is the perimeter. If you need multi-user, that's a different
  product (a federation gateway).
- **Wrapping non-OpenZFS implementations** (TrueNAS-specific tooling,
  Btrfs comparisons, etc.). arctern is OpenZFS ≥ 2.2 on Linux.
