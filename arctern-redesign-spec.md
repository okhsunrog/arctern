# arctern redesign — implementation & review spec (handoff)

**Purpose.** Self-contained brief for a strong model to (a) review the design and surface
nuances/risks, then (b) implement it. It folds in everything gathered from two deep
codebase explorations, a Fable-5 architecture review, and the Nuxt UI v4 component docs, so
you should NOT need to re-explore from scratch — but verify any `file:line` before editing;
line numbers are from the state on 2026-07-08 and may drift.

**Author of this spec:** Claude Opus 4.8 (planning/exploration session). Approved by the
repo owner. You are free to improve the design — this is a proposal, not a contract.

---

## 0. Decisions & scope (confirmed with owner)

- **Radical UI redesign** of `admin-ui/` (Vue 3 + Nuxt UI v4). Not cosmetic — new shell + all
  views. The current UI is thin/inconsistent and disliked.
- **Peer model redesign** to "one peer = one host with multiple prioritized routes" — done
  **fully, including the production migration** of nova's live config + pool state.
- **Delivered as one big push** (build everything, then deploy + migrate at the end), not phased.
- **Docs are low priority.** Do NOT perfect them (they were AI-written). Only keep them from
  drifting from code — sync the specific claims the review flagged as wrong.
- Owner runs this in production (nova laptop = sender, mira NAS = receiver). Changes to live
  config + pool bookmarks/holds must be careful and reversible-minded.

## 1. Build & deploy mechanics (so you can ship it)

- Monorepo: `crates/{api,config,transport,client}`, `daemon/` (binary `arctern-daemon`),
  `admin-ui/` (Vue SPA embedded into the daemon at build time via `build.rs` + `memory-serve`).
- Sibling crate `zfskit` at `../zfskit` (ZFS toolkit; add primitives there first if missing).
- `justfile`: `just build` = `build-ui` (`vp install && vp exec vue-tsc --build && vp build`)
  then `cargo build --release -p arctern-daemon`. `just openapi` regenerates
  `admin-ui/openapi.json` + the typed client (`admin-ui/src/client/`, **never hand-edit**).
  `just check|test|lint|fmt`, `just ci`.
- **`vp check` (eslint+prettier) does NOT run `vue-tsc`** — the real typecheck gate is
  `vue-tsc --build` inside `just build`. The project has `noUncheckedIndexedAccess` on; guard
  array indexing. Nuxt UI components are globally auto-registered (`main.ts` + `<UApp>` in `App.vue`).
- Deploy nova (glibc): `just build` → install `target/release/arctern` to `/usr/local/bin/arctern`
  via temp+atomic `mv` (a running executable can't be overwritten in place — ETXTBSY) → `systemctl restart arctern.service`.
- Deploy mira (Debian, static musl): `CC_x86_64_unknown_linux_musl=musl-gcc cargo build --release
  --target x86_64-unknown-linux-musl -p arctern-daemon` → `scp` → atomic `mv` → restart. mira's
  `arctern.service` serves its own local UI on `127.0.0.1:7878` AND receives pushes via sshd
  ForcedCommand `arctern stdinserver-dispatch laptop_nova` (separate short-lived processes).
- Both daemons bind UI+API on loopback `127.0.0.1:7878` and an API UDS `/run/arctern/arctern.sock`.

## 2. Full API surface / data model (what the UI can show today)

Response field names are exact (`crates/api/src/lib.rs` == `admin-ui/src/client/types.gen.ts`).

- **Config**: `GET /api/v1/config` → `{ path, content_toml }` (read-only).
- **Datasets/Snapshots/Holds**:
  - `GET /datasets` → `DatasetSummary[] { name, dataset_type, properties:{[k]:string} }` (filesystems+volumes; default zfs props incl. `used`).
  - `GET /datasets/{name}/snapshots?prefix` → `DatasetSummary[]` with `properties.creation` + `properties.used`.
  - `POST /datasets/{name}/snapshots` `{ snapshot_name, recursive?, properties? }` → 201; 409 `snapshot_exists`.
  - `POST /datasets/{name}/snapshots/{snapshot}/destroy` → 204; 409 `snapshot_held`.
  - `GET /datasets/{name}/snapshots/{snapshot}/holds` → `SnapshotHold[] { tag, timestamp }` (unix).
- **Jobs** (`JobStatus { name, kind, last_run?, next_run?, last_error?, running?, paused?, transfer?, targets? }`):
  - `transfer: TransferInfo { dataset, peer, kind, bytes_sent, total_bytes?, started_at }`.
  - `targets: TargetStatus[] { peer, mode, connected, auto_interval_secs?, last_success?, last_error? }`.
  - `GET /jobs`; `POST /jobs/{name}/{cancel|pause|resume|wakeup}` → 204; `POST /jobs/{name}/push/{peer}` → 204;
    `GET /jobs/{name}/runs?since&limit` → `JobRun[] { started_at, finished_at?, status, error_message?, bytes_sent? }`.
- **Peers**:
  - `GET /peers` → `PeerSummary[] { name, ssh_target, reachability }`;
    `PeerReachability = {kind:'connected'} | {kind:'reconnecting',since} | {kind:'failed',since,last_error}`.
  - `GET /peers/{peer}/events` (SSE, 503 if not connected); `GET /peers/{peer}/jobs`; `GET .../jobs/{name}`;
    `POST .../jobs/{name}/wakeup`; `GET /peers/{peer}/snapshots?dataset&prefix_regex` →
    `PeerSnapshotEntry[] { name, guid(u64 string), createtxg }`; `POST /peers/{peer}/snapshots/{name}/destroy`.
- **Pools**:
  - `GET /pools` → `PoolSummary[] { name, state, error_count, alloc_space, total_space, scan? }` (sizes are zpool strings e.g. "608G").
  - `GET /pools/{name}` → `PoolStatus { name, state, error_count, pool_guid, txg, scan?, vdevs[] }`.
    `ScanSummary { function, state, start_time?, end_time?, to_examine?, examined?, errors?, pass_start?, scrub_pause?, issued? }`.
    `VdevNode { name, vdev_type, state, alloc_space, total_space, read_errors, write_errors, checksum_errors, path?, children[] }` (recursive).
  - `POST /pools/{name}/scrub` `{ action: "start"|"pause"|"resume"|"stop" }` → 204.
- **ARC**: `GET /system/arc` → `ArcStats` (size, c, c_min, c_max, hits, misses, demand_/prefetch_ data+metadata hits/misses,
  mru/mfu/mru_ghost/mfu_ghost_hits, l2_size, l2_hits, l2_misses, compressed_size, uncompressed_size, hit_ratio?).
  `GET /system/arc/history?since&limit` → `ArcHistoryPoint[] { timestamp, size, c, hits, misses }` (slim by design).
- **Events**: `GET /events` (SSE) `LogEvent { id, timestamp, level, job_name?, message }`.
- **Not available anywhere** (would need daemon work if desired): pool iostat/latency/bandwidth, dataset
  property write-back, pool import/export, peer add/remove, snapshot rollback/clone.

### Actions wired vs available (UI gaps)
Wired: job wakeup (JobsView/JobDetail/JobsGrid), job cancel/pause/resume + push-to-peer (TransferPanel only),
pool scrub start/pause/stop (PoolDetail), snapshot create/destroy + holds list (SnapshotsView), peer job wakeup.
**Not surfaced though the API supports it:** pool scrub **resume** (never called — no button when `scan.scrub_pause`),
peer **snapshot destroy** (`destroyPeerSnapshot` unused), single **peer-job detail** (`getPeerJob` unused),
job cancel/pause/resume are **absent from the main Jobs table**.

### Fields returned but never displayed
`ScanSummary.issued` (the accurate scrub-progress metric — UI wrongly derives % from `examined/to_examine`) and
`ScanSummary.scrub_pause` (would drive a Resume button); `ArcStats.c_min`, `ArcStats.l2_misses`; `TransferInfo.started_at`
(rate/ETA is computed from live deltas instead); `JobRun` `since` query param unused.

## 3. Peer / config / cursor model — facts that constrain the redesign

- **Peer name is a purely local sender-side label.** It never goes on the wire and is not in the receiver
  ACL. Wire identity is the argv `<identity>` (e.g. `laptop_nova`) in the receiver's `authorized_keys`
  `command=`; ACL `AllowedClient{identity,fingerprint,jobs,operations,root_fs,recv}` (`crates/config/src/schema.rs:125-144`),
  dispatch matches on identity only (`daemon/src/stdinserver/dispatch.rs:101-156`). `RecvHeader`
  (`crates/transport/src/protocol.rs:195-199`) carries only version/target_dataset/send. **⇒ renaming/merging
  peers has zero receiver impact.**
- **Config**: `PeerConfig` (`schema.rs:86-105`) `{ name, ssh_target(93, handed verbatim to openssh), mode:PeerMode(96, Auto/Manual :108-118), auto_interval:Option<Duration>(104) }`.
  `PushJobConfig` (`schema.rs:298-336`) `{ peer:Option<String>(307, legacy), targets:Vec<String>(315), interval, filesystems:Vec<FilesystemFilter>, target:PushTarget{root_fs}(344-351), send:SendFlagsConfig(357-368, 4 flags default true), dry_run, snapshot_filter }`.
  Legacy `peer=` → `targets=[peer]` at load (`lib.rs:128-144`); validation `lib.rs:174-183` (unique names) + `:354-358` (every target matches a peer name).
- **Connection**: `PeerLink` (`daemon/src/peer/mod.rs:62-79`) is one `ssh_target` → one `openssh::Session` → one link;
  `connect` `:88-128` (`connect_mux(ssh_target)`), recv channels `open_recv :174-204`. Reachability
  (`daemon/src/peer/state.rs`): `PeerStatus` `Connected|Reconnecting{since}|Failed{since,last_error}` (:13-27),
  `PeerEntry{name,ssh_target,status,link}` (:29-36), `PeersState = Arc<RwLock<HashMap<name,PeerEntry>>>` (:38).
  `reconnect.rs`: `next_delay` exp 1→60s (:18-23); `run_for_peer` (:29) connects, then probes `ListJobs` every 15s
  (:65-101); **one task per `[[peers]]` entry**, ssh_target captured for the loop (`main.rs:245-252`).
- **Selection is NOT "first reachable wins".** `select_targets` (`daemon/src/jobs/push.rs:1018-1071`) iterates ALL
  targets and selects each that is (manual+connected) OR (Auto+connected+due); `run_cycle` (:1073-1126) replicates to
  **every** selected target. Today two connected auto targets → double push. The prod config only avoids this because
  mira-wg is `manual`. The `schema.rs:308-314` doc-comment claiming fallback semantics is a **lie vs code**.
- **Cursor/step-hold naming embeds the peer name.** Cursor bookmark `{dataset}#arctern_cursor_G_{guid:x}_J_{job}_P_{peer}`
  (`push.rs:468-471`, leaf matcher :474-475, `advance_cursor` :784-822 creates-new-then-destroys-stale = crash-safe);
  step-hold `arctern_step_J_{job}_P_{peer}` (:461-463). `P_{peer}` is the **config peer name (network path)**, so mira-lan
  and mira-wg mint separate cursors today.
- **Incremental base is route-independent.** `plan_one_filesystem` (`push.rs:481-541`) asks the receiver
  `ListReceiverGuids` (**filtered by job prefix** — see review #1) and `pick_plan` (:185-224) picks the youngest sender
  snapshot whose GUID the receiver has. Cursor bookmark is only consulted via the **name-agnostic GUID fallback**
  `apply_bookmark_fallback` (:232-271, lists all sender bookmarks unfiltered :276, intersects by GUID :252-256). Resume
  token path :522-538. **⇒ a single cursor per (job, physical-peer) is correct and cleaner than today's per-route pair.**
- **Receiver lineage is keyed on dataset path only.** `target = root_fs/sender_path` (`push.rs:1152`); receiver hold is
  `arctern_last_J_{job}` (`recv.rs:81-83`) — no peer name anywhere on the receiver.
- **state.db**: `push_syncs` PK `(job_name, peer)` (`state/mod.rs:95-101`, `state/push_syncs.rs`). Losing a row = one
  extra cheap sync (documented). `job_runs`/`log_events` keyed by job only.

## 4. Fable-5 architecture review — the defects driving Part B

1. **`prefix_regex` limits GUID intersection** (highest): planner sends `ListReceiverGuids` filtered by the job prefix, so
   a common snapshot/bookmark with a different prefix (migration/manual) is invisible → forced Full send; the
   bookmark-fallback migration path is likewise crippled. Fix: request receiver GUIDs **unfiltered** for the planner;
   keep the prefix filter only for UI `ListSnapshots`. One client-side line; receiver semantics unchanged.
2. **False reconnect under a saturated channel**: the 15s/20s `ListJobs` probe shares the ControlMaster TCP with bulk
   sends; a long WG send starves the probe → link teardown (in-flight transfer survives via `Arc<Session>`, but the peer
   drops from `select_targets` and the SSE re-subscribes). Fix: skip the probe while recv channels are active / lengthen
   its timeout / rely on ssh `ServerAliveInterval`.
3. **JoinSet leak** (`daemon/src/stdinserver/control.rs:52`): `inflight` grows per request but is only reaped at EOF;
   a days-long control channel accumulates ~5.7k finished tasks/day. Fix: `while inflight.try_join_next().is_some() {}` each loop.
4. **Doc drift**: "first available peer, falling back" (schema.rs + `docs/example-config.toml`) vs code replicating to all
   due targets. The peer-routes redesign (Part C) *implements* the promised priority-failover, resolving this.
5. **Peer-jobs federation is half-wired**: the receiver's control handler stubs `ListJobs`/`GetJobStatus`/`WakeupJob`
   (empty / NotFound), so nova's Peers tab shows mira's jobs as empty even though mira runs `databak`/`rootbak`/`received_prune`.
   The bridge already exists — UDS `/run/arctern/arctern.sock` + the `arctern-client` crate. Fix: the control handler proxies
   these three requests to mira's local daemon over the UDS.
6. **Minor**: snapshot tag is nanosecond precision (`snap.rs`) vs docs' second precision — truncate;
   `SubscribeEvents{since:None}` replays a day of `log_events` each reconnect — send the current `GetLogCursor`;
   unknown `/api/*` paths fall through to the SPA HTML (200) — return 404 (`daemon/src/router.rs`); empty push cycles
   (auto_interval=1d) write `ok, bytes 0` rows to `job_runs` (96/day noise) — skip when `selected` is empty; systemd unit
   carries QUIC-era `cert/key.pem` comments and could add `ProtectSystem`/`PrivateTmp`/`RestrictAddressFamilies`.
   Deploy debts (ops, not code): push runs as **root@mira** vs a dedicated `arctern-replicator` (own docs recommend the
   latter); `zrepl_*` snapshots on mira are protected as non-prefixed and need a manual destroy after ~2026-07-21;
   sshd `PermitRootLogin yes` → `prohibit-password`.

## 5. Part B — daemon correctness/hygiene (Rust)

Implement B1–B7 as in §4: **B1** peer log levels (`peer/mod.rs:116,235` — parse remote tracing level, re-emit at the
matching level tagged with peer/route, route into the peer event stream, not blanket `warn!`); **B2** unfiltered planner
GUIDs; **B3** reconnect probe under load; **B4** JoinSet reap; **B5** peer-jobs UDS federation via `arctern-client`;
**B6** new endpoints — `GET /api/v1/peers/{peer}/datasets` (proxied) and snapshot **hold create/release**
(`POST`/`DELETE` under the holds path; zfskit has hold support); **B7** the minor-hygiene set.

## 6. Part C — peer-routes model + production migration

- **C1 Config schema** (`crates/config/src/schema.rs`, `lib.rs`): `[[peers]]` gains ordered `routes` — each
  `{ name, ssh_target, priority }`; `mode`/`auto_interval` stay peer-level. Push-job `targets` reference the **peer**
  (physical host). Full migration ⇒ you may drop the old single-`ssh_target` peer form (update validation + legacy
  `peer=` resolution). Consider accepting a single-route shorthand for ergonomics.
- **C2 Connection** (`daemon/src/peer/{mod,state,reconnect}.rs`, `main.rs`): the per-peer link ranks routes by priority,
  connects the highest-priority **reachable** route, and re-ranks on reconnect (so it prefers LAN again once it returns).
  `PeerStatus`/`PeerEntry` carry the **active route** + per-route reachability. One reconnect task per peer (not per route).
- **C3 Selection** (`push.rs:1018-1126`): select the peer once; replicate via its active route — real priority failover
  (fixes review #4 and the double-push).
- **C4 ZFS/state naming**: cursor bookmark + step-hold `_P_{peer}` and `push_syncs` PK use the **physical-host peer id**
  (stable across routes), not the route name.
- **C5 Production migration** (nova only; mira untouched). Rewrite `/etc/arctern/arctern.toml` to one peer `mira` with
  routes `lan`(high priority, `arctern-mira-lan`, mode auto/1d) + `wg`(low, `arctern-mira-wg`, manual→now just a lower
  route). Reconcile live source-pool state:
  - Let the next successful cycle mint the fresh `_P_mira` cursor. The GUID fallback is name-agnostic, so **incrementals
    keep working — no 255G full resend** (verify this!). The old `_P_mira-lan`/`_P_mira-wg` cursor bookmarks become
    harmless orphans (bookmarks don't pin snapshots against prune); destroy them for cleanliness.
  - **Release any lingering `arctern_step_J_..._P_mira-lan|wg` holds** — these *do* pin a snapshot against prune and won't
    be swept under the new name. This is the sharpest migration item.
  - Drop the stale `push_syncs` rows `(job, 'mira-lan'|'mira-wg')`; the merged peer starts "never synced" → one redundant
    cheap GUID-diff cycle (harmless).
- **C6 API/UI**: `PeerSummary` carries `routes[]` + active route + per-route reachability (`crates/api`), rendered in the UI.

## 7. Part A — UI redesign (Vue, `admin-ui/`) — Nuxt UI v4

Component palette confirmed available (v4.6): `UDashboardGroup/Sidebar/Navbar/Panel/Toolbar`, `UTable` (TanStack column
defs, `column.toggleSorting()`, `#expanded` slot + `v-model:expanded`, selection, `loading`), `UTree` (items
`{label,icon,trailingIcon,children,defaultExpanded}`, `v-model` selection, `v-model:expanded`, `:get-key`, slots
`#item-leading/label/trailing`), `UCommandPalette`, `useToast()`, `USlideover/UDrawer/UModal`, `UBadge/UChip/UProgress/
UTimeline/USkeleton/UEmpty/UTooltip`. **No built-in charts** — `chart.js` is already a dep (used in `ArcView`/`RunsCharts`);
reuse it for sparklines (transfer rate, ARC hit-ratio) rather than adding a lib.

- **A0 Shell & system UX**: replace `AppNav` + `max-w-6xl` with `UDashboardGroup` + collapsible/persistent
  `UDashboardSidebar` + `UDashboardNavbar`; grouped nav (Overview / Replication / Storage / System). Add `⌘K`
  `UCommandPalette` (jump to pool/job/peer/dataset + quick actions: Send now, Scrub, Create snapshot). **Global toasts on
  every mutation** (currently silent; surface the daemon's error body). New `utils/status.ts` mapping pool state / job
  state / peer reachability / vdev health → color+icon+label, used everywhere. `USkeleton`/`UEmpty` states. Remove dead
  `PlaceholderView.vue`. Cache-bust the SPA (no-cache `index.html` + reload-on-chunk-load-error) — the "Snapshots doesn't
  open" report is almost certainly a stale cached `index.html` referencing an old chunk hash after a redeploy (hard-refresh
  resolves it; the reworked view type-checks and builds).
- **A1 Datasets & Snapshots**: `UTree` dataset hierarchy (sizes, snapshot counts, type icons) → snapshots `UTable`:
  sortable Created/Used, multi-select **bulk destroy**, **eager** holds badges, **fix the destroy-guard** (today Destroy is
  enabled before holds are fetched → 409), snapshot detail `USlideover` (all properties), hold create/release (B6). Keep Create modal.
- **A2 Peers & PeerDetail**: rich peer cards — reachability + **routes (active/priority)** + a summary of the peer's
  jobs/in-flight transfers + last error. **Fix the stale `EventSource`** (`PeerDetailView.vue:63` captures `peer.value`
  once and never re-points when switching peers). Jobs tab shows mira's real jobs (via B5). Snapshots tab gets the `UTree`
  dataset picker (via B6) + wired **destroy** with the holds guard. Single peer-job detail via `getPeerJob`.
- **A3 Jobs**: main table gains inline wakeup/pause/resume/cancel + sorting + clear status; JobDetail transfer rate/ETA
  from server `TransferInfo.started_at`; keep RunsCharts.
- **A4 Pools/PoolDetail**: scrub card fixed — progress from `ScanSummary.issued` (not `examined`), **Resume** button when
  `scan.scrub_pause`, ETA; keep `VdevTree`; per-vdev error drilldown.
- **A5 ARC & Config**: ARC add `c_min`, `l2_misses`, demand/prefetch breakdown. Config: sectioned render (peers/routes,
  jobs, defaults) + raw toggle.

## 8. Open design questions / nuances to weigh in review

1. **Route re-selection policy**: after failing over LAN→WG, should the link *actively* switch back to LAN mid-idle when
   it returns, or only re-rank at the next reconnect/cycle? (Proposal: re-rank at connect + on each push-cycle start; don't
   preempt an in-flight send.)
2. **Per-route reachability probing**: do we keep one live SSH session to the active route only, or keep the ControlMaster
   warm for all routes to report per-route reachability accurately? (Proposal: connect only the active route; mark lower
   routes "unknown/last-checked" and probe on failover — avoids N idle SSH sessions, esp. metered WG.)
3. **`mode` (auto/manual) at peer vs route level**: today mira-wg being `manual` is how "only replicate over WG on demand"
   is expressed. Under routes, is `manual` still meaningful, or does route `priority` + reachability subsume it? (Proposal:
   keep peer-level `mode`; a `manual` peer never auto-cycles regardless of route. Reconsider whether WG should instead be
   an always-eligible low-priority route — this changes replication behavior, so confirm with owner.)
4. **Migration safety**: is "let the next cycle mint the `_P_mira` cursor via GUID fallback" acceptable, or should the
   migration *rename* an existing `_P_mira-lan` bookmark to `_P_mira` up front to guarantee zero risk of a Full send? (I
   lean rename-up-front for belt-and-suspenders; verify GUID fallback empirically either way.)
5. **Live progress**: transfer/scrub progress is poll-bound (5s/3s) because SSE carries only log events. Worth a
   state-delta SSE channel now, or keep polling and just tighten intervals? (Out of scope unless cheap.)
6. **Config write-back**: everything config is read-only. A guided "add route / change priority" UI would need a daemon
   config-write endpoint + validation + reload. Big; probably a later effort — flag if owner wants it.

## 9. Docs (low priority — sync only)

Do NOT rewrite the docs for quality. Only fix drift the code changes create/expose:
`README.md`, `ARCHITECTURE.md`, `docs/example-config.toml` (peer-routes shape + remove the false "first reachable, falling
back" claim), `docs/deploy-full-mirror.md`. Keep it to matching reality, nothing more.

## 10. Verification

- `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`; `cd admin-ui && vp check`;
  `just build` (the `vue-tsc` gate must pass); `just openapi` clean (client regen matches).
- Deploy nova (native) + mira (musl); migrate nova config + pool state.
- **Replication correctness (must):** after the peer merge, trigger a push and confirm it is **incremental, not Full**
  (`ListReceiverGuids` unfiltered; fresh `_P_mira` cursor; no `_P_mira-lan/wg` step-holds left); channels reconnect;
  `push_to_mira` healthy; mira still receiving.
- **UI:** Snapshots opens + tree + bulk destroy + holds; Peers shows mira's real jobs + routes + active route; scrub Resume
  works; Jobs inline actions; toasts fire; `next_run` never shows "N ago" while running; ARC/pools/events render; unknown
  `/api/*` → 404.

---

### Appendix — current state snapshot (2026-07-08)

Recent commits already landed on `main` this session (reworked SnapshotsView will be replaced by A1; keep or supersede):
`feat(admin-ui): rework snapshot management with a dataset tree picker`, `fix(admin-ui): don't render a past next_run as
"N ago"`, `docs: state real production status`, `fix(admin-ui): guard optional job.targets to satisfy vue-tsc`. nova+mira
are both on today's build; replication healthy (mira has a fresh full `home_new` backup; old `home` backup dropped). The
`utils/format.ts` `formatNextRun`, `utils/datasets.ts` `visibleDatasetRows`, and `useDatasets` composable are new and can be
reused or replaced by `UTree`.
