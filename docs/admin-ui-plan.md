# admin-ui plan

Build the web UI using the same toolchain and embed pattern as
`~/code/rust/claude-proxy-rs`. This doc is a delivery plan, not a
spec. When implementation starts, work the commit sequence at the
bottom; this doc is then descriptive only.

## Toolchain (verbatim from claude-proxy-rs)

- **Vite+** (`vite-plus` package, `vp` CLI). Replaces direct
  vite/vitest/oxlint/oxfmt installs. `vp` is global; the project
  declares `packageManager = "bun@..."` in `package.json` so
  `vp install` uses bun under the hood.
- **`vp` invocations** (the only ones we use): `install`, `dev`,
  `build`, `check`, `lint`, `fmt`, `exec` (for `vue-tsc`,
  `openapi-ts`), `dlx`. Never call vite/vitest/eslint/prettier
  directly.
- **AGENTS.md** copied verbatim from claude-proxy-rs's
  `admin-ui/AGENTS.md`. Tells future contributors and AI agents
  the Vite+ pitfalls (don't use pnpm/npm directly, don't install
  vitest separately, etc.).
- **Lint + fmt config inline** in `vite.config.ts` under `lint:`
  and `fmt:` keys — no separate `.eslintrc`, no `.prettierrc`.
  Settings copied from claude-proxy-rs:
  - lint plugins: eslint, typescript, unicorn, oxc, vue
  - typeAware: true
  - ignored: `**/dist/**`, `**/src/client/**`
  - fmt: no semicolons, single quotes, 100-col, no
    sortPackageJson, ignore `**/src/client/**`
  - `staged: { '*': 'vp check --fix' }`

## Stack (verbatim from claude-proxy-rs unless noted)

- Vue 3 + TypeScript + `vue-router`
- `@nuxt/ui` v4 (NOT `@nuxt/ui-pro`; arctern doesn't need pro
  components)
- `tailwindcss` v4 (loaded via `@nuxt/ui`'s vite plugin)
- `@hey-api/openapi-ts` + `@hey-api/client-fetch` for the
  generated TS client
- `chart.js` + `vue-chartjs` for replication-throughput and
  cycle-duration charts
- `vite-plugin-vue-devtools` for the dev experience

## Embed pattern (verbatim from claude-proxy-rs)

- `cargo add memory-serve`
- `build.rs` calls `memory_serve::load_directory("admin-ui/dist")`
  plus emits `GIT_HASH` and `BUILD_TIME` env vars
- Static router built via:
  ```rust
  pub fn static_routes() -> Router<Arc<AppState>> {
      memory_serve::load!()
          .index_file(Some("/index.html"))
          .fallback(Some("/index.html"))
          .into_router()
  }
  ```
- Mounted at the daemon's HTTP root (NOT under `/admin` — see
  "Differences from claude-proxy-rs" below)
- `index.html` SPA fallback handles all client-side routes

## Differences from claude-proxy-rs

### Mount path: `/`, not `/admin`

claude-proxy-rs nests UI under `/admin` because it shares the
daemon with API endpoints at `/`. arctern is single-purpose; the
UI is THE front end. Mount static at `/`, keep `/api/v1/...` and
`/api-docs/openapi.json` for the API.

`vite.config.ts` `base: '/'` (not `/admin/`).

`@hey-api/client-fetch` `baseUrl` left as default (no
`/admin` prefix).

Dev server `proxy:` forwards `/api/v1` and `/api-docs` to
`localhost:7878` (the daemon's loopback bind per
`ARCHITECTURE.md`).

### No auth flow

claude-proxy-rs has `LoginView`, `useAuth`, 401-redirect interceptor.
arctern's `ARCHITECTURE.md` puts auth on the local UI's loopback bind
explicitly out of scope — UNIX socket permissions and loopback are
the perimeter. So skip:

- `LoginView.vue`
- `composables/useAuth.ts`
- `meta: { requiresAuth: true }` route guards
- 401-redirect interceptor in `main.ts`

Keep an interceptor for ergonomics, but route errors into a
`useToast` (Nuxt UI) notification rather than a redirect.

### SSE for live updates

claude-proxy-rs polls. arctern has `/api/v1/events` and
`/api/v1/peers/{peer}/events` (per `ARCHITECTURE.md`). Use the
native `EventSource`:

```ts
// composables/useEvents.ts
export function useEvents(peer?: string) {
  const events = ref<EventWire[]>([])
  const path = peer ? `/api/v1/peers/${peer}/events` : '/api/v1/events'
  const es = new EventSource(path)
  es.onmessage = (e) => events.value.push(JSON.parse(e.data))
  onUnmounted(() => es.close())
  return { events }
}
```

## File layout

```
admin-ui/
  src/
    assets/main.css
    client/                  ← generated; gitignored or committed
                                (claude-proxy-rs commits — match it)
    components/
      JobsGrid.vue
      JobCard.vue
      SnapshotsTable.vue
      EventsLog.vue
      ThroughputChart.vue
      DurationChart.vue
      DestroySnapshotModal.vue
      PeerReachability.vue
    composables/
      useEvents.ts
      useJobs.ts
      usePeers.ts
      useSnapshots.ts
    router/index.ts
    utils/
      format.ts              ← bytes, durations, RFC3339
    views/
      DashboardView.vue      "/"
      JobsView.vue           "/jobs"
      JobDetailView.vue      "/jobs/:name"
      DatasetsView.vue       "/datasets"
      SnapshotsView.vue      "/snapshots?dataset=..."
      PeersView.vue          "/peers"
      PeerDetailView.vue     "/peers/:peer/:tab(jobs|snapshots)"
      EventsView.vue         "/events"
    App.vue                  ← <UApp><RouterView/></UApp>
    main.ts
  index.html
  vite.config.ts
  openapi-ts.config.ts
  openapi.json               ← regenerated by `just openapi`
  package.json
  AGENTS.md
  bun.lock
```

## OpenAPI flow

```bash
cargo run --bin arctern -- --openapi > admin-ui/openapi.json
cd admin-ui && vp exec openapi-ts
```

Wrapped in `just openapi`. Run any time `crates/api` types change.

The Rust side adds an `--openapi` flag to clap that builds the
router, calls `ApiDoc::openapi()`, writes JSON to stdout, exits
zero. No daemon startup; cheap to invoke from scripts.

## Justfile additions

```just
build-ui:
    cd admin-ui && vp install && vp exec vue-tsc --build && vp build

build: build-ui
    cargo build --release

openapi:
    cargo run --bin arctern -- --openapi > admin-ui/openapi.json
    cd admin-ui && vp exec openapi-ts

check:
    cargo fmt --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    cd admin-ui && vp check
```

`build` is the new release target — embed depends on
`admin-ui/dist/` existing.

## Views in detail

### DashboardView "/"
- Top: small grid of all jobs (name, kind, last_run, last_error
  status badge)
- Bottom: live event tail (last 50, scroll-on-update, pause
  toggle)

### JobsView "/jobs"
- Table of all jobs with action buttons (`Wakeup`, `Reset` later)
- Click row → JobDetailView

### JobDetailView "/jobs/:name"
- Status panel (last_run, next_run, last_error, in-progress
  indicator)
- Two charts side-by-side: bytes-per-cycle bar chart, duration
  line chart. Both backed by `GET /api/v1/jobs/{name}/runs`
  (new endpoint; see "Backend additions" below).
- Recent runs table (last 50)

### DatasetsView "/datasets"
- Tree view of local datasets via `GET /api/v1/datasets`
- Per-peer tabs at top → switches to
  `GET /api/v1/peers/{peer}/datasets` (proxied)

### SnapshotsView "/snapshots?dataset=X"
- Table of snapshots for the dataset (name, creation, used,
  hold count)
- Destroy button → DestroySnapshotModal (double-confirm: type
  the snapshot name to enable the button)

### PeersView "/peers"
- One row per configured peer
- Reachability indicator (green/red dot, last reconnect attempt
  timestamp)
- Click → PeerDetailView with `:tab` selector for jobs vs.
  snapshots

### EventsView "/events"
- Full SSE log, filterable by job name + level
- Pause / resume button
- Limited client-side history (last 5000 events; older drops)

## Backend additions needed (separate from UI work)

- `GET /api/v1/jobs/{name}/runs?since=&limit=` — returns rows from
  the `job_runs` SQLite table the pivot agent is adding. Not
  blocking for the early UI views; chart-backed views need it.
- `--openapi` flag on the daemon binary (clap subcommand or
  top-level flag).
- Daemon HTTP bind on `127.0.0.1:7878` per ARCHITECTURE.md (the
  pivot agent likely already does this; confirm before starting
  UI work).

## Commit sequence (when pivot is done)

1. `feat(admin-ui): scaffold Vite+ project with Nuxt UI` — copy
   claude-proxy-rs's `vite.config.ts` + `package.json` +
   `AGENTS.md`, adapt `base` to `/`, swap proxy paths to
   `/api/v1` and `/api-docs`. Empty `App.vue` + `main.ts`.
2. `feat(daemon): --openapi flag dumps OpenAPI spec to stdout`
3. `feat(admin-ui): generate TS client from openapi.json` — run
   `just openapi`, commit `src/client/`
4. `feat(daemon): embed admin-ui via memory-serve` — `cargo add
   memory-serve`, `build.rs`, `static_routes()`, axum router merges
   it
5. `feat(admin-ui): router + DashboardView + JobsView` — first
   usable views, status grid, wakeup buttons, last_error display
6. `feat(admin-ui): SSE-backed EventsView` — composable + view
7. `feat(admin-ui): PeersView + PeerDetailView` — proxied views
8. `feat(api): GET /api/v1/jobs/{name}/runs` — chart data backend
9. `feat(admin-ui): JobDetailView with throughput + duration
   charts` — vue-chartjs
10. `feat(admin-ui): SnapshotsView with destroy confirm` — most
    dangerous endpoint, double-confirm UX
11. `chore(justfile): add build-ui, build, openapi, check recipes`
12. `docs(deploy): build the UI as part of the deploy flow` —
    update `deploy-snap-only.md` and `deploy-full-mirror.md` to
    use `just build` and note the bun + Vite+ build dependency

## When to start

After the pivot agent finishes. Three blocking dependencies:

1. **Pivot step 4** (the agent) adds peer-aware variants and
   `EventWire` to `crates/api`. UI must generate against the final
   types.
2. **Pivot step 11** adds SSE infrastructure. EventsView depends
   on the endpoint existing.
3. **Pivot step 5** moves the daemon HTTP bind from UDS to
   loopback TCP. Vite dev proxy needs the new bind to work.

If you want to pre-stage the scaffolding (commit 1) while the
pivot runs, that's safe — `admin-ui/` doesn't intersect with the
files the agent edits. But the OpenAPI generation (commit 3) must
wait.

## What I deliberately don't copy

- `@nuxt/ui-pro` — paid, unnecessary
- The OAuth/auth flow stack
- chart.js variants beyond what we need
- The deploy-by-rsync recipe — arctern's deployment uses the
  staged docs in `docs/deploy-*.md` instead, since systemd unit +
  config + binary install is a richer flow than rsync

## Open questions for later

- Should `src/client/` be committed (claude-proxy-rs does) or
  gitignored (cleaner diffs)? Recommend committing — guarantees
  the build works without running codegen, and changes to the
  client are visible in PR diffs.
- Should the UI bind a separate port from the API (7879 for UI,
  7878 for API) for cleaner CSP / CORS? Probably no — same-origin
  fetch with no auth is fine for loopback. Defer until a real
  reason appears.
- Tailwind v4 pulled by `@nuxt/ui` — do we need a separate
  `tailwind.config.ts`? claude-proxy-rs doesn't have one. Default
  is fine; only add a config if customization is needed.
