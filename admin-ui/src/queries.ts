// Every cached server resource, in one place. Keys are host-scoped by
// construction (`scope` is '' for this daemon, the peer name otherwise),
// which is what makes a peer console safe: two hosts can never share a
// cache entry, and switching hosts is a key change rather than a
// teardown of every panel.
//
// `staleTime` replaces the per-composable setInterval this console used
// to run. The auto-refetch plugin only revalidates queries that some
// mounted component is actually using, so a backgrounded browser tab
// stops driving `zfs list` on the daemon.

import { defineQueryOptions } from '@pinia/colada'
import {
  getArc,
  getArcHistory,
  getConfig,
  getPool,
  getSystemInfo,
  listDatasetHolds,
  listDatasets,
  listJobs,
  listPeers,
  listPools,
  listRuns,
  listSnapshots,
  recentTransfers,
} from './client'
import { baseUrlFor } from './composables/useHost'
import { unwrap } from './utils/errors'

/** Host scope: '' is this daemon, anything else is a peer name. */
export type Scope = string

const base = (scope: Scope) => baseUrlFor(scope || null)

// ── Jobs ────────────────────────────────────────────────────────
// Fed by the SSE stream (see stores/jobsStream.ts); the fetch here is
// the cold-start path and the fallback while the stream is down.
export const jobsQuery = defineQueryOptions((scope: Scope) => ({
  key: ['jobs', scope],
  query: () => listJobs({ baseUrl: base(scope) }).then(unwrap),
  // The stream keeps this fresh; polling on top of it would be waste.
  staleTime: 60_000,
}))

export const jobRunsQuery = defineQueryOptions(
  ({ scope, name, limit = 100 }: { scope: Scope; name: string; limit?: number }) => ({
    key: ['job-runs', scope, name, limit],
    query: () => listRuns({ path: { name }, query: { limit }, baseUrl: base(scope) }).then(unwrap),
    staleTime: 10_000,
  }),
)

// ── Peers ───────────────────────────────────────────────────────
// Peer links describe THIS daemon's outbound connections, so they are
// never proxied — the key has no scope on purpose.
export const peersQuery = defineQueryOptions(() => ({
  key: ['peers'],
  query: () => listPeers().then(unwrap),
  staleTime: 5_000,
}))

// ── Pools ───────────────────────────────────────────────────────
export const poolsQuery = defineQueryOptions((scope: Scope) => ({
  key: ['pools', scope],
  query: () => listPools({ baseUrl: base(scope) }).then(unwrap),
  staleTime: 5_000,
}))

export const poolQuery = defineQueryOptions(({ scope, name }: { scope: Scope; name: string }) => ({
  key: ['pool', scope, name],
  query: () => getPool({ path: { name }, baseUrl: base(scope) }).then(unwrap),
  // A running scrub moves; 3s matches the old poll and is cheap.
  staleTime: 3_000,
}))

// ── ARC ─────────────────────────────────────────────────────────
export const arcQuery = defineQueryOptions((scope: Scope) => ({
  key: ['arc', scope],
  query: () => getArc({ baseUrl: base(scope) }).then(unwrap),
  staleTime: 5_000,
}))

export const arcHistoryQuery = defineQueryOptions(
  ({ scope, limit }: { scope: Scope; limit: number }) => ({
    key: ['arc-history', scope, limit],
    query: () => getArcHistory({ query: { limit }, baseUrl: base(scope) }).then(unwrap),
    staleTime: 5_000,
  }),
)

// ── Datasets + snapshots ────────────────────────────────────────
export const datasetsQuery = defineQueryOptions((scope: Scope) => ({
  key: ['datasets', scope],
  query: () => listDatasets({ baseUrl: base(scope) }).then(unwrap),
  staleTime: 30_000,
}))

export const snapshotsQuery = defineQueryOptions(
  ({ scope, dataset }: { scope: Scope; dataset: string }) => ({
    key: ['snapshots', scope, dataset],
    query: () => listSnapshots({ path: { name: dataset }, baseUrl: base(scope) }).then(unwrap),
    staleTime: 15_000,
    enabled: !!dataset,
  }),
)

/**
 * Every hold on the dataset in one request. The per-snapshot endpoint
 * still exists, but asking it once per row turned a refresh into
 * hundreds of `zfs holds` spawns.
 */
export const datasetHoldsQuery = defineQueryOptions(
  ({ scope, dataset }: { scope: Scope; dataset: string }) => ({
    key: ['dataset-holds', scope, dataset],
    query: () => listDatasetHolds({ path: { name: dataset }, baseUrl: base(scope) }).then(unwrap),
    staleTime: 15_000,
    enabled: !!dataset,
  }),
)

// ── Misc ────────────────────────────────────────────────────────
export const configQuery = defineQueryOptions((scope: Scope) => ({
  key: ['config', scope],
  query: () => getConfig({ baseUrl: base(scope) }).then(unwrap),
  staleTime: 60_000,
}))

// Static per daemon: fetched once per host, never revalidated.
export const systemInfoQuery = defineQueryOptions((scope: Scope) => ({
  key: ['system-info', scope],
  query: () => getSystemInfo({ baseUrl: base(scope) }).then(unwrap),
  staleTime: Infinity,
}))

export const recentTransfersQuery = defineQueryOptions(
  ({ scope, limit = 20 }: { scope: Scope; limit?: number }) => ({
    key: ['recent-transfers', scope, limit],
    query: () => recentTransfers({ query: { limit }, baseUrl: base(scope) }).then(unwrap),
    staleTime: 10_000,
  }),
)
