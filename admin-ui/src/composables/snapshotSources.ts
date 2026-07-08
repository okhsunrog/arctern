// One data source behind the snapshot browser. Host scoping happens at
// the transport level (`baseUrl` routes through the peer proxy), so a
// peer exposes the SAME endpoints and the SAME capabilities as the
// local host — no parallel lesser implementation for peers.

import {
  createHold,
  createSnapshot,
  destroySnapshot,
  listDatasets,
  listHolds,
  listSnapshots,
  releaseHold,
} from '../client'
import type { DatasetSummary, SnapshotHold } from '../client'

export interface SnapshotRow {
  /** Tag — the part after `@`. */
  tag: string
  /** Unix seconds, when known. */
  creation: number | null
  /** Bytes, when known. */
  used: number | null
  /** ZFS GUID as string, when known. */
  guid?: string
  /** Full property map. */
  properties?: Record<string, string>
}

export interface CallResult {
  error?: unknown
}

export interface SnapshotSource {
  /** Where actions land, for toast titles: '' = this host. */
  hostLabel: string
  listDatasets(): Promise<{ data?: DatasetSummary[]; error?: unknown }>
  listSnapshots(dataset: string): Promise<{ data?: SnapshotRow[]; error?: unknown }>
  listHolds(dataset: string, tag: string): Promise<{ data?: SnapshotHold[]; error?: unknown }>
  destroy(dataset: string, tag: string): Promise<CallResult>
  create?(dataset: string, name: string, recursive: boolean): Promise<CallResult>
  holdCreate?(dataset: string, tag: string, holdTag: string): Promise<CallResult>
  holdRelease?(dataset: string, tag: string, holdTag: string): Promise<CallResult>
}

function tagOf(full: string): string {
  const at = full.indexOf('@')
  return at >= 0 ? full.slice(at + 1) : full
}

export function hostSource(baseUrl = '', hostLabel = ''): SnapshotSource {
  return {
    hostLabel,
    listDatasets: () => listDatasets({ baseUrl }),
    async listSnapshots(dataset) {
      const r = await listSnapshots({ path: { name: dataset }, baseUrl })
      if (r.error) return { error: r.error }
      return {
        data: (r.data ?? []).map((s) => ({
          tag: tagOf(s.name),
          creation: Number(s.properties?.creation ?? 0) || null,
          used: s.properties?.used != null ? Number(s.properties.used) : null,
          properties: s.properties,
        })),
      }
    },
    async listHolds(dataset, tag) {
      const r = await listHolds({ path: { name: dataset, snapshot: tag }, baseUrl })
      return r.error ? { error: r.error } : { data: r.data ?? [] }
    },
    destroy: (dataset, tag) => destroySnapshot({ path: { name: dataset, snapshot: tag }, baseUrl }),
    create: (dataset, name, recursive) =>
      createSnapshot({
        path: { name: dataset },
        body: { snapshot_name: name, recursive },
        baseUrl,
      }),
    holdCreate: (dataset, tag, holdTag) =>
      createHold({ path: { name: dataset, snapshot: tag }, body: { tag: holdTag }, baseUrl }),
    holdRelease: (dataset, tag, holdTag) =>
      releaseHold({ path: { name: dataset, snapshot: tag, tag: holdTag }, baseUrl }),
  }
}
