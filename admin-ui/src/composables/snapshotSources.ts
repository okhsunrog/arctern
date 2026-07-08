// One data-source interface behind the snapshot browser, so browsing
// the local pool and browsing a peer are the SAME interaction — the
// only differences are which endpoints answer and which capabilities
// (create, hold management) the source exposes.

import {
  createHold,
  createSnapshot,
  destroyPeerSnapshot,
  destroySnapshot,
  listDatasets,
  listHolds,
  listPeerDatasets,
  listPeerSnapshotHolds,
  listPeerSnapshots,
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
  /** ZFS GUID as string (peer sources; local zfs list omits it here). */
  guid?: string
  /** Full property map (local source only). */
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
  /** Optional capabilities — absent means the UI hides the control. */
  create?(dataset: string, name: string, recursive: boolean): Promise<CallResult>
  holdCreate?(dataset: string, tag: string, holdTag: string): Promise<CallResult>
  holdRelease?(dataset: string, tag: string, holdTag: string): Promise<CallResult>
}

function tagOf(full: string): string {
  const at = full.indexOf('@')
  return at >= 0 ? full.slice(at + 1) : full
}

export function localSource(): SnapshotSource {
  return {
    hostLabel: '',
    listDatasets: () => listDatasets(),
    async listSnapshots(dataset) {
      const r = await listSnapshots({ path: { name: dataset } })
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
      const r = await listHolds({ path: { name: dataset, snapshot: tag } })
      return r.error ? { error: r.error } : { data: r.data ?? [] }
    },
    destroy: (dataset, tag) => destroySnapshot({ path: { name: dataset, snapshot: tag } }),
    create: (dataset, name, recursive) =>
      createSnapshot({ path: { name: dataset }, body: { snapshot_name: name, recursive } }),
    holdCreate: (dataset, tag, holdTag) =>
      createHold({ path: { name: dataset, snapshot: tag }, body: { tag: holdTag } }),
    holdRelease: (dataset, tag, holdTag) =>
      releaseHold({ path: { name: dataset, snapshot: tag, tag: holdTag } }),
  }
}

export function peerSource(peer: string): SnapshotSource {
  return {
    hostLabel: peer,
    listDatasets: () => listPeerDatasets({ path: { peer } }),
    async listSnapshots(dataset) {
      const r = await listPeerSnapshots({ path: { peer }, query: { dataset } })
      if (r.error) return { error: r.error }
      return {
        data: (r.data ?? []).map((s) => ({
          tag: s.name,
          creation: s.creation ?? null,
          used: s.used ?? null,
          guid: s.guid,
        })),
      }
    },
    async listHolds(dataset, tag) {
      const r = await listPeerSnapshotHolds({ path: { peer, name: dataset, snapshot: tag } })
      return r.error ? { error: r.error } : { data: r.data ?? [] }
    },
    destroy: (dataset, tag) => destroyPeerSnapshot({ path: { peer, name: `${dataset}@${tag}` } }),
    // No create / hold management on a peer: the receiver's snapshots
    // are the sender's replicas, and the ACL surface stays minimal.
  }
}
