// Live job state arrives over SSE, not by polling: the daemon re-renders
// its status snapshot every 250ms and emits a frame only when it differs.
// The frame IS the full `JobStatus[]`, so it is written straight into the
// query cache — every consumer (sidebar badges, dashboard grid, job
// detail) then reads one `useQuery(jobsQuery(scope))` and there is
// exactly one connection per host instead of one per component.

import { defineStore } from 'pinia'
import { useQueryCache } from '@pinia/colada'
import { reactive } from 'vue'
import type { JobStatus } from '../client'
import { jobsQuery } from '../queries'
import { createReconnectingEventSource } from '../composables/reconnectingEventSource'

export type StreamStatus = 'connecting' | 'live' | 'down'

/** Peer job streams have their own route; they are not proxied. */
export function jobsStreamPath(scope: string): string {
  return scope ? `/api/v1/peers/${encodeURIComponent(scope)}/jobs/stream` : '/api/v1/jobs/stream'
}

interface Entry {
  subscribers: number
  connection: ReturnType<typeof createReconnectingEventSource>
}

export const useJobsStream = defineStore('jobs-stream', () => {
  const queryCache = useQueryCache()
  const entries = new Map<string, Entry>()
  /** Per-scope connection health, for the "live / reconnecting" chrome. */
  const status = reactive<Record<string, StreamStatus>>({})

  function open(scope: string): Entry {
    status[scope] = 'connecting'
    const connection = createReconnectingEventSource({
      url: () => jobsStreamPath(scope),
      subscribe(source) {
        source.addEventListener('jobs', (event) => {
          let parsed: JobStatus[]
          try {
            parsed = JSON.parse(event.data) as JobStatus[]
          } catch {
            // A malformed frame is not a reason to drop good cached data.
            return
          }
          // `ensure()` registers the entry against its query options first.
          // Writing with a bare `setQueryData` would leave the entry
          // detached and immediately stale, so the auto-refetch plugin
          // would start issuing HTTP fetches on top of a healthy stream.
          const entry = queryCache.ensure(jobsQuery(scope))
          queryCache.cancel(entry)
          queryCache.setEntryState(entry, {
            data: parsed,
            error: null,
            status: 'success',
          })
        })
      },
      onOpen() {
        status[scope] = 'live'
      },
      onDisconnect() {
        status[scope] = 'down'
      },
    })
    return { subscribers: 0, connection }
  }

  /**
   * Hold a connection open for `scope`. Returns the release function; the
   * stream closes when the last holder releases it, so navigating from
   * the local console into a peer's does not leave the previous host's
   * stream feeding the cache forever.
   */
  function subscribe(scope: string): () => void {
    let entry = entries.get(scope)
    if (!entry) {
      entry = open(scope)
      entries.set(scope, entry)
    }
    entry.subscribers += 1
    let released = false
    return () => {
      if (released) return
      released = true
      const current = entries.get(scope)
      if (!current) return
      current.subscribers -= 1
      if (current.subscribers <= 0) {
        current.connection.close()
        entries.delete(scope)
        delete status[scope]
      }
    }
  }

  return { status, subscribe }
})
