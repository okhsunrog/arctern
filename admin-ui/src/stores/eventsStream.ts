// The event log is a capped append-only tail, not a cached resource:
// it has no freshness to revalidate and no key to invalidate, so it
// stays a plain store rather than going through the query cache.
// Ref-counted per host scope, like the job stream.

import { defineStore } from 'pinia'
import { markRaw, reactive, ref } from 'vue'
import type { LogEvent } from '../client'
import { createReconnectingEventSource } from '../composables/reconnectingEventSource'

// One retention limit for the shared buffer. Consumers that want less
// (the dashboard shows a 50-line tail) slice what they render — they no
// longer get to shrink the buffer the events view depends on.
const CAP = 5000

/**
 * `since` resumes the replay after an event the client already holds.
 *
 * The browser sends `Last-Event-ID` only when it retries an EventSource
 * on its own; we also reconnect deliberately — on tab wake and on
 * `online` — by constructing a fresh one, and that carries no header. So
 * the cursor travels in the URL, which covers both paths. Without it,
 * every wake replayed the last 100 lines into a log that already had
 * them.
 */
export function eventsStreamPath(scope: string, since?: number): string {
  const base = scope ? `/api/v1/peers/${encodeURIComponent(scope)}/events` : '/api/v1/events'
  return since ? `${base}?since=${since}` : base
}

interface Entry {
  subscribers: number
  connection: ReturnType<typeof createReconnectingEventSource>
}

/** Id of the newest event held, or undefined for an empty buffer. */
function newestId(list: LogEvent[] | undefined): number | undefined {
  return list && list.length > 0 ? list[list.length - 1]!.id : undefined
}

export const useEventsStream = defineStore('events-stream', () => {
  const entries = new Map<string, Entry>()
  /** Scope -> events, exposed reactively; markRaw'd rows stay cheap. */
  const buffers = reactive<Record<string, LogEvent[]>>({})
  const connected = reactive<Record<string, boolean>>({})
  /** Global pause: stop appending, keep the connection open. */
  const paused = ref(false)

  function open(scope: string): Entry {
    connected[scope] = false
    buffers[scope] = []
    const entry: Entry = {
      subscribers: 0,
      connection: createReconnectingEventSource({
        // Read at connect time, so a reconnect resumes where this
        // buffer ends rather than replaying what it already shows.
        url: () => eventsStreamPath(scope, newestId(buffers[scope])),
        subscribe(source) {
          source.addEventListener('message', (e) => {
            if (paused.value) return
            let parsed: LogEvent
            try {
              parsed = JSON.parse(e.data) as LogEvent
            } catch {
              return
            }
            const list = buffers[scope]
            if (!list) return
            // The server resumes from the cursor, but a peer stream
            // bridges a separate backlog and a paused client's cursor
            // goes stale, so drop anything not newer than the tail.
            const newest = newestId(list)
            if (newest !== undefined && parsed.id <= newest) return
            list.push(markRaw(parsed))
            if (list.length > CAP) list.splice(0, list.length - CAP)
          })
        },
        onOpen() {
          connected[scope] = true
        },
        onDisconnect() {
          connected[scope] = false
        },
      }),
    }
    return entry
  }

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
        delete buffers[scope]
        delete connected[scope]
      }
    }
  }

  function clear(scope: string) {
    const list = buffers[scope]
    if (list) list.length = 0
  }

  function togglePause() {
    paused.value = !paused.value
  }

  return { buffers, connected, paused, subscribe, clear, togglePause }
})
