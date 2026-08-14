import { onUnmounted, ref, watch, type Ref } from 'vue'
import type { LogEvent } from '../client'
import { createReconnectingEventSource } from './reconnectingEventSource'

export interface UseEventsOptions {
  /** Static peer name, or a ref — the stream re-points when it changes. */
  peer?: string | Ref<string | undefined>
  /** Cap on retained events; oldest dropped when exceeded. Default 5000. */
  cap?: number
}

export function useEvents(options: UseEventsOptions = {}) {
  const cap = options.cap ?? 5000
  const events = ref<LogEvent[]>([])
  const connected = ref(false)
  const error = ref<string | null>(null)
  const paused = ref(false)

  function path() {
    const peer =
      options.peer && typeof options.peer === 'object' ? options.peer.value : options.peer
    return peer ? `/api/v1/peers/${encodeURIComponent(peer)}/events` : '/api/v1/events'
  }

  const connection = createReconnectingEventSource({
    url: path,
    subscribe(es) {
      es.addEventListener('message', (e) => {
        if (paused.value) return
        try {
          const ev = JSON.parse(e.data) as LogEvent
          events.value.push(ev)
          if (events.value.length > cap) {
            events.value.splice(0, events.value.length - cap)
          }
        } catch {
          // ignore malformed payloads
        }
      })
    },
    onOpen() {
      connected.value = true
      error.value = null
    },
    onDisconnect() {
      connected.value = false
      error.value = 'Live event updates interrupted. Reconnecting…'
    },
  })

  function switchPeer() {
    connected.value = false
    events.value = []
    connection.restart()
  }

  if (options.peer && typeof options.peer === 'object') {
    // Reactive peer: (re)open whenever it changes — the old stream
    // would otherwise keep feeding the previous peer's events into a
    // view that now shows another peer.
    watch(options.peer, switchPeer)
  }

  onUnmounted(() => connection.close())

  function clear() {
    events.value = []
  }

  function togglePause() {
    paused.value = !paused.value
  }

  return { events, connected, error, paused, clear, togglePause }
}
