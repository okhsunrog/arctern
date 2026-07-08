import { onUnmounted, ref, watch, type Ref } from 'vue'
import type { LogEvent } from '../client'

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

  let es: EventSource | null = null

  function open(peer: string | undefined) {
    es?.close()
    connected.value = false
    events.value = []
    const path = peer ? `/api/v1/peers/${encodeURIComponent(peer)}/events` : '/api/v1/events'
    es = new EventSource(path)
    es.addEventListener('open', () => {
      connected.value = true
      error.value = null
    })
    es.addEventListener('error', () => {
      connected.value = false
      error.value = 'event stream disconnected (auto-reconnecting)'
    })
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
  }

  if (options.peer && typeof options.peer === 'object') {
    // Reactive peer: (re)open whenever it changes — the old stream
    // would otherwise keep feeding the previous peer's events into a
    // view that now shows another peer.
    watch(options.peer, (p) => open(p), { immediate: true })
  } else {
    open(options.peer)
  }

  onUnmounted(() => es?.close())

  function clear() {
    events.value = []
  }

  function togglePause() {
    paused.value = !paused.value
  }

  return { events, connected, error, paused, clear, togglePause }
}
