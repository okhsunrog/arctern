import { onUnmounted, ref } from 'vue'
import type { LogEvent } from '../client'

export interface UseEventsOptions {
  peer?: string
  /** Cap on retained events; oldest dropped when exceeded. Default 5000. */
  cap?: number
}

export function useEvents(options: UseEventsOptions = {}) {
  const cap = options.cap ?? 5000
  const events = ref<LogEvent[]>([])
  const connected = ref(false)
  const error = ref<string | null>(null)
  const paused = ref(false)

  const path = options.peer
    ? `/api/v1/peers/${encodeURIComponent(options.peer)}/events`
    : '/api/v1/events'

  const es = new EventSource(path)

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

  onUnmounted(() => es.close())

  function clear() {
    events.value = []
  }

  function togglePause() {
    paused.value = !paused.value
  }

  return { events, connected, error, paused, clear, togglePause }
}
