import { beforeEach, describe, expect, it, vi } from 'vite-plus/test'
import { createApp, effectScope } from 'vue'
import { useEvents } from '../composables/useEvents'
import { createPinia, setActivePinia } from 'pinia'
import type { LogEvent } from '../client'
import { eventsStreamPath, useEventsStream } from './eventsStream'

class FakeEventSource extends EventTarget {
  static instances: FakeEventSource[] = []
  closed = false

  constructor(readonly url: string) {
    super()
    FakeEventSource.instances.push(this)
  }

  close() {
    this.closed = true
  }

  push(event: Partial<LogEvent> & { id: number }) {
    this.dispatchEvent(
      new MessageEvent('message', {
        data: JSON.stringify({
          timestamp: 0,
          level: 'INFO',
          job_name: null,
          message: `e${event.id}`,
          ...event,
        }),
      }),
    )
  }

  /** What the browser does when it loses the connection. */
  fail() {
    this.dispatchEvent(new Event('error'))
  }
}

function harness() {
  const pinia = createPinia()
  const app = createApp({ render: () => null })
  app.use(pinia)
  setActivePinia(pinia)
  return useEventsStream()
}

const latest = () => FakeEventSource.instances[FakeEventSource.instances.length - 1]!

beforeEach(() => {
  FakeEventSource.instances = []
  ;(globalThis as unknown as { EventSource: unknown }).EventSource = FakeEventSource
})

describe('eventsStreamPath', () => {
  it('uses the peer route for a scoped console', () => {
    expect(eventsStreamPath('')).toBe('/api/v1/events')
    expect(eventsStreamPath('mira')).toBe('/api/v1/peers/mira/events')
    expect(eventsStreamPath('mira backup')).toBe('/api/v1/peers/mira%20backup/events')
  })

  it('carries the resume cursor when there is one', () => {
    expect(eventsStreamPath('', 42)).toBe('/api/v1/events?since=42')
    expect(eventsStreamPath('mira', 42)).toBe('/api/v1/peers/mira/events?since=42')
  })

  // A fresh stream must ask for the backlog, and id 0 is not a cursor.
  it('asks for the full replay without one', () => {
    expect(eventsStreamPath('', undefined)).toBe('/api/v1/events')
    expect(eventsStreamPath('', 0)).toBe('/api/v1/events')
  })
})

describe('useEventsStream', () => {
  it('opens one connection per scope, shared by subscribers', () => {
    const store = harness()
    const a = store.subscribe('')
    const b = store.subscribe('')
    expect(FakeEventSource.instances).toHaveLength(1)

    a()
    expect(latest().closed).toBe(false)
    b()
    expect(latest().closed).toBe(true)
  })

  it('appends received events to its scope', () => {
    const store = harness()
    store.subscribe('')
    latest().push({ id: 1 })
    latest().push({ id: 2 })
    expect(store.buffers['']?.map((e) => e.id)).toEqual([1, 2])
  })

  // The reason the reconnect is worth resuming: the browser's own retry
  // and our deliberate ones both used to replay a backlog the log already
  // showed.
  it('names its newest event when reconnecting', () => {
    vi.useFakeTimers()
    try {
      const store = harness()
      store.subscribe('')
      expect(latest().url).toBe('/api/v1/events')

      latest().push({ id: 7 })
      latest().fail()
      // A stuck connection is replaced after the reconnect grace.
      vi.advanceTimersByTime(10_000)

      expect(FakeEventSource.instances.length).toBeGreaterThan(1)
      expect(latest().url).toBe('/api/v1/events?since=7')
    } finally {
      vi.useRealTimers()
    }
  })

  // A peer stream bridges a separate backlog, so the cursor is not
  // authoritative there and the tail has to reject duplicates itself.
  it('drops anything not newer than what it already holds', () => {
    const store = harness()
    store.subscribe('')
    latest().push({ id: 5 })
    latest().push({ id: 3 })
    latest().push({ id: 5 })
    latest().push({ id: 6 })
    expect(store.buffers['']?.map((e) => e.id)).toEqual([5, 6])
  })

  it('ignores a malformed frame without dropping the connection', () => {
    const store = harness()
    store.subscribe('')
    latest().push({ id: 1 })
    latest().dispatchEvent(new MessageEvent('message', { data: 'not json' }))
    latest().push({ id: 2 })
    expect(store.buffers['']?.map((e) => e.id)).toEqual([1, 2])
  })

  it('freezes the view while continuing to collect events for resume', () => {
    const store = harness()
    store.subscribe('')
    latest().push({ id: 1 })
    const scope = effectScope()
    const view = scope.run(() => useEvents(''))!
    view.togglePause()
    latest().push({ id: 2 })
    expect(view.events.value.map((e) => e.id)).toEqual([1])
    expect(store.buffers['']?.map((e) => e.id)).toEqual([1, 2])
    expect(latest().closed).toBe(false)

    view.togglePause()
    latest().push({ id: 3 })
    expect(view.events.value.map((e) => e.id)).toEqual([1, 2, 3])
    scope.stop()
  })

  it('keeps scopes apart', () => {
    const store = harness()
    store.subscribe('')
    const local = latest()
    store.subscribe('mira')
    const peer = latest()

    local.push({ id: 1 })
    peer.push({ id: 2 })
    expect(store.buffers['']?.map((e) => e.id)).toEqual([1])
    expect(store.buffers['mira']?.map((e) => e.id)).toEqual([2])
  })
})
