import { beforeEach, describe, expect, it } from 'vite-plus/test'
import { createApp } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import { PiniaColada, useQueryCache } from '@pinia/colada'
import type { JobStatus } from '../client'
import { jobsQuery } from '../queries'
import { useJobsStream } from './jobsStream'

// The daemon pushes whole `JobStatus[]` snapshots, so the stream writes
// straight into the query cache. That bridge is the riskiest piece of
// the data layer: if it opens a connection per consumer, or seeds the
// cache in a way that leaves the entry stale, the console quietly goes
// back to polling on top of a healthy stream.

class FakeEventSource extends EventTarget {
  static instances: FakeEventSource[] = []
  readyState = 1
  closed = false

  constructor(readonly url: string) {
    super()
    FakeEventSource.instances.push(this)
  }

  close() {
    this.closed = true
    this.readyState = 2
  }

  push(jobs: JobStatus[]) {
    this.dispatchEvent(new MessageEvent('jobs', { data: JSON.stringify(jobs) }))
  }
}

function harness() {
  const pinia = createPinia()
  const app = createApp({ render: () => null })
  app.use(pinia)
  app.use(PiniaColada, {})
  setActivePinia(pinia)
  return { pinia, app }
}

const JOB: JobStatus = {
  name: 'push_to_mira',
  kind: 'push',
  last_run: null,
  next_run: null,
  last_error: null,
  running: true,
  paused: false,
  cancellable: true,
  transfers: [],
  targets: [],
}

beforeEach(() => {
  FakeEventSource.instances = []
  // The store builds its connection with the global constructor.
  ;(globalThis as unknown as { EventSource: unknown }).EventSource = FakeEventSource
})

describe('useJobsStream', () => {
  it('opens exactly one connection however many consumers subscribe', () => {
    harness()
    const stream = useJobsStream()
    const a = stream.subscribe('')
    const b = stream.subscribe('')
    expect(FakeEventSource.instances).toHaveLength(1)
    a()
    // Still held by the second consumer.
    expect(FakeEventSource.instances[0]!.closed).toBe(false)
    b()
    expect(FakeEventSource.instances[0]!.closed).toBe(true)
  })

  it('opens a separate connection per host scope', () => {
    harness()
    const stream = useJobsStream()
    stream.subscribe('')
    stream.subscribe('mira')
    expect(FakeEventSource.instances.map((s) => s.url)).toEqual([
      '/api/v1/jobs/stream',
      '/api/v1/peers/mira/jobs/stream',
    ])
  })

  it('writes a received frame into the query cache under the scoped key', () => {
    harness()
    const cache = useQueryCache()
    const stream = useJobsStream()
    stream.subscribe('mira')
    FakeEventSource.instances[0]!.push([JOB])
    expect(cache.getQueryData(jobsQuery('mira').key)).toEqual([JOB])
    // The local scope is a different entry, and must stay untouched.
    expect(cache.getQueryData(jobsQuery('').key)).toBeUndefined()
  })

  it('seeds the entry as fresh so the stream does not trigger refetches', () => {
    harness()
    const cache = useQueryCache()
    const stream = useJobsStream()
    stream.subscribe('')
    FakeEventSource.instances[0]!.push([JOB])
    const entry = cache.ensure(jobsQuery(''))
    expect(entry.state.value.status).toBe('success')
    expect(entry.state.value.error).toBeNull()
  })

  it('keeps the last good data when a malformed frame arrives', () => {
    harness()
    const cache = useQueryCache()
    const stream = useJobsStream()
    stream.subscribe('')
    const source = FakeEventSource.instances[0]!
    source.push([JOB])
    source.dispatchEvent(new MessageEvent('jobs', { data: 'not json' }))
    expect(cache.getQueryData(jobsQuery('').key)).toEqual([JOB])
  })

  it('reports connection health per scope', () => {
    harness()
    const stream = useJobsStream()
    const release = stream.subscribe('')
    expect(stream.status['']).toBe('connecting')
    FakeEventSource.instances[0]!.dispatchEvent(new Event('open'))
    expect(stream.status['']).toBe('live')
    FakeEventSource.instances[0]!.dispatchEvent(new Event('error'))
    expect(stream.status['']).toBe('down')
    release()
    expect(stream.status['']).toBeUndefined()
  })
})
