import { afterEach, describe, expect, it, vi } from 'vite-plus/test'
import { createReconnectingEventSource } from './reconnectingEventSource'

class FakeEventSource extends EventTarget {
  readyState = 0
  closed = false

  close() {
    this.closed = true
    this.readyState = 2
  }

  emit(type: string) {
    this.dispatchEvent(new Event(type))
  }
}

class FakeDocument extends EventTarget {
  visibilityState: DocumentVisibilityState = 'hidden'
}

afterEach(() => vi.useRealTimers())

describe('createReconnectingEventSource', () => {
  it('clears the disconnected state when native EventSource reconnects', () => {
    const sources: FakeEventSource[] = []
    const onOpen = vi.fn()
    const onDisconnect = vi.fn()
    const connection = createReconnectingEventSource({
      url: () => '/stream',
      subscribe: vi.fn(),
      onOpen,
      onDisconnect,
      factory: () => {
        const source = new FakeEventSource()
        sources.push(source)
        return source as unknown as EventSource
      },
    })

    const source = sources[0]
    if (!source) throw new Error('initial EventSource was not created')
    source.emit('error')
    source.readyState = 1
    source.emit('open')

    expect(onDisconnect).toHaveBeenCalledOnce()
    expect(onOpen).toHaveBeenCalledOnce()
    expect(sources).toHaveLength(1)
    connection.close()
  })

  it('replaces a source that remains stuck while reconnecting', () => {
    vi.useFakeTimers()
    const sources: FakeEventSource[] = []
    const connection = createReconnectingEventSource({
      url: () => '/stream',
      subscribe: vi.fn(),
      onOpen: vi.fn(),
      onDisconnect: vi.fn(),
      reconnectGraceMs: 100,
      factory: () => {
        const source = new FakeEventSource()
        sources.push(source)
        return source as unknown as EventSource
      },
    })

    const source = sources[0]
    if (!source) throw new Error('initial EventSource was not created')
    source.emit('error')
    vi.advanceTimersByTime(100)

    expect(source.closed).toBe(true)
    expect(sources).toHaveLength(2)
    connection.close()
  })

  it('reconnects immediately when a suspended tab becomes visible', () => {
    const sources: FakeEventSource[] = []
    const page = new FakeDocument()
    const connection = createReconnectingEventSource({
      url: () => '/stream',
      subscribe: vi.fn(),
      onOpen: vi.fn(),
      onDisconnect: vi.fn(),
      document: page,
      factory: () => {
        const source = new FakeEventSource()
        sources.push(source)
        return source as unknown as EventSource
      },
    })

    page.visibilityState = 'visible'
    page.dispatchEvent(new Event('visibilitychange'))

    expect(sources).toHaveLength(2)
    expect(sources[0]?.closed).toBe(true)
    connection.close()
  })
})
