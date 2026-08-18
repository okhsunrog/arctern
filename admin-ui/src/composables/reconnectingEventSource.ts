const OPEN = 1
const RECONNECT_GRACE_MS = 5_000

export interface ReconnectingEventSourceOptions {
  url: () => string
  subscribe: (source: EventSource) => void
  onOpen: () => void
  onDisconnect: () => void
  factory?: (url: string) => EventSource
  document?: Pick<Document, 'visibilityState' | 'addEventListener' | 'removeEventListener'>
  window?: Pick<Window, 'addEventListener' | 'removeEventListener'>
  reconnectGraceMs?: number
}

/**
 * Keep an EventSource alive across browser tab suspension and network changes.
 * Native EventSource retry remains the fast path; if it stays stuck in CONNECTING,
 * replace it with a fresh connection after a short grace period.
 */
export function createReconnectingEventSource(options: ReconnectingEventSourceOptions) {
  const factory = options.factory ?? ((url: string) => new EventSource(url))
  const page = options.document ?? (typeof document === 'undefined' ? undefined : document)
  const browserWindow = options.window ?? (typeof window === 'undefined' ? undefined : window)
  const reconnectGraceMs = options.reconnectGraceMs ?? RECONNECT_GRACE_MS

  let source: EventSource | null = null
  let retry: ReturnType<typeof setTimeout> | null = null
  let closed = false

  function clearRetry() {
    if (retry !== null) {
      clearTimeout(retry)
      retry = null
    }
  }

  function connect() {
    if (closed) return
    clearRetry()
    source?.close()

    const candidate = factory(options.url())
    source = candidate
    options.subscribe(candidate)

    candidate.addEventListener('open', () => {
      if (source !== candidate) return
      clearRetry()
      options.onOpen()
    })
    candidate.addEventListener('error', () => {
      if (source !== candidate) return
      options.onDisconnect()
      clearRetry()
      retry = setTimeout(() => {
        retry = null
        if (source === candidate && candidate.readyState !== OPEN) connect()
      }, reconnectGraceMs)
    })
  }

  // Force a fresh connection rather than trusting readyState. After a tab
  // suspension or a network change the socket can be half-open: the browser
  // still reports OPEN and fires no 'error', so a readyState check would
  // leave a silently-dead stream in place — the exact failure this module
  // exists to prevent. Keep-alive comments aren't visible at the JS layer,
  // so there's no cheaper liveness signal to gate on. The reconnect is cheap:
  // the consumer re-pulls its backlog and dedups by id.
  function forceReconnect() {
    connect()
  }

  function onVisibilityChange() {
    if (page?.visibilityState === 'visible') forceReconnect()
  }

  page?.addEventListener('visibilitychange', onVisibilityChange)
  browserWindow?.addEventListener('online', forceReconnect)
  connect()

  return {
    restart: connect,
    close() {
      closed = true
      clearRetry()
      source?.close()
      source = null
      page?.removeEventListener('visibilitychange', onVisibilityChange)
      browserWindow?.removeEventListener('online', forceReconnect)
    },
  }
}
