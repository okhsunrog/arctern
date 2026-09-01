import { beforeEach, describe, expect, it, vi } from 'vite-plus/test'
import { createApp, effectScope, nextTick } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import { PiniaColada } from '@pinia/colada'

// The toaster reaches for Nuxt UI's injected toast provider, which needs a
// mounted <UApp>; the action copy it renders is tested in actions.test.ts.
vi.mock('./useToaster', () => ({
  useToaster: () => ({ report: vi.fn(), success: vi.fn(), failure: vi.fn() }),
}))

const deferred: { resolve: () => void }[] = []
function pending() {
  return new Promise<{ data: undefined; error: undefined; response: Response }>((resolve) => {
    deferred.push({
      resolve: () => resolve({ data: undefined, error: undefined, response: new Response() }),
    })
  })
}

vi.mock('../client', () => ({
  // The query behind useJobs; the stream normally fills this cache entry.
  listJobs: () => Promise.resolve({ data: [], error: undefined, response: new Response() }),
  wakeup: () => pending(),
  cancel: () => pending(),
  pause: () => pending(),
  resume: () => pending(),
  pushToPeer: () => pending(),
}))

class SilentEventSource extends EventTarget {
  close() {}
}

const { useJobs } = await import('./useJobs')

/** One app + pinia; `use()` builds a composable instance inside it. */
function harness() {
  const pinia = createPinia()
  const app = createApp({ render: () => null })
  app.use(pinia)
  app.use(PiniaColada, {})
  setActivePinia(pinia)
  return {
    use: <T>(fn: () => T): T => effectScope().run(fn) as T,
  }
}

function run<T>(fn: () => T): T {
  return harness().use(fn)
}

beforeEach(() => {
  deferred.length = 0
  ;(globalThis as unknown as { EventSource: unknown }).EventSource = SilentEventSource
})

describe('useJobs in-flight tracking', () => {
  it('marks only the job that was acted on as busy', async () => {
    const jobs = run(() => useJobs(''))
    expect(jobs.isWaking('snap_nova')).toBe(false)

    jobs.wake('snap_nova')
    await nextTick()
    expect(jobs.isWaking('snap_nova')).toBe(true)
    expect(jobs.isWaking('push_to_mira')).toBe(false)
    // Different action on the same job is a different key.
    expect(jobs.isPausing('snap_nova')).toBe(false)
  })

  // A mutation's own `variables` ref holds only the most recent call, so
  // this is the case that used to clear the first button while its
  // request was still running.
  it('keeps both buttons busy across concurrent calls', async () => {
    const jobs = run(() => useJobs(''))

    jobs.wake('job_a')
    await nextTick()
    jobs.wake('job_b')
    await nextTick()

    expect(jobs.isWaking('job_a')).toBe(true)
    expect(jobs.isWaking('job_b')).toBe(true)

    // First request settles: only its own button is released.
    deferred[0]!.resolve()
    await vi.waitFor(() => expect(jobs.isWaking('job_a')).toBe(false))
    expect(jobs.isWaking('job_b')).toBe(true)

    deferred[1]!.resolve()
    await vi.waitFor(() => expect(jobs.isWaking('job_b')).toBe(false))
  })

  // The shell and the view are separate `useJobs()` instances; a wake
  // fired from the command palette has to grey out the card's button.
  it('shares in-flight state across composable instances', async () => {
    const { use } = harness()
    const shell = use(() => useJobs(''))
    const view = use(() => useJobs(''))

    shell.wake('snap_nova')
    await nextTick()
    expect(view.isWaking('snap_nova')).toBe(true)

    deferred[0]!.resolve()
    await vi.waitFor(() => expect(view.isWaking('snap_nova')).toBe(false))
  })

  // The same job name exists on this daemon and on a peer's console.
  it('does not leak busy state between host scopes', async () => {
    const { use } = harness()
    const local = use(() => useJobs(''))
    const peer = use(() => useJobs('mira'))

    local.wake('push_to_mira')
    await nextTick()

    expect(local.isWaking('push_to_mira')).toBe(true)
    expect(peer.isWaking('push_to_mira')).toBe(false)
  })

  it('tracks a push per peer, not per job', async () => {
    const jobs = run(() => useJobs(''))

    jobs.pushTo('push_to_mira', 'mira')
    await nextTick()

    expect(jobs.isPushing('push_to_mira', 'mira')).toBe(true)
    expect(jobs.isPushing('push_to_mira', 'nova')).toBe(false)
  })
})
