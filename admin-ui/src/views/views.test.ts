// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from 'vite-plus/test'
import { shallowMount, flushPromises } from '@vue/test-utils'
import { createPinia } from 'pinia'
import { PiniaColada } from '@pinia/colada'
import { createMemoryHistory, createRouter } from 'vue-router'
import DashboardView from './DashboardView.vue'
import JobsView from './JobsView.vue'
import JobDetailView from './JobDetailView.vue'
import SnapshotsView from './SnapshotsView.vue'
import PoolsView from './PoolsView.vue'
import PoolDetailView from './PoolDetailView.vue'
import ArcView from './ArcView.vue'
import EventsView from './EventsView.vue'
import ConfigView from './ConfigView.vue'
import PeersView from './PeersView.vue'
import { listJobs } from '../client'

vi.mock('../composables/useToaster', () => ({
  useToaster: () => ({ report: vi.fn(), success: vi.fn(), failure: vi.fn() }),
}))
vi.mock('../client', async (original) => ({
  ...(await original<typeof import('../client')>()),
  listJobs: vi.fn(async () => ({ data: [] })),
  listRuns: vi.fn(async () => ({ data: [] })),
  listPools: vi.fn(async () => ({ data: [] })),
  listPeers: vi.fn(async () => ({ data: [] })),
  getPool: vi.fn(async () => ({ data: null })),
  getArc: vi.fn(async () => ({ data: null })),
  getArcHistory: vi.fn(async () => ({ data: [] })),
  getConfig: vi.fn(async () => ({
    data: { path: '/test/arctern.toml', content_toml: 'jobs = []' },
  })),
  recentTransfers: vi.fn(async () => ({ data: [] })),
}))

class SilentEventSource extends EventTarget {
  close() {}
}
const wrappers: ReturnType<typeof shallowMount>[] = []
afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount())
  vi.unstubAllGlobals()
})
const views = [
  DashboardView,
  JobsView,
  JobDetailView,
  SnapshotsView,
  PoolsView,
  PoolDetailView,
  ArcView,
  EventsView,
  ConfigView,
  PeersView,
]

async function mountPage(component: (typeof views)[number], prefix = '') {
  vi.stubGlobal('EventSource', SilentEventSource)
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/:name', component },
      { path: '/h/:host/:name', component },
    ],
  })
  await router.push(`${prefix}/backup`)
  const wrapper = shallowMount(component, {
    global: {
      plugins: [router, createPinia(), [PiniaColada, {}]],
      stubs: {
        DashboardPanel: { template: '<main><slot name="header"/><slot name="body"/></main>' },
      },
    },
  })
  wrappers.push(wrapper)
  return wrapper
}

describe.each(['', '/h/mira'])('page startup in scope %j', (prefix) => {
  it.each(views)('mounts $__name', async (component) => {
    const wrapper = await mountPage(component, prefix)
    await flushPromises()
    expect(wrapper.find('main').exists()).toBe(true)
  })
})

it('distinguishes a job still loading from an unavailable job list', async () => {
  let reject!: (reason: Error) => void
  vi.mocked(listJobs).mockImplementationOnce(
    () =>
      new Promise<never>((_resolve, fail) => {
        reject = fail
      }),
  )
  const wrapper = await mountPage(JobDetailView)
  expect(wrapper.text()).toContain('Loading job…')
  expect(wrapper.html()).not.toContain('Job not found')
  reject(new Error('Peer unavailable'))
  await flushPromises()
  expect(wrapper.html()).toContain('Peer unavailable')
  expect(wrapper.html()).not.toContain('Job not found')
  expect(wrapper.text()).not.toContain('Loading job…')
})

it('shows not found only after loading an empty job list', async () => {
  const wrapper = await mountPage(JobDetailView)
  await flushPromises()
  expect(wrapper.html()).toContain('Job not found')
})
