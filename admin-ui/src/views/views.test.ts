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

describe.each(['', '/h/mira'])('page startup in scope %j', (prefix) => {
  it.each(views)('mounts $__name', async (component) => {
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
    await flushPromises()
    expect(wrapper.find('main').exists()).toBe(true)
  })
})
