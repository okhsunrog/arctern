// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from 'vite-plus/test'
import { shallowMount, flushPromises } from '@vue/test-utils'
import { createPinia } from 'pinia'
import { PiniaColada } from '@pinia/colada'
import SnapshotBrowser from './SnapshotBrowser.vue'
import { listDatasetHolds } from '../client'

vi.mock('../composables/useToaster', () => ({
  useToaster: () => ({ report: vi.fn(), success: vi.fn(), failure: vi.fn() }),
}))
vi.mock('../client', async (original) => ({
  ...(await original<typeof import('../client')>()),
  listDatasets: vi.fn(async () => ({ data: [] })),
  listSnapshots: vi.fn(async () => ({ data: [] })),
  listDatasetHolds: vi.fn(async () => ({ data: {} })),
}))

const wrappers: ReturnType<typeof shallowMount>[] = []
function mountBrowser(dataset = '') {
  const wrapper = shallowMount(SnapshotBrowser, {
    props: { scope: '', dataset },
    global: { plugins: [createPinia(), [PiniaColada, {}]] },
  })
  wrappers.push(wrapper)
  return wrapper
}
afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount())
  vi.clearAllMocks()
})

describe('SnapshotBrowser startup', () => {
  it.each(['', 'tank/data'])('opens with dataset %j', async (dataset) => {
    const wrapper = mountBrowser(dataset)
    await flushPromises()
    expect(wrapper.exists()).toBe(true)
  })

  it('reports unavailable holds instead of treating a failed request as an empty list', async () => {
    vi.mocked(listDatasetHolds).mockRejectedValueOnce(new Error('Peer unavailable'))
    const wrapper = mountBrowser('tank/data')
    await flushPromises()
    expect(wrapper.html()).toContain('Snapshot holds unavailable')
    expect(wrapper.html()).toContain('Peer unavailable')
    expect(wrapper.html()).not.toContain('destroy-eligible')
  })
})
