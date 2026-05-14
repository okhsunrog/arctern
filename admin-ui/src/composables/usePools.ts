import { onUnmounted, ref } from 'vue'
import { getPool, listPools, poolScrub } from '../client'
import type { PoolStatus, PoolSummary, ScrubRequest } from '../client'

function errMessage(e: unknown): string {
  if (e && typeof e === 'object' && 'message' in e) {
    return String((e as { message: unknown }).message)
  }
  return String(e)
}

export function usePools(refreshMs = 5000) {
  const pools = ref<PoolSummary[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const r = await listPools()
    if (r.error) error.value = errMessage(r.error)
    else {
      pools.value = r.data ?? []
      error.value = null
    }
    loading.value = false
  }

  void refresh()
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  return { pools, error, loading, refresh }
}

export function usePool(name: string, refreshMs = 3000) {
  const pool = ref<PoolStatus | null>(null)
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const r = await getPool({ path: { name } })
    if (r.error) error.value = errMessage(r.error)
    else {
      pool.value = r.data ?? null
      error.value = null
    }
    loading.value = false
  }

  async function scrub(action: ScrubRequest['action']) {
    const r = await poolScrub({ path: { name }, body: { action } })
    if (r.error) error.value = errMessage(r.error)
    await refresh()
  }

  void refresh()
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  return { pool, error, loading, refresh, scrub }
}
