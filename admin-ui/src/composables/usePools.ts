import { onUnmounted, ref, toValue, watch, type MaybeRefOrGetter } from 'vue'
import { getPool, listPools, poolScrub } from '../client'
import type { PoolStatus, PoolSummary, ScrubRequest } from '../client'

function errMessage(e: unknown): string {
  if (e && typeof e === 'object' && 'message' in e) {
    return String((e as { message: unknown }).message)
  }
  return String(e)
}

export function usePools(refreshMs = 5000, baseUrl: MaybeRefOrGetter<string> = '') {
  const pools = ref<PoolSummary[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const requestedBaseUrl = toValue(baseUrl)
    const r = await listPools({ baseUrl: requestedBaseUrl })
    if (requestedBaseUrl !== toValue(baseUrl)) return
    if (r.error) error.value = errMessage(r.error)
    else {
      pools.value = r.data ?? []
      error.value = null
    }
    loading.value = false
  }

  watch(
    () => toValue(baseUrl),
    () => void refresh(),
    { immediate: true },
  )
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  return { pools, error, loading, refresh }
}

export function usePool(
  name: MaybeRefOrGetter<string>,
  refreshMs = 3000,
  baseUrl: MaybeRefOrGetter<string> = '',
) {
  const pool = ref<PoolStatus | null>(null)
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const requestedName = toValue(name)
    const requestedBaseUrl = toValue(baseUrl)
    const r = await getPool({ path: { name: requestedName }, baseUrl: requestedBaseUrl })
    if (requestedName !== toValue(name) || requestedBaseUrl !== toValue(baseUrl)) return
    if (r.error) error.value = errMessage(r.error)
    else {
      pool.value = r.data ?? null
      error.value = null
    }
    loading.value = false
  }

  /// Returns the raw call result so callers can toast the outcome.
  async function scrub(action: ScrubRequest['action']): Promise<{ error?: unknown }> {
    const r = await poolScrub({
      path: { name: toValue(name) },
      body: { action },
      baseUrl: toValue(baseUrl),
    })
    if (r.error) error.value = errMessage(r.error)
    await refresh()
    return { error: r.error }
  }

  watch([() => toValue(name), () => toValue(baseUrl)], () => void refresh(), { immediate: true })
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  return { pool, error, loading, refresh, scrub }
}
