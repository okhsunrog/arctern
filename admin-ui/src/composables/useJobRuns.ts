import { onUnmounted, ref, toValue, watch, type MaybeRefOrGetter } from 'vue'
import { listRuns } from '../client'
import type { JobRun } from '../client'

export function useJobRuns(
  name: MaybeRefOrGetter<string>,
  refreshMs = 10_000,
  limit = 100,
  baseUrl: MaybeRefOrGetter<string> = '',
) {
  const runs = ref<JobRun[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const requestedName = toValue(name)
    const requestedBaseUrl = toValue(baseUrl)
    const r = await listRuns({
      path: { name: requestedName },
      query: { limit },
      baseUrl: requestedBaseUrl,
    })
    if (requestedName !== toValue(name) || requestedBaseUrl !== toValue(baseUrl)) return
    if (r.error) {
      const e: unknown = r.error
      error.value =
        e && typeof e === 'object' && 'message' in e && typeof e.message === 'string'
          ? e.message
          : JSON.stringify(e)
    } else {
      runs.value = r.data ?? []
      error.value = null
    }
    loading.value = false
  }

  watch([() => toValue(name), () => toValue(baseUrl)], () => void refresh(), { immediate: true })
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  return { runs, error, loading, refresh }
}
