import { onUnmounted, ref } from 'vue'
import { listRuns } from '../client'
import type { JobRun } from '../client'

export function useJobRuns(name: string, refreshMs = 10_000, limit = 100, baseUrl = '') {
  const runs = ref<JobRun[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const r = await listRuns({ path: { name }, query: { limit }, baseUrl })
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

  void refresh()
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  return { runs, error, loading, refresh }
}
