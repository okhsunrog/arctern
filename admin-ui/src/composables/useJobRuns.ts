import { onUnmounted, ref } from 'vue'
import { listRuns } from '../client'
import type { JobRun } from '../client'

export function useJobRuns(name: string, refreshMs = 10_000, limit = 100) {
  const runs = ref<JobRun[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const r = await listRuns({ path: { name }, query: { limit } })
    if (r.error) {
      error.value =
        r.error && typeof r.error === 'object' && 'message' in r.error
          ? String((r.error as { message: unknown }).message)
          : String(r.error)
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
