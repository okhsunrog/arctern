import { onUnmounted, ref } from 'vue'
import { listJobs, wakeup } from '../client'
import type { JobStatus } from '../client'

export function useJobs(refreshMs = 5000) {
  const jobs = ref<JobStatus[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const r = await listJobs()
    if (r.error) {
      error.value = errMessage(r.error)
    } else {
      jobs.value = r.data ?? []
      error.value = null
    }
    loading.value = false
  }

  void refresh()
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  async function wake(name: string) {
    const r = await wakeup({ path: { name } })
    if (r.error) error.value = errMessage(r.error)
    void refresh()
  }

  function errMessage(e: unknown): string {
    if (e && typeof e === 'object' && 'message' in e) {
      return String((e as { message: unknown }).message)
    }
    return String(e)
  }

  return { jobs, error, loading, refresh, wake }
}
