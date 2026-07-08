import { onUnmounted, ref } from 'vue'
import {
  cancel as cancelJob,
  listJobs,
  pause as pauseJob,
  pushToPeer,
  resume as resumeJob,
  wakeup,
} from '../client'
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

  async function cancel(name: string) {
    const r = await cancelJob({ path: { name } })
    if (r.error) error.value = errMessage(r.error)
    void refresh()
  }

  async function pause(name: string) {
    const r = await pauseJob({ path: { name } })
    if (r.error) error.value = errMessage(r.error)
    void refresh()
  }

  async function resume(name: string) {
    const r = await resumeJob({ path: { name } })
    if (r.error) error.value = errMessage(r.error)
    void refresh()
  }

  async function pushTo(name: string, peer: string) {
    const r = await pushToPeer({ path: { name, peer } })
    if (r.error) error.value = errMessage(r.error)
    void refresh()
  }

  function errMessage(e: unknown): string {
    if (e && typeof e === 'object' && 'message' in e) {
      return String((e as { message: unknown }).message)
    }
    return String(e)
  }

  return { jobs, error, loading, refresh, wake, cancel, pause, resume, pushTo }
}
