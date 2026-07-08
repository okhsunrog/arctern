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
import { apiErrorMessage, useMutation } from './useMutation'

export function useJobs(refreshMs = 5000, baseUrl = '') {
  const jobs = ref<JobStatus[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)
  const { mutate } = useMutation()

  async function refresh() {
    const r = await listJobs({ baseUrl })
    if (r.error) {
      error.value = apiErrorMessage(r.error)
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
    await mutate(`Woke up ${name}`, () => wakeup({ path: { name }, baseUrl }))
    void refresh()
  }

  async function cancel(name: string) {
    await mutate(`Cancelled ${name}`, () => cancelJob({ path: { name }, baseUrl }), {
      successDescription: 'Partial recv state on the receiver keeps the transfer resumable.',
    })
    void refresh()
  }

  async function pause(name: string) {
    await mutate(`Paused ${name}`, () => pauseJob({ path: { name }, baseUrl }))
    void refresh()
  }

  async function resume(name: string) {
    await mutate(`Resumed ${name}`, () => resumeJob({ path: { name }, baseUrl }))
    void refresh()
  }

  async function pushTo(name: string, peer: string) {
    await mutate(`Queued push to ${peer}`, () => pushToPeer({ path: { name, peer }, baseUrl }), {
      successDescription: `${name} will replicate to ${peer} within seconds.`,
    })
    void refresh()
  }

  return { jobs, error, loading, refresh, wake, cancel, pause, resume, pushTo }
}
