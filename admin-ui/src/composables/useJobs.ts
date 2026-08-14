import { onUnmounted, ref, watch, type Ref } from 'vue'
import {
  cancel as cancelJob,
  pause as pauseJob,
  pushToPeer,
  resume as resumeJob,
  wakeup,
} from '../client'
import type { JobStatus } from '../client'
import { useMutation } from './useMutation'
import { createReconnectingEventSource } from './reconnectingEventSource'

export function jobsStreamPath(baseUrl: string): string {
  const peer = /^\/api\/v1\/peers\/([^/]+)\/proxy\/?$/.exec(baseUrl)
  return peer ? `/api/v1/peers/${peer[1]}/jobs/stream` : '/api/v1/jobs/stream'
}

export function useJobs(baseUrl: string | Ref<string> = '') {
  const jobs = ref<JobStatus[]>([])
  const error = ref<string | null>(null)
  const warning = ref<string | null>(null)
  const loading = ref(true)
  const { mutate } = useMutation()

  function currentBaseUrl(): string {
    return typeof baseUrl === 'object' ? baseUrl.value : baseUrl
  }

  const connection = createReconnectingEventSource({
    url: () => jobsStreamPath(currentBaseUrl()),
    subscribe(stream) {
      loading.value = jobs.value.length === 0
      stream.addEventListener('jobs', (event) => {
        try {
          jobs.value = JSON.parse(event.data) as JobStatus[]
          error.value = null
          warning.value = null
          loading.value = false
        } catch {
          error.value = 'invalid job status update'
        }
      })
    },
    onOpen() {
      warning.value = null
    },
    onDisconnect() {
      warning.value = 'Live job updates interrupted. Reconnecting…'
    },
  })

  if (typeof baseUrl === 'object') {
    watch(baseUrl, connection.restart)
  }
  onUnmounted(() => connection.close())

  async function wake(name: string) {
    await mutate(`Woke up ${name}`, () => wakeup({ path: { name }, baseUrl: currentBaseUrl() }))
  }

  async function cancel(name: string) {
    await mutate(
      `Stopping ${name}`,
      () => cancelJob({ path: { name }, baseUrl: currentBaseUrl() }),
      {
        successDescription: 'Waiting for the receiver to release the dataset safely.',
      },
    )
  }

  async function pause(name: string) {
    await mutate(`Paused ${name}`, () => pauseJob({ path: { name }, baseUrl: currentBaseUrl() }))
  }

  async function resume(name: string) {
    await mutate(`Resumed ${name}`, () => resumeJob({ path: { name }, baseUrl: currentBaseUrl() }))
  }

  async function pushTo(name: string, peer: string) {
    await mutate(
      `Queued push to ${peer}`,
      () => pushToPeer({ path: { name, peer }, baseUrl: currentBaseUrl() }),
      {
        successDescription: `${name} will replicate to ${peer} within seconds.`,
      },
    )
  }

  return { jobs, error, warning, loading, wake, cancel, pause, resume, pushTo }
}
