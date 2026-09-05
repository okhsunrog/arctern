import { computed, onScopeDispose, toValue, watch, type MaybeRefOrGetter } from 'vue'
import { useMutation, useQuery } from '@pinia/colada'
import {
  cancel as cancelJob,
  pause as pauseJob,
  pushToPeer,
  resume as resumeJob,
  wakeup,
} from '../client'
import type { JobStatus } from '../client'
import { baseUrlFor } from './useHost'
import { jobsQuery } from '../queries'
import { jobActionKey, useJobActions } from '../stores/jobActions'
import { useJobsStream } from '../stores/jobsStream'
import { useToaster } from './useToaster'
import { pushOutcome, wakeOutcome } from '../utils/actions'
import { unwrap } from '../utils/errors'

/**
 * Live job state for one host scope ('' = this daemon, otherwise a peer
 * name). The SSE stream owns the data and writes it into the query
 * cache, so calling this from the sidebar and from a view costs one
 * connection, not two.
 */
export function useJobs(scope: MaybeRefOrGetter<string> = '') {
  const stream = useJobsStream()
  const toaster = useToaster()

  // One subscription per consumer, moved with the scope and released
  // with the component. Subscribing writes to the store, so it belongs
  // in a watcher rather than inside the query's options getter — that
  // getter runs during dependency tracking, where a store mutation can
  // re-trigger the very effect that caused it.
  let release: (() => void) | null = null
  watch(
    () => toValue(scope),
    (next) => {
      release?.()
      release = stream.subscribe(next)
    },
    { immediate: true },
  )
  onScopeDispose(() => release?.())

  const query = useQuery(() => jobsQuery(toValue(scope)))

  const jobs = computed<JobStatus[]>(() => query.data.value ?? [])
  const loading = computed(() => query.isPending.value && jobs.value.length === 0)
  const error = computed(() => (query.error.value ? query.error.value.message : null))
  const warning = computed(() =>
    stream.status[toValue(scope)] === 'down' ? 'Live job updates interrupted. Reconnecting…' : null,
  )
  /** True while the stream is connected — drives the "live" chrome. */
  const live = computed(() => stream.status[toValue(scope)] === 'live')

  interface ActionTarget {
    scope: string
    name: string
    peer?: string
    job?: JobStatus
  }

  const actions = useJobActions()
  const target = (name: string, peer?: string): ActionTarget => ({
    scope: toValue(scope),
    name,
    peer,
    job: jobs.value.find((j) => j.name === name),
  })
  const baseUrl = (v: ActionTarget) => baseUrlFor(v.scope || null)
  const actionKey = (action: Parameters<typeof jobActionKey>[0], v: ActionTarget) =>
    jobActionKey(action, v.scope, v.name, v.peer)
  const busy = (action: Parameters<typeof jobActionKey>[0], name: string, peer?: string) =>
    actions.inFlight.has(actionKey(action, target(name, peer)))

  // Pin the host in the mutation variables: the shell survives scope changes.
  function tracking(action: Parameters<typeof jobActionKey>[0]) {
    return {
      onMutate: (v: ActionTarget) => {
        actions.inFlight.add(actionKey(action, v))
      },
      onSettled: (_data: unknown, _error: unknown, v: ActionTarget) => {
        actions.inFlight.delete(actionKey(action, v))
      },
    }
  }

  const wakeMutation = useMutation({
    mutation: (v: ActionTarget) =>
      wakeup({ path: { name: v.name }, baseUrl: baseUrl(v) }).then(unwrap),
    onSuccess: (_data, v) => toaster.report(wakeOutcome(v.job, v.name)),
    onError: (e, v) => toaster.failure(`Waking ${v.name} failed`, e),
    ...tracking('wake'),
  })
  const cancelMutation = useMutation({
    mutation: (v: ActionTarget) =>
      cancelJob({ path: { name: v.name }, baseUrl: baseUrl(v) }).then(unwrap),
    onSuccess: (_data, v) =>
      toaster.report({
        title: `Stopping ${v.name}`,
        description: 'Waiting for the receiver to release the dataset safely.',
        tone: 'success',
      }),
    onError: (e, v) => toaster.failure(`Stopping ${v.name} failed`, e),
    ...tracking('cancel'),
  })
  const pauseMutation = useMutation({
    mutation: (v: ActionTarget) =>
      pauseJob({ path: { name: v.name }, baseUrl: baseUrl(v) }).then(unwrap),
    onSuccess: (_data, v) =>
      toaster.report({
        title: `Paused ${v.name}`,
        description: 'The partial transfer is kept; resume continues from it.',
        tone: 'success',
      }),
    onError: (e, v) => toaster.failure(`Pausing ${v.name} failed`, e),
    ...tracking('pause'),
  })
  const resumeMutation = useMutation({
    mutation: (v: ActionTarget) =>
      resumeJob({ path: { name: v.name }, baseUrl: baseUrl(v) }).then(unwrap),
    onSuccess: (_data, v) => toaster.report({ title: `Resumed ${v.name}`, tone: 'success' }),
    onError: (e, v) => toaster.failure(`Resuming ${v.name} failed`, e),
    ...tracking('resume'),
  })
  const pushMutation = useMutation({
    mutation: (v: ActionTarget & { peer: string }) =>
      pushToPeer({ path: { name: v.name, peer: v.peer }, baseUrl: baseUrl(v) }).then(unwrap),
    onSuccess: (_data, v) => toaster.report(pushOutcome(v.job, v.name, v.peer)),
    onError: (e, v) => toaster.failure(`Push from ${v.name} to ${v.peer} failed`, e),
    ...tracking('push'),
  })

  return {
    jobs,
    error,
    warning,
    loading,
    live,
    wake: (name: string) => {
      if (!busy('wake', name)) wakeMutation.mutate(target(name))
    },
    cancel: (name: string) => {
      if (!busy('cancel', name)) cancelMutation.mutate(target(name))
    },
    pause: (name: string) => {
      if (!busy('pause', name)) pauseMutation.mutate(target(name))
    },
    resume: (name: string) => {
      if (!busy('resume', name)) resumeMutation.mutate(target(name))
    },
    pushTo: (name: string, peer: string) => {
      if (!busy('push', name, peer)) pushMutation.mutate({ ...target(name, peer), peer })
    },
    isWaking: (name: string) => busy('wake', name),
    isCancelling: (name: string) => busy('cancel', name),
    isPausing: (name: string) => busy('pause', name),
    isResuming: (name: string) => busy('resume', name),
    isPushing: (name: string, peer: string) => busy('push', name, peer),
  }
}
