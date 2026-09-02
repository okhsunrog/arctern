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

  function jobNamed(name: string): JobStatus | undefined {
    return jobs.value.find((j) => j.name === name)
  }

  const baseUrl = () => baseUrlFor(toValue(scope) || null)

  // In-flight actions live in a shared store, so an action triggered from
  // the command palette greys out the matching button on the card behind
  // it. A mutation's own `variables` ref would not do: it holds only the
  // LAST call, so two quick clicks on different jobs would clear the
  // first button while its request is still running — exactly the
  // double-submit the busy state exists to prevent.
  const actions = useJobActions()
  const busy = (key: string) => actions.inFlight.has(key)

  // Key builders are shared by the mutation hooks and the `isX` helpers,
  // so the two cannot drift, and each is typed against its mutation's
  // variables rather than compared as opaque JSON.
  const wakeKey = (name: string) => jobActionKey('wake', toValue(scope), name)
  const cancelKey = (name: string) => jobActionKey('cancel', toValue(scope), name)
  const pauseKey = (name: string) => jobActionKey('pause', toValue(scope), name)
  const resumeKey = (name: string) => jobActionKey('resume', toValue(scope), name)
  const pushKey = (v: { name: string; peer: string }) =>
    jobActionKey('push', toValue(scope), v.name, v.peer)

  /**
   * Mark the action in flight for its key, and release it when it ends.
   *
   * Deliberately does NOT refetch. The daemon re-renders its status every
   * 250ms and streams a frame on any change, so a mutation's effect
   * arrives on its own — while an invalidation raced it: the HTTP
   * response carries the daemon's state from when the request was made,
   * so landing after a newer stream frame overwrote it with older data
   * and the card flickered backwards until the next frame.
   */
  function tracking<TVars>(key: (vars: TVars) => string) {
    return {
      onMutate: (vars: TVars) => {
        actions.inFlight.add(key(vars))
      },
      onSettled: (_data: unknown, _error: unknown, vars: TVars) => {
        actions.inFlight.delete(key(vars))
      },
    }
  }

  const wakeMutation = useMutation({
    mutation: (name: string) => wakeup({ path: { name }, baseUrl: baseUrl() }).then(unwrap),
    onSuccess: (_data, name) => toaster.report(wakeOutcome(jobNamed(name), name)),
    onError: (e, name) => toaster.failure(`Waking ${name} failed`, e),
    ...tracking(wakeKey),
  })

  const cancelMutation = useMutation({
    mutation: (name: string) => cancelJob({ path: { name }, baseUrl: baseUrl() }).then(unwrap),
    onSuccess: (_data, name) =>
      toaster.report({
        title: `Stopping ${name}`,
        description: 'Waiting for the receiver to release the dataset safely.',
        tone: 'success',
      }),
    onError: (e, name) => toaster.failure(`Stopping ${name} failed`, e),
    ...tracking(cancelKey),
  })

  const pauseMutation = useMutation({
    mutation: (name: string) => pauseJob({ path: { name }, baseUrl: baseUrl() }).then(unwrap),
    onSuccess: (_data, name) =>
      toaster.report({
        title: `Paused ${name}`,
        description: 'The partial transfer is kept; resume continues from it.',
        tone: 'success',
      }),
    onError: (e, name) => toaster.failure(`Pausing ${name} failed`, e),
    ...tracking(pauseKey),
  })

  const resumeMutation = useMutation({
    mutation: (name: string) => resumeJob({ path: { name }, baseUrl: baseUrl() }).then(unwrap),
    onSuccess: (_data, name) => toaster.report({ title: `Resumed ${name}`, tone: 'success' }),
    onError: (e, name) => toaster.failure(`Resuming ${name} failed`, e),
    ...tracking(resumeKey),
  })

  const pushMutation = useMutation({
    mutation: ({ name, peer }: { name: string; peer: string }) =>
      pushToPeer({ path: { name, peer }, baseUrl: baseUrl() }).then(unwrap),
    onSuccess: (_data, { name, peer }) => toaster.report(pushOutcome(jobNamed(name), name, peer)),
    onError: (e, { name, peer }) => toaster.failure(`Push from ${name} to ${peer} failed`, e),
    ...tracking(pushKey),
  })

  return {
    jobs,
    error,
    warning,
    loading,
    live,
    wake: (name: string) => wakeMutation.mutate(name),
    cancel: (name: string) => cancelMutation.mutate(name),
    pause: (name: string) => pauseMutation.mutate(name),
    resume: (name: string) => resumeMutation.mutate(name),
    pushTo: (name: string, peer: string) => pushMutation.mutate({ name, peer }),
    isWaking: (name: string) => busy(wakeKey(name)),
    isCancelling: (name: string) => busy(cancelKey(name)),
    isPausing: (name: string) => busy(pauseKey(name)),
    isResuming: (name: string) => busy(resumeKey(name)),
    isPushing: (name: string, peer: string) => busy(pushKey({ name, peer })),
  }
}
