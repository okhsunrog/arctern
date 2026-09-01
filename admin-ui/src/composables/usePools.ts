import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import { useMutation, useQuery, useQueryCache } from '@pinia/colada'
import { poolScrub } from '../client'
import type { ScrubRequest } from '../client'
import { baseUrlFor } from './useHost'
import { poolQuery, poolsQuery } from '../queries'
import { useToaster } from './useToaster'
import { unwrap } from '../utils/errors'

export function usePools(scope: MaybeRefOrGetter<string> = '') {
  const query = useQuery(() => poolsQuery(toValue(scope)))
  return {
    pools: computed(() => query.data.value ?? []),
    error: computed(() => query.error.value?.message ?? null),
    loading: computed(() => query.isPending.value && !query.data.value),
    refresh: () => void query.refetch(),
  }
}

export function usePool(name: MaybeRefOrGetter<string>, scope: MaybeRefOrGetter<string> = '') {
  const queryCache = useQueryCache()
  const toaster = useToaster()
  const query = useQuery(() => poolQuery({ scope: toValue(scope), name: toValue(name) }))

  const scrubMutation = useMutation({
    mutation: (action: ScrubRequest['action']) =>
      poolScrub({
        path: { name: toValue(name) },
        body: { action },
        baseUrl: baseUrlFor(toValue(scope) || null),
      }).then(unwrap),
    onSuccess: (_data, action) => toaster.success(`Scrub ${action} on ${toValue(name)}`),
    onError: (e, action) => toaster.failure(`Scrub ${action} on ${toValue(name)} failed`, e),
    // The pool's scan block only reflects the new state after zpool has
    // applied it, so re-read rather than guessing.
    onSettled: () => queryCache.invalidateQueries({ key: ['pool', toValue(scope), toValue(name)] }),
  })

  return {
    pool: computed(() => query.data.value ?? null),
    error: computed(() => query.error.value?.message ?? null),
    loading: computed(() => query.isPending.value && !query.data.value),
    refresh: () => void query.refetch(),
    scrub: (action: ScrubRequest['action']) => scrubMutation.mutate(action),
    /** True while THIS action is in flight, so only its button goes busy. */
    isScrubbing: (action: ScrubRequest['action']) =>
      scrubMutation.isLoading.value && scrubMutation.variables.value === action,
    scrubBusy: computed(() => scrubMutation.isLoading.value),
  }
}
