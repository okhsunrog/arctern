import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import { useQuery } from '@pinia/colada'
import { jobRunsQuery } from '../queries'

export function useJobRuns(
  name: MaybeRefOrGetter<string>,
  scope: MaybeRefOrGetter<string> = '',
  limit = 100,
) {
  const query = useQuery(() => jobRunsQuery({ scope: toValue(scope), name: toValue(name), limit }))
  return {
    runs: computed(() => query.data.value ?? []),
    error: computed(() => query.error.value?.message ?? null),
    loading: computed(() => query.isPending.value && !query.data.value),
  }
}
