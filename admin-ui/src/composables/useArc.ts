import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import { useQuery } from '@pinia/colada'
import { arcHistoryQuery, arcQuery } from '../queries'

export function useArc(scope: MaybeRefOrGetter<string> = '') {
  const query = useQuery(() => arcQuery(toValue(scope)))
  return {
    arc: computed(() => query.data.value ?? null),
    error: computed(() => query.error.value?.message ?? null),
    loading: computed(() => query.isPending.value && !query.data.value),
  }
}

export function useArcHistory(scope: MaybeRefOrGetter<string> = '', limit = 120) {
  const query = useQuery(() => arcHistoryQuery({ scope: toValue(scope), limit }))
  return {
    history: computed(() => query.data.value ?? []),
    loading: computed(() => query.isPending.value && !query.data.value),
    error: computed(() => query.error.value?.message ?? null),
  }
}
