import { computed, toValue, type MaybeRefOrGetter } from 'vue'
import { useQuery } from '@pinia/colada'
import { systemInfoQuery } from '../queries'

// The daemon's version, host-scoped. Static per daemon, so the query
// never goes stale — switching hosts is a key change, not a refetch.
export function useSystemInfo(scope: MaybeRefOrGetter<string> = '') {
  const query = useQuery(() => systemInfoQuery(toValue(scope)))
  return { version: computed(() => query.data.value?.version ?? null) }
}
