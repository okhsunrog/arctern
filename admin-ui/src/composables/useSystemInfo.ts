import { ref, toValue, watch, type MaybeRefOrGetter } from 'vue'
import { getSystemInfo } from '../client'

// The daemon's version, host-scoped: re-fetched when baseUrl changes so
// switching to a peer's console reports that peer's version. Static per
// daemon, so no polling -- one fetch per host.
export function useSystemInfo(baseUrl: MaybeRefOrGetter<string> = '') {
  const version = ref<string | null>(null)

  async function refresh() {
    const requestedBaseUrl = toValue(baseUrl)
    const r = await getSystemInfo({ baseUrl: requestedBaseUrl })
    if (requestedBaseUrl !== toValue(baseUrl)) return
    version.value = r.data?.version ?? null
  }

  void refresh()
  watch(() => toValue(baseUrl), refresh)

  return { version, refresh }
}
