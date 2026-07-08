import { computed } from 'vue'
import { useRoute } from 'vue-router'

// Host scope: the whole console is host-scoped. `null` host = the local
// daemon; a peer name routes every API call through the generic
// control-channel proxy, so a peer's console is the local console with
// a different base URL — same views, same composables, same actions.
export function useHost() {
  const route = useRoute()
  const host = computed(() => {
    const h = route.params.host
    return typeof h === 'string' && h ? h : null
  })
  const baseUrl = computed(() =>
    host.value ? `/api/v1/peers/${encodeURIComponent(host.value)}/proxy` : '',
  )
  /** Route prefix for host-scoped navigation links. */
  const prefix = computed(() => (host.value ? `/h/${host.value}` : ''))
  return { host, baseUrl, prefix }
}
