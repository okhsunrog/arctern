import { computed } from 'vue'
import { useRoute } from 'vue-router'

// Host scope: the whole console is host-scoped. `null` host = the local
// daemon; a peer name routes every API call through the generic
// control-channel proxy, so a peer's console is the local console with
// a different base URL — same views, same queries, same actions.

/** `''` = this daemon. Pure, so stores and query keys can use it too. */
export function baseUrlFor(host: string | null): string {
  return host ? `/api/v1/peers/${encodeURIComponent(host)}/proxy` : ''
}

/** Route prefix for host-scoped navigation links. */
export function prefixFor(host: string | null): string {
  return host ? `/h/${host}` : ''
}

export function useHost() {
  const route = useRoute()
  const host = computed(() => {
    const h = route.params.host
    return typeof h === 'string' && h ? h : null
  })
  /** Query-key scope: never null, so keys stay serializable and stable. */
  const scope = computed(() => host.value ?? '')
  const baseUrl = computed(() => baseUrlFor(host.value))
  const prefix = computed(() => prefixFor(host.value))
  return { host, scope, baseUrl, prefix }
}
