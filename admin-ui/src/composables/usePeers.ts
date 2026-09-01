import { computed } from 'vue'
import { useQuery } from '@pinia/colada'
import { peersQuery } from '../queries'

// Peer links are always this daemon's own outbound connections: inside a
// peer's console they would describe that peer's (usually empty) peer
// list, which is why this query is deliberately not host-scoped.
export function usePeers() {
  const query = useQuery(() => peersQuery())
  return {
    peers: computed(() => query.data.value ?? []),
    error: computed(() => query.error.value?.message ?? null),
    loading: computed(() => query.isPending.value && !query.data.value),
    refresh: () => void query.refetch(),
  }
}
