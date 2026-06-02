import { onUnmounted, ref } from 'vue'
import { listPeers } from '../client'
import type { PeerSummary } from '../client'

export function usePeers(refreshMs = 5000) {
  const peers = ref<PeerSummary[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const r = await listPeers()
    if (r.error) {
      const e: unknown = r.error
      error.value =
        e && typeof e === 'object' && 'message' in e && typeof e.message === 'string'
          ? e.message
          : JSON.stringify(e)
    } else {
      peers.value = r.data ?? []
      error.value = null
    }
    loading.value = false
  }

  void refresh()
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  return { peers, error, loading, refresh }
}
