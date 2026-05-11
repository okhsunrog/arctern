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
      error.value =
        r.error && typeof r.error === 'object' && 'message' in r.error
          ? String((r.error as { message: unknown }).message)
          : String(r.error)
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
