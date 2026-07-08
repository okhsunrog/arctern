import { onUnmounted, ref } from 'vue'
import { getArc, getArcHistory } from '../client'
import type { ArcHistoryPoint, ArcStats } from '../client'

function errMessage(e: unknown): string {
  if (e && typeof e === 'object' && 'message' in e) {
    return String((e as { message: unknown }).message)
  }
  return String(e)
}

export function useArc(refreshMs = 5000, includeHistory = false, limit = 120, baseUrl = '') {
  const arc = ref<ArcStats | null>(null)
  const history = ref<ArcHistoryPoint[]>([])
  const error = ref<string | null>(null)
  const loading = ref(true)

  async function refresh() {
    const a = await getArc({ baseUrl })
    if (a.error) error.value = errMessage(a.error)
    else {
      arc.value = a.data ?? null
      error.value = null
    }
    if (includeHistory) {
      const h = await getArcHistory({ query: { limit }, baseUrl })
      if (h.error) error.value = errMessage(h.error)
      else history.value = h.data ?? []
    }
    loading.value = false
  }

  void refresh()
  const handle = setInterval(() => void refresh(), refreshMs)
  onUnmounted(() => clearInterval(handle))

  return { arc, history, error, loading, refresh }
}
