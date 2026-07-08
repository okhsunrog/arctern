import { onMounted, ref } from 'vue'
import { listDatasets } from '../client'
import type { DatasetSummary } from '../client'

// Loads the full dataset list once (and on demand). The Snapshots view
// turns this into a searchable tree so the operator never has to type a
// dataset path by hand.
export function useDatasets() {
  const datasets = ref<DatasetSummary[]>([])
  const error = ref<string | null>(null)
  const loading = ref(false)

  function setError(e: unknown) {
    if (e && typeof e === 'object' && 'message' in e) {
      error.value = String((e as { message: unknown }).message)
    } else {
      error.value = String(e)
    }
  }

  async function refresh() {
    loading.value = true
    const r = await listDatasets()
    if (r.error) {
      setError(r.error)
    } else {
      datasets.value = r.data ?? []
      error.value = null
    }
    loading.value = false
  }

  onMounted(refresh)

  return { datasets, error, loading, refresh }
}
