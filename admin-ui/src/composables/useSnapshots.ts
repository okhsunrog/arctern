import { onUnmounted, ref } from 'vue'
import { destroySnapshot, listSnapshots } from '../client'
import type { DatasetSummary } from '../client'

export function useSnapshots() {
  const dataset = ref<string>('')
  const prefix = ref<string>('')
  const snapshots = ref<DatasetSummary[]>([])
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
    if (!dataset.value) {
      snapshots.value = []
      error.value = null
      return
    }
    loading.value = true
    const r = await listSnapshots({
      path: { name: dataset.value },
      query: prefix.value ? { prefix: prefix.value } : undefined,
    })
    if (r.error) setError(r.error)
    else {
      snapshots.value = r.data ?? []
      error.value = null
    }
    loading.value = false
  }

  async function destroy(snapshotName: string) {
    const at = snapshotName.indexOf('@')
    if (at < 0) {
      setError(`malformed snapshot name: ${snapshotName}`)
      return
    }
    const ds = snapshotName.slice(0, at)
    const tag = snapshotName.slice(at + 1)
    const r = await destroySnapshot({ path: { name: ds, snapshot: tag } })
    if (r.error) setError(r.error)
    await refresh()
  }

  // Poll every 10s while a dataset is selected — keeps the list fresh
  // when the snap job is making + destroying snapshots underneath us.
  const handle = setInterval(() => {
    if (dataset.value) void refresh()
  }, 10_000)
  onUnmounted(() => clearInterval(handle))

  return { dataset, prefix, snapshots, error, loading, refresh, destroy }
}
