import { onUnmounted, ref } from 'vue'
import { createSnapshot, destroySnapshot, listHolds, listSnapshots } from '../client'
import type { DatasetSummary, SnapshotHold } from '../client'

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

  // Lazy hold cache: snapshot name → list of holds. Filled on demand
  // when the user clicks a row's holds button. Null means "not yet
  // fetched"; empty array means "fetched, no holds".
  const holdsCache = ref<Map<string, SnapshotHold[]>>(new Map())

  function splitName(snapshotName: string): { ds: string; tag: string } | null {
    const at = snapshotName.indexOf('@')
    if (at < 0) return null
    return { ds: snapshotName.slice(0, at), tag: snapshotName.slice(at + 1) }
  }

  async function loadHolds(snapshotName: string) {
    const parts = splitName(snapshotName)
    if (!parts) return
    const r = await listHolds({
      path: { name: parts.ds, snapshot: parts.tag },
    })
    if (r.error) {
      setError(r.error)
      return
    }
    holdsCache.value.set(snapshotName, r.data ?? [])
  }

  async function create(snapshotName: string, recursive: boolean): Promise<boolean> {
    if (!dataset.value) return false
    const r = await createSnapshot({
      path: { name: dataset.value },
      body: { snapshot_name: snapshotName, recursive },
    })
    if (r.error) {
      const body = r.error as { error?: string } | null
      if (body?.error === 'snapshot_exists') {
        setError(`Snapshot ${dataset.value}@${snapshotName} already exists.`)
      } else {
        setError(r.error)
      }
      return false
    }
    error.value = null
    await refresh()
    return true
  }

  async function destroy(snapshotName: string) {
    const parts = splitName(snapshotName)
    if (!parts) {
      setError(`malformed snapshot name: ${snapshotName}`)
      return
    }
    const r = await destroySnapshot({
      path: { name: parts.ds, snapshot: parts.tag },
    })
    if (r.error) {
      // Detect the held case so the UI can surface the holds inline
      // instead of just dumping the daemon's error string.
      const body = r.error as { error?: string; message?: string } | null
      if (body?.error === 'snapshot_held') {
        await loadHolds(snapshotName)
        const holds = holdsCache.value.get(snapshotName) ?? []
        const tagList = holds.map((h) => h.tag).join(', ') || 'unknown'
        setError(
          `Cannot destroy ${snapshotName}: held by ${holds.length} tag(s) — ${tagList}. Run 'zfs release <tag>' to release.`,
        )
      } else {
        setError(r.error)
      }
    }
    await refresh()
  }

  // Poll every 10s while a dataset is selected — keeps the list fresh
  // when the snap job is making + destroying snapshots underneath us.
  const handle = setInterval(() => {
    if (dataset.value) void refresh()
  }, 10_000)
  onUnmounted(() => clearInterval(handle))

  return {
    dataset,
    prefix,
    snapshots,
    error,
    loading,
    refresh,
    create,
    destroy,
    loadHolds,
    holdsCache,
  }
}
