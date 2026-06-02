<script setup lang="ts">
import { computed, h, onMounted, ref, resolveComponent, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useSnapshots } from '../composables/useSnapshots'
import { formatBytes, formatRelative, formatTimestamp } from '../utils/format'
import DestroySnapshotModal from '../components/DestroySnapshotModal.vue'
import type { DatasetSummary } from '../client'

const route = useRoute()
const router = useRouter()
const { dataset, prefix, snapshots, error, loading, refresh, destroy, loadHolds, holdsCache } =
  useSnapshots()

onMounted(() => {
  const q = route.query.dataset
  if (typeof q === 'string') dataset.value = q
  void refresh()
})

watch(dataset, (v) => {
  void router.replace({ query: v ? { dataset: v } : {} })
  void refresh()
})

const modalOpen = ref(false)
const pending = ref<string | null>(null)
const focusedHolds = ref<string | null>(null)

function ask(name: string) {
  pending.value = name
  modalOpen.value = true
}

async function onConfirm(name: string) {
  await destroy(name)
  pending.value = null
}

async function showHolds(name: string) {
  focusedHolds.value = name
  if (!holdsCache.value.has(name)) await loadHolds(name)
}

function holdsLabel(name: string): string {
  const cached = holdsCache.value.get(name)
  if (cached == null) return 'Holds'
  if (cached.length === 0) return 'No holds'
  return `${cached.length} hold${cached.length === 1 ? '' : 's'}`
}

const UButton = resolveComponent('UButton')

const columns = computed(() => [
  {
    accessorKey: 'name',
    header: 'Snapshot',
    cell: ({ row }: { row: { original: DatasetSummary } }) => {
      const at = row.original.name.indexOf('@')
      return h(
        'span',
        { class: 'font-mono text-xs' },
        at >= 0 ? row.original.name.slice(at) : row.original.name,
      )
    },
  },
  {
    id: 'creation',
    header: 'Created',
    cell: ({ row }: { row: { original: DatasetSummary } }) => {
      const sec = Number(row.original.properties?.creation ?? '')
      if (!Number.isFinite(sec)) return '—'
      return formatRelative(new Date(sec * 1000).toISOString())
    },
  },
  {
    id: 'used',
    header: 'Used',
    cell: ({ row }: { row: { original: DatasetSummary } }) => {
      const n = Number(row.original.properties?.used ?? '')
      return Number.isFinite(n) ? formatBytes(n) : '—'
    },
  },
  {
    id: 'holds',
    header: 'Holds',
    cell: ({ row }: { row: { original: DatasetSummary } }) => {
      const cached = holdsCache.value.get(row.original.name)
      const variant: 'soft' | 'ghost' = cached && cached.length > 0 ? 'soft' : 'ghost'
      const color: 'error' | 'neutral' = cached && cached.length > 0 ? 'error' : 'neutral'
      return h(
        UButton,
        {
          icon: 'i-lucide-lock',
          variant,
          color,
          size: 'xs',
          onClick: () => showHolds(row.original.name),
        },
        () => holdsLabel(row.original.name),
      )
    },
  },
  {
    id: 'actions',
    header: '',
    cell: ({ row }: { row: { original: DatasetSummary } }) => {
      const cached = holdsCache.value.get(row.original.name)
      const blocked = cached != null && cached.length > 0
      return h(
        UButton,
        {
          icon: 'i-lucide-trash-2',
          color: 'error',
          variant: 'soft',
          size: 'xs',
          disabled: blocked,
          title: blocked ? 'Release the holds first' : '',
          onClick: () => ask(row.original.name),
        },
        () => 'Destroy',
      )
    },
  },
])

const focusedHoldsList = computed(() => {
  if (!focusedHolds.value) return null
  return holdsCache.value.get(focusedHolds.value) ?? null
})
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6 space-y-4">
    <h1 class="text-2xl font-semibold">Snapshots</h1>

    <div class="flex flex-wrap gap-2 items-end">
      <UFormField label="Dataset" class="flex-1 min-w-[20rem]">
        <UInput
          v-model="dataset"
          placeholder="e.g. novafs/arctern-test/src/home"
          class="font-mono"
        />
      </UFormField>
      <UFormField label="Prefix filter">
        <UInput v-model="prefix" placeholder="optional" @change="refresh" />
      </UFormField>
      <UButton :loading="loading" @click="refresh">Refresh</UButton>
    </div>

    <UAlert v-if="error" color="error" :title="error" />

    <div v-if="!dataset" class="text-gray-500">
      Enter a dataset path above to list its snapshots.
    </div>
    <div v-else-if="loading && snapshots.length === 0" class="text-gray-500">Loading…</div>
    <div v-else-if="snapshots.length === 0" class="text-gray-500">No snapshots match.</div>
    <UTable v-else :data="snapshots" :columns="columns" />

    <UCard v-if="focusedHolds">
      <template #header>
        <div class="flex items-center justify-between">
          <div class="font-semibold font-mono text-sm">{{ focusedHolds }}</div>
          <UButton icon="i-lucide-x" variant="ghost" size="xs" @click="focusedHolds = null" />
        </div>
      </template>
      <div v-if="focusedHoldsList == null" class="text-gray-500 text-sm">Loading…</div>
      <div v-else-if="focusedHoldsList.length === 0" class="text-gray-500 text-sm">
        No user holds. Destroy will succeed unless a clone or zfs send hold blocks it.
      </div>
      <ul v-else class="space-y-2 text-sm">
        <li
          v-for="h in focusedHoldsList"
          :key="h.tag"
          class="flex items-center justify-between rounded px-3 py-2 bg-gray-50 dark:bg-gray-900"
        >
          <div>
            <span class="font-mono font-semibold">{{ h.tag }}</span>
            <span class="ml-3 text-xs text-gray-500">
              held since {{ formatTimestamp(new Date(Number(h.timestamp) * 1000).toISOString()) }}
            </span>
          </div>
          <code class="text-xs text-gray-500"> zfs release {{ h.tag }} {{ focusedHolds }} </code>
        </li>
      </ul>
    </UCard>

    <DestroySnapshotModal v-model:open="modalOpen" :snapshot-name="pending" @confirm="onConfirm" />
  </div>
</template>
