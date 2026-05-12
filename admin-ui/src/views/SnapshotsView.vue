<script setup lang="ts">
import { computed, h, onMounted, ref, resolveComponent, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useSnapshots } from '../composables/useSnapshots'
import { formatBytes, formatRelative } from '../utils/format'
import DestroySnapshotModal from '../components/DestroySnapshotModal.vue'
import type { DatasetSummary } from '../client'

const route = useRoute()
const router = useRouter()
const { dataset, prefix, snapshots, error, loading, refresh, destroy } = useSnapshots()

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

function ask(name: string) {
  pending.value = name
  modalOpen.value = true
}

async function onConfirm(name: string) {
  await destroy(name)
  pending.value = null
}

const UButton = resolveComponent('UButton')

const columns = computed(() => [
  {
    accessorKey: 'name',
    header: 'Snapshot',
    cell: ({ row }: { row: { original: DatasetSummary } }) => {
      const at = row.original.name.indexOf('@')
      return h('span', { class: 'font-mono text-xs' }, at >= 0 ? row.original.name.slice(at) : row.original.name)
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
    id: 'actions',
    header: '',
    cell: ({ row }: { row: { original: DatasetSummary } }) =>
      h(
        UButton,
        {
          icon: 'i-lucide-trash-2',
          color: 'error',
          variant: 'soft',
          size: 'xs',
          onClick: () => ask(row.original.name),
        },
        () => 'Destroy',
      ),
  },
])
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

    <DestroySnapshotModal
      v-model:open="modalOpen"
      :snapshot-name="pending"
      @confirm="onConfirm"
    />
  </div>
</template>
