<script setup lang="ts">
import { computed, h, onMounted, ref, resolveComponent, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useSnapshots } from '../composables/useSnapshots'
import { useDatasets } from '../composables/useDatasets'
import { visibleDatasetRows } from '../utils/datasets'
import { formatBytes, formatRelative, formatTimestamp } from '../utils/format'
import DestroySnapshotModal from '../components/DestroySnapshotModal.vue'
import CreateSnapshotModal from '../components/CreateSnapshotModal.vue'
import type { DatasetSummary } from '../client'

const route = useRoute()
const router = useRouter()
const {
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
} = useSnapshots()
const {
  datasets,
  loading: datasetsLoading,
  error: datasetsError,
  refresh: refreshDatasets,
} = useDatasets()

onMounted(() => {
  const q = route.query.dataset
  if (typeof q === 'string') dataset.value = q
  void refresh()
})

watch(dataset, (v) => {
  void router.replace({ query: v ? { dataset: v } : {} })
  void refresh()
})

// --- dataset picker (left pane) ---
const dsSearch = ref('')
const collapsed = ref<Set<string>>(new Set())

const rows = computed(() => visibleDatasetRows(datasets.value, collapsed.value, dsSearch.value))

function selectDataset(name: string) {
  dataset.value = name
}

function toggleCollapse(name: string) {
  const next = new Set(collapsed.value)
  if (next.has(name)) next.delete(name)
  else next.add(name)
  collapsed.value = next
}

// --- create / destroy (right pane) ---
const createOpen = ref(false)
async function onCreate(payload: { name: string; recursive: boolean }) {
  await create(payload.name, payload.recursive)
}

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
      const iso = new Date(sec * 1000).toISOString()
      return h('span', { title: formatTimestamp(iso) }, formatRelative(iso))
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

    <UAlert v-if="datasetsError" color="error" :title="datasetsError" />

    <div class="grid grid-cols-1 lg:grid-cols-[19rem_1fr] gap-4 items-start">
      <!-- Left: dataset tree picker -->
      <div
        class="rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-3 space-y-2 lg:sticky lg:top-4"
      >
        <div class="flex items-center justify-between">
          <span class="text-sm font-semibold">Datasets</span>
          <UButton
            icon="i-lucide-refresh-cw"
            variant="ghost"
            size="xs"
            :loading="datasetsLoading"
            title="Reload dataset list"
            @click="refreshDatasets"
          />
        </div>
        <UInput
          v-model="dsSearch"
          icon="i-lucide-search"
          size="sm"
          placeholder="Filter datasets…"
          class="w-full"
        />
        <div class="max-h-[70vh] overflow-auto -mx-1 pr-1">
          <div v-if="datasetsLoading && rows.length === 0" class="text-gray-500 text-sm px-2 py-1">
            Loading…
          </div>
          <div v-else-if="rows.length === 0" class="text-gray-500 text-sm px-2 py-1">
            No datasets match.
          </div>
          <button
            v-for="row in rows"
            :key="row.name"
            type="button"
            class="w-full flex items-center gap-1 rounded px-1 py-1 text-left text-sm hover:bg-gray-100 dark:hover:bg-gray-800"
            :class="
              row.name === dataset
                ? 'bg-primary-50 dark:bg-primary-950 text-primary-700 dark:text-primary-300'
                : ''
            "
            :style="{ paddingLeft: `${row.depth * 0.85 + 0.25}rem` }"
            @click="selectDataset(row.name)"
          >
            <span
              v-if="row.hasChildren"
              class="shrink-0 rounded p-0.5 hover:bg-gray-200 dark:hover:bg-gray-700"
              @click.stop="toggleCollapse(row.name)"
            >
              <UIcon
                :name="row.collapsed ? 'i-lucide-chevron-right' : 'i-lucide-chevron-down'"
                class="size-4 text-gray-400"
              />
            </span>
            <span v-else class="shrink-0 w-5" />
            <UIcon
              :name="row.type === 'volume' ? 'i-lucide-hard-drive' : 'i-lucide-folder'"
              class="size-4 shrink-0 text-gray-400"
            />
            <span class="font-mono truncate flex-1">{{ row.label }}</span>
            <span v-if="row.used != null" class="text-xs text-gray-400 shrink-0">
              {{ formatBytes(row.used) }}
            </span>
          </button>
        </div>
      </div>

      <!-- Right: snapshots of the selected dataset -->
      <div class="space-y-3 min-w-0">
        <div
          v-if="!dataset"
          class="rounded-md border border-dashed border-gray-300 dark:border-gray-700 p-8 text-center text-gray-500"
        >
          Pick a dataset on the left to view and manage its snapshots.
        </div>

        <template v-else>
          <div class="flex flex-wrap items-center gap-2">
            <div class="min-w-0 flex-1">
              <div class="font-mono text-sm truncate" :title="dataset">{{ dataset }}</div>
              <div class="text-xs text-gray-500">
                {{ snapshots.length }} snapshot{{ snapshots.length === 1 ? '' : 's' }}
              </div>
            </div>
            <UInput
              v-model="prefix"
              size="sm"
              placeholder="Prefix filter"
              class="font-mono w-40"
              @change="refresh"
            />
            <UButton
              icon="i-lucide-refresh-cw"
              variant="ghost"
              size="sm"
              :loading="loading"
              @click="refresh"
            />
            <UButton icon="i-lucide-camera" color="primary" size="sm" @click="createOpen = true">
              Create
            </UButton>
          </div>

          <UAlert v-if="error" color="error" :title="error" />

          <div v-if="loading && snapshots.length === 0" class="text-gray-500">Loading…</div>
          <div v-else-if="snapshots.length === 0" class="text-gray-500">No snapshots match.</div>
          <UTable v-else :data="snapshots" :columns="columns" />

          <UCard v-if="focusedHolds">
            <template #header>
              <div class="flex items-center justify-between">
                <div class="font-semibold font-mono text-sm break-all">{{ focusedHolds }}</div>
                <UButton icon="i-lucide-x" variant="ghost" size="xs" @click="focusedHolds = null" />
              </div>
            </template>
            <div v-if="focusedHoldsList == null" class="text-gray-500 text-sm">Loading…</div>
            <div v-else-if="focusedHoldsList.length === 0" class="text-gray-500 text-sm">
              No user holds. Destroy will succeed unless a clone or zfs send hold blocks it.
            </div>
            <ul v-else class="space-y-2 text-sm">
              <li
                v-for="hold in focusedHoldsList"
                :key="hold.tag"
                class="flex items-center justify-between rounded px-3 py-2 bg-gray-50 dark:bg-gray-900"
              >
                <div>
                  <span class="font-mono font-semibold">{{ hold.tag }}</span>
                  <span class="ml-3 text-xs text-gray-500">
                    held since
                    {{ formatTimestamp(new Date(Number(hold.timestamp) * 1000).toISOString()) }}
                  </span>
                </div>
                <code class="text-xs text-gray-500">
                  zfs release {{ hold.tag }} {{ focusedHolds }}
                </code>
              </li>
            </ul>
          </UCard>
        </template>
      </div>
    </div>

    <CreateSnapshotModal v-model:open="createOpen" :dataset="dataset" @confirm="onCreate" />
    <DestroySnapshotModal v-model:open="modalOpen" :snapshot-name="pending" @confirm="onConfirm" />
  </div>
</template>
