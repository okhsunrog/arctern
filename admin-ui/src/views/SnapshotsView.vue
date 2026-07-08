<script setup lang="ts">
import { computed, h, onMounted, onUnmounted, ref, resolveComponent, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import type { TableColumn, TreeItem } from '@nuxt/ui'
import {
  createHold,
  createSnapshot,
  destroySnapshot,
  listHolds,
  listSnapshots,
  releaseHold,
} from '../client'
import type { DatasetSummary, SnapshotHold } from '../client'
import { useDatasets } from '../composables/useDatasets'
import { useMutation } from '../composables/useMutation'
import { formatBytes } from '../utils/format'
import CreateSnapshotModal from '../components/CreateSnapshotModal.vue'
import DestroySnapshotModal from '../components/DestroySnapshotModal.vue'

const route = useRoute()
const router = useRouter()
const { datasets, error: dsError, loading: dsLoading, refresh: refreshDatasets } = useDatasets()
const { mutate } = useMutation()

// ── Dataset tree ────────────────────────────────────────────────
interface DsNode extends TreeItem {
  label: string
  value: string
  children?: DsNode[]
}

const treeFilter = ref('')

const tree = computed<DsNode[]>(() => {
  const fs = datasets.value
    .filter((d) => d.dataset_type === 'filesystem' || d.dataset_type === 'volume')
    .slice()
    .sort((a, b) => a.name.localeCompare(b.name))
  const q = treeFilter.value.trim().toLowerCase()
  const matching = q ? fs.filter((d) => d.name.toLowerCase().includes(q)) : fs
  const byPath = new Map<string, DsNode>()
  const roots: DsNode[] = []
  for (const d of matching) {
    const parts = d.name.split('/')
    const node: DsNode = {
      label: parts[parts.length - 1] ?? d.name,
      value: d.name,
      icon: d.dataset_type === 'volume' ? 'i-lucide-box' : 'i-lucide-database',
      defaultExpanded: parts.length <= 2 || q.length > 0,
      children: undefined,
    }
    byPath.set(d.name, node)
    const parentPath = parts.slice(0, -1).join('/')
    const parent = byPath.get(parentPath)
    if (parent) {
      parent.children = parent.children ?? []
      parent.children.push(node)
    } else {
      // Parent filtered out (or a pool root): promote to top level with
      // the full path as label so context isn't lost.
      node.label = d.name
      roots.push(node)
    }
  }
  return roots
})

const selectedNode = ref<DsNode>()
const dataset = computed(() => selectedNode.value?.value ?? '')

// Deep-link: /snapshots?dataset=tank/data selects on load.
onMounted(() => {
  const q = route.query.dataset
  if (typeof q === 'string' && q) {
    selectedNode.value = { label: q, value: q }
  }
})
watch(dataset, (d) => {
  void router.replace({ query: d ? { dataset: d } : {} })
  void refreshSnapshots()
})

// ── Snapshots of the selected dataset ───────────────────────────
const snapshots = ref<DatasetSummary[]>([])
const snapsLoading = ref(false)
const holds = ref<Map<string, SnapshotHold[]>>(new Map())
const rowSelection = ref<Record<string, boolean>>({})

function tagOf(full: string): string {
  const at = full.indexOf('@')
  return at >= 0 ? full.slice(at + 1) : full
}

async function refreshSnapshots() {
  rowSelection.value = {}
  if (!dataset.value) {
    snapshots.value = []
    return
  }
  snapsLoading.value = true
  const r = await listSnapshots({ path: { name: dataset.value } })
  if (!r.error) {
    snapshots.value = r.data ?? []
    void loadAllHolds()
  }
  snapsLoading.value = false
}

// Eager holds: a lock badge must be visible BEFORE the operator clicks
// destroy, not discovered via a 409 after.
async function loadAllHolds() {
  const ds = dataset.value
  const names = snapshots.value.map((s) => s.name)
  const next = new Map<string, SnapshotHold[]>()
  await Promise.all(
    names.map(async (full) => {
      const r = await listHolds({ path: { name: ds, snapshot: tagOf(full) } })
      if (!r.error) next.set(full, r.data ?? [])
    }),
  )
  if (dataset.value === ds) holds.value = next
}

// Poll while a dataset is selected — the snap job mutates underneath.
const pollHandle = setInterval(() => {
  if (dataset.value && !snapsLoading.value) void refreshSnapshots()
}, 15_000)
onUnmounted(() => clearInterval(pollHandle))

// ── Detail slideover ────────────────────────────────────────────
const detailOpen = ref(false)
const detailSnap = ref<DatasetSummary | null>(null)
const newHoldTag = ref('')

function openDetail(s: DatasetSummary) {
  detailSnap.value = s
  newHoldTag.value = ''
  detailOpen.value = true
}

const detailHolds = computed(() =>
  detailSnap.value ? (holds.value.get(detailSnap.value.name) ?? []) : [],
)

async function addHold() {
  const s = detailSnap.value
  const tag = newHoldTag.value.trim()
  if (!s || !tag) return
  const ok = await mutate(`Held ${s.name}`, () =>
    createHold({ path: { name: dataset.value, snapshot: tagOf(s.name) }, body: { tag } }),
  )
  if (ok) {
    newHoldTag.value = ''
    await loadAllHolds()
  }
}

async function releaseHoldTag(tag: string) {
  const s = detailSnap.value
  if (!s) return
  const ok = await mutate(`Released ${tag}`, () =>
    releaseHold({ path: { name: dataset.value, snapshot: tagOf(s.name), tag } }),
  )
  if (ok) await loadAllHolds()
}

// ── Create / destroy ────────────────────────────────────────────
const createOpen = ref(false)

async function confirmCreate(payload: { name: string; recursive: boolean }) {
  await mutate(`Created ${dataset.value}@${payload.name}`, () =>
    createSnapshot({
      path: { name: dataset.value },
      body: { snapshot_name: payload.name, recursive: payload.recursive },
    }),
  )
  await refreshSnapshots()
}

const destroyOpen = ref(false)
const destroyTarget = ref<string | null>(null)

function askDestroy(full: string) {
  destroyTarget.value = full
  destroyOpen.value = true
}

async function confirmDestroy(full: string) {
  await mutate(`Destroyed ${full}`, () =>
    destroySnapshot({ path: { name: dataset.value, snapshot: tagOf(full) } }),
  )
  await refreshSnapshots()
}

// Bulk destroy: selected rows, skipping held ones loudly.
const bulkOpen = ref(false)
const selectedNames = computed(() =>
  Object.entries(rowSelection.value)
    .filter(([, v]) => v)
    .map(([k]) => k),
)

async function confirmBulkDestroy() {
  bulkOpen.value = false
  for (const full of selectedNames.value) {
    await mutate(
      `Destroyed ${full}`,
      () => destroySnapshot({ path: { name: dataset.value, snapshot: tagOf(full) } }),
      { silentSuccess: true },
    )
  }
  await refreshSnapshots()
}

// ── Table ───────────────────────────────────────────────────────
const UBadge = resolveComponent('UBadge')
const UButton = resolveComponent('UButton')
const UCheckbox = resolveComponent('UCheckbox')

const sorting = ref([{ id: 'created', desc: true }])

const columns = computed<TableColumn<DatasetSummary>[]>(() => [
  {
    id: 'select',
    header: ({ table }) =>
      h(UCheckbox, {
        modelValue: table.getIsSomePageRowsSelected()
          ? 'indeterminate'
          : table.getIsAllPageRowsSelected(),
        'onUpdate:modelValue': (v: boolean | 'indeterminate') =>
          table.toggleAllPageRowsSelected(!!v),
        'aria-label': 'Select all',
      }),
    cell: ({ row }) =>
      h(UCheckbox, {
        modelValue: row.getIsSelected(),
        'onUpdate:modelValue': (v: boolean | 'indeterminate') => row.toggleSelected(!!v),
        'aria-label': 'Select row',
      }),
    enableSorting: false,
  },
  {
    id: 'name',
    accessorFn: (r) => tagOf(r.name),
    header: 'Snapshot',
    cell: ({ row }) =>
      h(
        'button',
        {
          class: 'font-mono text-left hover:underline',
          onClick: () => openDetail(row.original),
        },
        tagOf(row.original.name),
      ),
  },
  {
    id: 'created',
    accessorFn: (r) => Number(r.properties?.creation ?? 0),
    header: ({ column }) =>
      h(UButton, {
        color: 'neutral',
        variant: 'ghost',
        label: 'Created',
        icon:
          column.getIsSorted() === 'asc'
            ? 'i-lucide-arrow-up-narrow-wide'
            : 'i-lucide-arrow-down-wide-narrow',
        class: '-mx-2.5',
        onClick: () => column.toggleSorting(column.getIsSorted() === 'asc'),
      }),
    cell: ({ row }) => {
      const t = Number(row.original.properties?.creation ?? 0)
      return t ? new Date(t * 1000).toLocaleString() : '—'
    },
  },
  {
    id: 'used',
    accessorFn: (r) => Number(r.properties?.used ?? 0),
    header: ({ column }) =>
      h(UButton, {
        color: 'neutral',
        variant: 'ghost',
        label: 'Used',
        icon:
          column.getIsSorted() === 'asc'
            ? 'i-lucide-arrow-up-narrow-wide'
            : 'i-lucide-arrow-down-wide-narrow',
        class: '-mx-2.5',
        onClick: () => column.toggleSorting(column.getIsSorted() === 'asc'),
      }),
    cell: ({ row }) => formatBytes(Number(row.original.properties?.used ?? 0)),
  },
  {
    id: 'holds',
    header: 'Holds',
    enableSorting: false,
    cell: ({ row }) => {
      const hs = holds.value.get(row.original.name)
      if (!hs || hs.length === 0) return ''
      return h(
        'div',
        { class: 'flex gap-1 flex-wrap' },
        hs.map((hold) =>
          h(
            UBadge,
            { color: 'warning', variant: 'subtle', size: 'sm', icon: 'i-lucide-lock' },
            () => hold.tag,
          ),
        ),
      )
    },
  },
  {
    id: 'actions',
    header: '',
    enableSorting: false,
    cell: ({ row }) =>
      h('div', { class: 'flex justify-end gap-0.5' }, [
        h(UButton, {
          size: 'xs',
          color: 'neutral',
          variant: 'ghost',
          icon: 'i-lucide-info',
          'aria-label': 'Details',
          onClick: () => openDetail(row.original),
        }),
        h(UButton, {
          size: 'xs',
          color: 'error',
          variant: 'ghost',
          icon: 'i-lucide-trash-2',
          'aria-label': 'Destroy',
          onClick: () => askDestroy(row.original.name),
        }),
      ]),
  },
])
</script>

<template>
  <UDashboardPanel id="snapshots">
    <template #header>
      <UDashboardNavbar title="Snapshots">
        <template #right>
          <UButton v-if="dataset" icon="i-lucide-camera" size="sm" @click="createOpen = true">
            Create snapshot
          </UButton>
        </template>
      </UDashboardNavbar>
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl">
        <UAlert v-if="dsError" color="error" :title="dsError" icon="i-lucide-circle-x" />
        <div class="grid grid-cols-1 lg:grid-cols-[minmax(16rem,20rem)_1fr] gap-4">
          <!-- Dataset tree -->
          <div class="rounded-md border border-default bg-default p-2 self-start">
            <UInput
              v-model="treeFilter"
              icon="i-lucide-search"
              placeholder="Filter datasets…"
              size="sm"
              class="w-full mb-2 font-mono"
            />
            <div v-if="dsLoading && tree.length === 0" class="text-muted text-sm p-2">Loading…</div>
            <UTree
              v-else
              v-model="selectedNode"
              :items="tree"
              :get-key="(i: DsNode) => i.value"
              size="sm"
              class="font-mono"
            />
            <UButton
              size="xs"
              variant="ghost"
              color="neutral"
              icon="i-lucide-refresh-cw"
              class="mt-2"
              @click="refreshDatasets"
            >
              Refresh
            </UButton>
          </div>

          <!-- Snapshot table -->
          <div class="space-y-3 min-w-0">
            <UEmpty
              v-if="!dataset"
              icon="i-lucide-database"
              title="Pick a dataset"
              description="Select a dataset on the left to browse its snapshots."
            />
            <template v-else>
              <div class="flex items-center gap-2 flex-wrap">
                <span class="font-mono text-sm font-medium truncate">{{ dataset }}</span>
                <span class="text-xs text-muted">{{ snapshots.length }} snapshots</span>
                <span class="ms-auto flex gap-2">
                  <UButton
                    v-if="selectedNames.length"
                    color="error"
                    variant="soft"
                    size="xs"
                    icon="i-lucide-trash-2"
                    @click="bulkOpen = true"
                  >
                    Destroy {{ selectedNames.length }} selected
                  </UButton>
                  <UButton
                    size="xs"
                    variant="ghost"
                    color="neutral"
                    icon="i-lucide-refresh-cw"
                    :loading="snapsLoading"
                    @click="refreshSnapshots"
                  >
                    Refresh
                  </UButton>
                </span>
              </div>
              <UTable
                v-model:row-selection="rowSelection"
                v-model:sorting="sorting"
                :data="snapshots"
                :columns="columns"
                :get-row-id="(r: DatasetSummary) => r.name"
                :loading="snapsLoading && snapshots.length === 0"
                class="rounded-md border border-default bg-default"
              />
            </template>
          </div>
        </div>
      </div>

      <CreateSnapshotModal v-model:open="createOpen" :dataset="dataset" @confirm="confirmCreate" />
      <DestroySnapshotModal
        v-model:open="destroyOpen"
        :snapshot-name="destroyTarget"
        @confirm="confirmDestroy"
      />

      <!-- Bulk destroy confirm -->
      <UModal
        v-model:open="bulkOpen"
        title="Destroy selected snapshots?"
        :description="`${selectedNames.length} snapshots will be permanently removed.`"
      >
        <template #body>
          <ul class="font-mono text-xs space-y-1 max-h-64 overflow-y-auto">
            <li v-for="n in selectedNames" :key="n" class="flex items-center gap-2">
              <UIcon
                v-if="(holds.get(n)?.length ?? 0) > 0"
                name="i-lucide-lock"
                class="text-warning shrink-0"
              />
              <span class="break-all">{{ n }}</span>
            </li>
          </ul>
          <p
            v-if="selectedNames.some((n) => (holds.get(n)?.length ?? 0) > 0)"
            class="text-warning text-xs mt-3"
          >
            Locked snapshots are held and will fail to destroy until their holds are released.
          </p>
        </template>
        <template #footer>
          <div class="flex justify-end gap-2 w-full">
            <UButton variant="ghost" @click="bulkOpen = false">Cancel</UButton>
            <UButton color="error" icon="i-lucide-trash-2" @click="confirmBulkDestroy">
              Destroy {{ selectedNames.length }}
            </UButton>
          </div>
        </template>
      </UModal>

      <!-- Snapshot detail slideover -->
      <USlideover v-model:open="detailOpen" :title="detailSnap ? tagOf(detailSnap.name) : ''">
        <template #body>
          <div v-if="detailSnap" class="space-y-5 text-sm">
            <div>
              <div class="microlabel mb-1">full name</div>
              <code class="font-mono break-all">{{ detailSnap.name }}</code>
            </div>

            <div>
              <div class="microlabel mb-2">holds</div>
              <div v-if="detailHolds.length === 0" class="text-muted text-xs">
                No holds — destroy-eligible.
              </div>
              <div v-else class="space-y-1">
                <div
                  v-for="hold in detailHolds"
                  :key="hold.tag"
                  class="flex items-center justify-between gap-2"
                >
                  <UBadge color="warning" variant="subtle" icon="i-lucide-lock">
                    {{ hold.tag }}
                  </UBadge>
                  <span class="text-xs text-muted">
                    {{ new Date(hold.timestamp * 1000).toLocaleString() }}
                  </span>
                  <UButton
                    size="xs"
                    color="warning"
                    variant="ghost"
                    icon="i-lucide-lock-open"
                    @click="releaseHoldTag(hold.tag)"
                  >
                    Release
                  </UButton>
                </div>
              </div>
              <div class="flex gap-2 mt-2">
                <UInput
                  v-model="newHoldTag"
                  size="xs"
                  placeholder="hold tag (e.g. keep_forever)"
                  class="font-mono flex-1"
                  @keydown.enter="addHold"
                />
                <UButton size="xs" variant="soft" icon="i-lucide-lock" @click="addHold">
                  Hold
                </UButton>
              </div>
            </div>

            <div>
              <div class="microlabel mb-2">properties</div>
              <dl class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 font-mono text-xs">
                <template v-for="(v, k) in detailSnap.properties" :key="k">
                  <dt class="text-muted">{{ k }}</dt>
                  <dd class="break-all">{{ v }}</dd>
                </template>
              </dl>
            </div>

            <UButton
              color="error"
              variant="soft"
              icon="i-lucide-trash-2"
              block
              @click="(askDestroy(detailSnap.name), (detailOpen = false))"
            >
              Destroy snapshot
            </UButton>
          </div>
        </template>
      </USlideover>
    </template>
  </UDashboardPanel>
</template>
