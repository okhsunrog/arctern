<script setup lang="ts">
import { computed, h, onUnmounted, ref, resolveComponent, watch } from 'vue'
import type { TableColumn, TreeItem } from '@nuxt/ui'
import type { DatasetSummary, SnapshotHold } from '../client'
import type { SnapshotRow, SnapshotSource } from '../composables/snapshotSources'
import { useMutation } from '../composables/useMutation'
import { formatBytes } from '../utils/format'
import CreateSnapshotModal from './CreateSnapshotModal.vue'
import DestroySnapshotModal from './DestroySnapshotModal.vue'

const props = defineProps<{ source: SnapshotSource }>()

/** Selected dataset — parents can v-model it (deep links). */
const dataset = defineModel<string>('dataset', { default: '' })

const { mutate } = useMutation()

function onHost(action: string): string {
  return props.source.hostLabel ? `${action} on ${props.source.hostLabel}` : action
}

// ── Datasets + tree ─────────────────────────────────────────────
const datasets = ref<DatasetSummary[]>([])
const dsError = ref<string | null>(null)
const dsLoading = ref(false)

async function refreshDatasets() {
  dsLoading.value = true
  const r = await props.source.listDatasets()
  if (r.error) {
    dsError.value = String((r.error as { message?: string })?.message ?? JSON.stringify(r.error))
  } else {
    datasets.value = r.data ?? []
    dsError.value = null
  }
  dsLoading.value = false
}
void refreshDatasets()

const treeFilter = ref('')
/** 'name' | 'size' — size ordering answers "what eats my space". */
const treeSort = ref<'name' | 'size'>('name')

const usedByName = computed(() => {
  const m = new Map<string, number>()
  for (const d of datasets.value) {
    const n = Number(d.properties?.used ?? '')
    if (Number.isFinite(n)) m.set(d.name, n)
  }
  return m
})

interface DsNode extends TreeItem {
  label: string
  value: string
  used: number | null
  children?: DsNode[]
}

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
      used: usedByName.value.get(d.name) ?? null,
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
      // Promoted root (pool root or filtered-out parent): keep the full
      // path as label and open it — a collapsed sole root is a dead end.
      node.label = d.name
      node.defaultExpanded = true
      roots.push(node)
    }
  }
  if (treeSort.value === 'size') {
    const bySize = (a: DsNode, b: DsNode) => (b.used ?? -1) - (a.used ?? -1)
    const sortRec = (nodes: DsNode[]) => {
      nodes.sort(bySize)
      for (const n of nodes) if (n.children) sortRec(n.children)
    }
    sortRec(roots)
  }
  return roots
})

const selectedNode = ref<DsNode>()
watch(selectedNode, (n) => {
  if (n?.value) dataset.value = n.value
})
// Parent-driven selection (deep link) before the tree loads.
watch(
  dataset,
  (d) => {
    if (d && selectedNode.value?.value !== d) {
      selectedNode.value = { label: d, value: d, used: null }
    }
    void refreshSnapshots()
  },
  { immediate: true },
)

const selectedSummary = computed(() => datasets.value.find((d) => d.name === dataset.value))
const selectedUsedBySnapshots = computed(() => {
  const n = Number(selectedSummary.value?.properties?.usedbysnapshots ?? '')
  return Number.isFinite(n) ? n : null
})

// ── Snapshots ───────────────────────────────────────────────────
const snapshots = ref<SnapshotRow[]>([])
const snapsError = ref<string | null>(null)
const snapsLoading = ref(false)
const holds = ref<Map<string, SnapshotHold[]>>(new Map())
const rowSelection = ref<Record<string, boolean>>({})

async function refreshSnapshots() {
  rowSelection.value = {}
  if (!dataset.value) {
    snapshots.value = []
    return
  }
  snapsLoading.value = true
  const r = await props.source.listSnapshots(dataset.value)
  if (r.error) {
    snapsError.value = String((r.error as { message?: string })?.message ?? JSON.stringify(r.error))
  } else {
    snapsError.value = null
    snapshots.value = r.data ?? []
    void loadAllHolds()
  }
  snapsLoading.value = false
}

// Eager holds: the lock must be visible BEFORE a destroy attempt.
async function loadAllHolds() {
  const ds = dataset.value
  const rows = snapshots.value
  const next = new Map<string, SnapshotHold[]>()
  await Promise.all(
    rows.map(async (row) => {
      const r = await props.source.listHolds(ds, row.tag)
      if (!r.error) next.set(row.tag, r.data ?? [])
    }),
  )
  if (dataset.value === ds) holds.value = next
}

const pollHandle = setInterval(() => {
  if (dataset.value && !snapsLoading.value) void refreshSnapshots()
}, 15_000)
onUnmounted(() => clearInterval(pollHandle))

// Sum of listed snapshot `used` — the quick "who eats space" readout
// next to the authoritative usedbysnapshots property.
const snapshotsUsedSum = computed(() => snapshots.value.reduce((acc, s) => acc + (s.used ?? 0), 0))

// ── Detail slideover ────────────────────────────────────────────
const detailOpen = ref(false)
const detailSnap = ref<SnapshotRow | null>(null)
const newHoldTag = ref('')

function openDetail(s: SnapshotRow) {
  detailSnap.value = s
  newHoldTag.value = ''
  detailOpen.value = true
}

const detailHolds = computed(() =>
  detailSnap.value ? (holds.value.get(detailSnap.value.tag) ?? []) : [],
)

async function addHold() {
  const s = detailSnap.value
  const tag = newHoldTag.value.trim()
  if (!s || !tag || !props.source.holdCreate) return
  const ok = await mutate(onHost(`Held ${dataset.value}@${s.tag}`), () =>
    props.source.holdCreate!(dataset.value, s.tag, tag),
  )
  if (ok) {
    newHoldTag.value = ''
    await loadAllHolds()
  }
}

async function releaseHoldTag(holdTag: string) {
  const s = detailSnap.value
  if (!s || !props.source.holdRelease) return
  const ok = await mutate(onHost(`Released ${holdTag}`), () =>
    props.source.holdRelease!(dataset.value, s.tag, holdTag),
  )
  if (ok) await loadAllHolds()
}

// ── Create / destroy ────────────────────────────────────────────
const createOpen = ref(false)

async function confirmCreate(payload: { name: string; recursive: boolean }) {
  if (!props.source.create) return
  await mutate(onHost(`Created ${dataset.value}@${payload.name}`), () =>
    props.source.create!(dataset.value, payload.name, payload.recursive),
  )
  await refreshSnapshots()
}

const destroyOpen = ref(false)
const destroyTarget = ref<string | null>(null)

function askDestroy(tag: string) {
  destroyTarget.value = `${dataset.value}@${tag}`
  destroyOpen.value = true
}

async function confirmDestroy(full: string) {
  const at = full.indexOf('@')
  const tag = at >= 0 ? full.slice(at + 1) : full
  await mutate(onHost(`Destroyed ${full}`), () => props.source.destroy(dataset.value, tag))
  await refreshSnapshots()
}

const bulkOpen = ref(false)
const selectedTags = computed(() =>
  Object.entries(rowSelection.value)
    .filter(([, v]) => v)
    .map(([k]) => k),
)

async function confirmBulkDestroy() {
  bulkOpen.value = false
  for (const tag of selectedTags.value) {
    await mutate(
      onHost(`Destroyed ${dataset.value}@${tag}`),
      () => props.source.destroy(dataset.value, tag),
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

function sortHeader(label: string) {
  return ({
    column,
  }: {
    column: { getIsSorted: () => false | string; toggleSorting: (v: boolean) => void }
  }) =>
    h(UButton, {
      color: 'neutral',
      variant: 'ghost',
      label,
      icon:
        column.getIsSorted() === 'asc'
          ? 'i-lucide-arrow-up-narrow-wide'
          : 'i-lucide-arrow-down-wide-narrow',
      class: '-mx-2.5',
      onClick: () => column.toggleSorting(column.getIsSorted() === 'asc'),
    })
}

const columns = computed<TableColumn<SnapshotRow>[]>(() => [
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
    accessorFn: (r) => r.tag,
    header: 'Snapshot',
    cell: ({ row }) =>
      h(
        'button',
        { class: 'font-mono text-left hover:underline', onClick: () => openDetail(row.original) },
        row.original.tag,
      ),
  },
  {
    id: 'created',
    accessorFn: (r) => r.creation ?? 0,
    header: sortHeader('Created'),
    cell: ({ row }) =>
      row.original.creation ? new Date(row.original.creation * 1000).toLocaleString() : '—',
  },
  {
    id: 'used',
    accessorFn: (r) => r.used ?? 0,
    header: sortHeader('Used'),
    cell: ({ row }) => formatBytes(row.original.used),
  },
  {
    id: 'holds',
    header: 'Holds',
    enableSorting: false,
    cell: ({ row }) => {
      const hs = holds.value.get(row.original.tag)
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
          onClick: () => askDestroy(row.original.tag),
        }),
      ]),
  },
])
</script>

<template>
  <div>
    <UAlert v-if="dsError" color="error" :title="dsError" icon="i-lucide-circle-x" class="mb-4" />
    <div class="grid grid-cols-1 lg:grid-cols-[minmax(17rem,22rem)_1fr] gap-4">
      <!-- Dataset tree -->
      <div class="rounded-md border border-default bg-default p-2 self-start">
        <div class="flex gap-1 mb-2">
          <UInput
            v-model="treeFilter"
            icon="i-lucide-search"
            placeholder="Filter datasets…"
            size="sm"
            class="flex-1 font-mono"
          />
          <UTooltip :text="treeSort === 'name' ? 'Sort by size' : 'Sort by name'">
            <UButton
              size="sm"
              color="neutral"
              :variant="treeSort === 'size' ? 'soft' : 'ghost'"
              :icon="
                treeSort === 'size' ? 'i-lucide-arrow-down-wide-narrow' : 'i-lucide-arrow-down-a-z'
              "
              :aria-label="treeSort === 'name' ? 'Sort by size' : 'Sort by name'"
              @click="treeSort = treeSort === 'name' ? 'size' : 'name'"
            />
          </UTooltip>
        </div>
        <div v-if="dsLoading && tree.length === 0" class="text-muted text-sm p-2">Loading…</div>
        <UTree
          v-else
          v-model="selectedNode"
          :items="tree"
          :get-key="(i: DsNode) => i.value"
          size="sm"
          class="font-mono"
        >
          <!-- Overriding the trailing slot replaces the built-in expand
               chevron, so render both: size, then a manual chevron that
               follows the expanded state. -->
          <template #item-trailing="{ item, expanded }">
            <span class="ms-auto flex items-center gap-1 ps-2 shrink-0">
              <span v-if="(item as DsNode).used != null" class="text-[11px] text-muted">
                {{ formatBytes((item as DsNode).used) }}
              </span>
              <UIcon
                v-if="(item as DsNode).children?.length"
                name="i-lucide-chevron-right"
                class="size-4 text-dimmed transition-transform"
                :class="expanded ? 'rotate-90' : ''"
              />
            </span>
          </template>
        </UTree>
        <UButton
          size="xs"
          variant="ghost"
          color="neutral"
          icon="i-lucide-refresh-cw"
          class="mt-2"
          :loading="dsLoading"
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
          :description="
            source.hostLabel
              ? `The tree shows what ${source.hostLabel}'s ACL allows this host to see.`
              : 'Select a dataset on the left to browse its snapshots.'
          "
        />
        <template v-else>
          <UAlert v-if="snapsError" color="error" :title="snapsError" icon="i-lucide-circle-x" />
          <div class="flex items-center gap-x-3 gap-y-1 flex-wrap">
            <span class="font-mono text-sm font-medium truncate">{{ dataset }}</span>
            <span class="text-xs text-muted">
              {{ snapshots.length }} snapshots
              <template v-if="selectedUsedBySnapshots != null">
                · {{ formatBytes(selectedUsedBySnapshots) }} held by snapshots
              </template>
              <template v-else-if="snapshotsUsedSum > 0">
                · ≥{{ formatBytes(snapshotsUsedSum) }} in snapshots
              </template>
            </span>
            <span class="ms-auto flex gap-2">
              <UButton
                v-if="selectedTags.length"
                color="error"
                variant="soft"
                size="xs"
                icon="i-lucide-trash-2"
                @click="bulkOpen = true"
              >
                Destroy {{ selectedTags.length }} selected
              </UButton>
              <UButton
                v-if="source.create"
                size="xs"
                variant="soft"
                icon="i-lucide-camera"
                @click="createOpen = true"
              >
                Create snapshot
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
            :get-row-id="(r: SnapshotRow) => r.tag"
            :loading="snapsLoading && snapshots.length === 0"
            class="rounded-md border border-default bg-default"
          />
        </template>
      </div>
    </div>

    <CreateSnapshotModal
      v-if="source.create"
      v-model:open="createOpen"
      :dataset="dataset"
      @confirm="confirmCreate"
    />
    <DestroySnapshotModal
      v-model:open="destroyOpen"
      :snapshot-name="destroyTarget"
      @confirm="confirmDestroy"
    />

    <!-- Bulk destroy confirm -->
    <UModal
      v-model:open="bulkOpen"
      title="Destroy selected snapshots?"
      :description="`${selectedTags.length} snapshots will be permanently removed${source.hostLabel ? ` on ${source.hostLabel}` : ''}.`"
    >
      <template #body>
        <ul class="font-mono text-xs space-y-1 max-h-64 overflow-y-auto">
          <li v-for="t in selectedTags" :key="t" class="flex items-center gap-2">
            <UIcon
              v-if="(holds.get(t)?.length ?? 0) > 0"
              name="i-lucide-lock"
              class="text-warning shrink-0"
            />
            <span class="break-all">{{ dataset }}@{{ t }}</span>
          </li>
        </ul>
        <p
          v-if="selectedTags.some((t) => (holds.get(t)?.length ?? 0) > 0)"
          class="text-warning text-xs mt-3"
        >
          Locked snapshots are held and will fail to destroy until their holds are released.
        </p>
      </template>
      <template #footer>
        <div class="flex justify-end gap-2 w-full">
          <UButton variant="ghost" @click="bulkOpen = false">Cancel</UButton>
          <UButton color="error" icon="i-lucide-trash-2" @click="confirmBulkDestroy">
            Destroy {{ selectedTags.length }}
          </UButton>
        </div>
      </template>
    </UModal>

    <!-- Snapshot detail modal -->
    <UModal v-model:open="detailOpen" :title="detailSnap?.tag ?? ''">
      <template #body>
        <div v-if="detailSnap" class="space-y-5 text-sm">
          <div>
            <div class="microlabel mb-1">full name</div>
            <code class="font-mono break-all">{{ dataset }}@{{ detailSnap.tag }}</code>
          </div>

          <div class="grid grid-cols-2 gap-3">
            <div>
              <div class="microlabel mb-1">created</div>
              <span>{{
                detailSnap.creation ? new Date(detailSnap.creation * 1000).toLocaleString() : '—'
              }}</span>
            </div>
            <div>
              <div class="microlabel mb-1">used</div>
              <span class="font-mono">{{ formatBytes(detailSnap.used) }}</span>
            </div>
            <div v-if="detailSnap.guid" class="col-span-2">
              <div class="microlabel mb-1">guid</div>
              <code class="font-mono text-xs">{{ detailSnap.guid }}</code>
            </div>
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
                  v-if="source.holdRelease"
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
            <div v-if="source.holdCreate" class="flex gap-2 mt-2">
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

          <div v-if="detailSnap.properties">
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
            @click="(askDestroy(detailSnap.tag), (detailOpen = false))"
          >
            Destroy snapshot
          </UButton>
        </div>
      </template>
    </UModal>
  </div>
</template>
