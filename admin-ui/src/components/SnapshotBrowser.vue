<script setup lang="ts">
import { computed, h, ref, resolveComponent, watch } from 'vue'
import { useMutation, useQuery, useQueryCache } from '@pinia/colada'
import type { TableColumn, TreeItem } from '@nuxt/ui'
import {
  createHold,
  createSnapshot,
  destroySnapshot,
  releaseHold,
  type DatasetSummary,
  type SnapshotHold,
} from '../client'
import { baseUrlFor } from '../composables/useHost'
import { useToaster } from '../composables/useToaster'
import { datasetHoldsQuery, datasetsQuery, snapshotsQuery } from '../queries'
import { formatBytes } from '../utils/format'
import { apiErrorCode, unwrap } from '../utils/errors'
import CreateSnapshotModal from './CreateSnapshotModal.vue'
import DestroySnapshotModal from './DestroySnapshotModal.vue'

// Host scoping happens at the transport level: a peer exposes the SAME
// endpoints as the local host, so the browser is the same component
// against a different query scope — no parallel lesser implementation.
const props = defineProps<{
  /** '' for this daemon, otherwise the peer name. */
  scope: string
}>()

/** Selected dataset — parents can v-model it (deep links). */
const dataset = defineModel<string>('dataset', { default: '' })

const toaster = useToaster()
const queryCache = useQueryCache()
const baseUrl = () => baseUrlFor(props.scope || null)
const hostLabel = computed(() => props.scope)

function onHost(action: string): string {
  return hostLabel.value ? `${action} on ${hostLabel.value}` : action
}

/** One snapshot row, flattened from the dataset listing's property map. */
export interface SnapshotRow {
  tag: string
  creation: number | null
  used: number | null
  properties?: Record<string, string>
}

function tagOf(full: string): string {
  const at = full.indexOf('@')
  return at >= 0 ? full.slice(at + 1) : full
}

// ── Datasets + tree ─────────────────────────────────────────────
const datasetsResult = useQuery(() => datasetsQuery(props.scope))
const datasets = computed<DatasetSummary[]>(() => datasetsResult.data.value ?? [])
const dsError = computed(() => datasetsResult.error.value?.message ?? null)
const dsLoading = computed(() => datasetsResult.isLoading.value)
const refreshDatasets = () => void datasetsResult.refetch()

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
    rowSelection.value = {}
  },
  { immediate: true },
)

const selectedSummary = computed(() => datasets.value.find((d) => d.name === dataset.value))
const selectedUsedBySnapshots = computed(() => {
  const n = Number(selectedSummary.value?.properties?.usedbysnapshots ?? '')
  return Number.isFinite(n) ? n : null
})

// ── Snapshots ───────────────────────────────────────────────────
const snapshotsResult = useQuery(() =>
  snapshotsQuery({ scope: props.scope, dataset: dataset.value }),
)
const snapshots = computed<SnapshotRow[]>(() =>
  (snapshotsResult.data.value ?? []).map((s) => ({
    tag: tagOf(s.name),
    creation: Number(s.properties?.creation ?? 0) || null,
    used: s.properties?.used != null ? Number(s.properties.used) : null,
    properties: s.properties,
  })),
)
const snapsError = computed(() => snapshotsResult.error.value?.message ?? null)
const snapsLoading = computed(() => snapshotsResult.isLoading.value)
const rowSelection = ref<Record<string, boolean>>({})

// Every hold on the dataset in ONE request. Asking per snapshot turned a
// 15s refresh of a dataset with hundreds of snapshots into hundreds of
// `zfs holds` spawns — through the SSH control channel for a peer.
// A tag absent from the map has no holds; the response covers the whole
// dataset, so absence is an answer rather than a gap.
const holdsResult = useQuery(() =>
  datasetHoldsQuery({ scope: props.scope, dataset: dataset.value }),
)
const holds = computed<Record<string, SnapshotHold[]>>(() => holdsResult.data.value ?? {})
function holdsFor(tag: string): SnapshotHold[] {
  return holds.value[tag] ?? []
}

// Sum of listed snapshot `used` — the quick "who eats space" readout
// next to the authoritative usedbysnapshots property.
const snapshotsUsedSum = computed(() => snapshots.value.reduce((acc, s) => acc + (s.used ?? 0), 0))

function invalidateDataset() {
  const ds = dataset.value
  return Promise.all([
    queryCache.invalidateQueries({ key: ['snapshots', props.scope, ds] }),
    queryCache.invalidateQueries({ key: ['dataset-holds', props.scope, ds] }),
    // `usedbysnapshots` on the parent moves with every create/destroy.
    queryCache.invalidateQueries({ key: ['datasets', props.scope] }),
  ])
}

function refreshSnapshots() {
  void invalidateDataset()
}

// ── Detail slideover ────────────────────────────────────────────
const detailOpen = ref(false)
const detailSnap = ref<SnapshotRow | null>(null)
const newHoldTag = ref('')

function openDetail(s: SnapshotRow) {
  detailSnap.value = s
  newHoldTag.value = ''
  detailOpen.value = true
}

const detailHolds = computed(() => (detailSnap.value ? holdsFor(detailSnap.value.tag) : []))

// ── Mutations ───────────────────────────────────────────────────
const holdMutation = useMutation({
  mutation: ({ tag, holdTag }: { tag: string; holdTag: string }) =>
    createHold({
      path: { name: dataset.value, snapshot: tag },
      body: { tag: holdTag },
      baseUrl: baseUrl(),
    }).then(unwrap),
  onSuccess: (_d, { tag }) => toaster.success(onHost(`Held ${dataset.value}@${tag}`)),
  onError: (e, { tag }) => toaster.failure(onHost(`Holding ${dataset.value}@${tag} failed`), e),
  onSettled: invalidateDataset,
})

const releaseMutation = useMutation({
  mutation: ({ tag, holdTag }: { tag: string; holdTag: string }) =>
    releaseHold({
      path: { name: dataset.value, snapshot: tag, tag: holdTag },
      baseUrl: baseUrl(),
    }).then(unwrap),
  onSuccess: (_d, { holdTag }) => toaster.success(onHost(`Released ${holdTag}`)),
  onError: (e, { holdTag }) => toaster.failure(onHost(`Releasing ${holdTag} failed`), e),
  onSettled: invalidateDataset,
})

const createMutation = useMutation({
  mutation: ({ name, recursive }: { name: string; recursive: boolean }) =>
    createSnapshot({
      path: { name: dataset.value },
      body: { snapshot_name: name, recursive },
      baseUrl: baseUrl(),
    }).then(unwrap),
  onSuccess: (_d, { name }) => toaster.success(onHost(`Created ${dataset.value}@${name}`)),
  onError: (e, { name }) => {
    if (apiErrorCode(e) === 'snapshot_exists') {
      toaster.report({
        title: `${dataset.value}@${name} already exists`,
        description: 'Pick another name, or destroy the existing snapshot first.',
        tone: 'warning',
      })
      return
    }
    toaster.failure(onHost(`Creating ${dataset.value}@${name} failed`), e)
  },
  onSettled: invalidateDataset,
})

const destroyMutation = useMutation({
  mutation: ({ tag }: { tag: string; silent?: boolean }) =>
    destroySnapshot({
      path: { name: dataset.value, snapshot: tag },
      baseUrl: baseUrl(),
    }).then(unwrap),
  onSuccess: (_d, { tag, silent }) => {
    if (!silent) toaster.success(onHost(`Destroyed ${dataset.value}@${tag}`))
  },
  onError: (e, { tag }) => {
    // Surface the lock itself rather than the daemon's raw error: the
    // holds are already loaded, so name the tags that block the destroy.
    if (apiErrorCode(e) === 'snapshot_held') {
      const tags = holdsFor(tag).map((x) => x.tag)
      toaster.failure(`Cannot destroy ${dataset.value}@${tag}`, {
        message: `Held by ${tags.length || 'unknown'} tag(s)${
          tags.length ? ` — ${tags.join(', ')}` : ''
        }. Release them before destroying.`,
      })
      return
    }
    toaster.failure(onHost(`Destroying ${dataset.value}@${tag} failed`), e)
  },
  onSettled: invalidateDataset,
})

async function addHold() {
  const s = detailSnap.value
  const tag = newHoldTag.value.trim()
  if (!s || !tag) return
  await holdMutation.mutateAsync({ tag: s.tag, holdTag: tag })
  newHoldTag.value = ''
}

function releaseHoldTag(holdTag: string) {
  const s = detailSnap.value
  if (!s) return
  releaseMutation.mutate({ tag: s.tag, holdTag })
}

// ── Create / destroy ────────────────────────────────────────────
const createOpen = ref(false)

function confirmCreate(payload: { name: string; recursive: boolean }) {
  createMutation.mutate(payload)
}

const destroyOpen = ref(false)
const destroyTarget = ref<string | null>(null)

function askDestroy(tag: string) {
  destroyTarget.value = `${dataset.value}@${tag}`
  destroyOpen.value = true
}

function confirmDestroy(full: string) {
  destroyMutation.mutate({ tag: tagOf(full) })
}

const bulkOpen = ref(false)
const selectedTags = computed(() =>
  Object.entries(rowSelection.value)
    .filter(([, v]) => v)
    .map(([k]) => k),
)

async function confirmBulkDestroy() {
  bulkOpen.value = false
  const tags = selectedTags.value
  let failed = 0
  for (const tag of tags) {
    try {
      await destroyMutation.mutateAsync({ tag, silent: true })
    } catch {
      failed += 1
    }
  }
  rowSelection.value = {}
  // One summary instead of N toasts; failures already toasted their own
  // reason, so this only has to report the count.
  if (failed === 0) {
    toaster.success(onHost(`Destroyed ${tags.length} snapshots`))
  } else {
    toaster.report({
      title: onHost(`Destroyed ${tags.length - failed} of ${tags.length} snapshots`),
      description: `${failed} could not be destroyed.`,
      tone: 'warning',
    })
  }
}

const bulkBusy = computed(() => destroyMutation.isLoading.value)

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
      const hs = holdsFor(row.original.tag)
      if (hs.length === 0) return ''
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
            hostLabel
              ? `The tree shows what ${hostLabel}'s ACL allows this host to see.`
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
              <UButton size="xs" variant="soft" icon="i-lucide-camera" @click="createOpen = true">
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
      :description="`${selectedTags.length} snapshots will be permanently removed${hostLabel ? ` on ${hostLabel}` : ''}.`"
    >
      <template #body>
        <ul class="font-mono text-xs space-y-1 max-h-64 overflow-y-auto">
          <li v-for="t in selectedTags" :key="t" class="flex items-center gap-2">
            <UIcon
              v-if="holdsFor(t).length > 0"
              name="i-lucide-lock"
              class="text-warning shrink-0"
            />
            <span class="break-all">{{ dataset }}@{{ t }}</span>
          </li>
        </ul>
        <p
          v-if="selectedTags.some((t) => holdsFor(t).length > 0)"
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
            <div v-if="detailSnap.properties?.guid" class="col-span-2">
              <div class="microlabel mb-1">guid</div>
              <code class="font-mono text-xs">{{ detailSnap.properties.guid }}</code>
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
