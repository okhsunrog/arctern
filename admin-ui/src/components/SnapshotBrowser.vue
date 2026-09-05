<script setup lang="ts">
import { computed, h, ref, resolveComponent, watch } from 'vue'
import { useMutation, useQuery, useQueryCache } from '@pinia/colada'
import type { TableColumn } from '@nuxt/ui'
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
import BulkDestroySnapshotModal from './BulkDestroySnapshotModal.vue'
import DatasetTree from './DatasetTree.vue'

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

const rowSelection = ref<Record<string, boolean>>({})
watch(dataset, () => {
  rowSelection.value = {}
})

const selectedSummary = computed(() => datasets.value.find((d) => d.name === dataset.value))
const selectedUsedBySnapshots = computed(() => {
  const n = Number(selectedSummary.value?.properties?.usedbysnapshots ?? NaN)
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

// Every hold on the dataset in ONE request. Asking per snapshot turned a
// 15s refresh of a dataset with hundreds of snapshots into hundreds of
// `zfs holds` spawns — through the SSH control channel for a peer.
// A tag absent from the map has no holds; the response covers the whole
// dataset, so absence is an answer rather than a gap.
const holdsResult = useQuery(() =>
  datasetHoldsQuery({ scope: props.scope, dataset: dataset.value }),
)
const holdsError = computed(() => holdsResult.error.value?.message ?? null)
const holdsKnown = computed(() => !!holdsResult.data.value && !holdsResult.error.value)
const holds = computed<Record<string, SnapshotHold[]>>(() => holdsResult.data.value ?? {})
function holdsFor(tag: string): SnapshotHold[] {
  return holds.value[tag] ?? []
}

// Sum of listed snapshot `used` — the quick "who eats space" readout
// next to the authoritative usedbysnapshots property.
const snapshotsUsedSum = computed(() => snapshots.value.reduce((acc, s) => acc + (s.used ?? 0), 0))

// Every mutation carries the dataset it was issued against. Reading
// `dataset.value` at execution time meant a bulk destroy — which loops
// with the tree still clickable — would follow the selection: click
// another dataset mid-loop and the remaining tags are destroyed THERE.
// Snap jobs give every dataset identically named snapshots, so a
// same-named victim is the norm, not a coincidence.
function invalidateDataset(ds: string) {
  return Promise.all([
    queryCache.invalidateQueries({ key: ['snapshots', props.scope, ds] }),
    queryCache.invalidateQueries({ key: ['dataset-holds', props.scope, ds] }),
    // `usedbysnapshots` on the parent moves with every create/destroy.
    queryCache.invalidateQueries({ key: ['datasets', props.scope] }),
  ])
}

function refreshSnapshots() {
  void invalidateDataset(dataset.value)
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
interface OnDataset {
  ds: string
}

const holdMutation = useMutation({
  mutation: ({ ds, tag, holdTag }: OnDataset & { tag: string; holdTag: string }) =>
    createHold({
      path: { name: ds, snapshot: tag },
      body: { tag: holdTag },
      baseUrl: baseUrl(),
    }).then(unwrap),
  onSuccess: (_d, { ds, tag }) => toaster.success(onHost(`Held ${ds}@${tag}`)),
  onError: (e, { ds, tag }) => toaster.failure(onHost(`Holding ${ds}@${tag} failed`), e),
  onSettled: (_d, _e, { ds }) => invalidateDataset(ds),
})

const releaseMutation = useMutation({
  mutation: ({ ds, tag, holdTag }: OnDataset & { tag: string; holdTag: string }) =>
    releaseHold({
      path: { name: ds, snapshot: tag, tag: holdTag },
      baseUrl: baseUrl(),
    }).then(unwrap),
  onSuccess: (_d, { holdTag }) => toaster.success(onHost(`Released ${holdTag}`)),
  onError: (e, { holdTag }) => toaster.failure(onHost(`Releasing ${holdTag} failed`), e),
  onSettled: (_d, _e, { ds }) => invalidateDataset(ds),
})

const createMutation = useMutation({
  mutation: ({ ds, name, recursive }: OnDataset & { name: string; recursive: boolean }) =>
    createSnapshot({
      path: { name: ds },
      body: { snapshot_name: name, recursive },
      baseUrl: baseUrl(),
    }).then(unwrap),
  onSuccess: (_d, { ds, name }) => toaster.success(onHost(`Created ${ds}@${name}`)),
  onError: (e, { ds, name }) => {
    if (apiErrorCode(e) === 'snapshot_exists') {
      toaster.report({
        title: `${ds}@${name} already exists`,
        description: 'Pick another name, or destroy the existing snapshot first.',
        tone: 'warning',
      })
      return
    }
    toaster.failure(onHost(`Creating ${ds}@${name} failed`), e)
  },
  onSettled: (_d, _e, { ds }) => invalidateDataset(ds),
})

const destroyMutation = useMutation({
  mutation: ({ ds, tag }: OnDataset & { tag: string; silent?: boolean }) =>
    destroySnapshot({
      path: { name: ds, snapshot: tag },
      baseUrl: baseUrl(),
    }).then(unwrap),
  onSuccess: (_d, { ds, tag, silent }) => {
    if (!silent) toaster.success(onHost(`Destroyed ${ds}@${tag}`))
  },
  onError: (e, { ds, tag }) => {
    // Surface the lock itself rather than the daemon's raw error: the
    // holds are already loaded, so name the tags that block the destroy.
    if (apiErrorCode(e) === 'snapshot_held') {
      const tags = holdsFor(tag).map((x) => x.tag)
      toaster.failure(`Cannot destroy ${ds}@${tag}`, {
        message: `Held by ${tags.length || 'unknown'} tag(s)${
          tags.length ? ` — ${tags.join(', ')}` : ''
        }. Release them before destroying.`,
      })
      return
    }
    toaster.failure(onHost(`Destroying ${ds}@${tag} failed`), e)
  },
  onSettled: (_d, _e, { ds }) => invalidateDataset(ds),
})

async function addHold() {
  const s = detailSnap.value
  const tag = newHoldTag.value.trim()
  if (!s || !tag) return
  await holdMutation.mutateAsync({ ds: dataset.value, tag: s.tag, holdTag: tag })
  newHoldTag.value = ''
}

function releaseHoldTag(holdTag: string) {
  const s = detailSnap.value
  if (!s) return
  releaseMutation.mutate({ ds: dataset.value, tag: s.tag, holdTag })
}

// ── Create / destroy ────────────────────────────────────────────
const createOpen = ref(false)

function confirmCreate(payload: { name: string; recursive: boolean }) {
  createMutation.mutate({ ds: dataset.value, ...payload })
}

const destroyOpen = ref(false)
const destroyTarget = ref<string | null>(null)

function askDestroy(tag: string) {
  destroyTarget.value = `${dataset.value}@${tag}`
  destroyOpen.value = true
}

function confirmDestroy(full: string) {
  const at = full.indexOf('@')
  destroyMutation.mutate({ ds: full.slice(0, at), tag: full.slice(at + 1) })
}

const bulkOpen = ref(false)
const bulkTarget = ref({ ds: '', tags: [] as string[] })
function askBulkDestroy() {
  bulkTarget.value = { ds: dataset.value, tags: [...selectedTags.value] }
  bulkOpen.value = true
}
const selectedTags = computed(() =>
  Object.entries(rowSelection.value)
    .filter(([, v]) => v)
    .map(([k]) => k),
)

async function confirmBulkDestroy() {
  bulkOpen.value = false
  // Pinned once: the loop below awaits, and the tree stays clickable.
  const { ds, tags } = bulkTarget.value
  let failed = 0
  for (const tag of tags) {
    try {
      await destroyMutation.mutateAsync({ ds, tag, silent: true })
    } catch {
      failed += 1
    }
  }
  rowSelection.value = {}
  // One summary instead of N toasts; failures already toasted their own
  // reason, so this only has to report the count.
  if (failed === 0) {
    toaster.success(onHost(`Destroyed ${tags.length} snapshots in ${ds}`))
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

const compact = ref(true)
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
        'aria-label': 'Select all snapshots',
      }),
    cell: ({ row }) =>
      h(UCheckbox, {
        modelValue: row.getIsSelected(),
        'onUpdate:modelValue': (v: boolean | 'indeterminate') => row.toggleSelected(!!v),
        'aria-label': `Select ${row.original.tag}`,
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
      if (!holdsKnown.value) return holdsError.value ? 'Unavailable' : 'Loading…'
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
      <DatasetTree
        v-model="dataset"
        :datasets="datasets"
        :loading="dsLoading"
        @refresh="refreshDatasets"
      />

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
          <UAlert
            v-if="holdsError"
            color="warning"
            title="Snapshot holds unavailable"
            :description="holdsError"
          />
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
                :loading="bulkBusy"
                :disabled="bulkBusy"
                @click="askBulkDestroy"
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
          <div class="flex justify-between gap-3 text-xs text-muted">
            <span>{{ selectedTags.length }} selected across all snapshots</span>
            <USwitch v-model="compact" label="Compact rows" size="sm" />
          </div>
          <UTable
            :key="compact ? 'compact' : 'comfortable'"
            sticky
            :virtualize="{ estimateSize: compact ? 40 : 60 }"
            :ui="{ td: compact ? 'px-3 py-1.5' : 'px-3 py-3', th: 'px-3 py-2' }"
            :aria-label="`Snapshots in ${dataset}`"
            v-model:row-selection="rowSelection"
            v-model:sorting="sorting"
            :data="snapshots"
            :columns="columns"
            :get-row-id="(r: SnapshotRow) => r.tag"
            :loading="snapsLoading && snapshots.length === 0"
            class="max-h-[65vh] rounded-md border border-default bg-default"
          />
        </template>
      </div>
    </div>

    <CreateSnapshotModal
      :host="hostLabel || 'this host'"
      v-model:open="createOpen"
      :dataset="dataset"
      @confirm="confirmCreate"
    />
    <DestroySnapshotModal
      :host="hostLabel || 'this host'"
      v-model:open="destroyOpen"
      :snapshot-name="destroyTarget"
      @confirm="confirmDestroy"
    />

    <BulkDestroySnapshotModal
      v-model:open="bulkOpen"
      :dataset="bulkTarget.ds"
      :snapshots="bulkTarget.tags"
      :host="hostLabel || 'this host'"
      :loading="bulkBusy"
      @confirm="confirmBulkDestroy"
    />

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
            <p v-if="!holdsKnown" role="status" class="text-muted text-xs">
              {{ holdsError ? `Holds unavailable: ${holdsError}` : 'Loading holds…' }}
            </p>
            <div v-else-if="detailHolds.length === 0" class="text-muted text-xs">
              No holds reported. Other ZFS constraints may still prevent deletion.
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
