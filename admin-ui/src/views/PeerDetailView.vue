<script setup lang="ts">
import { computed, h, ref, resolveComponent, watch } from 'vue'
import { useRoute } from 'vue-router'
import type { TableColumn } from '@nuxt/ui'
import {
  destroyPeerSnapshot,
  listPeerDatasets,
  listPeerJobs,
  listPeerSnapshots,
  wakeupPeerJob,
} from '../client'
import type { DatasetSummary, JobStatus, PeerSnapshotEntry } from '../client'
import { formatNextRun, formatRelative } from '../utils/format'
import { jobStatus } from '../utils/status'
import { apiErrorMessage, useMutation } from '../composables/useMutation'
import { useEvents } from '../composables/useEvents'
import EventsLog from '../components/EventsLog.vue'
import DestroySnapshotModal from '../components/DestroySnapshotModal.vue'

const route = useRoute()
const peer = computed(() => String(route.params.peer))
const tab = computed(() => String(route.params.tab ?? 'jobs'))
const { mutate } = useMutation()

const jobs = ref<JobStatus[]>([])
const error = ref<string | null>(null)
const loading = ref(true)

async function loadJobs() {
  const r = await listPeerJobs({ path: { peer: peer.value } })
  if (r.error) error.value = apiErrorMessage(r.error)
  else {
    jobs.value = r.data ?? []
    error.value = null
  }
}

// Receiver dataset browser: the daemon proxies a root_fs-scoped listing
// over the control channel, so the operator picks instead of typing.
const datasets = ref<DatasetSummary[]>([])
const dataset = ref<string>('')
const snapshots = ref<PeerSnapshotEntry[]>([])
const snapsLoading = ref(false)

async function loadDatasets() {
  const r = await listPeerDatasets({ path: { peer: peer.value } })
  if (r.error) error.value = apiErrorMessage(r.error)
  else {
    datasets.value = r.data ?? []
    error.value = null
  }
}

async function loadSnapshots() {
  if (!dataset.value) {
    snapshots.value = []
    return
  }
  snapsLoading.value = true
  const r = await listPeerSnapshots({
    path: { peer: peer.value },
    query: { dataset: dataset.value },
  })
  if (r.error) error.value = apiErrorMessage(r.error)
  else {
    snapshots.value = r.data ?? []
    error.value = null
  }
  snapsLoading.value = false
}

const datasetItems = computed(() =>
  datasets.value
    .map((d) => d.name)
    .slice()
    .sort((a, b) => a.localeCompare(b)),
)

async function refresh() {
  error.value = null
  if (tab.value === 'jobs') await loadJobs()
  else {
    await loadDatasets()
    await loadSnapshots()
  }
  loading.value = false
}

async function wake(name: string) {
  await mutate(`Woke up ${name} on ${peer.value}`, () =>
    wakeupPeerJob({ path: { peer: peer.value, name } }),
  )
  await loadJobs()
}

// Destroy with type-to-confirm; the receiver's ACL and holds still have
// the final word (403/409 surface as toasts).
const destroyOpen = ref(false)
const destroyTarget = ref<string | null>(null)

function askDestroy(name: string) {
  destroyTarget.value = `${dataset.value}@${name}`
  destroyOpen.value = true
}

async function confirmDestroy(full: string) {
  const at = full.indexOf('@')
  const snapName = at >= 0 ? full.slice(at + 1) : full
  await mutate(`Destroyed ${full} on ${peer.value}`, () =>
    destroyPeerSnapshot({
      path: { peer: peer.value, name: `${dataset.value}@${snapName}` },
    }),
  )
  await loadSnapshots()
}

watch([peer, tab], () => void refresh(), { immediate: true })
watch(dataset, () => void loadSnapshots())

const { events } = useEvents({ peer, cap: 200 })
const tail = computed(() => events.value.slice(-30))

const UButton = resolveComponent('UButton')

const snapColumns = computed<TableColumn<PeerSnapshotEntry>[]>(() => [
  {
    accessorKey: 'name',
    header: 'Name',
    cell: ({ row }) => h('span', { class: 'font-mono' }, row.original.name),
  },
  {
    accessorKey: 'guid',
    header: 'GUID',
    cell: ({ row }) => h('span', { class: 'font-mono text-xs text-muted' }, row.original.guid),
  },
  { accessorKey: 'createtxg', header: 'createtxg' },
  {
    id: 'actions',
    header: '',
    cell: ({ row }) =>
      h('div', { class: 'flex justify-end' }, [
        h(UButton, {
          size: 'xs',
          color: 'error',
          variant: 'ghost',
          icon: 'i-lucide-trash-2',
          'aria-label': 'Destroy snapshot on peer',
          onClick: () => askDestroy(row.original.name),
        }),
      ]),
  },
])
</script>

<template>
  <UDashboardPanel id="peer-detail">
    <template #header>
      <UDashboardNavbar :title="peer">
        <template #leading>
          <UButton
            to="/peers"
            icon="i-lucide-arrow-left"
            variant="ghost"
            color="neutral"
            size="sm"
            aria-label="Back to peers"
          />
        </template>
      </UDashboardNavbar>
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-6">
        <UTabs
          :items="[
            {
              label: 'Jobs',
              value: 'jobs',
              to: `/peers/${peer}/jobs`,
              icon: 'i-lucide-list-checks',
            },
            {
              label: 'Snapshots',
              value: 'snapshots',
              to: `/peers/${peer}/snapshots`,
              icon: 'i-lucide-camera',
            },
          ]"
          :model-value="tab"
          color="neutral"
          variant="link"
        />

        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />

        <template v-if="tab === 'jobs'">
          <div v-if="loading && jobs.length === 0" class="text-muted text-sm">Loading…</div>
          <UEmpty
            v-else-if="jobs.length === 0"
            icon="i-lucide-list-checks"
            title="Peer reports no jobs"
            description="The peer's own daemon may be stopped — job status is proxied through it."
          />
          <div v-else class="space-y-2">
            <UCard v-for="j in jobs" :key="j.name" :class="jobStatus(j).rail">
              <div class="flex items-center justify-between">
                <div class="min-w-0">
                  <div class="flex items-center gap-2">
                    <span class="font-semibold font-mono">{{ j.name }}</span>
                    <UBadge color="neutral" variant="outline" size="sm">{{ j.kind }}</UBadge>
                    <UBadge
                      :color="jobStatus(j).color"
                      variant="subtle"
                      size="sm"
                      :icon="jobStatus(j).icon"
                    >
                      {{ jobStatus(j).label }}
                    </UBadge>
                  </div>
                  <div class="text-xs text-muted mt-1">
                    last {{ formatRelative(j.last_run) }} · next
                    {{ formatNextRun(j.next_run, j.running) }}
                  </div>
                  <div v-if="j.last_error" class="text-error text-xs mt-1 break-all">
                    {{ j.last_error }}
                  </div>
                </div>
                <UButton
                  icon="i-lucide-alarm-clock"
                  size="xs"
                  variant="soft"
                  class="shrink-0"
                  @click="wake(j.name)"
                >
                  Wake up
                </UButton>
              </div>
            </UCard>
          </div>
        </template>

        <template v-else>
          <div class="flex gap-2 items-center flex-wrap">
            <USelectMenu
              v-model="dataset"
              :items="datasetItems"
              placeholder="Pick a dataset on the peer…"
              icon="i-lucide-database"
              class="min-w-80 font-mono"
              :search-input="{ placeholder: 'Filter datasets…' }"
            />
            <UButton
              variant="soft"
              icon="i-lucide-refresh-cw"
              :loading="snapsLoading"
              @click="loadSnapshots"
            >
              Refresh
            </UButton>
            <span v-if="dataset" class="text-xs text-muted font-mono">
              {{ snapshots.length }} snapshots
            </span>
          </div>
          <UEmpty
            v-if="!dataset"
            icon="i-lucide-database"
            title="Pick a dataset"
            description="The list shows what the peer's ACL allows this host to see."
          />
          <UEmpty
            v-else-if="snapshots.length === 0 && !snapsLoading"
            icon="i-lucide-camera-off"
            title="No snapshots"
          />
          <UTable
            v-else
            :data="snapshots"
            :columns="snapColumns"
            :loading="snapsLoading"
            class="rounded-md border border-default bg-default"
          />
        </template>

        <div>
          <div class="microlabel mb-2">live events from peer</div>
          <EventsLog :events="tail" max-height-class="max-h-64" />
        </div>
      </div>

      <DestroySnapshotModal
        v-model:open="destroyOpen"
        :snapshot-name="destroyTarget"
        @confirm="confirmDestroy"
      />
    </template>
  </UDashboardPanel>
</template>
