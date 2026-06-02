<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { listPeerJobs, listPeerSnapshots, wakeupPeerJob } from '../client'
import type { JobStatus, PeerSnapshotEntry } from '../client'
import { formatRelative } from '../utils/format'
import { useEvents } from '../composables/useEvents'
import EventsLog from '../components/EventsLog.vue'

const route = useRoute()
const peer = computed(() => String(route.params.peer))
const tab = computed(() => String(route.params.tab ?? 'jobs'))

const jobs = ref<JobStatus[]>([])
const snapshots = ref<PeerSnapshotEntry[]>([])
const error = ref<string | null>(null)
const loading = ref(true)

function setError(e: unknown) {
  if (e && typeof e === 'object' && 'message' in e) {
    error.value = String((e as { message: unknown }).message)
  } else {
    error.value = String(e)
  }
}

async function loadJobs() {
  const r = await listPeerJobs({ path: { peer: peer.value } })
  if (r.error) setError(r.error)
  else jobs.value = r.data ?? []
}

const datasetInput = ref('')

async function loadSnapshots() {
  if (!datasetInput.value) {
    snapshots.value = []
    return
  }
  const r = await listPeerSnapshots({
    path: { peer: peer.value },
    query: { dataset: datasetInput.value },
  })
  if (r.error) setError(r.error)
  else snapshots.value = r.data ?? []
}

async function refresh() {
  error.value = null
  if (tab.value === 'jobs') await loadJobs()
  else await loadSnapshots()
  loading.value = false
}

async function wake(name: string) {
  const r = await wakeupPeerJob({ path: { peer: peer.value, name } })
  if (r.error) setError(r.error)
  await loadJobs()
}

watch([peer, tab], () => void refresh(), { immediate: true })

const { events } = useEvents({ peer: peer.value, cap: 200 })
const tail = computed(() => events.value.slice(-30))
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6 space-y-6">
    <div>
      <RouterLink to="/peers" class="text-sm text-primary-500 hover:underline">
        ← Peers
      </RouterLink>
      <h1 class="text-2xl font-semibold mt-1">{{ peer }}</h1>
    </div>

    <UTabs
      :items="[
        { label: 'Jobs', value: 'jobs', to: `/peers/${peer}/jobs` },
        { label: 'Snapshots', value: 'snapshots', to: `/peers/${peer}/snapshots` },
      ]"
      :model-value="tab"
    />

    <UAlert v-if="error" color="error" :title="error" />

    <template v-if="tab === 'jobs'">
      <div v-if="loading && jobs.length === 0" class="text-gray-500">Loading…</div>
      <div v-else-if="jobs.length === 0" class="text-gray-500">Peer reports no jobs.</div>
      <div v-else class="space-y-2">
        <UCard v-for="j in jobs" :key="j.name">
          <div class="flex items-center justify-between">
            <div>
              <div class="font-semibold">{{ j.name }}</div>
              <div class="text-xs text-gray-500">
                {{ j.kind }} · last {{ formatRelative(j.last_run) }} · next
                {{ formatRelative(j.next_run) }}
              </div>
              <div v-if="j.last_error" class="text-error-600 text-xs mt-1">
                {{ j.last_error }}
              </div>
            </div>
            <UButton icon="i-lucide-zap" size="xs" variant="soft" @click="wake(j.name)">
              Wakeup
            </UButton>
          </div>
        </UCard>
      </div>
    </template>

    <template v-else>
      <div class="flex gap-2 mb-3">
        <UInput
          v-model="datasetInput"
          placeholder="dataset path on peer (e.g. tank/backups/laptop)"
          class="flex-1"
          @keydown.enter="loadSnapshots"
        />
        <UButton @click="loadSnapshots">Query</UButton>
      </div>
      <div v-if="!datasetInput" class="text-gray-500">
        Enter a dataset path on the peer to list its snapshots.
      </div>
      <div v-else-if="snapshots.length === 0" class="text-gray-500">No snapshots match.</div>
      <UTable
        v-else
        :data="snapshots"
        :columns="[
          { accessorKey: 'name', header: 'Name' },
          { accessorKey: 'guid', header: 'GUID' },
          { accessorKey: 'createtxg', header: 'createtxg' },
        ]"
      />
    </template>

    <div>
      <h2 class="text-lg font-semibold mb-2">Recent events from peer</h2>
      <EventsLog :events="tail" max-height-class="max-h-64" />
    </div>
  </div>
</template>
