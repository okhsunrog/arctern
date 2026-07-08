<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { listPeerJobs, wakeupPeerJob } from '../client'
import type { JobStatus } from '../client'
import { formatNextRun, formatRelative } from '../utils/format'
import { jobStatus } from '../utils/status'
import { apiErrorMessage, useMutation } from '../composables/useMutation'
import { peerSource } from '../composables/snapshotSources'
import { useEvents } from '../composables/useEvents'
import EventsLog from '../components/EventsLog.vue'
import SnapshotBrowser from '../components/SnapshotBrowser.vue'

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

async function refresh() {
  error.value = null
  if (tab.value === 'jobs') await loadJobs()
  loading.value = false
}

async function wake(name: string) {
  await mutate(`Woke up ${name} on ${peer.value}`, () =>
    wakeupPeerJob({ path: { peer: peer.value, name } }),
  )
  await loadJobs()
}

watch([peer, tab], () => void refresh(), { immediate: true })

// The SAME browser component as the local Snapshots view — a peer's
// data source proxies over the control channel, capabilities shrink
// (no create / hold management), the interaction stays identical.
const source = computed(() => peerSource(peer.value))

const { events } = useEvents({ peer, cap: 200 })
const tail = computed(() => events.value.slice(-30))
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

        <SnapshotBrowser v-else :key="peer" :source="source" />

        <div>
          <div class="microlabel mb-2">live events from peer</div>
          <EventsLog :events="tail" max-height-class="max-h-64" />
        </div>
      </div>
    </template>
  </UDashboardPanel>
</template>
