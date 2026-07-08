<script setup lang="ts">
import { computed } from 'vue'
import { useJobs } from '../composables/useJobs'
import { useEvents } from '../composables/useEvents'
import { useArc } from '../composables/useArc'
import { usePools } from '../composables/usePools'
import { usePeers } from '../composables/usePeers'
import { poolUsedPercent } from '../utils/pool'
import { peerStatus, poolStatus } from '../utils/status'
import JobsGrid from '../components/JobsGrid.vue'
import EventsLog from '../components/EventsLog.vue'

const { jobs, error, loading, wake, cancel, pause, resume, pushTo } = useJobs()
const { events, connected } = useEvents({ cap: 200 })
const { arc } = useArc(5000)
const { pools } = usePools()
const { peers } = usePeers()

const tail = computed(() => events.value.slice(-50))

const arcRatio = computed(() => {
  const r = arc.value?.hit_ratio
  return r == null ? null : Math.round(r * 1000) / 10
})

const jobsSummary = computed(() => {
  const failing = jobs.value.filter((j) => j.last_error).length
  const running = jobs.value.filter((j) => j.running).length
  return { total: jobs.value.length, failing, running }
})
</script>

<template>
  <UDashboardPanel id="dashboard">
    <template #header>
      <UDashboardNavbar title="Dashboard">
        <template #right>
          <UBadge
            :color="connected ? 'success' : 'neutral'"
            variant="subtle"
            :icon="connected ? 'i-lucide-signal' : 'i-lucide-signal-zero'"
          >
            {{ connected ? 'live' : 'idle' }}
          </UBadge>
        </template>
      </UDashboardNavbar>
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-6">
        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />

        <!-- Stat tiles -->
        <div class="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <UCard
            v-for="p in pools"
            :key="p.name"
            :class="poolStatus(p.state).rail"
            :ui="{ body: 'p-4 sm:p-4' }"
          >
            <RouterLink :to="`/pools/${encodeURIComponent(p.name)}`" class="block">
              <div class="microlabel mb-1">pool · {{ p.state }}</div>
              <div class="font-mono font-semibold truncate">{{ p.name }}</div>
              <div class="mt-2 flex items-center gap-2">
                <UProgress
                  :model-value="poolUsedPercent(p.alloc_space, p.total_space)"
                  size="xs"
                  :color="poolStatus(p.state).color"
                />
                <span class="text-xs text-muted whitespace-nowrap font-mono">
                  {{ p.alloc_space }}/{{ p.total_space }}
                </span>
              </div>
            </RouterLink>
          </UCard>

          <UCard :ui="{ body: 'p-4 sm:p-4' }" class="rail rail-info">
            <RouterLink to="/arc" class="block">
              <div class="microlabel mb-1">arc hit ratio</div>
              <div class="text-2xl font-semibold font-mono">
                <template v-if="arcRatio != null">{{ arcRatio }}%</template>
                <template v-else>—</template>
              </div>
              <div class="text-xs text-muted mt-1">
                {{ arc ? `${(arc.size / 2 ** 30).toFixed(1)} GiB cached` : '…' }}
              </div>
            </RouterLink>
          </UCard>

          <UCard
            :ui="{ body: 'p-4 sm:p-4' }"
            :class="jobsSummary.failing ? 'rail rail-err' : 'rail rail-ok'"
          >
            <RouterLink to="/jobs" class="block">
              <div class="microlabel mb-1">jobs</div>
              <div class="text-2xl font-semibold font-mono">{{ jobsSummary.total }}</div>
              <div class="text-xs mt-1" :class="jobsSummary.failing ? 'text-error' : 'text-muted'">
                {{ jobsSummary.failing }} failing · {{ jobsSummary.running }} running
              </div>
            </RouterLink>
          </UCard>

          <UCard
            v-for="p in peers"
            :key="p.name"
            :ui="{ body: 'p-4 sm:p-4' }"
            :class="peerStatus(p.reachability).rail"
          >
            <RouterLink :to="`/peers/${encodeURIComponent(p.name)}/jobs`" class="block">
              <div class="microlabel mb-1">peer · {{ peerStatus(p.reachability).label }}</div>
              <div class="font-mono font-semibold truncate">{{ p.name }}</div>
              <div class="text-xs text-muted mt-1 truncate">
                <template v-if="p.active_route">via {{ p.active_route }}</template>
                <template v-else>no active route</template>
              </div>
            </RouterLink>
          </UCard>
        </div>

        <!-- Jobs -->
        <div>
          <div class="microlabel mb-2">replication jobs</div>
          <div v-if="loading && jobs.length === 0" class="text-muted text-sm">Loading…</div>
          <UEmpty
            v-else-if="jobs.length === 0"
            icon="i-lucide-list-checks"
            title="No jobs configured"
            description="Add snap/push/prune jobs to /etc/arctern/arctern.toml and restart the daemon."
          />
          <JobsGrid
            v-else
            :jobs="jobs"
            :on-wake="wake"
            :on-cancel="cancel"
            :on-pause="pause"
            :on-resume="resume"
            :on-push-to="pushTo"
          />
        </div>

        <!-- Events tail -->
        <div>
          <div class="microlabel mb-2">recent events</div>
          <EventsLog :events="tail" max-height-class="max-h-80" />
        </div>
      </div>
    </template>
  </UDashboardPanel>
</template>
