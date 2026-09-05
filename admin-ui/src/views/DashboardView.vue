<script setup lang="ts">
import { computed } from 'vue'
import { useHost } from '../composables/useHost'
import { useJobs } from '../composables/useJobs'
import { jobFailureMessage } from '../utils/status'
import { useEvents } from '../composables/useEvents'
import { useArc } from '../composables/useArc'
import { usePools } from '../composables/usePools'
import { usePeers } from '../composables/usePeers'
import { poolUsedPercent } from '../utils/pool'
import { peerStatus, poolStatus } from '../utils/status'
import JobsGrid from '../components/JobsGrid.vue'
import EventsLog from '../components/EventsLog.vue'

const { host, scope, prefix } = useHost()
const {
  jobs,
  error,
  warning,
  loading,
  wake,
  cancel,
  pause,
  resume,
  pushTo,
  isWaking,
  isCancelling,
  isPausing,
  isResuming,
  isPushing,
} = useJobs(scope)
const { events, connected, error: eventsError } = useEvents(scope)
const { arc, error: arcError } = useArc(scope)
const { pools, error: poolsError } = usePools(scope)
// The peer tiles describe THIS host's outbound links; inside a peer's
// console they would show the peer's own (usually empty) peer list.
const { peers, error: peersError } = usePeers()
const title = computed(() => (host.value ? `${host.value} · Dashboard` : 'Dashboard'))

const tail = computed(() => events.value.slice(-50))

const arcRatio = computed(() => {
  const r = arc.value?.hit_ratio
  return r == null ? null : Math.round(r * 1000) / 10
})

const jobsSummary = computed(() => {
  const failing = jobs.value.filter((j) => jobFailureMessage(j)).length
  const running = jobs.value.filter((j) => j.running).length
  return { total: jobs.value.length, failing, running }
})
</script>

<template>
  <UDashboardPanel id="dashboard">
    <template #header>
      <UDashboardNavbar :title="title">
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
        <UAlert v-if="warning" color="warning" :title="warning" icon="i-lucide-triangle-alert" />

        <UAlert
          v-if="poolsError"
          color="warning"
          title="Pools unavailable"
          :description="poolsError"
        />
        <UAlert v-if="arcError" color="warning" title="ARC unavailable" :description="arcError" />
        <UAlert
          v-if="!host && peersError"
          color="warning"
          title="Peer links unavailable"
          :description="peersError"
        />
        <!-- Stat tiles -->
        <div class="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <UCard
            v-for="p in pools"
            :key="p.name"
            :class="poolStatus(p.state).rail"
            :ui="{ body: 'p-4 sm:p-4' }"
          >
            <RouterLink :to="`${prefix}/pools/${encodeURIComponent(p.name)}`" class="block">
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
            <RouterLink :to="`${prefix}/arc`" class="block">
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
            :class="
              error || loading
                ? 'rail rail-idle'
                : jobsSummary.failing
                  ? 'rail rail-err'
                  : 'rail rail-ok'
            "
          >
            <RouterLink :to="`${prefix}/jobs`" class="block">
              <div class="microlabel mb-1">jobs</div>
              <div class="text-2xl font-semibold font-mono">
                {{ loading || error ? '—' : jobsSummary.total }}
              </div>
              <div class="text-xs mt-1" :class="jobsSummary.failing ? 'text-error' : 'text-muted'">
                <template v-if="error">Status unavailable</template
                ><template v-else-if="loading">Loading…</template
                ><template v-else
                  >{{ jobsSummary.failing }} failing · {{ jobsSummary.running }} running</template
                >
              </div>
            </RouterLink>
          </UCard>

          <UCard
            v-for="p in host ? [] : peers"
            :key="p.name"
            :ui="{ body: 'p-4 sm:p-4' }"
            :class="peerStatus(p.reachability).rail"
          >
            <RouterLink :to="`/h/${encodeURIComponent(p.name)}/dashboard`" class="block">
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
            v-else-if="jobs.length === 0 && !error"
            icon="i-lucide-list-checks"
            title="No jobs configured"
            description="Add snap/push/prune jobs to /etc/arctern/arctern.toml and restart the daemon."
          />
          <JobsGrid
            v-if="jobs.length"
            :jobs="jobs"
            :on-wake="wake"
            :on-cancel="cancel"
            :on-pause="pause"
            :on-resume="resume"
            :on-push-to="pushTo"
            :is-waking="isWaking"
            :is-cancelling="isCancelling"
            :is-pausing="isPausing"
            :is-resuming="isResuming"
            :is-pushing="isPushing"
          />
        </div>

        <!-- Events tail -->
        <div>
          <div class="microlabel mb-2">recent events</div>
          <UAlert v-if="eventsError" color="warning" :title="eventsError" class="mb-2" />
          <EventsLog :events="tail" max-height-class="max-h-80" />
        </div>
      </div>
    </template>
  </UDashboardPanel>
</template>
