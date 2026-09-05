<script setup lang="ts">
import { computed, defineAsyncComponent, h, resolveComponent } from 'vue'
import { useRoute } from 'vue-router'
import type { TableColumn } from '@nuxt/ui'
import { useHost } from '../composables/useHost'
import { useJobs } from '../composables/useJobs'
import { useJobRuns } from '../composables/useJobRuns'
import { formatBytes, formatTimestamp } from '../utils/format'
import { formatLastSync, formatSyncState } from '../utils/pushTimes'
import { jobStatus, runStatus } from '../utils/status'
import JobActions from '../components/JobActions.vue'
import TransferPanel from '../components/TransferPanel.vue'
import type { JobRun } from '../client'

// Load charts only after there is run history to display.
const RunsCharts = defineAsyncComponent(() => import('../components/RunsCharts.vue'))

const route = useRoute()
const name = computed(() => String(route.params.name))

const { host, scope, prefix } = useHost()
const {
  jobs,
  error: jobsError,
  loading: jobsLoading,
  warning: jobsWarning,
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
const job = computed(() => jobs.value.find((j) => j.name === name.value))

const { runs, error: runsError, loading: runsLoading } = useJobRuns(name, scope)

const UBadge = resolveComponent('UBadge')

const tableColumns = computed<TableColumn<JobRun>[]>(() => [
  {
    accessorKey: 'started_at',
    header: 'Started',
    cell: ({ row }) => formatTimestamp(new Date(row.original.started_at * 1000).toISOString()),
  },
  {
    id: 'duration',
    header: 'Duration',
    cell: ({ row }) => {
      const r = row.original
      if (!r.finished_at) return '—'
      return `${Math.max(0, r.finished_at - r.started_at)}s`
    },
  },
  {
    id: 'status',
    header: 'Status',
    cell: ({ row }) => {
      const s = runStatus(row.original.status)
      return h(UBadge, { color: s.color, variant: 'subtle', icon: s.icon }, () => s.label)
    },
  },
  {
    accessorKey: 'bytes_sent',
    header: 'Bytes',
    cell: ({ row }) => formatBytes(row.original.bytes_sent),
  },
  {
    accessorKey: 'error_message',
    header: 'Details',
    cell: ({ row }) => row.original.error_message ?? '',
  },
])
</script>

<template>
  <UDashboardPanel id="job-detail">
    <template #header>
      <UDashboardNavbar :title="host ? `${host} · ${name}` : name">
        <template #leading>
          <UButton
            :to="`${prefix}/jobs`"
            icon="i-lucide-arrow-left"
            variant="ghost"
            color="neutral"
            size="sm"
            aria-label="Back to jobs"
          />
        </template>
        <template #right>
          <UBadge
            v-if="job"
            :color="jobStatus(job).color"
            variant="subtle"
            :icon="jobStatus(job).icon"
          >
            {{ jobStatus(job).label }}
          </UBadge>
          <JobActions
            v-if="job"
            :job="job"
            variant="label"
            :on-wake="wake"
            :on-cancel="cancel"
            :on-pause="pause"
            :on-resume="resume"
            :is-waking="isWaking"
            :is-cancelling="isCancelling"
            :is-pausing="isPausing"
            :is-resuming="isResuming"
          />
        </template>
      </UDashboardNavbar>
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-6">
        <UAlert v-if="jobsError" color="error" :title="jobsError" icon="i-lucide-circle-x" />
        <UAlert
          v-if="jobsWarning"
          color="warning"
          :title="jobsWarning"
          icon="i-lucide-triangle-alert"
        />
        <p v-if="jobsLoading" role="status" class="text-muted text-sm">Loading job…</p>
        <UEmpty
          v-else-if="!job && !jobsError"
          icon="i-lucide-search-x"
          title="Job not found"
          :description="`No job named ${name} is configured.`"
        />
        <template v-if="job">
          <UCard :class="jobStatus(job).rail">
            <div class="flex items-start justify-between gap-6 flex-wrap">
              <dl class="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1 text-sm">
                <dt class="text-muted">Kind</dt>
                <dd class="font-mono">{{ job.kind }}</dd>
                <template v-if="job.kind === 'push'">
                  <dt class="text-muted">Last sync</dt>
                  <dd>{{ formatLastSync(job) }}</dd>
                  <dt class="text-muted">Next sync</dt>
                  <dd>{{ formatSyncState(job) }}</dd>
                </template>
                <template v-else>
                  <dt class="text-muted">Last run</dt>
                  <dd>{{ formatTimestamp(job.last_run) }}</dd>
                  <dt class="text-muted">Next run</dt>
                  <dd>{{ formatTimestamp(job.next_run) }}</dd>
                </template>
                <template v-if="job.last_error">
                  <dt class="text-muted">Last error</dt>
                  <dd class="text-error break-all">{{ job.last_error }}</dd>
                </template>
              </dl>
            </div>
            <div v-if="job.transfers?.length || job.targets?.length || job.paused" class="mt-4">
              <TransferPanel :job="job" :on-push-to="pushTo" :is-pushing="isPushing" />
            </div>
          </UCard>

          <UAlert v-if="runsError" color="error" :title="runsError" icon="i-lucide-circle-x" />
          <div v-if="runsLoading && runs.length === 0" class="text-muted text-sm">
            Loading runs…
          </div>
          <UEmpty
            v-else-if="runs.length === 0 && !runsError"
            icon="i-lucide-history"
            title="No runs recorded yet"
          />
          <template v-if="runs.length">
            <RunsCharts :runs="runs" />
            <div>
              <div class="microlabel mb-2">recent runs</div>
              <UTable
                :data="runs"
                :columns="tableColumns"
                class="rounded-md border border-default bg-default"
              />
            </div>
          </template>
        </template>
      </div>
    </template>
  </UDashboardPanel>
</template>
