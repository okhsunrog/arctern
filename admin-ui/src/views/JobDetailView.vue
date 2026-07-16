<script setup lang="ts">
import { computed, h, resolveComponent } from 'vue'
import { useRoute } from 'vue-router'
import type { TableColumn } from '@nuxt/ui'
import { useHost } from '../composables/useHost'
import { useJobs } from '../composables/useJobs'
import { useJobRuns } from '../composables/useJobRuns'
import { formatBytes, formatTimestamp } from '../utils/format'
import { formatLastSync, formatNextSync } from '../utils/pushTimes'
import { jobStatus, runStatus } from '../utils/status'
import RunsCharts from '../components/RunsCharts.vue'
import TransferPanel from '../components/TransferPanel.vue'
import type { JobRun } from '../client'

const route = useRoute()
const name = computed(() => String(route.params.name))

const { host, baseUrl, prefix } = useHost()
const { jobs, error: jobsError, wake, cancel, pause, resume, pushTo } = useJobs(baseUrl)
const job = computed(() => jobs.value.find((j) => j.name === name.value))

const {
  runs,
  error: runsError,
  loading: runsLoading,
} = useJobRuns(name.value, 10_000, 100, baseUrl.value)

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
    header: 'Error',
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
          <UButton v-if="job" icon="i-lucide-alarm-clock" size="sm" @click="wake(job.name)">
            Wake up
          </UButton>
        </template>
      </UDashboardNavbar>
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-6">
        <UAlert v-if="jobsError" color="error" :title="jobsError" icon="i-lucide-circle-x" />
        <UEmpty
          v-if="!job"
          icon="i-lucide-search-x"
          title="Job not found"
          :description="`No job named ${name} is configured.`"
        />
        <template v-else>
          <UCard :class="jobStatus(job).rail">
            <div class="flex items-start justify-between gap-6 flex-wrap">
              <dl class="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1 text-sm">
                <dt class="text-muted">Kind</dt>
                <dd class="font-mono">{{ job.kind }}</dd>
                <template v-if="job.kind === 'push'">
                  <dt class="text-muted">Last sync</dt>
                  <dd>{{ formatLastSync(job) }}</dd>
                  <dt class="text-muted">Next sync</dt>
                  <dd>{{ job.running ? 'replicating now' : formatNextSync(job) }}</dd>
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
              <TransferPanel
                :job="job"
                :on-cancel="cancel"
                :on-pause="pause"
                :on-resume="resume"
                :on-push-to="pushTo"
              />
            </div>
          </UCard>

          <UAlert v-if="runsError" color="error" :title="runsError" icon="i-lucide-circle-x" />
          <div v-if="runsLoading && runs.length === 0" class="text-muted text-sm">
            Loading runs…
          </div>
          <UEmpty
            v-else-if="runs.length === 0"
            icon="i-lucide-history"
            title="No runs recorded yet"
          />
          <template v-else>
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
