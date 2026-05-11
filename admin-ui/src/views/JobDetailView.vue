<script setup lang="ts">
import { computed, h, resolveComponent } from 'vue'
import { useRoute } from 'vue-router'
import { useJobs } from '../composables/useJobs'
import { useJobRuns } from '../composables/useJobRuns'
import { formatBytes, formatTimestamp } from '../utils/format'
import RunsCharts from '../components/RunsCharts.vue'
import type { JobRun } from '../client'

const route = useRoute()
const name = computed(() => String(route.params.name))

const { jobs, error: jobsError, wake } = useJobs()
const job = computed(() => jobs.value.find((j) => j.name === name.value))

const { runs, error: runsError, loading: runsLoading } = useJobRuns(name.value)

const UBadge = resolveComponent('UBadge')

const tableColumns = computed(() => [
  {
    accessorKey: 'started_at',
    header: 'Started',
    cell: ({ row }: { row: { original: JobRun } }) =>
      formatTimestamp(new Date(row.original.started_at * 1000).toISOString()),
  },
  {
    id: 'duration',
    header: 'Duration',
    cell: ({ row }: { row: { original: JobRun } }) => {
      const r = row.original
      if (!r.finished_at) return '—'
      return `${Math.max(0, r.finished_at - r.started_at)}s`
    },
  },
  {
    id: 'status',
    header: 'Status',
    cell: ({ row }: { row: { original: JobRun } }) => {
      const r = row.original
      const color =
        r.status === 'ok'
          ? 'success'
          : r.status === 'error'
            ? 'error'
            : r.status === 'running'
              ? 'info'
              : 'neutral'
      return h(UBadge, { color, variant: 'subtle' }, () => r.status)
    },
  },
  {
    accessorKey: 'bytes_sent',
    header: 'Bytes',
    cell: ({ row }: { row: { original: JobRun } }) => formatBytes(row.original.bytes_sent),
  },
  {
    accessorKey: 'error_message',
    header: 'Error',
    cell: ({ row }: { row: { original: JobRun } }) => row.original.error_message ?? '',
  },
])
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6 space-y-6">
    <RouterLink to="/jobs" class="text-sm text-primary-500 hover:underline">
      ← Jobs
    </RouterLink>
    <UAlert v-if="jobsError" color="error" :title="jobsError" />
    <div v-if="!job" class="text-gray-500">Job not found.</div>
    <template v-else>
      <div class="flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-semibold">{{ job.name }}</h1>
          <p class="text-sm text-gray-500">{{ job.kind }}</p>
        </div>
        <UButton icon="i-lucide-zap" @click="wake(job.name)">Wakeup</UButton>
      </div>
      <UCard>
        <dl class="grid grid-cols-2 gap-y-3 text-sm">
          <dt class="text-gray-500">Last run</dt>
          <dd>{{ formatTimestamp(job.last_run) }}</dd>
          <dt class="text-gray-500">Next run</dt>
          <dd>{{ formatTimestamp(job.next_run) }}</dd>
          <dt class="text-gray-500">Last error</dt>
          <dd class="text-error-600">{{ job.last_error ?? '—' }}</dd>
        </dl>
      </UCard>

      <UAlert v-if="runsError" color="error" :title="runsError" />
      <div v-if="runsLoading && runs.length === 0" class="text-gray-500">Loading runs…</div>
      <div v-else-if="runs.length === 0" class="text-gray-500">No runs recorded yet.</div>
      <template v-else>
        <RunsCharts :runs="runs" />
        <div>
          <h2 class="text-lg font-semibold mb-2">Recent runs</h2>
          <UTable :data="runs" :columns="tableColumns" />
        </div>
      </template>
    </template>
  </div>
</template>
