<script setup lang="ts">
import { computed, h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { useHost } from '../composables/useHost'
import IncomingTransfers from '../components/IncomingTransfers.vue'
import JobActions from '../components/JobActions.vue'
import { useJobs } from '../composables/useJobs'
import { formatNextRun, formatRelative } from '../utils/format'
import { formatLastSync, formatSyncState } from '../utils/pushTimes'
import { jobFailureMessage, jobStatus } from '../utils/status'
import type { JobStatus } from '../client'

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
  isWaking,
  isCancelling,
  isPausing,
  isResuming,
} = useJobs(scope)
const title = computed(() => (host.value ? `${host.value} · Jobs` : 'Jobs'))

const UBadge = resolveComponent('UBadge')
const UTooltip = resolveComponent('UTooltip')

const columns = computed<TableColumn<JobStatus>[]>(() => [
  {
    accessorKey: 'name',
    header: 'Name',
    cell: ({ row }) =>
      h(
        resolveComponent('RouterLink'),
        {
          to: `${prefix.value}/jobs/${encodeURIComponent(row.original.name)}`,
          class: 'font-mono font-medium text-primary hover:underline',
        },
        () => row.original.name,
      ),
  },
  {
    accessorKey: 'kind',
    header: 'Kind',
    cell: ({ row }) => h(UBadge, { color: 'neutral', variant: 'outline' }, () => row.original.kind),
  },
  {
    id: 'status',
    header: 'Status',
    cell: ({ row }) => {
      const s = jobStatus(row.original)
      const badge = h(UBadge, { color: s.color, variant: 'subtle', icon: s.icon }, () => s.label)
      const failure = jobFailureMessage(row.original)
      return failure ? h(UTooltip, { text: failure }, () => badge) : badge
    },
  },
  {
    id: 'targets',
    header: 'Targets',
    cell: ({ row }) => {
      const targets = row.original.targets ?? []
      if (targets.length === 0) return ''
      return h(
        'div',
        { class: 'flex gap-1 flex-wrap' },
        targets.map((t) =>
          h(
            UBadge,
            {
              color: t.connected ? 'success' : 'neutral',
              variant: 'subtle',
              size: 'sm',
            },
            () => (t.route ? `${t.peer} · ${t.route}` : t.peer),
          ),
        ),
      )
    },
  },
  {
    // For push jobs the run columns carry sync semantics — the
    // scheduler tick is invisible to the operator.
    accessorKey: 'last_run',
    header: 'Last',
    cell: ({ row }) =>
      row.original.kind === 'push'
        ? `synced ${formatLastSync(row.original)}`
        : formatRelative(row.original.last_run),
  },
  {
    accessorKey: 'next_run',
    header: 'Next',
    cell: ({ row }) =>
      row.original.kind === 'push'
        ? formatSyncState(row.original)
        : formatNextRun(row.original.next_run, row.original.running),
  },
  {
    id: 'actions',
    header: '',
    cell: ({ row }) =>
      h(
        'div',
        { class: 'flex justify-end' },
        h(JobActions, {
          job: row.original,
          variant: 'icon',
          onWake: wake,
          onCancel: cancel,
          onPause: pause,
          onResume: resume,
          isWaking,
          isCancelling,
          isPausing,
          isResuming,
        }),
      ),
  },
])
</script>

<template>
  <UDashboardPanel id="jobs">
    <template #header>
      <UDashboardNavbar :title="title" />
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-4">
        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />
        <UAlert v-if="warning" color="warning" :title="warning" icon="i-lucide-triangle-alert" />
        <UTable
          :data="jobs"
          :columns="columns"
          :loading="loading && jobs.length === 0"
          class="rounded-md border border-default bg-default"
        />
        <IncomingTransfers :scope="scope" />
      </div>
    </template>
  </UDashboardPanel>
</template>
