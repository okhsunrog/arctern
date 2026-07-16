<script setup lang="ts">
import { computed, h, resolveComponent } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { useHost } from '../composables/useHost'
import IncomingTransfers from '../components/IncomingTransfers.vue'
import { useJobs } from '../composables/useJobs'
import { formatNextRun, formatRelative } from '../utils/format'
import { formatLastSync, formatNextSync } from '../utils/pushTimes'
import { jobStatus } from '../utils/status'
import type { JobStatus } from '../client'

const { host, baseUrl } = useHost()
const { jobs, error, loading, wake, cancel, pause, resume } = useJobs(baseUrl)
const title = computed(() => (host.value ? `${host.value} · Jobs` : 'Jobs'))

const UButton = resolveComponent('UButton')
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
          to: `${host ? `/h/${host}` : ''}/jobs/${row.original.name}`,
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
      return row.original.last_error
        ? h(UTooltip, { text: row.original.last_error }, () => badge)
        : badge
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
        ? row.original.running
          ? 'replicating now'
          : formatNextSync(row.original)
        : formatNextRun(row.original.next_run, row.original.running),
  },
  {
    id: 'actions',
    header: '',
    cell: ({ row }) => {
      const j = row.original
      const btn = (
        icon: string,
        label: string,
        onClick: () => void,
        color: 'neutral' | 'warning' | 'success' | 'error' = 'neutral',
      ) =>
        h(UTooltip, { text: label }, () =>
          h(UButton, { size: 'xs', variant: 'ghost', color, icon, 'aria-label': label, onClick }),
        )
      const actions = [btn('i-lucide-alarm-clock', 'Wake up now', () => void wake(j.name))]
      if (j.running && !j.paused) {
        actions.push(
          btn('i-lucide-circle-pause', 'Pause (resumable)', () => void pause(j.name), 'warning'),
        )
      }
      if (j.paused) {
        actions.push(btn('i-lucide-circle-play', 'Resume', () => void resume(j.name), 'success'))
      }
      if (j.running) {
        actions.push(
          btn('i-lucide-circle-x', 'Cancel transfer', () => void cancel(j.name), 'error'),
        )
      }
      return h('div', { class: 'flex justify-end gap-0.5' }, actions)
    },
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
        <UTable
          :data="jobs"
          :columns="columns"
          :loading="loading && jobs.length === 0"
          class="rounded-md border border-default bg-default"
        />
        <IncomingTransfers :base-url="baseUrl" />
      </div>
    </template>
  </UDashboardPanel>
</template>
