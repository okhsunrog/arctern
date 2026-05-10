<script setup lang="ts">
import { computed, h, resolveComponent } from 'vue'
import { useJobs } from '../composables/useJobs'
import { formatRelative } from '../utils/format'
import type { JobStatus } from '../client'

const { jobs, error, loading, wake } = useJobs()

const UButton = resolveComponent('UButton')
const UBadge = resolveComponent('UBadge')

const columns = computed(() => [
  {
    accessorKey: 'name',
    header: 'Name',
    cell: ({ row }: { row: { original: JobStatus } }) =>
      h(
        resolveComponent('RouterLink'),
        { to: `/jobs/${row.original.name}`, class: 'text-primary-500 hover:underline' },
        () => row.original.name,
      ),
  },
  { accessorKey: 'kind', header: 'Kind' },
  {
    id: 'status',
    header: 'Status',
    cell: ({ row }: { row: { original: JobStatus } }) => {
      const j = row.original
      const color = j.last_error ? 'error' : j.last_run ? 'success' : 'neutral'
      const label = j.last_error ? 'error' : j.last_run ? 'ok' : 'idle'
      return h(UBadge, { color, variant: 'subtle' }, () => label)
    },
  },
  {
    accessorKey: 'last_run',
    header: 'Last run',
    cell: ({ row }: { row: { original: JobStatus } }) => formatRelative(row.original.last_run),
  },
  {
    accessorKey: 'next_run',
    header: 'Next run',
    cell: ({ row }: { row: { original: JobStatus } }) => formatRelative(row.original.next_run),
  },
  {
    id: 'actions',
    header: '',
    cell: ({ row }: { row: { original: JobStatus } }) =>
      h(
        UButton,
        {
          size: 'xs',
          variant: 'soft',
          icon: 'i-lucide-zap',
          onClick: () => wake(row.original.name),
        },
        () => 'Wakeup',
      ),
  },
])
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6">
    <h1 class="text-2xl font-semibold mb-4">Jobs</h1>
    <UAlert v-if="error" color="error" :title="error" class="mb-4" />
    <div v-if="loading && jobs.length === 0" class="text-gray-500">Loading…</div>
    <UTable v-else :data="jobs" :columns="columns" />
  </div>
</template>
