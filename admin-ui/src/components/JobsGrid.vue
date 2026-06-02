<script setup lang="ts">
import type { JobStatus } from '../client'
import { formatRelative } from '../utils/format'

defineProps<{
  jobs: JobStatus[]
  onWake?: (name: string) => void
}>()

function statusBadge(j: JobStatus): { label: string; color: 'success' | 'error' | 'neutral' } {
  if (j.last_error) return { label: 'error', color: 'error' }
  if (j.last_run) return { label: 'ok', color: 'success' }
  return { label: 'idle', color: 'neutral' }
}
</script>

<template>
  <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
    <UCard v-for="j in jobs" :key="j.name">
      <template #header>
        <div class="flex items-center justify-between">
          <div>
            <RouterLink :to="`/jobs/${j.name}`" class="font-semibold hover:underline">{{
              j.name
            }}</RouterLink>
            <span class="ml-2 text-sm text-gray-500">{{ j.kind }}</span>
          </div>
          <UBadge :color="statusBadge(j).color" variant="subtle">{{ statusBadge(j).label }}</UBadge>
        </div>
      </template>
      <dl class="text-sm space-y-1">
        <div class="flex justify-between">
          <dt class="text-gray-500">Last run</dt>
          <dd>{{ formatRelative(j.last_run) }}</dd>
        </div>
        <div class="flex justify-between">
          <dt class="text-gray-500">Next run</dt>
          <dd>{{ formatRelative(j.next_run) }}</dd>
        </div>
        <div v-if="j.last_error" class="text-error-600 text-xs mt-2 truncate" :title="j.last_error">
          {{ j.last_error }}
        </div>
      </dl>
      <template #footer>
        <UButton size="xs" variant="soft" icon="i-lucide-zap" @click="onWake?.(j.name)"
          >Wakeup</UButton
        >
      </template>
    </UCard>
  </div>
</template>
