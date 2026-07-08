<script setup lang="ts">
import type { JobStatus } from '../client'
import { formatNextRun, formatRelative } from '../utils/format'
import TransferPanel from './TransferPanel.vue'

defineProps<{
  jobs: JobStatus[]
  onWake?: (name: string) => void
  onCancel?: (name: string) => void
  onPause?: (name: string) => void
  onResume?: (name: string) => void
  onPushTo?: (name: string, peer: string) => void
}>()

function statusBadge(j: JobStatus): {
  label: string
  color: 'success' | 'error' | 'neutral' | 'info'
} {
  // running/paused win: last_error/last_run describe the previous
  // cycle and are stale while a long send is in flight.
  if (j.paused) return { label: 'paused', color: 'neutral' }
  if (j.running) return { label: 'running', color: 'info' }
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
          <dd>{{ formatNextRun(j.next_run, j.running) }}</dd>
        </div>
        <div v-if="j.last_error" class="text-error-600 text-xs mt-2 truncate" :title="j.last_error">
          {{ j.last_error }}
        </div>
      </dl>
      <div v-if="j.transfer || j.targets.length || j.paused" class="mt-3">
        <TransferPanel
          :job="j"
          :on-cancel="onCancel"
          :on-pause="onPause"
          :on-resume="onResume"
          :on-push-to="onPushTo"
        />
      </div>
      <template #footer>
        <UButton size="xs" variant="soft" icon="i-lucide-zap" @click="onWake?.(j.name)"
          >Wakeup</UButton
        >
      </template>
    </UCard>
  </div>
</template>
