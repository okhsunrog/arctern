<script setup lang="ts">
import { useHost } from '../composables/useHost'
import type { JobStatus } from '../client'
import { formatNextRun, formatRelative } from '../utils/format'
import { formatLastSync, formatNextSync } from '../utils/pushTimes'
import { jobStatus } from '../utils/status'
import TransferPanel from './TransferPanel.vue'

const { prefix } = useHost()

defineProps<{
  jobs: JobStatus[]
  onWake?: (name: string) => void
  onCancel?: (name: string) => void
  onPause?: (name: string) => void
  onResume?: (name: string) => void
  onPushTo?: (name: string, peer: string) => void
}>()
</script>

<template>
  <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
    <UCard v-for="j in jobs" :key="j.name" :class="jobStatus(j).rail">
      <template #header>
        <div class="flex items-center justify-between">
          <div class="min-w-0">
            <RouterLink
              :to="`${prefix}/jobs/${j.name}`"
              class="font-semibold font-mono hover:underline truncate block"
            >
              {{ j.name }}
            </RouterLink>
            <span class="microlabel">{{ j.kind }}</span>
          </div>
          <UBadge
            :color="jobStatus(j).color"
            variant="subtle"
            :icon="jobStatus(j).icon"
            class="shrink-0"
          >
            {{ jobStatus(j).label }}
          </UBadge>
        </div>
      </template>
      <!-- Push jobs live on their per-target sync schedule; the 15m
           scheduler tick is noise, so their card talks about syncs. -->
      <dl class="text-sm space-y-1">
        <template v-if="j.kind === 'push'">
          <div class="flex justify-between">
            <dt class="text-muted">Last sync</dt>
            <dd>{{ formatLastSync(j) }}</dd>
          </div>
          <div class="flex justify-between">
            <dt class="text-muted">Next sync</dt>
            <dd>{{ j.running ? 'replicating now' : formatNextSync(j) }}</dd>
          </div>
        </template>
        <template v-else>
          <div class="flex justify-between">
            <dt class="text-muted">Last run</dt>
            <dd>{{ formatRelative(j.last_run) }}</dd>
          </div>
          <div class="flex justify-between">
            <dt class="text-muted">Next run</dt>
            <dd>{{ formatNextRun(j.next_run, j.running) }}</dd>
          </div>
        </template>
        <div v-if="j.last_error" class="text-error text-xs mt-2 truncate" :title="j.last_error">
          {{ j.last_error }}
        </div>
      </dl>
      <div v-if="j.transfer || j.targets?.length || j.paused" class="mt-3">
        <TransferPanel
          :job="j"
          :on-cancel="onCancel"
          :on-pause="onPause"
          :on-resume="onResume"
          :on-push-to="onPushTo"
        />
      </div>
      <template #footer>
        <UButton size="xs" variant="soft" icon="i-lucide-alarm-clock" @click="onWake?.(j.name)">
          Wake up
        </UButton>
      </template>
    </UCard>
  </div>
</template>
