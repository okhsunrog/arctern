<script setup lang="ts">
// The jobs table, the job card and the command palette each grew their
// own subset of the same actions — the table could not send at all, and
// it offered "cancel" in states where the card correctly hid it. One
// component, one answer.

import { ref } from 'vue'
import type { JobStatus } from '../client'
import ConfirmModal from './ConfirmModal.vue'

const props = defineProps<{
  job: JobStatus
  /** `icon` for dense table rows, `label` for cards. */
  variant?: 'icon' | 'label'
  onWake?: (name: string) => void
  onCancel?: (name: string) => void
  onPause?: (name: string) => void
  onResume?: (name: string) => void
  isWaking?: (name: string) => boolean
  isCancelling?: (name: string) => boolean
  isPausing?: (name: string) => boolean
  isResuming?: (name: string) => boolean
}>()

const confirmStop = ref(false)
const iconOnly = () => (props.variant ?? 'label') === 'icon'

function askStop() {
  confirmStop.value = true
}
</script>

<template>
  <div class="flex items-center gap-0.5">
    <UTooltip text="Wake up now">
      <UButton
        :size="iconOnly() ? 'xs' : 'xs'"
        :variant="iconOnly() ? 'ghost' : 'soft'"
        color="neutral"
        icon="i-lucide-alarm-clock"
        :label="iconOnly() ? undefined : 'Wake up'"
        aria-label="Wake up now"
        :loading="isWaking?.(job.name)"
        @click="onWake?.(job.name)"
      />
    </UTooltip>

    <UTooltip v-if="job.running && !job.paused" text="Pause (keeps the partial transfer)">
      <UButton
        size="xs"
        :variant="iconOnly() ? 'ghost' : 'soft'"
        color="warning"
        icon="i-lucide-circle-pause"
        :label="iconOnly() ? undefined : 'Pause'"
        aria-label="Pause"
        :loading="isPausing?.(job.name)"
        @click="onPause?.(job.name)"
      />
    </UTooltip>

    <UTooltip v-if="job.paused" text="Resume from the partial transfer">
      <UButton
        size="xs"
        :variant="iconOnly() ? 'ghost' : 'soft'"
        color="success"
        icon="i-lucide-circle-play"
        :label="iconOnly() ? undefined : 'Resume'"
        aria-label="Resume"
        :loading="isResuming?.(job.name)"
        @click="onResume?.(job.name)"
      />
    </UTooltip>

    <!-- `cancellable` is the daemon's own answer: it drops to false once
         every slot has handed off to zfs recv, where cancel is a no-op. -->
    <UTooltip v-if="job.cancellable" text="Stop transfer">
      <UButton
        size="xs"
        :variant="iconOnly() ? 'ghost' : 'soft'"
        color="error"
        icon="i-lucide-circle-x"
        :label="iconOnly() ? undefined : 'Stop transfer'"
        aria-label="Stop transfer"
        :loading="isCancelling?.(job.name)"
        @click="askStop"
      />
    </UTooltip>

    <ConfirmModal
      v-model:open="confirmStop"
      title="Stop this transfer?"
      description="The send is aborted and the receiver releases the dataset. The partial state is kept, so a later cycle resumes from it rather than starting over."
      :subject="job.name"
      confirm-label="Stop transfer"
      :loading="isCancelling?.(job.name)"
      @confirm="onCancel?.(job.name)"
    />
  </div>
</template>
