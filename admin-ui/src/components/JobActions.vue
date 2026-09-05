<script setup lang="ts">
// The jobs table, the job card and the command palette each grew their
// own subset of the same actions — the table could not send at all, and
// it offered "cancel" in states where the card correctly hid it. One
// component, one answer.

import { computed, ref } from 'vue'
import type { JobStatus } from '../client'
import { asPushJob } from '../utils/jobs'
import { jobActivity } from '../utils/status'
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
// Pause/resume/stop only exist for push jobs; the others just wake.
const push = computed(() => asPushJob(props.job))

function askStop() {
  confirmStop.value = true
}

// A cycle is abortable before it has anything to send — it is still
// listing snapshots and asking the receiver for its GUIDs — but calling
// that "stop transfer" names a transfer that does not exist.
const stop = computed(() =>
  jobActivity(props.job) === 'sending'
    ? {
        label: 'Stop transfer',
        title: 'Stop this transfer?',
        description:
          'The send is aborted and the receiver releases the dataset. The partial state is kept, so a later cycle resumes from it rather than starting over.',
      }
    : {
        label: 'Stop cycle',
        title: 'Stop this cycle?',
        description:
          'The job is still working out what to send. Nothing has been transferred yet, so stopping now simply ends the cycle.',
      },
)
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

    <UTooltip v-if="push && job.running && !push.paused" text="Pause (keeps the partial transfer)">
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

    <UTooltip v-if="push?.paused" text="Resume from the partial transfer">
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
    <UTooltip v-if="push?.cancellable" :text="stop.label">
      <UButton
        size="xs"
        :variant="iconOnly() ? 'ghost' : 'soft'"
        color="error"
        icon="i-lucide-circle-x"
        :label="iconOnly() ? undefined : stop.label"
        :aria-label="stop.label"
        :loading="isCancelling?.(job.name)"
        @click="askStop"
      />
    </UTooltip>

    <ConfirmModal
      v-model:open="confirmStop"
      :title="stop.title"
      :description="stop.description"
      :subject="job.name"
      :confirm-label="stop.label"
      :loading="isCancelling?.(job.name)"
      @confirm="onCancel?.(job.name)"
    />
  </div>
</template>
