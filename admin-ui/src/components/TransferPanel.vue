<script setup lang="ts">
import type { JobStatus } from '../client'
import TransferSlot from './TransferSlot.vue'

defineProps<{
  job: JobStatus
  onCancel?: (name: string) => void
  onPause?: (name: string) => void
  onResume?: (name: string) => void
  onPushTo?: (name: string, peer: string) => void
}>()

function age(unixSec?: number | null): string {
  if (!unixSec) return 'never'
  const s = Math.max(0, Math.floor(Date.now() / 1000) - unixSec)
  if (s < 90) return `${s}s ago`
  if (s < 5400) return `${Math.round(s / 60)}m ago`
  if (s < 129600) return `${Math.round(s / 3600)}h ago`
  return `${Math.round(s / 86400)}d ago`
}

function fmtIn(s: number): string {
  if (s < 90) return `${Math.max(1, Math.round(s))}s`
  if (s < 5400) return `${Math.round(s / 60)}m`
  if (s < 129600) return `${Math.round(s / 3600)}h`
  return `${Math.round(s / 86400)}d`
}

/** One human line per target: last sync + (for auto) when the next
 * automatic sync becomes due. */
function targetLine(tg: {
  mode: string
  route_auto?: boolean
  connected: boolean
  auto_interval_secs?: number | null
  last_success?: number | null
  last_error?: string | null
}): string {
  if (tg.last_error) return `error: ${tg.last_error}`
  const synced = tg.last_success ? `synced ${age(tg.last_success)}` : 'never synced'
  if (tg.mode !== 'auto') return synced
  if (tg.connected && !tg.route_auto) return `${synced} · route is manual-only`
  if (!tg.auto_interval_secs || !tg.last_success) return `${synced} · auto: every cycle`
  const due = tg.last_success + tg.auto_interval_secs - Math.floor(Date.now() / 1000)
  return due <= 0 ? `${synced} · auto: due now` : `${synced} · next auto in ~${fmtIn(due)}`
}
</script>

<template>
  <div class="space-y-3">
    <!-- In-flight transfers, one block per parallel send slot -->
    <div v-if="job.transfers?.length" class="space-y-3">
      <TransferSlot
        v-for="t in job.transfers"
        :key="`${t.peer}:${t.dataset}`"
        :transfer="t"
        :show-peer="(job.targets?.length ?? 0) > 1"
      />
    </div>

    <!-- Per-target policy + manual trigger -->
    <div v-if="job.targets?.length" class="space-y-2">
      <div v-for="tg in job.targets ?? []" :key="tg.peer" class="space-y-0.5">
        <div class="flex items-center gap-2 text-sm min-w-0">
          <span
            class="inline-block w-2 h-2 rounded-full shrink-0"
            :class="tg.connected ? 'bg-success pulse-dot' : 'bg-neutral-400 dark:bg-neutral-600'"
            :title="tg.connected ? 'reachable' : 'unreachable'"
          />
          <span class="font-medium font-mono truncate">{{ tg.peer }}</span>
          <UBadge
            v-if="tg.route"
            variant="outline"
            size="sm"
            color="neutral"
            class="shrink-0 whitespace-nowrap"
          >
            via {{ tg.route }}
          </UBadge>
          <UBadge
            variant="subtle"
            size="sm"
            :color="tg.mode === 'auto' ? 'info' : 'neutral'"
            class="shrink-0"
          >
            {{ tg.mode }}
          </UBadge>
          <UButton
            size="xs"
            variant="soft"
            icon="i-lucide-send"
            class="shrink-0 ms-auto"
            :disabled="!tg.connected"
            @click="onPushTo?.(job.name, tg.peer)"
            >Send now</UButton
          >
        </div>
        <!-- For single-target jobs the card-level Last/Next sync rows
             already say this; repeat per-target only when there is more
             than one target or something is wrong. -->
        <div
          v-if="(job.targets?.length ?? 0) > 1 || tg.last_error || (tg.connected && !tg.route_auto)"
          class="text-xs ml-4 truncate"
          :class="tg.last_error ? 'text-error' : 'text-muted'"
          :title="tg.last_error ?? targetLine(tg)"
        >
          {{ targetLine(tg) }}
        </div>
      </div>
    </div>

    <!-- Controls -->
    <div class="flex gap-2">
      <UButton
        v-if="job.running && !job.paused"
        size="xs"
        color="warning"
        variant="soft"
        icon="i-lucide-circle-pause"
        @click="onPause?.(job.name)"
        >Pause</UButton
      >
      <UButton
        v-if="job.paused"
        size="xs"
        color="success"
        variant="soft"
        icon="i-lucide-circle-play"
        @click="onResume?.(job.name)"
        >Resume</UButton
      >
      <UButton
        v-if="job.running"
        size="xs"
        color="error"
        variant="soft"
        icon="i-lucide-circle-x"
        @click="onCancel?.(job.name)"
        >Cancel</UButton
      >
    </div>
  </div>
</template>
