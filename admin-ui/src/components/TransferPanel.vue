<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { JobStatus } from '../client'
import { formatBytes } from '../utils/format'

const props = defineProps<{
  job: JobStatus
  onCancel?: (name: string) => void
  onPause?: (name: string) => void
  onResume?: (name: string) => void
  onPushTo?: (name: string, peer: string) => void
}>()

// Speed: seeded from the server's `started_at` (whole-transfer average,
// meaningful even on the first poll), then EMA-refined from bytes_sent
// deltas between polls.
const lastSample = ref<{ bytes: number; at: number } | null>(null)
const rate = ref<number | null>(null)

watch(
  () => props.job.transfer?.bytes_sent,
  (bytes) => {
    const now = Date.now()
    const t = props.job.transfer
    if (bytes == null || !t) {
      lastSample.value = null
      rate.value = null
      return
    }
    if (rate.value == null && t.started_at) {
      const elapsed = now / 1000 - t.started_at
      if (elapsed > 2 && bytes > 0) rate.value = bytes / elapsed
    }
    if (lastSample.value && bytes >= lastSample.value.bytes) {
      const dt = (now - lastSample.value.at) / 1000
      if (dt > 0.5) {
        const inst = (bytes - lastSample.value.bytes) / dt
        rate.value = rate.value == null ? inst : rate.value * 0.6 + inst * 0.4
        lastSample.value = { bytes, at: now }
      }
    } else {
      lastSample.value = { bytes, at: now }
    }
  },
  { immediate: true },
)

const t = computed(() => props.job.transfer)
const pct = computed(() => {
  if (!t.value?.total_bytes) return null
  return Math.min(100, (t.value.bytes_sent / t.value.total_bytes) * 100)
})
const eta = computed(() => {
  if (!t.value?.total_bytes || !rate.value || rate.value < 1) return null
  const left = (t.value.total_bytes - t.value.bytes_sent) / rate.value
  if (left < 90) return `${Math.round(left)}s`
  if (left < 5400) return `${Math.round(left / 60)}m`
  return `${(left / 3600).toFixed(1)}h`
})
const elapsed = computed(() => {
  if (!t.value?.started_at) return null
  const s = Math.max(0, Math.floor(Date.now() / 1000) - t.value.started_at)
  return fmtIn(s)
})

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
    <!-- In-flight transfer -->
    <div v-if="t" class="space-y-1">
      <div class="flex items-center justify-between text-sm">
        <span class="font-mono truncate" :title="t.dataset">
          {{ t.dataset }} <span class="text-muted">→ {{ t.peer }} ({{ t.kind }})</span>
        </span>
        <span class="text-muted shrink-0 ml-2 font-mono text-xs">
          {{ formatBytes(t.bytes_sent)
          }}<template v-if="t.total_bytes"> / {{ formatBytes(t.total_bytes) }}</template>
          <template v-if="rate"> · {{ formatBytes(rate) }}/s</template>
          <template v-if="eta"> · ~{{ eta }} left</template>
          <template v-else-if="elapsed"> · {{ elapsed }} elapsed</template>
        </span>
      </div>
      <UProgress v-if="pct != null" :model-value="pct" size="sm" />
      <UProgress v-else size="sm" animation="carousel" />
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
