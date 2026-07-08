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

// Speed from bytes_sent deltas between polls (EMA-smoothed).
const lastSample = ref<{ bytes: number; at: number } | null>(null)
const rate = ref<number | null>(null)

watch(
  () => props.job.transfer?.bytes_sent,
  (bytes) => {
    const now = Date.now()
    if (bytes == null) {
      lastSample.value = null
      rate.value = null
      return
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

function age(unixSec?: number | null): string {
  if (!unixSec) return 'never'
  const s = Math.max(0, Math.floor(Date.now() / 1000) - unixSec)
  if (s < 90) return `${s}s ago`
  if (s < 5400) return `${Math.round(s / 60)}m ago`
  if (s < 129600) return `${Math.round(s / 3600)}h ago`
  return `${Math.round(s / 86400)}d ago`
}
</script>

<template>
  <div class="space-y-3">
    <!-- In-flight transfer -->
    <div v-if="t" class="space-y-1">
      <div class="flex items-center justify-between text-sm">
        <span class="font-mono truncate" :title="t.dataset">
          {{ t.dataset }} <span class="text-gray-500">→ {{ t.peer }} ({{ t.kind }})</span>
        </span>
        <span class="text-gray-500 shrink-0 ml-2">
          {{ formatBytes(t.bytes_sent)
          }}<template v-if="t.total_bytes"> / {{ formatBytes(t.total_bytes) }}</template>
          <template v-if="rate"> · {{ formatBytes(rate) }}/s</template>
          <template v-if="eta"> · ~{{ eta }}</template>
        </span>
      </div>
      <UProgress v-if="pct != null" :model-value="pct" size="sm" />
      <UProgress v-else size="sm" animation="carousel" />
    </div>

    <!-- Per-target policy + manual trigger -->
    <div v-if="job.targets.length" class="space-y-1">
      <div
        v-for="tg in job.targets"
        :key="tg.peer"
        class="flex items-center justify-between text-sm gap-2"
      >
        <div class="flex items-center gap-2 min-w-0">
          <span
            class="inline-block w-2 h-2 rounded-full shrink-0"
            :class="tg.connected ? 'bg-success-500' : 'bg-gray-400'"
            :title="tg.connected ? 'reachable' : 'unreachable'"
          />
          <span class="truncate">{{ tg.peer }}</span>
          <UBadge variant="subtle" size="xs" :color="tg.mode === 'auto' ? 'info' : 'neutral'">
            {{ tg.mode }}
          </UBadge>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <span
            class="text-xs"
            :class="tg.last_error ? 'text-error-500' : 'text-gray-500'"
            :title="tg.last_error ?? undefined"
          >
            {{ tg.last_error ? 'error' : age(tg.last_success) }}
          </span>
          <UButton
            size="xs"
            variant="soft"
            icon="i-lucide-send"
            :disabled="!tg.connected"
            @click="onPushTo?.(job.name, tg.peer)"
            >Send now</UButton
          >
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
