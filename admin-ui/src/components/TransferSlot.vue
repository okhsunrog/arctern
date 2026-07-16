<script setup lang="ts">
import { computed, onUnmounted, ref, watch } from 'vue'
import type { TransferInfo } from '../client'
import { formatBytes } from '../utils/format'

const props = defineProps<{
  transfer: TransferInfo
  /** Name the destination peer in the stats line — only useful when the
   * job replicates to more than one target. */
  showPeer?: boolean
}>()

// Speed: seeded from the server's `started_at` (whole-transfer average,
// meaningful even on the first update), then EMA-refined from bytes_sent
// deltas between live snapshots.
const lastSample = ref<{ bytes: number; at: number } | null>(null)
const rate = ref<number | null>(null)
const now = ref(Math.floor(Date.now() / 1000))
const clock = setInterval(() => (now.value = Math.floor(Date.now() / 1000)), 1000)
onUnmounted(() => clearInterval(clock))

watch(
  () => props.transfer.bytes_sent,
  (bytes) => {
    const now = Date.now()
    const t = props.transfer
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

const pct = computed(() => {
  const t = props.transfer
  if (!t.total_bytes) return null
  return Math.min(100, (t.bytes_sent / t.total_bytes) * 100)
})
const eta = computed(() => {
  const t = props.transfer
  if (phase.value !== 'sending' || !t.total_bytes || !rate.value || rate.value < 1) return null
  const left = (t.total_bytes - t.bytes_sent) / rate.value
  if (left < 90) return `${Math.round(left)}s`
  if (left < 5400) return `${Math.round(left / 60)}m`
  return `${(left / 3600).toFixed(1)}h`
})
const elapsed = computed(() => {
  const t = props.transfer
  if (!t.started_at) return null
  const s = Math.max(0, now.value - t.started_at)
  if (s < 90) return `${Math.max(1, Math.round(s))}s`
  if (s < 5400) return `${Math.round(s / 60)}m`
  return `${Math.round(s / 3600)}h`
})

const phase = computed(() => props.transfer.phase || 'sending')
const phaseSeconds = computed(() =>
  props.transfer.phase_since ? Math.max(0, now.value - props.transfer.phase_since) : 0,
)
const phaseView = computed(() => {
  const seconds = phaseSeconds.value
  switch (phase.value) {
    case 'waiting_sender':
      return {
        icon: 'i-lucide-hourglass',
        label: `waiting for sender ZFS · ${seconds}s`,
        detail: 'zfs send is still producing the next block',
        color: 'text-warning',
      }
    case 'waiting_receiver':
      return {
        icon: 'i-lucide-hard-drive',
        label: `waiting for receiver I/O · ${seconds}s`,
        detail: 'the network or receiver storage is applying backpressure',
        color: 'text-warning',
      }
    case 'finalizing':
      return {
        icon: 'i-lucide-loader-circle',
        label: `finalizing snapshot · ${seconds}s`,
        detail: 'all stream bytes were sent; waiting for zfs recv to commit',
        color: 'text-info',
      }
    case 'committing':
      return {
        icon: 'i-lucide-bookmark-check',
        label: `updating replication cursor · ${seconds}s`,
        detail: 'the received snapshot is complete',
        color: 'text-info',
      }
    default:
      return null
  }
})
</script>

<template>
  <!-- Two-line layout: a one-line row let the stats squeeze the dataset
       name into "novafs…" in narrow cards. Name and destination get the
       first line; metrics breathe on their own line below. -->
  <div class="space-y-1.5">
    <div class="flex items-center justify-between gap-2 text-sm min-w-0">
      <span class="font-mono truncate" :title="transfer.dataset">{{ transfer.dataset }}</span>
      <span class="text-muted shrink-0 text-xs">{{ transfer.kind }}</span>
    </div>
    <div class="text-muted font-mono text-xs">
      <template v-if="showPeer">→ {{ transfer.peer }} · </template
      >{{ formatBytes(transfer.bytes_sent)
      }}<template v-if="transfer.total_bytes"> / ~{{ formatBytes(transfer.total_bytes) }}</template>
      <template v-if="rate && phase === 'sending'"> · {{ formatBytes(rate) }}/s</template>
      <template v-if="eta"> · ~{{ eta }} left</template>
      <template v-else-if="elapsed"> · {{ elapsed }} elapsed</template>
    </div>
    <div
      v-if="phaseView"
      class="flex items-center gap-1.5 text-xs"
      :class="phaseView.color"
      :title="phaseView.detail"
    >
      <UIcon
        :name="phaseView.icon"
        class="size-3.5 shrink-0"
        :class="phase === 'finalizing' ? 'animate-spin' : ''"
      />
      <span>{{ phaseView.label }}</span>
    </div>
    <UProgress
      v-if="pct != null && phase !== 'finalizing' && phase !== 'committing'"
      :model-value="pct"
      size="sm"
    />
    <UProgress v-else size="sm" animation="carousel" />
  </div>
</template>
