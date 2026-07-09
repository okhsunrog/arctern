<script setup lang="ts">
import { onUnmounted, ref } from 'vue'
import { recentTransfers } from '../client'
import type { RecvTransfer } from '../client'
import { formatBytes } from '../utils/format'

// Inbound replication received BY this host, as recorded by its recv
// channels. Rendered only when the host has ever received anything —
// a pure sender (the laptop) never shows the section.
const props = defineProps<{ baseUrl?: string }>()

const rows = ref<RecvTransfer[]>([])
const loaded = ref(false)

async function refresh() {
  const r = await recentTransfers({
    baseUrl: props.baseUrl ?? '',
    query: { limit: 20 },
  })
  if (!r.error) rows.value = r.data ?? []
  loaded.value = true
}

void refresh()
const handle = setInterval(() => void refresh(), 10000)
onUnmounted(() => clearInterval(handle))

function age(unixSec: number): string {
  const s = Math.max(0, Math.floor(Date.now() / 1000) - unixSec)
  if (s < 90) return `${s}s ago`
  if (s < 5400) return `${Math.round(s / 60)}m ago`
  if (s < 129600) return `${Math.round(s / 3600)}h ago`
  return `${Math.round(s / 86400)}d ago`
}

function speed(t: RecvTransfer): string | null {
  if (t.duration_ms < 200 || t.bytes < 1024) return null
  return `${formatBytes((t.bytes * 1000) / t.duration_ms)}/s`
}
</script>

<template>
  <UCard v-if="loaded && rows.length">
    <template #header>
      <div class="flex items-center gap-2">
        <UIcon name="i-lucide-download" class="text-muted" />
        <span class="font-medium">Incoming</span>
        <span class="text-muted text-xs">last transfers received by this host</span>
      </div>
    </template>
    <ul class="divide-y divide-default">
      <li
        v-for="t in rows"
        :key="t.id"
        class="flex items-center justify-between gap-3 py-1.5 text-sm min-w-0"
      >
        <span class="font-mono truncate" :title="`${t.dataset}@${t.to_snapshot}`">
          {{ t.dataset }}<span class="text-muted">@{{ t.to_snapshot }}</span>
        </span>
        <span class="text-muted shrink-0 font-mono text-xs">
          from {{ t.identity }} · {{ formatBytes(t.bytes)
          }}<template v-if="speed(t)"> · {{ speed(t) }}</template> · {{ age(t.completed_at) }}
        </span>
      </li>
    </ul>
  </UCard>
</template>
