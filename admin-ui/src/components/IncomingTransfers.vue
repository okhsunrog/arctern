<script setup lang="ts">
import { computed } from 'vue'
import { useQuery } from '@pinia/colada'
import { recentTransfersQuery } from '../queries'
import { formatAge, formatBytes } from '../utils/format'
import type { RecvTransfer } from '../client'

// Inbound replication received BY this host, as recorded by its recv
// channels. Rendered only when the host has ever received anything —
// a pure sender (the laptop) never shows the section.
const props = defineProps<{ scope?: string }>()

const query = useQuery(() => recentTransfersQuery({ scope: props.scope ?? '', limit: 20 }))
const rows = computed(() => query.data.value ?? [])

function speed(t: RecvTransfer): string | null {
  if (t.duration_ms < 200 || t.bytes < 1024) return null
  return `${formatBytes((t.bytes * 1000) / t.duration_ms)}/s`
}
</script>

<template>
  <UAlert
    v-if="query.error.value"
    color="warning"
    title="Incoming transfers unavailable"
    :description="query.error.value.message"
    class="mb-3"
  />
  <UCard v-if="rows.length">
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
          }}<template v-if="speed(t)"> · {{ speed(t) }}</template> · {{ formatAge(t.completed_at) }}
        </span>
      </li>
    </ul>
  </UCard>
</template>
