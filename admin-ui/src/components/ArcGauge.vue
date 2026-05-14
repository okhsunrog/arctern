<script setup lang="ts">
import { computed } from 'vue'
import type { ArcStats } from '../client'
import { formatBytes } from '../utils/format'

const props = defineProps<{ arc: ArcStats | null }>()

const ratioPercent = computed(() => {
  if (!props.arc || props.arc.hit_ratio == null) return null
  return Math.round(props.arc.hit_ratio * 1000) / 10
})

const sizePercent = computed(() => {
  if (!props.arc || props.arc.c_max === 0) return 0
  return Math.round((props.arc.size / props.arc.c_max) * 100)
})

const ratioColor = computed<'success' | 'warning' | 'error' | 'neutral'>(() => {
  const p = ratioPercent.value
  if (p == null) return 'neutral'
  if (p >= 95) return 'success'
  if (p >= 80) return 'warning'
  return 'error'
})
</script>

<template>
  <UCard>
    <template #header>
      <div class="font-semibold">ARC</div>
    </template>
    <div v-if="!arc" class="text-gray-500 text-sm">Loading…</div>
    <template v-else>
      <div class="flex items-center justify-between mb-3">
        <div>
          <div class="text-xs text-gray-500">Hit ratio</div>
          <div class="text-3xl font-semibold">
            <template v-if="ratioPercent != null">{{ ratioPercent }}%</template>
            <template v-else>—</template>
          </div>
        </div>
        <UBadge :color="ratioColor" variant="subtle">
          {{ arc.hits.toLocaleString() }} hits / {{ arc.misses.toLocaleString() }} misses
        </UBadge>
      </div>
      <div class="space-y-1">
        <div class="flex justify-between text-xs text-gray-500">
          <span>{{ formatBytes(arc.size) }} / {{ formatBytes(arc.c_max) }} max</span>
          <span>{{ sizePercent }}% of c_max</span>
        </div>
        <UProgress :model-value="sizePercent" />
      </div>
    </template>
  </UCard>
</template>
