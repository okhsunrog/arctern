<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import type { LogEvent } from '../client'

const props = withDefaults(
  defineProps<{
    events: LogEvent[]
    autoScroll?: boolean
    maxHeightClass?: string
    levelFilter?: string
    jobFilter?: string
  }>(),
  { autoScroll: true, maxHeightClass: 'max-h-[60vh]' },
)

const scrollRoot = ref<HTMLElement | null>(null)

const filtered = computed(() =>
  props.events.filter((e) => {
    if (props.levelFilter && e.level !== props.levelFilter) return false
    if (props.jobFilter && e.job_name !== props.jobFilter) return false
    return true
  }),
)

watch(
  () => filtered.value.length,
  async () => {
    if (!props.autoScroll || !scrollRoot.value) return
    await nextTick()
    scrollRoot.value.scrollTop = scrollRoot.value.scrollHeight
  },
)

function levelColor(level: string): 'error' | 'warning' | 'info' | 'neutral' {
  if (level === 'ERROR') return 'error'
  if (level === 'WARN') return 'warning'
  if (level === 'INFO') return 'info'
  return 'neutral'
}

function formatTime(unixSec: number): string {
  return new Date(unixSec * 1000).toLocaleTimeString()
}
</script>

<template>
  <div
    ref="scrollRoot"
    class="rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 overflow-y-auto font-mono text-xs"
    :class="maxHeightClass"
  >
    <div v-if="filtered.length === 0" class="p-4 text-gray-400">No events yet.</div>
    <div
      v-for="e in filtered"
      :key="e.id"
      class="px-3 py-1 border-b border-gray-100 dark:border-gray-800 flex gap-3"
    >
      <span class="text-gray-500 shrink-0">{{ formatTime(e.timestamp) }}</span>
      <UBadge :color="levelColor(e.level)" variant="subtle" size="xs" class="shrink-0">
        {{ e.level }}
      </UBadge>
      <span v-if="e.job_name" class="text-primary-500 shrink-0">{{ e.job_name }}</span>
      <span class="whitespace-pre-wrap break-all">{{ e.message }}</span>
    </div>
  </div>
</template>
