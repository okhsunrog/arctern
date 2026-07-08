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

// Level → dot + row treatment. INFO stays visually silent (a dot, no
// tint); WARN/ERROR rows carry a faint wash + colored message so scan
// order is severity-first.
function dotClass(level: string): string {
  switch (level) {
    case 'ERROR':
      return 'bg-error'
    case 'WARN':
      return 'bg-warning'
    case 'INFO':
      return 'bg-info/70'
    default:
      return 'bg-neutral-400 dark:bg-neutral-600'
  }
}

function rowClass(level: string): string {
  switch (level) {
    case 'ERROR':
      return 'bg-error/5 text-error'
    case 'WARN':
      return 'bg-warning/5 text-warning'
    default:
      return ''
  }
}

function formatTime(unixSec: number): string {
  return new Date(unixSec * 1000).toLocaleTimeString(undefined, { hour12: false })
}
</script>

<template>
  <div
    ref="scrollRoot"
    class="rounded-md border border-default bg-default overflow-y-auto font-mono text-xs"
    :class="maxHeightClass"
  >
    <UEmpty
      v-if="filtered.length === 0"
      icon="i-lucide-activity"
      title="No events yet"
      :ui="{ root: 'py-10' }"
    />
    <div v-else class="py-1">
      <div
        v-for="e in filtered"
        :key="e.id"
        class="group flex items-baseline gap-2.5 px-3 py-[3px] hover:bg-elevated/60 transition-colors"
        :class="rowClass(e.level)"
      >
        <span class="text-muted/70 tabular-nums shrink-0 select-none">
          {{ formatTime(e.timestamp) }}
        </span>
        <span
          class="inline-block size-1.5 rounded-full shrink-0 self-center"
          :class="dotClass(e.level)"
          :title="e.level"
        />
        <span v-if="e.job_name" class="text-primary/80 shrink-0 select-none">{{ e.job_name }}</span>
        <span
          class="whitespace-pre-wrap break-all text-highlighted/90 group-hover:text-highlighted"
        >
          {{ e.message }}
        </span>
      </div>
    </div>
  </div>
</template>
