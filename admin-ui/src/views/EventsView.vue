<script setup lang="ts">
import { computed, ref } from 'vue'
import { useEvents } from '../composables/useEvents'
import EventsLog from '../components/EventsLog.vue'

const { events, connected, error, paused, togglePause, clear } = useEvents()

const levelFilter = ref<string>('')
const jobFilter = ref<string>('')

const levelOptions = ['', 'ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE']
const jobOptions = computed(() => {
  const set = new Set<string>()
  for (const e of events.value) if (e.job_name) set.add(e.job_name)
  return ['', ...Array.from(set).sort()]
})
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6">
    <div class="flex items-center justify-between mb-4">
      <h1 class="text-2xl font-semibold">Events</h1>
      <div class="flex items-center gap-2">
        <UBadge :color="connected ? 'success' : 'error'" variant="subtle">
          {{ connected ? 'live' : 'disconnected' }}
        </UBadge>
        <UButton
          :icon="paused ? 'i-lucide-play' : 'i-lucide-pause'"
          variant="soft"
          size="xs"
          @click="togglePause"
        >{{ paused ? 'Resume' : 'Pause' }}</UButton>
        <UButton icon="i-lucide-trash-2" variant="soft" size="xs" @click="clear">Clear</UButton>
      </div>
    </div>
    <UAlert v-if="error" color="warning" :title="error" class="mb-4" />
    <div class="flex gap-3 mb-3 text-sm">
      <USelect v-model="levelFilter" :items="levelOptions" placeholder="all levels" class="w-32" />
      <USelect v-model="jobFilter" :items="jobOptions" placeholder="all jobs" class="w-48" />
    </div>
    <EventsLog
      :events="events"
      :level-filter="levelFilter || undefined"
      :job-filter="jobFilter || undefined"
    />
  </div>
</template>
