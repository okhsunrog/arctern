<script setup lang="ts">
import { computed, ref } from 'vue'
import { useHost } from '../composables/useHost'
import { useEvents } from '../composables/useEvents'
import EventsLog from '../components/EventsLog.vue'

const { host } = useHost()
const { events, connected, error, paused, togglePause, clear } = useEvents({
  peer: computed(() => host.value ?? undefined),
})
const title = computed(() => (host.value ? `${host.value} · Events` : 'Events'))

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
  <UDashboardPanel id="events">
    <template #header>
      <UDashboardNavbar :title="title">
        <template #right>
          <UBadge
            :color="connected ? 'success' : 'error'"
            variant="subtle"
            :icon="connected ? 'i-lucide-signal' : 'i-lucide-signal-zero'"
          >
            {{ connected ? 'live' : 'disconnected' }}
          </UBadge>
          <UButton
            :icon="paused ? 'i-lucide-play' : 'i-lucide-pause'"
            variant="soft"
            size="xs"
            @click="togglePause"
            >{{ paused ? 'Resume' : 'Pause' }}</UButton
          >
          <UButton icon="i-lucide-eraser" variant="soft" size="xs" @click="clear">Clear</UButton>
        </template>
      </UDashboardNavbar>
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-3">
        <UAlert v-if="error" color="warning" :title="error" icon="i-lucide-triangle-alert" />
        <div class="flex gap-3 text-sm">
          <USelect
            v-model="levelFilter"
            :items="levelOptions"
            placeholder="all levels"
            class="w-32"
          />
          <USelect v-model="jobFilter" :items="jobOptions" placeholder="all jobs" class="w-48" />
        </div>
        <EventsLog
          :events="events"
          :level-filter="levelFilter || undefined"
          :job-filter="jobFilter || undefined"
          max-height-class="max-h-[70vh]"
        />
      </div>
    </template>
  </UDashboardPanel>
</template>
