<script setup lang="ts">
import { computed } from 'vue'
import { useJobs } from '../composables/useJobs'
import { useEvents } from '../composables/useEvents'
import { useArc } from '../composables/useArc'
import JobsGrid from '../components/JobsGrid.vue'
import EventsLog from '../components/EventsLog.vue'
import ArcGauge from '../components/ArcGauge.vue'

const { jobs, error, loading, wake, cancel, pause, resume, pushTo } = useJobs()
const { events, connected } = useEvents({ cap: 200 })
const { arc } = useArc(5000)

const tail = computed(() => events.value.slice(-50))
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6 space-y-6">
    <div>
      <h1 class="text-2xl font-semibold mb-4">Dashboard</h1>
      <UAlert v-if="error" color="error" :title="error" class="mb-4" />
      <ArcGauge :arc="arc" />
    </div>
    <div>
      <h2 class="text-lg font-semibold mb-2">Jobs</h2>
      <div v-if="loading && jobs.length === 0" class="text-gray-500">Loading…</div>
      <div v-else-if="jobs.length === 0" class="text-gray-500">No jobs configured.</div>
      <JobsGrid
        v-else
        :jobs="jobs"
        :on-wake="wake"
        :on-cancel="cancel"
        :on-pause="pause"
        :on-resume="resume"
        :on-push-to="pushTo"
      />
    </div>
    <div>
      <div class="flex items-center justify-between mb-2">
        <h2 class="text-lg font-semibold">Recent events</h2>
        <UBadge :color="connected ? 'success' : 'neutral'" variant="subtle">
          {{ connected ? 'live' : 'idle' }}
        </UBadge>
      </div>
      <EventsLog :events="tail" max-height-class="max-h-80" />
    </div>
  </div>
</template>
