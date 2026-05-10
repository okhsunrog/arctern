<script setup lang="ts">
import { useJobs } from '../composables/useJobs'
import JobsGrid from '../components/JobsGrid.vue'

const { jobs, error, loading, wake } = useJobs()
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6">
    <h1 class="text-2xl font-semibold mb-4">Dashboard</h1>
    <UAlert v-if="error" color="error" :title="error" class="mb-4" />
    <div v-if="loading && jobs.length === 0" class="text-gray-500">Loading…</div>
    <div v-else-if="jobs.length === 0" class="text-gray-500">No jobs configured.</div>
    <JobsGrid v-else :jobs="jobs" :on-wake="wake" />
  </div>
</template>
