<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { useJobs } from '../composables/useJobs'
import { formatTimestamp } from '../utils/format'

const route = useRoute()
const { jobs, error, wake } = useJobs()

const job = computed(() => jobs.value.find((j) => j.name === route.params.name))
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-6">
    <UAlert v-if="error" color="error" :title="error" class="mb-4" />
    <div v-if="!job" class="text-gray-500">Job not found.</div>
    <template v-else>
      <div class="flex items-center justify-between mb-6">
        <div>
          <h1 class="text-2xl font-semibold">{{ job.name }}</h1>
          <p class="text-sm text-gray-500">{{ job.kind }}</p>
        </div>
        <UButton icon="i-lucide-zap" @click="wake(job.name)">Wakeup</UButton>
      </div>
      <UCard>
        <dl class="grid grid-cols-2 gap-y-3 text-sm">
          <dt class="text-gray-500">Last run</dt>
          <dd>{{ formatTimestamp(job.last_run) }}</dd>
          <dt class="text-gray-500">Next run</dt>
          <dd>{{ formatTimestamp(job.next_run) }}</dd>
          <dt class="text-gray-500">Last error</dt>
          <dd class="text-error-600">{{ job.last_error ?? '—' }}</dd>
        </dl>
      </UCard>
      <p class="text-sm text-gray-400 mt-4">
        Throughput + duration charts land in a later commit (per docs/admin-ui-plan.md).
      </p>
    </template>
  </div>
</template>
