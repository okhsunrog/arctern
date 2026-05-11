<script setup lang="ts">
import { computed } from 'vue'
import {
  BarController,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from 'chart.js'
import { Bar, Line } from 'vue-chartjs'
import type { JobRun } from '../client'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarController,
  BarElement,
  LineController,
  LineElement,
  PointElement,
  Tooltip,
)

const props = defineProps<{ runs: JobRun[] }>()

// Reverse so oldest is leftmost on the X axis (API returns newest-first).
const ordered = computed(() => [...props.runs].reverse())

const labels = computed(() =>
  ordered.value.map((r) => new Date(r.started_at * 1000).toLocaleTimeString()),
)

const durationData = computed(() => ({
  labels: labels.value,
  datasets: [
    {
      label: 'Duration (s)',
      data: ordered.value.map((r) =>
        r.finished_at ? Math.max(0, r.finished_at - r.started_at) : 0,
      ),
      borderColor: '#3b82f6',
      backgroundColor: 'rgba(59, 130, 246, 0.2)',
      tension: 0.25,
    },
  ],
}))

const bytesData = computed(() => ({
  labels: labels.value,
  datasets: [
    {
      label: 'Bytes sent',
      data: ordered.value.map((r) => Number(r.bytes_sent ?? 0)),
      backgroundColor: ordered.value.map((r) =>
        r.status === 'error' ? '#ef4444' : '#10b981',
      ),
    },
  ],
}))

const chartOptions = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: { legend: { display: false } },
  scales: { y: { beginAtZero: true } },
}
</script>

<template>
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
    <UCard>
      <template #header>
        <div class="font-semibold">Cycle duration</div>
      </template>
      <div class="h-64">
        <Line :data="durationData" :options="chartOptions" />
      </div>
    </UCard>
    <UCard>
      <template #header>
        <div class="font-semibold">Bytes sent per cycle</div>
      </template>
      <div class="h-64">
        <Bar :data="bytesData" :options="chartOptions" />
      </div>
    </UCard>
  </div>
</template>
