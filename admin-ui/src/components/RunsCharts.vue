<script setup lang="ts">
import { computed } from 'vue'
import {
  BarController,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from 'chart.js'
import { Bar, Line } from 'vue-chartjs'
import type { ChartData, ChartOptions } from 'chart.js'
import { useColorMode } from '@vueuse/core'
import { areaGradient, baseOptions, chartColors, lineDataset } from '../utils/chartTheme'
import { formatBytes } from '../utils/format'
import type { JobRun } from '../client'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarController,
  BarElement,
  LineController,
  LineElement,
  PointElement,
  Filler,
  Tooltip,
)

const mode = useColorMode({ emitAuto: false })
const props = defineProps<{ runs: JobRun[] }>()

// Reverse so oldest is leftmost on the X axis (API returns newest-first).
const ordered = computed(() => [...props.runs].reverse())

const labels = computed(() =>
  ordered.value.map((r) =>
    new Date(r.started_at * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
  ),
)

const durationData = computed(() => {
  void mode.value
  const c = chartColors()
  return {
    labels: labels.value,
    datasets: [
      lineDataset({
        label: 'duration',
        data: ordered.value.map((r) =>
          r.finished_at ? Math.max(0, r.finished_at - r.started_at) : 0,
        ),
        borderColor: c.primary,
        backgroundColor: areaGradient(c.primary),
        fill: true,
      }),
    ],
  } as ChartData<'line', number[], string>
})

const bytesData = computed(() => {
  void mode.value
  const c = chartColors()
  return {
    labels: labels.value,
    datasets: [
      {
        label: 'bytes sent',
        data: ordered.value.map((r) => Number(r.bytes_sent ?? 0)),
        backgroundColor: ordered.value.map((r) => (r.status === 'error' ? c.error : c.success)),
        borderRadius: 3,
        maxBarThickness: 22,
      },
    ],
  } as ChartData<'bar', number[], string>
})

const durationOpts = computed(() => {
  void mode.value
  const o = baseOptions({
    yTick: (v) => `${v}s`,
    tooltipValue: (ctx) => ` duration: ${ctx.parsed.y}s`,
  })
  o.scales.y = { ...o.scales.y, beginAtZero: true } as typeof o.scales.y
  return o as unknown as ChartOptions<'line'>
})
const bytesOpts = computed(() => {
  void mode.value
  const o = baseOptions({
    yTick: (v) => formatBytes(v),
    tooltipValue: (ctx) => ` sent: ${formatBytes(ctx.parsed.y)}`,
  })
  o.scales.y = { ...o.scales.y, beginAtZero: true } as typeof o.scales.y
  return o as unknown as ChartOptions<'bar'>
})
</script>

<template>
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
    <UCard>
      <template #header>
        <div class="font-semibold">Cycle duration</div>
      </template>
      <div class="h-64">
        <Line :data="durationData" :options="durationOpts" />
      </div>
    </UCard>
    <UCard>
      <template #header>
        <div class="font-semibold">Bytes sent per cycle</div>
      </template>
      <div class="h-64">
        <Bar :data="bytesData" :options="bytesOpts" />
      </div>
    </UCard>
  </div>
</template>
