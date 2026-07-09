<script setup lang="ts">
import { computed } from 'vue'
import {
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from 'chart.js'
import type { ChartData, ChartOptions } from 'chart.js'
import { Line } from 'vue-chartjs'
import { useColorMode } from '@vueuse/core'
import { areaGradient, baseOptions, chartColors, lineDataset } from '../utils/chartTheme'
import { useHost } from '../composables/useHost'
import { useArc } from '../composables/useArc'
import { formatBytes } from '../utils/format'
import ArcGauge from '../components/ArcGauge.vue'

ChartJS.register(
  CategoryScale,
  LinearScale,
  LineController,
  LineElement,
  PointElement,
  Filler,
  Tooltip,
  Legend,
)

const mode = useColorMode({ emitAuto: false })
const { host, baseUrl } = useHost()
const { arc, history, error, loading } = useArc(5000, true, 720, baseUrl.value)
const title = computed(() => (host.value ? `${host.value} · ARC` : 'ARC'))

// Reverse so the chart goes oldest → newest.
const ordered = computed(() => [...history.value].reverse())

const hhmm = (ts: number) =>
  new Date(ts * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
const labels = computed(() => ordered.value.map((p) => hhmm(p.timestamp)))

// Hit ratio is computed *per-sample-delta* — kernel counters are
// monotonic since boot, so the running ratio drifts toward whatever
// state the cache has been in since boot. The interesting signal is
// "hit ratio over the last minute": diff hits/misses against the
// previous sample, divide.
const hitRateSeries = computed(() => {
  const out: { x: string; y: number }[] = []
  for (let i = 1; i < ordered.value.length; i++) {
    const prev = ordered.value[i - 1]
    const cur = ordered.value[i]
    if (!prev || !cur) continue
    const dh = Number(cur.hits) - Number(prev.hits)
    const dm = Number(cur.misses) - Number(prev.misses)
    const total = dh + dm
    if (total === 0) continue
    out.push({
      x: hhmm(cur.timestamp),
      y: Math.round((dh / total) * 1000) / 10,
    })
  }
  return out
})

const sizeData = computed(() => {
  void mode.value
  const c = chartColors()
  return {
    labels: labels.value,
    datasets: [
      lineDataset({
        label: 'size',
        data: ordered.value.map((p) => Number(p.size)),
        borderColor: c.primary,
        backgroundColor: areaGradient(c.primary),
        fill: true,
      }),
      lineDataset({
        label: 'c (target)',
        data: ordered.value.map((p) => Number(p.c)),
        borderColor: c.neutral,
        borderWidth: 1.5,
        borderDash: [5, 5],
        tension: 0,
        fill: false,
      }),
    ],
  } as ChartData<'line', number[], string>
})

const rateData = computed(() => {
  void mode.value
  const c = chartColors()
  return {
    labels: hitRateSeries.value.map((p) => p.x),
    datasets: [
      lineDataset({
        label: 'hit rate',
        data: hitRateSeries.value.map((p) => p.y),
        borderColor: c.success,
        backgroundColor: areaGradient(c.success),
        fill: true,
      }),
    ],
  } as ChartData<'line', number[], string>
})

const sizeOpts = computed(() => {
  void mode.value
  const o = baseOptions({
    yTick: (v) => formatBytes(v),
    tooltipValue: (ctx) =>
      ` ${ctx.dataset.label}: ${ctx.parsed.y == null ? '—' : formatBytes(ctx.parsed.y)}`,
  })
  o.scales.y = { ...o.scales.y, beginAtZero: true } as typeof o.scales.y
  return o as unknown as ChartOptions<'line'>
})

const rateOpts = computed(() => {
  void mode.value
  const o = baseOptions({
    yTick: (v) => `${v}%`,
    tooltipValue: (ctx) => ` hit rate: ${ctx.parsed.y}%`,
  })
  // Zoom into where the signal lives: pinning the axis at 0-100 flattens
  // a cache that hovers near 100% into a line glued to the ceiling.
  const floor = Math.min(90, ...hitRateSeries.value.map((p) => p.y))
  o.scales.y = {
    ...o.scales.y,
    min: Math.max(0, Math.floor(floor / 10) * 10 - 5),
    max: 100,
  } as typeof o.scales.y
  return o as unknown as ChartOptions<'line'>
})
</script>

<template>
  <UDashboardPanel id="arc">
    <template #header>
      <UDashboardNavbar :title="title" />
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-4">
        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />

        <ArcGauge :arc="arc" />

        <div v-if="loading && history.length === 0" class="text-gray-500">
          Collecting samples… the daemon writes one row per minute.
        </div>
        <div v-else-if="history.length < 2" class="text-gray-500">
          Need at least 2 samples for the hit-rate delta chart. Check back in a minute.
        </div>
        <div v-else class="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <UCard>
            <template #header>
              <div class="flex items-center justify-between">
                <div class="font-semibold">Size vs. target</div>
                <div class="flex items-center gap-3 text-xs text-muted">
                  <span class="inline-flex items-center gap-1.5">
                    <span class="inline-block w-3 h-0.5 rounded bg-primary" /> size
                  </span>
                  <span class="inline-flex items-center gap-1.5">
                    <span
                      class="inline-block w-3 border-t-2 border-dashed border-neutral-400 dark:border-neutral-500"
                    />
                    c (target)
                  </span>
                </div>
              </div>
            </template>
            <div class="h-64">
              <Line :data="sizeData" :options="sizeOpts" />
            </div>
          </UCard>
          <UCard>
            <template #header>
              <div class="font-semibold">Hit rate (per-minute delta)</div>
            </template>
            <div class="h-64">
              <Line :data="rateData" :options="rateOpts" />
            </div>
          </UCard>
        </div>

        <UCard v-if="arc">
          <template #header><div class="font-semibold">Breakdown</div></template>
          <dl class="grid grid-cols-2 md:grid-cols-4 gap-y-2 text-sm">
            <dt class="text-gray-500">Demand data</dt>
            <dd>
              {{ arc.demand_data_hits.toLocaleString() }} /
              {{ arc.demand_data_misses.toLocaleString() }}
            </dd>
            <dt class="text-gray-500">Demand metadata</dt>
            <dd>
              {{ arc.demand_metadata_hits.toLocaleString() }} /
              {{ arc.demand_metadata_misses.toLocaleString() }}
            </dd>
            <dt class="text-gray-500">Prefetch data</dt>
            <dd>
              {{ arc.prefetch_data_hits.toLocaleString() }} /
              {{ arc.prefetch_data_misses.toLocaleString() }}
            </dd>
            <dt class="text-gray-500">Prefetch metadata</dt>
            <dd>
              {{ arc.prefetch_metadata_hits.toLocaleString() }} /
              {{ arc.prefetch_metadata_misses.toLocaleString() }}
            </dd>
            <dt class="text-gray-500">MRU / MFU</dt>
            <dd>{{ arc.mru_hits.toLocaleString() }} / {{ arc.mfu_hits.toLocaleString() }}</dd>
            <dt class="text-gray-500">Ghost MRU / MFU</dt>
            <dd>
              {{ arc.mru_ghost_hits.toLocaleString() }} / {{ arc.mfu_ghost_hits.toLocaleString() }}
            </dd>
            <dt class="text-gray-500">Compression</dt>
            <dd v-if="arc.compressed_size > 0">
              {{ formatBytes(arc.uncompressed_size) }} → {{ formatBytes(arc.compressed_size) }} ({{
                (arc.uncompressed_size / arc.compressed_size).toFixed(2)
              }}×)
            </dd>
            <dd v-else>—</dd>
            <dt class="text-gray-500">L2ARC</dt>
            <dd v-if="arc.l2_size > 0">
              {{ formatBytes(arc.l2_size) }} · {{ arc.l2_hits.toLocaleString() }} hits /
              {{ arc.l2_misses.toLocaleString() }} misses
            </dd>
            <dd v-else>not configured</dd>
            <dt class="text-gray-500">c bounds</dt>
            <dd class="font-mono">{{ formatBytes(arc.c_min) }} … {{ formatBytes(arc.c_max) }}</dd>
          </dl>
        </UCard>
      </div>
    </template>
  </UDashboardPanel>
</template>
