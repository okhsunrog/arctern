<script setup lang="ts">
import { computed } from 'vue'
import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from 'chart.js'
import { Line } from 'vue-chartjs'
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
  Tooltip,
  Legend,
)

const { host, baseUrl } = useHost()
const { arc, history, error, loading } = useArc(5000, true, 720, baseUrl.value)
const title = computed(() => (host.value ? `${host.value} · ARC` : 'ARC'))

// Reverse so the chart goes oldest → newest.
const ordered = computed(() => [...history.value].reverse())

const labels = computed(() =>
  ordered.value.map((p) => new Date(p.timestamp * 1000).toLocaleTimeString()),
)

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
      x: new Date(cur.timestamp * 1000).toLocaleTimeString(),
      y: Math.round((dh / total) * 1000) / 10,
    })
  }
  return out
})

const sizeData = computed(() => ({
  labels: labels.value,
  datasets: [
    {
      label: 'size',
      data: ordered.value.map((p) => Number(p.size)),
      borderColor: '#3b82f6',
      backgroundColor: 'rgba(59, 130, 246, 0.15)',
      tension: 0.25,
      fill: true,
    },
    {
      label: 'c (target)',
      data: ordered.value.map((p) => Number(p.c)),
      borderColor: '#94a3b8',
      borderDash: [4, 4],
      tension: 0,
      fill: false,
    },
  ],
}))

const rateData = computed(() => ({
  labels: hitRateSeries.value.map((p) => p.x),
  datasets: [
    {
      label: 'hit rate (delta %)',
      data: hitRateSeries.value.map((p) => p.y),
      borderColor: '#10b981',
      backgroundColor: 'rgba(16, 185, 129, 0.15)',
      tension: 0.25,
      fill: true,
    },
  ],
}))

const sizeOpts = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { position: 'top' as const },
    tooltip: {
      callbacks: {
        label: (ctx: { dataset: { label?: string }; parsed: { y: number | null } }) =>
          `${ctx.dataset.label}: ${ctx.parsed.y == null ? '—' : formatBytes(ctx.parsed.y)}`,
      },
    },
  },
  scales: {
    y: {
      beginAtZero: true,
      ticks: {
        callback: (v: string | number) => formatBytes(typeof v === 'number' ? v : Number(v)),
      },
    },
  },
}

const rateOpts = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: { legend: { display: false } },
  scales: {
    y: { beginAtZero: true, max: 100, ticks: { callback: (v: string | number) => `${v}%` } },
  },
}
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
              <div class="font-semibold">Size vs. target</div>
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
