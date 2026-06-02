<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { usePool } from '../composables/usePools'
import { parseZpoolSize, poolStateColor, poolUsedPercent } from '../utils/pool'
import { formatRelative } from '../utils/format'
import VdevTree from '../components/VdevTree.vue'

const route = useRoute()
const name = computed(() => String(route.params.name))
const { pool, error, scrub } = usePool(name.value)

// Top-level vdev capacity totals so the UI can show a real "X% used".
const totals = computed(() => {
  const vs = pool.value?.vdevs ?? []
  let alloc = 0
  let total = 0
  for (const v of vs) {
    alloc += parseZpoolSize(v.alloc_space)
    total += parseZpoolSize(v.total_space)
  }
  return { alloc, total, percent: total > 0 ? Math.round((alloc / total) * 100) : 0 }
})

const scrubActive = computed(() => pool.value?.scan?.state === 'SCANNING')

function scrubProgressPercent(): number {
  const s = pool.value?.scan
  if (!s) return 0
  const examined = parseZpoolSize(s.examined ?? null)
  const toExamine = parseZpoolSize(s.to_examine ?? null)
  return toExamine > 0 ? Math.min(100, Math.round((examined / toExamine) * 100)) : 0
}
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6 space-y-6">
    <RouterLink to="/pools" class="text-sm text-primary-500 hover:underline"> ← Pools </RouterLink>
    <UAlert v-if="error" color="error" :title="error" />

    <div v-if="!pool" class="text-gray-500">Loading…</div>
    <template v-else>
      <div class="flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-semibold">{{ pool.name }}</h1>
          <p class="text-xs text-gray-500 font-mono">
            guid {{ pool.pool_guid }} · txg {{ pool.txg }}
          </p>
        </div>
        <UBadge :color="poolStateColor(pool.state)" variant="subtle">{{ pool.state }}</UBadge>
      </div>

      <UCard>
        <template #header>
          <div class="flex items-center justify-between">
            <div class="font-semibold">Capacity</div>
            <span class="text-sm text-gray-500">{{ totals.percent }}% used</span>
          </div>
        </template>
        <UProgress
          :model-value="totals.percent"
          :color="totals.percent > 90 ? 'error' : totals.percent > 75 ? 'warning' : 'primary'"
        />
        <div class="text-xs text-gray-500 mt-2">
          <span>
            {{ (totals.alloc / 1024 ** 3).toFixed(1) }} GiB allocated of
            {{ (totals.total / 1024 ** 3).toFixed(1) }} GiB total
          </span>
        </div>
      </UCard>

      <UCard>
        <template #header>
          <div class="flex items-center justify-between">
            <div class="font-semibold">Scrub</div>
            <div class="flex gap-2">
              <UButton v-if="!scrubActive" size="xs" icon="i-lucide-play" @click="scrub('start')"
                >Start</UButton
              >
              <template v-else>
                <UButton size="xs" icon="i-lucide-pause" variant="soft" @click="scrub('pause')"
                  >Pause</UButton
                >
                <UButton
                  size="xs"
                  icon="i-lucide-square"
                  variant="soft"
                  color="error"
                  @click="scrub('stop')"
                  >Stop</UButton
                >
              </template>
            </div>
          </div>
        </template>

        <div v-if="!pool.scan" class="text-gray-500 text-sm">No scrub recorded.</div>
        <template v-else>
          <dl class="grid grid-cols-2 gap-y-2 text-sm">
            <dt class="text-gray-500">Function</dt>
            <dd>{{ pool.scan.function }}</dd>
            <dt class="text-gray-500">State</dt>
            <dd>{{ pool.scan.state }}</dd>
            <dt v-if="pool.scan.start_time" class="text-gray-500">Started</dt>
            <dd v-if="pool.scan.start_time">{{ pool.scan.start_time }}</dd>
            <dt v-if="pool.scan.end_time" class="text-gray-500">Ended</dt>
            <dd v-if="pool.scan.end_time">{{ pool.scan.end_time }}</dd>
            <dt v-if="pool.scan.examined" class="text-gray-500">Examined</dt>
            <dd v-if="pool.scan.examined">
              {{ pool.scan.examined
              }}<template v-if="pool.scan.to_examine"> of {{ pool.scan.to_examine }} </template>
            </dd>
            <dt v-if="pool.scan.errors" class="text-gray-500">Errors</dt>
            <dd
              v-if="pool.scan.errors"
              :class="pool.scan.errors !== '0' ? 'text-error-600 font-semibold' : ''"
            >
              {{ pool.scan.errors }}
            </dd>
            <dt v-if="pool.scan.pass_start" class="text-gray-500">Pass started</dt>
            <dd v-if="pool.scan.pass_start">
              {{ formatRelative(new Date(Number(pool.scan.pass_start) * 1000).toISOString()) }}
            </dd>
          </dl>
          <div v-if="scrubActive" class="mt-3">
            <UProgress :model-value="scrubProgressPercent()" />
            <div class="text-xs text-gray-500 mt-1">{{ scrubProgressPercent() }}% scanned</div>
          </div>
        </template>
      </UCard>

      <UCard>
        <template #header>
          <div class="flex items-center justify-between">
            <div class="font-semibold">Vdev tree</div>
            <UBadge :color="pool.error_count !== '0' ? 'error' : 'success'" variant="subtle"
              >{{ pool.error_count }} aggregate errors</UBadge
            >
          </div>
        </template>
        <VdevTree :vdevs="pool.vdevs" />
      </UCard>
    </template>
  </div>
</template>
