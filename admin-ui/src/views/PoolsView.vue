<script setup lang="ts">
import { usePools } from '../composables/usePools'
import { poolStateColor, poolUsedPercent } from '../utils/pool'
import { formatRelative } from '../utils/format'

const { pools, error, loading } = usePools()
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6 space-y-4">
    <h1 class="text-2xl font-semibold">Pools</h1>
    <UAlert v-if="error" color="error" :title="error" />
    <div v-if="loading && pools.length === 0" class="text-gray-500">Loading…</div>
    <div v-else-if="pools.length === 0" class="text-gray-500">No pools imported.</div>
    <div v-else class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <RouterLink
        v-for="p in pools"
        :key="p.name"
        :to="`/pools/${encodeURIComponent(p.name)}`"
        class="block rounded-md border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-4 hover:border-primary-500 transition-colors space-y-3"
      >
        <div class="flex items-center justify-between">
          <div class="font-semibold text-lg">{{ p.name }}</div>
          <UBadge :color="poolStateColor(p.state)" variant="subtle">{{ p.state }}</UBadge>
        </div>
        <div>
          <div class="flex justify-between text-xs text-gray-500 mb-1">
            <span>{{ p.alloc_space }} of {{ p.total_space }}</span>
            <span>{{ poolUsedPercent(p.alloc_space, p.total_space) }}%</span>
          </div>
          <UProgress
            :model-value="poolUsedPercent(p.alloc_space, p.total_space)"
            :color="
              poolUsedPercent(p.alloc_space, p.total_space) > 90
                ? 'error'
                : poolUsedPercent(p.alloc_space, p.total_space) > 75
                  ? 'warning'
                  : 'primary'
            "
          />
        </div>
        <dl class="grid grid-cols-2 gap-1 text-xs">
          <dt class="text-gray-500">Errors</dt>
          <dd :class="p.error_count !== '0' ? 'text-error-600 font-semibold' : ''">
            {{ p.error_count }}
          </dd>
          <template v-if="p.scan">
            <dt class="text-gray-500">Last {{ p.scan.function.toLowerCase() }}</dt>
            <dd>
              {{ p.scan.state }}
              <template v-if="p.scan.end_time">
                · {{ p.scan.end_time }}
              </template>
              <template v-else-if="p.scan.pass_start">
                · started {{ formatRelative(new Date(Number(p.scan.pass_start) * 1000).toISOString()) }}
              </template>
            </dd>
          </template>
        </dl>
      </RouterLink>
    </div>
  </div>
</template>
