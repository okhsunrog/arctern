<script setup lang="ts">
import { computed } from 'vue'
import { useHost } from '../composables/useHost'
import { usePools } from '../composables/usePools'
import { poolUsedPercent } from '../utils/pool'
import { poolStatus, scanStatus } from '../utils/status'
import { formatRelative } from '../utils/format'

const { host, baseUrl, prefix } = useHost()
const { pools, error, loading } = usePools(5000, baseUrl.value)
const title = computed(() => (host.value ? `${host.value} · Pools` : 'Pools'))
</script>

<template>
  <UDashboardPanel id="pools">
    <template #header>
      <UDashboardNavbar :title="title" />
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-4">
        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />
        <div v-if="loading && pools.length === 0" class="text-muted text-sm">Loading…</div>
        <UEmpty
          v-else-if="pools.length === 0"
          icon="i-lucide-hard-drive"
          title="No pools imported"
        />
        <div v-else class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <UCard v-for="p in pools" :key="p.name" :class="poolStatus(p.state).rail">
            <template #header>
              <div class="flex items-center justify-between">
                <RouterLink
                  :to="`${prefix}/pools/${encodeURIComponent(p.name)}`"
                  class="font-mono font-semibold text-lg hover:underline"
                >
                  {{ p.name }}
                </RouterLink>
                <UBadge
                  :color="poolStatus(p.state).color"
                  variant="subtle"
                  :icon="poolStatus(p.state).icon"
                >
                  {{ p.state }}
                </UBadge>
              </div>
            </template>
            <div class="space-y-3">
              <div>
                <div class="flex justify-between text-xs text-muted mb-1 font-mono">
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
                <dt class="text-muted">Errors</dt>
                <dd :class="p.error_count !== '0' ? 'text-error font-semibold' : ''">
                  {{ p.error_count }}
                </dd>
                <template v-if="p.scan">
                  <dt class="text-muted">Last {{ p.scan.function.toLowerCase() }}</dt>
                  <dd class="flex items-center gap-1">
                    <UBadge :color="scanStatus(p.scan.state).color" variant="subtle" size="sm">
                      {{ scanStatus(p.scan.state).label }}
                    </UBadge>
                    <template v-if="p.scan.end_time">{{ p.scan.end_time }}</template>
                    <template v-else-if="p.scan.pass_start">
                      started
                      {{ formatRelative(new Date(Number(p.scan.pass_start) * 1000).toISOString()) }}
                    </template>
                  </dd>
                </template>
              </dl>
            </div>
          </UCard>
        </div>
      </div>
    </template>
  </UDashboardPanel>
</template>
