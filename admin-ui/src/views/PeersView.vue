<script setup lang="ts">
import { usePeers } from '../composables/usePeers'
import { formatRelative } from '../utils/format'
import { peerStatus, routeHealthStatus } from '../utils/status'

const { peers, error, loading } = usePeers()
</script>

<template>
  <UDashboardPanel id="peers">
    <template #header>
      <UDashboardNavbar title="Peer links" />
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-4">
        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />
        <div v-if="loading && peers.length === 0" class="text-muted text-sm">Loading…</div>
        <UEmpty
          v-else-if="peers.length === 0"
          icon="i-lucide-radio-tower"
          title="No peers configured"
          description="Add [[peers]] entries to arctern.toml to replicate to another host."
        />
        <div v-else class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <UCard v-for="p in peers" :key="p.name" :class="peerStatus(p.reachability).rail">
            <template #header>
              <div class="flex items-center justify-between">
                <RouterLink
                  :to="`/h/${encodeURIComponent(p.name)}/dashboard`"
                  class="font-mono font-semibold hover:underline"
                >
                  {{ p.name }}
                </RouterLink>
                <UBadge
                  :color="peerStatus(p.reachability).color"
                  variant="subtle"
                  :icon="peerStatus(p.reachability).icon"
                >
                  {{ peerStatus(p.reachability).label }}
                  <template v-if="p.reachability.kind !== 'connected'">
                    · {{ formatRelative(p.reachability.since) }}
                  </template>
                </UBadge>
              </div>
            </template>

            <!-- Routes, priority order. The active one carries the beam. -->
            <div class="space-y-2">
              <div class="microlabel">routes</div>
              <div v-for="(r, i) in p.routes" :key="r.name" class="flex items-center gap-2 text-sm">
                <span class="text-muted font-mono text-xs w-4 text-right">{{ i + 1 }}</span>
                <span
                  class="inline-block w-2 h-2 rounded-full shrink-0"
                  :class="
                    p.active_route === r.name
                      ? 'bg-success pulse-dot'
                      : r.health === 'failed'
                        ? 'bg-error'
                        : 'bg-neutral-400 dark:bg-neutral-600'
                  "
                />
                <span class="font-mono font-medium">{{ r.name }}</span>
                <span class="text-muted font-mono text-xs truncate">{{ r.ssh_target }}</span>
                <span class="ms-auto flex items-center gap-1 shrink-0">
                  <UBadge
                    v-if="p.active_route === r.name"
                    color="success"
                    variant="subtle"
                    size="sm"
                  >
                    active
                  </UBadge>
                  <UBadge
                    v-else
                    :color="routeHealthStatus(r.health).color"
                    variant="outline"
                    size="sm"
                    :title="r.last_error ?? undefined"
                  >
                    {{ routeHealthStatus(r.health).label }}
                  </UBadge>
                  <UBadge :color="r.auto ? 'info' : 'neutral'" variant="outline" size="sm">
                    {{ r.auto ? 'auto' : 'manual' }}
                  </UBadge>
                </span>
              </div>
              <div
                v-if="p.reachability.kind === 'failed'"
                class="text-xs text-error break-all"
                :title="p.reachability.last_error"
              >
                {{ p.reachability.last_error }}
              </div>
            </div>

            <template #footer>
              <div class="flex gap-2">
                <UButton
                  :to="`/h/${encodeURIComponent(p.name)}/dashboard`"
                  size="xs"
                  variant="soft"
                  icon="i-lucide-square-arrow-out-up-right"
                >
                  Open console
                </UButton>
              </div>
            </template>
          </UCard>
        </div>
      </div>
    </template>
  </UDashboardPanel>
</template>
