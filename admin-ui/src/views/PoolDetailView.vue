<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { useHost } from '../composables/useHost'
import { usePool } from '../composables/usePools'
import type { ScrubRequest } from '../client'
import { parseZpoolSize } from '../utils/pool'
import { poolStatus, scanStatus } from '../utils/status'
import { formatRelative } from '../utils/format'
import { useMutation } from '../composables/useMutation'
import VdevTree from '../components/VdevTree.vue'

const route = useRoute()
const name = computed(() => String(route.params.name))
const { host, baseUrl, prefix } = useHost()
const { pool, error, scrub } = usePool(name.value, 3000, baseUrl.value)
const { mutate } = useMutation()

async function scrubAction(action: ScrubRequest['action']) {
  await mutate(`Scrub ${action} on ${name.value}`, () => scrub(action))
}

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
// zpool sets `scrub_pause` to the pause timestamp while paused; when
// not paused the field is absent / "-" / "0" depending on version, so
// truthiness alone would misread a running scrub as paused.
const scrubPaused = computed(() => {
  const p = pool.value?.scan?.scrub_pause?.trim()
  return !!p && p !== '-' && p !== '0'
})

// A scrub has two counters: `examined` (scanned ahead sequentially)
// and `issued` (verification I/O actually completed — what zpool's own
// "done" percentage means). Show both; the bar tracks issued and never
// silently substitutes examined.
const scrubPct = computed(() => {
  const s = pool.value?.scan
  if (!s) return null
  const toExamine = parseZpoolSize(s.to_examine ?? null)
  if (toExamine <= 0) return null
  const pct = (v: string | null | undefined) =>
    Math.min(100, Math.round((parseZpoolSize(v ?? null) / toExamine) * 1000) / 10)
  return { issued: pct(s.issued), scanned: pct(s.examined) }
})
</script>

<template>
  <UDashboardPanel id="pool-detail">
    <template #header>
      <UDashboardNavbar :title="host ? `${host} · ${name}` : name">
        <template #leading>
          <UButton
            :to="`${prefix}/pools`"
            icon="i-lucide-arrow-left"
            variant="ghost"
            color="neutral"
            size="sm"
            aria-label="Back to pools"
          />
        </template>
        <template #right>
          <UBadge
            v-if="pool"
            :color="poolStatus(pool.state).color"
            variant="subtle"
            :icon="poolStatus(pool.state).icon"
          >
            {{ pool.state }}
          </UBadge>
        </template>
      </UDashboardNavbar>
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl space-y-6">
        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />

        <div v-if="!pool" class="text-muted text-sm">Loading…</div>
        <template v-else>
          <div class="text-xs text-muted font-mono">
            guid {{ pool.pool_guid }} · txg {{ pool.txg }}
          </div>

          <UCard>
            <template #header>
              <div class="flex items-center justify-between">
                <div class="font-semibold">Capacity</div>
                <span class="text-sm text-muted font-mono">{{ totals.percent }}% used</span>
              </div>
            </template>
            <UProgress
              :model-value="totals.percent"
              :color="totals.percent > 90 ? 'error' : totals.percent > 75 ? 'warning' : 'primary'"
            />
            <div class="text-xs text-muted mt-2 font-mono">
              {{ (totals.alloc / 1024 ** 3).toFixed(1) }} GiB allocated of
              {{ (totals.total / 1024 ** 3).toFixed(1) }} GiB total
            </div>
          </UCard>

          <UCard :class="scrubPaused ? 'rail rail-warn' : scrubActive ? 'rail rail-info' : ''">
            <template #header>
              <div class="flex items-center justify-between">
                <div class="flex items-center gap-2">
                  <div class="font-semibold">Scrub</div>
                  <UBadge
                    v-if="pool.scan"
                    :color="scrubPaused ? 'warning' : scanStatus(pool.scan.state).color"
                    variant="subtle"
                    size="sm"
                    :icon="scrubPaused ? 'i-lucide-circle-pause' : scanStatus(pool.scan.state).icon"
                  >
                    {{ scrubPaused ? 'paused' : scanStatus(pool.scan.state).label }}
                  </UBadge>
                </div>
                <div class="flex gap-2">
                  <UButton
                    v-if="!scrubActive"
                    size="xs"
                    icon="i-lucide-play"
                    @click="scrubAction('start')"
                    >Start</UButton
                  >
                  <template v-else>
                    <UButton
                      v-if="scrubPaused"
                      size="xs"
                      icon="i-lucide-play"
                      color="success"
                      @click="scrubAction('resume')"
                      >Resume</UButton
                    >
                    <UButton
                      v-else
                      size="xs"
                      icon="i-lucide-pause"
                      variant="soft"
                      @click="scrubAction('pause')"
                      >Pause</UButton
                    >
                    <UButton
                      size="xs"
                      icon="i-lucide-square"
                      variant="soft"
                      color="error"
                      @click="scrubAction('stop')"
                      >Stop</UButton
                    >
                  </template>
                </div>
              </div>
            </template>

            <div v-if="!pool.scan" class="text-muted text-sm">No scrub recorded.</div>
            <template v-else>
              <dl class="grid grid-cols-2 gap-y-2 text-sm">
                <dt class="text-muted">Function</dt>
                <dd>{{ pool.scan.function }}</dd>
                <dt v-if="pool.scan.start_time" class="text-muted">Started</dt>
                <dd v-if="pool.scan.start_time">{{ pool.scan.start_time }}</dd>
                <dt v-if="pool.scan.end_time" class="text-muted">Ended</dt>
                <dd v-if="pool.scan.end_time">{{ pool.scan.end_time }}</dd>
                <dt v-if="pool.scan.issued" class="text-muted">Issued</dt>
                <dd v-if="pool.scan.issued" class="font-mono">
                  {{ pool.scan.issued
                  }}<template v-if="pool.scan.to_examine"> of {{ pool.scan.to_examine }}</template>
                </dd>
                <dt v-if="pool.scan.examined" class="text-muted">Examined</dt>
                <dd v-if="pool.scan.examined" class="font-mono">{{ pool.scan.examined }}</dd>
                <dt v-if="pool.scan.errors" class="text-muted">Errors</dt>
                <dd
                  v-if="pool.scan.errors"
                  :class="pool.scan.errors !== '0' ? 'text-error font-semibold' : ''"
                >
                  {{ pool.scan.errors }}
                </dd>
                <dt v-if="scrubPaused" class="text-muted">Paused since</dt>
                <dd v-if="scrubPaused" class="text-warning">{{ pool.scan.scrub_pause }}</dd>
                <dt v-if="pool.scan.pass_start && !scrubPaused" class="text-muted">Pass started</dt>
                <dd v-if="pool.scan.pass_start && !scrubPaused">
                  {{ formatRelative(new Date(Number(pool.scan.pass_start) * 1000).toISOString()) }}
                </dd>
              </dl>
              <div v-if="scrubActive && scrubPct != null" class="mt-3">
                <UProgress
                  :model-value="scrubPct.issued"
                  :color="scrubPaused ? 'warning' : 'primary'"
                />
                <div class="text-xs text-muted mt-1">
                  issued {{ scrubPct.issued }}% · scanned {{ scrubPct.scanned }}%
                  <template v-if="scrubPaused"> · frozen while paused</template>
                </div>
              </div>
            </template>
          </UCard>

          <UCard>
            <template #header>
              <div class="flex items-center justify-between">
                <div class="font-semibold">Vdev tree</div>
                <UBadge :color="pool.error_count !== '0' ? 'error' : 'success'" variant="subtle">
                  {{ pool.error_count }} aggregate errors
                </UBadge>
              </div>
            </template>
            <VdevTree :vdevs="pool.vdevs" />
          </UCard>
        </template>
      </div>
    </template>
  </UDashboardPanel>
</template>
