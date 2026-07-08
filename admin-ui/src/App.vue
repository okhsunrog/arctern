<script setup lang="ts">
import { computed } from 'vue'
import { RouterView, useRouter } from 'vue-router'
import { useColorMode } from '@vueuse/core'
import type { NavigationMenuItem } from '@nuxt/ui'
import { useJobs } from './composables/useJobs'
import { usePeers } from './composables/usePeers'
import { usePools } from './composables/usePools'

const router = useRouter()
const mode = useColorMode({ initialValue: 'dark' })

// Shell-level polling doubles as the command palette's data source and
// the sidebar's health chips.
const { jobs, wake, pushTo } = useJobs(10_000)
const { peers } = usePeers(10_000)
const { pools } = usePools(15_000)

const failingJobs = computed(() => jobs.value.filter((j) => j.last_error).length)
const runningJobs = computed(() => jobs.value.filter((j) => j.running).length)
const sickPools = computed(() => pools.value.filter((p) => p.state !== 'ONLINE').length)
const connectedPeers = computed(
  () => peers.value.filter((p) => p.reachability.kind === 'connected').length,
)

function chip(count: number, color: 'error' | 'info'): NavigationMenuItem['badge'] {
  return count > 0 ? { label: String(count), color, variant: 'subtle', size: 'sm' } : undefined
}

const nav = computed<NavigationMenuItem[]>(() => [
  { label: 'Overview', type: 'label' },
  { label: 'Dashboard', to: '/', icon: 'i-lucide-layout-dashboard' },
  { label: 'Replication', type: 'label' },
  {
    label: 'Jobs',
    to: '/jobs',
    icon: 'i-lucide-list-checks',
    badge: chip(failingJobs.value, 'error') ?? chip(runningJobs.value, 'info'),
  },
  {
    label: 'Peers',
    to: '/peers',
    icon: 'i-lucide-radio-tower',
    badge:
      peers.value.length > 0
        ? {
            label: `${connectedPeers.value}/${peers.value.length}`,
            color: connectedPeers.value > 0 ? 'success' : 'neutral',
            variant: 'subtle',
            size: 'sm',
          }
        : undefined,
  },
  { label: 'Snapshots', to: '/snapshots', icon: 'i-lucide-camera' },
  { label: 'Storage', type: 'label' },
  {
    label: 'Pools',
    to: '/pools',
    icon: 'i-lucide-hard-drive',
    badge: chip(sickPools.value, 'error'),
  },
  { label: 'ARC', to: '/arc', icon: 'i-lucide-zap' },
  { label: 'System', type: 'label' },
  { label: 'Events', to: '/events', icon: 'i-lucide-activity' },
  { label: 'Config', to: '/config', icon: 'i-lucide-file-code-2' },
])

// ⌘K palette: navigation + live quick actions.
const searchGroups = computed(() => [
  {
    id: 'goto',
    label: 'Go to',
    items: [
      { label: 'Dashboard', icon: 'i-lucide-layout-dashboard', to: '/' },
      { label: 'Jobs', icon: 'i-lucide-list-checks', to: '/jobs' },
      { label: 'Peers', icon: 'i-lucide-radio-tower', to: '/peers' },
      { label: 'Snapshots', icon: 'i-lucide-camera', to: '/snapshots' },
      { label: 'Pools', icon: 'i-lucide-hard-drive', to: '/pools' },
      { label: 'ARC', icon: 'i-lucide-zap', to: '/arc' },
      { label: 'Events', icon: 'i-lucide-activity', to: '/events' },
      { label: 'Config', icon: 'i-lucide-file-code-2', to: '/config' },
    ],
  },
  {
    id: 'jobs',
    label: 'Jobs',
    items: jobs.value.flatMap((j) => [
      {
        label: `${j.name} — open`,
        suffix: j.kind,
        icon: 'i-lucide-square-arrow-out-up-right',
        onSelect: () => router.push(`/jobs/${j.name}`),
      },
      {
        label: `${j.name} — wake up now`,
        suffix: j.kind,
        icon: 'i-lucide-alarm-clock',
        onSelect: () => void wake(j.name),
      },
      ...(j.targets ?? [])
        .filter((t) => t.connected)
        .map((t) => ({
          label: `${j.name} — send now to ${t.peer}`,
          suffix: t.route ? `via ${t.route}` : undefined,
          icon: 'i-lucide-send',
          onSelect: () => void pushTo(j.name, t.peer),
        })),
    ]),
  },
  {
    id: 'peers',
    label: 'Peers',
    items: peers.value.map((p) => ({
      label: `${p.name} — open`,
      suffix: p.reachability.kind,
      icon: 'i-lucide-radio-tower',
      onSelect: () => router.push(`/peers/${encodeURIComponent(p.name)}/jobs`),
    })),
  },
  {
    id: 'pools',
    label: 'Pools',
    items: pools.value.map((p) => ({
      label: `${p.name} — open`,
      suffix: p.state,
      icon: 'i-lucide-hard-drive',
      onSelect: () => router.push(`/pools/${encodeURIComponent(p.name)}`),
    })),
  },
])

function toggleMode() {
  mode.value = mode.value === 'dark' ? 'light' : 'dark'
}
</script>

<template>
  <UApp :toaster="{ position: 'bottom-right' }">
    <UDashboardGroup unit="rem">
      <UDashboardSidebar
        collapsible
        :min-size="14"
        :default-size="15"
        :max-size="20"
        :ui="{ footer: 'border-t border-default' }"
      >
        <template #header="{ collapsed }">
          <RouterLink to="/" class="flex items-center gap-2 font-mono select-none">
            <span
              class="inline-flex size-6 items-center justify-center rounded-sm bg-primary text-inverted text-xs font-semibold"
              >a</span
            >
            <span v-if="!collapsed" class="text-sm font-semibold tracking-wide">arctern</span>
          </RouterLink>
        </template>

        <template #default="{ collapsed }">
          <UDashboardSearchButton :collapsed="collapsed" class="mb-2" />
          <UNavigationMenu :collapsed="collapsed" :items="nav" orientation="vertical" highlight />
        </template>

        <template #footer="{ collapsed }">
          <div class="flex items-center gap-1 w-full" :class="collapsed ? 'flex-col' : ''">
            <UButton
              :icon="mode === 'dark' ? 'i-lucide-sun' : 'i-lucide-moon'"
              color="neutral"
              variant="ghost"
              size="sm"
              :aria-label="mode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'"
              @click="toggleMode"
            />
            <UDashboardSidebarCollapse />
            <span v-if="!collapsed" class="microlabel ms-auto">zfs console</span>
          </div>
        </template>
      </UDashboardSidebar>

      <UDashboardSearch :groups="searchGroups" placeholder="Jump to, wake, send, scrub…" />

      <RouterView />
    </UDashboardGroup>
  </UApp>
</template>
