<script setup lang="ts">
import { computed } from 'vue'
import { RouterView, useRouter } from 'vue-router'
import { useColorMode } from '@vueuse/core'
import type { NavigationMenuItem } from '@nuxt/ui'
import { useHost } from './composables/useHost'
import { useJobs } from './composables/useJobs'
import { usePeers } from './composables/usePeers'
import { usePools } from './composables/usePools'

const router = useRouter()
const { host, prefix } = useHost()
const mode = useColorMode({ initialValue: 'dark' })

// Shell-level polling doubles as the command palette's data source and
// the sidebar's health chips.
const { jobs, wake, pushTo } = useJobs(10_000)
const { peers } = usePeers(10_000)
const { pools } = usePools(15_000)

const failingJobs = computed(() => jobs.value.filter((j) => j.last_error).length)
const runningJobs = computed(() => jobs.value.filter((j) => j.running).length)
const sickPools = computed(() => pools.value.filter((p) => p.state !== 'ONLINE').length)
function chip(count: number, color: 'error' | 'info'): NavigationMenuItem['badge'] {
  return count > 0 ? { label: String(count), color, variant: 'subtle', size: 'sm' } : undefined
}

// The hosts group makes scope switching a first-class navigation act:
// the local daemon and every peer are equal entries, and the section
// nav below always talks about the SELECTED host.
const hostsNav = computed<NavigationMenuItem[]>(() => [
  { label: 'Hosts', type: 'label' },
  {
    label: 'this host',
    to: '/dashboard',
    icon: 'i-lucide-house',
    active: host.value === null,
  },
  ...peers.value.map((p) => ({
    label: p.name,
    to: `/h/${encodeURIComponent(p.name)}/dashboard`,
    icon: 'i-lucide-radio-tower',
    active: host.value === p.name,
    badge: {
      label: p.reachability.kind === 'connected' ? (p.active_route ?? 'up') : 'down',
      color: (p.reachability.kind === 'connected' ? 'success' : 'error') as 'success' | 'error',
      variant: 'subtle' as const,
      size: 'sm' as const,
    },
  })),
  ...(host.value === null
    ? [
        {
          label: 'Peer links',
          to: '/peers',
          icon: 'i-lucide-waypoints',
        } satisfies NavigationMenuItem,
      ]
    : []),
])

const nav = computed<NavigationMenuItem[]>(() => {
  const pre = prefix.value
  const local = host.value === null
  return [
    { label: host.value ?? 'this host', type: 'label' },
    { label: 'Dashboard', to: `${pre}/dashboard`, icon: 'i-lucide-layout-dashboard' },
    {
      label: 'Jobs',
      to: `${pre}/jobs`,
      icon: 'i-lucide-list-checks',
      badge: local
        ? (chip(failingJobs.value, 'error') ?? chip(runningJobs.value, 'info'))
        : undefined,
    },
    { label: 'Snapshots', to: `${pre}/snapshots`, icon: 'i-lucide-camera' },
    {
      label: 'Pools',
      to: `${pre}/pools`,
      icon: 'i-lucide-hard-drive',
      badge: local ? chip(sickPools.value, 'error') : undefined,
    },
    { label: 'ARC', to: `${pre}/arc`, icon: 'i-lucide-zap' },
    { label: 'Events', to: `${pre}/events`, icon: 'i-lucide-activity' },
    { label: 'Config', to: `${pre}/config`, icon: 'i-lucide-file-code-2' },
  ]
})

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
      label: `${p.name} — open console`,
      suffix: p.reachability.kind,
      icon: 'i-lucide-radio-tower',
      onSelect: () => router.push(`/h/${encodeURIComponent(p.name)}/dashboard`),
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
          <UNavigationMenu
            :collapsed="collapsed"
            :items="hostsNav"
            orientation="vertical"
            highlight
          />
          <USeparator class="my-2" />
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
