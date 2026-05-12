<script setup lang="ts">
const links = [
  { label: 'Dashboard', to: '/', icon: 'i-lucide-layout-dashboard' },
  { label: 'Jobs', to: '/jobs', icon: 'i-lucide-list-checks' },
  { label: 'Snapshots', to: '/snapshots', icon: 'i-lucide-camera' },
  { label: 'Peers', to: '/peers', icon: 'i-lucide-server' },
  { label: 'Events', to: '/events', icon: 'i-lucide-activity' },
]
</script>

<template>
  <header class="border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900">
    <div class="max-w-6xl mx-auto px-4 py-3 flex items-center gap-6">
      <RouterLink to="/" class="font-bold text-lg">arctern</RouterLink>
      <nav class="flex gap-1">
        <RouterLink
          v-for="l in links"
          :key="l.to"
          :to="l.to"
          v-slot="{ isActive }"
        >
          <UButton
            :color="isActive ? 'primary' : 'neutral'"
            :variant="isActive ? 'soft' : 'ghost'"
            :icon="l.icon"
            size="sm"
          >{{ l.label }}</UButton>
        </RouterLink>
      </nav>
    </div>
  </header>
</template>
