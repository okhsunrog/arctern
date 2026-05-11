<script setup lang="ts">
import { usePeers } from '../composables/usePeers'
import PeerReachabilityBadge from '../components/PeerReachabilityBadge.vue'

const { peers, error, loading } = usePeers()
</script>

<template>
  <div class="max-w-6xl mx-auto px-4 py-6">
    <h1 class="text-2xl font-semibold mb-4">Peers</h1>
    <UAlert v-if="error" color="error" :title="error" class="mb-4" />
    <div v-if="loading && peers.length === 0" class="text-gray-500">Loading…</div>
    <div v-else-if="peers.length === 0" class="text-gray-500">No peers configured.</div>
    <div v-else class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <RouterLink
        v-for="p in peers"
        :key="p.name"
        :to="`/peers/${encodeURIComponent(p.name)}/jobs`"
        class="block rounded-md border border-gray-200 dark:border-gray-800 p-4 bg-white dark:bg-gray-900 hover:border-primary-500 transition-colors"
      >
        <div class="flex items-center justify-between mb-2">
          <div class="font-semibold">{{ p.name }}</div>
          <PeerReachabilityBadge :reachability="p.reachability" />
        </div>
        <div class="text-xs text-gray-500 font-mono">{{ p.ssh_target }}</div>
      </RouterLink>
    </div>
  </div>
</template>
