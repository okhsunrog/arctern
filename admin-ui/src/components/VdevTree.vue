<script setup lang="ts">
import type { VdevNode } from '../types/pool'
import { poolStateColor } from '../utils/pool'

defineProps<{ vdevs: VdevNode[]; depth?: number }>()
</script>

<template>
  <ul class="space-y-1" :style="{ paddingLeft: (depth ?? 0) > 0 ? '1.25rem' : '0' }">
    <li v-for="v in vdevs" :key="v.name">
      <div
        class="flex items-center gap-3 rounded px-2 py-1 hover:bg-gray-50 dark:hover:bg-gray-900"
      >
        <UBadge :color="poolStateColor(v.state)" variant="subtle" size="xs">{{ v.state }}</UBadge>
        <span class="text-xs text-gray-500 font-mono w-16">{{ v.vdev_type }}</span>
        <span class="font-mono text-xs break-all flex-1">{{ v.name }}</span>
        <span class="text-xs text-gray-500">{{ v.alloc_space }} / {{ v.total_space }}</span>
        <span
          v-if="v.read_errors !== '0' || v.write_errors !== '0' || v.checksum_errors !== '0'"
          class="text-xs text-error-600 font-mono"
        >
          R{{ v.read_errors }} W{{ v.write_errors }} C{{ v.checksum_errors }}
        </span>
      </div>
      <VdevTree v-if="v.children?.length" :vdevs="v.children" :depth="(depth ?? 0) + 1" />
    </li>
  </ul>
</template>
