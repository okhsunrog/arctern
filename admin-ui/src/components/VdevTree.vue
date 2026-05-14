<script setup lang="ts">
import { computed, ref } from 'vue'
import type { VdevNode } from '../client'
import { poolStateColor } from '../utils/pool'
import { displayName, subtreeHealth } from '../utils/vdev'

const props = withDefaults(
  defineProps<{ vdevs: VdevNode[]; depth?: number }>(),
  { depth: 0 },
)

// Track which interior nodes are collapsed. By default an interior
// node is auto-expanded iff its subtree has anything unhealthy — so a
// faulted child of a mirror pops open without operator clicks. The
// chevron lets you override either direction.
const collapsed = ref<Set<string>>(new Set())

function nodeId(v: VdevNode): string {
  // Names are unique within a pool (ZFS enforces it across the tree).
  return v.name
}

function isCollapsed(v: VdevNode): boolean {
  const health = subtreeHealth(v)
  // Default: collapsed when healthy, expanded when not.
  const defaultCollapsed = !health.unhealthy
  // The Set tracks *overrides* — explicit user toggles. A toggle
  // inverts whichever default the node had.
  const overridden = collapsed.value.has(nodeId(v))
  return overridden ? !defaultCollapsed : defaultCollapsed
}

function toggle(v: VdevNode) {
  const id = nodeId(v)
  if (collapsed.value.has(id)) collapsed.value.delete(id)
  else collapsed.value.add(id)
}

function leafErrorsLabel(v: VdevNode): string | null {
  const r = Number.parseInt(v.read_errors, 10) || 0
  const w = Number.parseInt(v.write_errors, 10) || 0
  const c = Number.parseInt(v.checksum_errors, 10) || 0
  if (r === 0 && w === 0 && c === 0) return null
  return `R${r} W${w} C${c}`
}

function subtreeErrorsLabel(v: VdevNode): string | null {
  const h = subtreeHealth(v)
  if (h.readErrors === 0 && h.writeErrors === 0 && h.checksumErrors === 0) {
    return null
  }
  return `R${h.readErrors} W${h.writeErrors} C${h.checksumErrors}`
}

const rows = computed(() =>
  props.vdevs.map((v) => ({
    v,
    health: subtreeHealth(v),
    hasChildren: (v.children?.length ?? 0) > 0,
  })),
)
</script>

<template>
  <ul class="space-y-0.5" :style="{ paddingLeft: depth > 0 ? '1.5rem' : '0' }">
    <li v-for="{ v, health, hasChildren } in rows" :key="v.name">
      <div
        class="group flex items-center gap-2 rounded px-2 py-1 transition-colors"
        :class="[
          health.unhealthy
            ? 'bg-error-50 dark:bg-error-950/30 hover:bg-error-100 dark:hover:bg-error-950/60'
            : 'hover:bg-gray-50 dark:hover:bg-gray-900',
        ]"
      >
        <!-- chevron / leaf indicator -->
        <button
          v-if="hasChildren"
          type="button"
          class="w-5 h-5 flex items-center justify-center text-gray-400 hover:text-gray-600 rounded"
          :aria-expanded="!isCollapsed(v)"
          :aria-label="isCollapsed(v) ? `Expand ${v.name}` : `Collapse ${v.name}`"
          @click="toggle(v)"
        >
          <UIcon
            :name="isCollapsed(v) ? 'i-lucide-chevron-right' : 'i-lucide-chevron-down'"
            class="w-4 h-4"
          />
        </button>
        <span v-else class="w-5 h-5 inline-block" />

        <!-- state badge — uses worstState so a healthy mirror parent of a faulted disk shows DEGRADED -->
        <UBadge
          :color="poolStateColor(hasChildren ? health.worstState : v.state)"
          variant="subtle"
          size="xs"
          class="shrink-0 w-20 justify-center"
        >{{ hasChildren ? health.worstState : v.state }}</UBadge>

        <!-- vdev kind -->
        <span class="text-xs text-gray-500 font-mono w-14 shrink-0">{{ v.vdev_type }}</span>

        <!-- name -->
        <span
          class="font-mono text-xs flex-1 truncate"
          :title="v.path ?? v.name"
        >{{ displayName(v.name) }}</span>

        <!-- capacity -->
        <span class="text-xs text-gray-500 shrink-0 tabular-nums">
          {{ v.alloc_space }} / {{ v.total_space }}
        </span>

        <!-- errors: leaf shows its own; interior shows subtree sum -->
        <span
          v-if="hasChildren ? subtreeErrorsLabel(v) : leafErrorsLabel(v)"
          class="text-xs font-mono shrink-0 px-1.5 py-0.5 rounded bg-error-100 dark:bg-error-950/70 text-error-700 dark:text-error-200"
          :title="
            hasChildren
              ? 'Aggregate errors across this subtree'
              : 'Read / Write / Checksum errors on this vdev'
          "
        >{{ hasChildren ? subtreeErrorsLabel(v) : leafErrorsLabel(v) }}</span>
      </div>
      <VdevTree
        v-if="hasChildren && !isCollapsed(v)"
        :vdevs="v.children"
        :depth="depth + 1"
      />
    </li>
  </ul>
</template>
