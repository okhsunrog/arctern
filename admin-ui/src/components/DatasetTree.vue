<script setup lang="ts">
import { computed, ref } from 'vue'
import type { TreeItem } from '@nuxt/ui'
import type { DatasetSummary } from '../client'
import { formatBytes } from '../utils/format'

const props = defineProps<{ datasets: DatasetSummary[]; loading: boolean }>()
const dataset = defineModel<string>({ default: '' })
const emit = defineEmits<{ refresh: [] }>()

const treeFilter = ref('')
/** 'name' | 'size' — size ordering answers "what eats my space". */
const treeSort = ref<'name' | 'size'>('name')
function toggleSort() {
  treeSort.value = treeSort.value === 'name' ? 'size' : 'name'
}

const usedByName = computed(() => {
  const m = new Map<string, number>()
  for (const d of props.datasets) {
    const n = Number(d.properties?.used ?? NaN)
    if (Number.isFinite(n)) m.set(d.name, n)
  }
  return m
})

interface DsNode extends TreeItem {
  label: string
  value: string
  used: number | null
  children?: DsNode[]
}

const tree = computed<DsNode[]>(() => {
  const fs = props.datasets
    .filter((d) => d.dataset_type === 'filesystem' || d.dataset_type === 'volume')
    .slice()
    .sort((a, b) => a.name.localeCompare(b.name))
  const q = treeFilter.value.trim().toLowerCase()
  const matching = q ? fs.filter((d) => d.name.toLowerCase().includes(q)) : fs
  const byPath = new Map<string, DsNode>()
  const roots: DsNode[] = []
  for (const d of matching) {
    const parts = d.name.split('/')
    const node: DsNode = {
      label: parts[parts.length - 1] ?? d.name,
      value: d.name,
      used: usedByName.value.get(d.name) ?? null,
      icon: d.dataset_type === 'volume' ? 'i-lucide-box' : 'i-lucide-database',
      defaultExpanded: parts.length <= 2 || q.length > 0,
      children: undefined,
    }
    byPath.set(d.name, node)
    const parentPath = parts.slice(0, -1).join('/')
    const parent = byPath.get(parentPath)
    if (parent) {
      parent.children = parent.children ?? []
      parent.children.push(node)
    } else {
      // Promoted root (pool root or filtered-out parent): keep the full
      // path as label and open it — a collapsed sole root is a dead end.
      node.label = d.name
      node.defaultExpanded = true
      roots.push(node)
    }
  }
  if (treeSort.value === 'size') {
    const bySize = (a: DsNode, b: DsNode) => (b.used ?? -1) - (a.used ?? -1)
    const sortRec = (nodes: DsNode[]) => {
      nodes.sort(bySize)
      for (const n of nodes) if (n.children) sortRec(n.children)
    }
    sortRec(roots)
  }
  return roots
})

const selectedNode = computed<DsNode | undefined>({
  get: () =>
    dataset.value
      ? {
          label: dataset.value,
          value: dataset.value,
          used: usedByName.value.get(dataset.value) ?? null,
        }
      : undefined,
  set: (node) => {
    dataset.value = node?.value ?? ''
  },
})
</script>

<template>
  <div class="rounded-md border border-default bg-default p-2 self-start">
    <div class="flex gap-1 mb-2">
      <UInput
        v-model="treeFilter"
        icon="i-lucide-search"
        aria-label="Filter datasets"
        placeholder="Filter datasets…"
        size="sm"
        class="flex-1 font-mono"
      />
      <UTooltip :text="treeSort === 'name' ? 'Sort by size' : 'Sort by name'">
        <UButton
          size="sm"
          color="neutral"
          :variant="treeSort === 'size' ? 'soft' : 'ghost'"
          :icon="
            treeSort === 'size' ? 'i-lucide-arrow-down-wide-narrow' : 'i-lucide-arrow-down-a-z'
          "
          :aria-label="treeSort === 'name' ? 'Sort by size' : 'Sort by name'"
          @click="toggleSort"
        />
      </UTooltip>
    </div>
    <div v-if="loading && tree.length === 0" class="text-muted text-sm p-2">Loading…</div>
    <UTree
      v-else
      v-model="selectedNode"
      :items="tree"
      :get-key="(i: DsNode) => i.value"
      size="sm"
      class="font-mono"
    >
      <!-- Overriding the trailing slot replaces the built-in expand
               chevron, so render both: size, then a manual chevron that
               follows the expanded state. -->
      <template #item-trailing="{ item, expanded }">
        <span class="ms-auto flex items-center gap-1 ps-2 shrink-0">
          <span v-if="(item as DsNode).used != null" class="text-[11px] text-muted">
            {{ formatBytes((item as DsNode).used) }}
          </span>
          <UIcon
            v-if="(item as DsNode).children?.length"
            name="i-lucide-chevron-right"
            class="size-4 text-dimmed transition-transform"
            :class="expanded ? 'rotate-90' : ''"
          />
        </span>
      </template>
    </UTree>
    <UButton
      size="xs"
      variant="ghost"
      color="neutral"
      icon="i-lucide-refresh-cw"
      class="mt-2"
      :loading="loading"
      @click="emit('refresh')"
    >
      Refresh
    </UButton>
  </div>
</template>
