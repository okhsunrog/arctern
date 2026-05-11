<script setup lang="ts">
import { computed } from 'vue'
import type { PeerReachability } from '../client'
import { formatRelative } from '../utils/format'

const props = defineProps<{ reachability: PeerReachability }>()

const tone = computed<'success' | 'warning' | 'error'>(() => {
  if (props.reachability.kind === 'connected') return 'success'
  if (props.reachability.kind === 'reconnecting') return 'warning'
  return 'error'
})

const label = computed(() => {
  const r = props.reachability
  if (r.kind === 'connected') return 'connected'
  if (r.kind === 'reconnecting') return `reconnecting (down ${formatRelative(r.since)})`
  return `failed (${formatRelative(r.since)})`
})

const tooltip = computed(() => {
  const r = props.reachability
  return r.kind === 'failed' ? r.last_error : ''
})
</script>

<template>
  <UBadge :color="tone" variant="subtle" :title="tooltip">{{ label }}</UBadge>
</template>
