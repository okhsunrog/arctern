<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { localSource } from '../composables/snapshotSources'
import SnapshotBrowser from '../components/SnapshotBrowser.vue'

const route = useRoute()
const router = useRouter()
const source = localSource()

const dataset = ref('')

// Deep-link: /snapshots?dataset=tank/data selects on load.
onMounted(() => {
  const q = route.query.dataset
  if (typeof q === 'string' && q) dataset.value = q
})
watch(dataset, (d) => {
  void router.replace({ query: d ? { dataset: d } : {} })
})
</script>

<template>
  <UDashboardPanel id="snapshots">
    <template #header>
      <UDashboardNavbar title="Snapshots" />
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-7xl">
        <SnapshotBrowser v-model:dataset="dataset" :source="source" />
      </div>
    </template>
  </UDashboardPanel>
</template>
