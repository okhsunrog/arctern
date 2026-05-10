<script setup lang="ts">
import { useRoute } from 'vue-router'
const route = useRoute()
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-6">
    <h1 class="text-2xl font-semibold mb-2">{{ route.meta.title ?? 'Coming soon' }}</h1>
    <p class="text-gray-500">This view lands in a later commit. See docs/admin-ui-plan.md.</p>
  </div>
</template>
