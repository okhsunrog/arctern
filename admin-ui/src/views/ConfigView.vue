<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { getConfig } from '../client'
import type { ConfigView as ConfigViewT } from '../client'

const config = ref<ConfigViewT | null>(null)
const error = ref<string | null>(null)
const copied = ref(false)

async function refresh() {
  const r = await getConfig()
  if (r.error) {
    error.value =
      r.error && typeof r.error === 'object' && 'message' in r.error
        ? String((r.error as { message: unknown }).message)
        : String(r.error)
  } else {
    config.value = r.data ?? null
    error.value = null
  }
}

onMounted(refresh)

async function copy() {
  if (!config.value) return
  await navigator.clipboard.writeText(config.value.content_toml)
  copied.value = true
  setTimeout(() => (copied.value = false), 1500)
}
</script>

<template>
  <div class="max-w-5xl mx-auto px-4 py-6 space-y-4">
    <div class="flex items-center justify-between">
      <h1 class="text-2xl font-semibold">Config</h1>
      <div class="flex gap-2">
        <UButton icon="i-lucide-refresh-cw" variant="soft" size="xs" @click="refresh">
          Reload from disk
        </UButton>
        <UButton
          :icon="copied ? 'i-lucide-check' : 'i-lucide-copy'"
          variant="soft"
          size="xs"
          :disabled="!config"
          @click="copy"
          >{{ copied ? 'Copied' : 'Copy' }}</UButton
        >
      </div>
    </div>

    <UAlert v-if="error" color="error" :title="error" />

    <UAlert
      color="info"
      variant="subtle"
      title="Read-only"
      description="To change the config, edit the file on disk, then 'systemctl reload arctern' (or restart the daemon). The 'Reload from disk' button above only refreshes this view — it does not signal the daemon."
    />

    <div v-if="config" class="space-y-2">
      <div class="text-xs text-gray-500 font-mono">{{ config.path }}</div>
      <pre
        class="rounded-md border border-gray-200 dark:border-gray-800 bg-gray-50 dark:bg-gray-900 p-4 overflow-x-auto text-xs font-mono whitespace-pre"
        >{{ config.content_toml }}</pre
      >
    </div>
    <div v-else-if="!error" class="text-gray-500">Loading…</div>
  </div>
</template>
