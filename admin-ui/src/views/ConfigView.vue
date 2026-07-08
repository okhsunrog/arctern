<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { getConfig } from '../client'
import type { ConfigView as ConfigViewT } from '../client'
import { apiErrorMessage } from '../composables/useMutation'

const config = ref<ConfigViewT | null>(null)
const error = ref<string | null>(null)
const copied = ref(false)
const raw = ref(false)

async function refresh() {
  const r = await getConfig()
  if (r.error) {
    error.value = apiErrorMessage(r.error)
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

// Sectioned render: split the TOML on top-level tables so the operator
// scans peers / jobs / ACL blocks instead of one long wall. Purely
// lexical — comments travel with the section that follows them.
interface Section {
  title: string
  icon: string
  body: string
}

function sectionIcon(header: string): string {
  if (header.startsWith('peers')) return 'i-lucide-radio-tower'
  if (header.startsWith('jobs')) return 'i-lucide-list-checks'
  if (header.startsWith('allowed_clients')) return 'i-lucide-shield-check'
  if (header.startsWith('defaults')) return 'i-lucide-sliders-horizontal'
  return 'i-lucide-file-code-2'
}

const sections = computed<Section[]>(() => {
  const text = config.value?.content_toml ?? ''
  if (!text) return []
  const lines = text.split('\n')
  const out: Section[] = []
  let title = 'top level'
  let icon = 'i-lucide-file-code-2'
  let buf: string[] = []
  let pending: string[] = []
  const flush = () => {
    const body = [...buf].join('\n').trim()
    if (body) out.push({ title, icon, body })
    buf = []
  }
  for (const line of lines) {
    const m = /^\[\[?([a-zA-Z0-9_.-]+)\]?\]/.exec(line.trim())
    if (m && m[1]) {
      const root = m[1].split('.')[0] ?? m[1]
      const isNewGroup = !title.startsWith(root)
      if (isNewGroup) {
        flush()
        title = root
        icon = sectionIcon(root)
        buf.push(...pending)
        pending = []
      } else {
        buf.push(...pending)
        pending = []
      }
      buf.push(line)
    } else if (line.trim().startsWith('#') || line.trim() === '') {
      // Comments/blank lines attach to whatever section starts next.
      pending.push(line)
    } else {
      buf.push(...pending)
      pending = []
      buf.push(line)
    }
  }
  buf.push(...pending)
  flush()
  return out
})
</script>

<template>
  <UDashboardPanel id="config">
    <template #header>
      <UDashboardNavbar title="Config">
        <template #right>
          <USwitch v-model="raw" label="Raw" size="sm" />
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
        </template>
      </UDashboardNavbar>
    </template>
    <template #body>
      <div class="mx-auto w-full max-w-5xl space-y-4">
        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />

        <UAlert
          color="info"
          variant="subtle"
          icon="i-lucide-lock"
          title="Read-only"
          description="To change the config, edit the file on disk and restart the daemon. 'Reload from disk' only refreshes this view."
        />

        <div v-if="config" class="space-y-3">
          <div class="text-xs text-muted font-mono">{{ config.path }}</div>

          <pre
            v-if="raw"
            class="rounded-md border border-default bg-default p-4 overflow-x-auto text-xs font-mono whitespace-pre"
            >{{ config.content_toml }}</pre
          >

          <template v-else>
            <UCard v-for="s in sections" :key="s.title" :ui="{ body: 'p-0 sm:p-0' }">
              <template #header>
                <div class="flex items-center gap-2">
                  <UIcon :name="s.icon" class="text-muted" />
                  <span class="font-mono text-sm font-medium">{{ s.title }}</span>
                </div>
              </template>
              <pre class="p-4 overflow-x-auto text-xs font-mono whitespace-pre">{{ s.body }}</pre>
            </UCard>
          </template>
        </div>
        <div v-else-if="!error" class="text-muted text-sm">Loading…</div>
      </div>
    </template>
  </UDashboardPanel>
</template>
