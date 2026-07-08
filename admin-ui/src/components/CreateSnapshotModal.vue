<script setup lang="ts">
import { computed, ref, watch } from 'vue'

const props = defineProps<{
  open: boolean
  dataset: string | null
}>()

const emit = defineEmits<{
  (e: 'update:open', v: boolean): void
  (e: 'confirm', payload: { name: string; recursive: boolean }): void
}>()

const name = ref('')
const recursive = ref(false)

// zrepl/arctern convention is <prefix><RFC3339-utc-no-colons>; offer a
// manual-prefixed default so a hand-taken snapshot is protected from the
// prune grid and easy to spot.
function defaultName(): string {
  const d = new Date()
  const p = (n: number) => String(n).padStart(2, '0')
  const stamp =
    `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}` +
    `T${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}Z`
  return `manual_${stamp}`
}

watch(
  () => props.open,
  (o) => {
    if (o) {
      name.value = defaultName()
      recursive.value = false
    }
  },
)

const trimmed = computed(() => name.value.trim())

function close() {
  emit('update:open', false)
}

function go() {
  if (!trimmed.value) return
  emit('confirm', { name: trimmed.value, recursive: recursive.value })
  close()
}
</script>

<template>
  <UModal :open="open" title="Create snapshot" @update:open="emit('update:open', $event)">
    <template #body>
      <div class="space-y-3 text-sm">
        <div>
          <span class="text-gray-500">Dataset:</span>
          <code class="ml-1 font-mono break-all">{{ dataset }}</code>
        </div>
        <UFormField label="Snapshot name">
          <UInput
            v-model="name"
            autofocus
            class="font-mono w-full"
            placeholder="manual_20260708T120000Z"
            @keydown.enter="go"
          />
        </UFormField>
        <div class="text-xs text-gray-500 break-all">
          Will create <code class="font-mono">{{ dataset }}@{{ trimmed || '…' }}</code>
        </div>
        <UCheckbox v-model="recursive" label="Recursive — also snapshot child datasets (-r)" />
      </div>
    </template>
    <template #footer>
      <div class="flex justify-end gap-2 w-full">
        <UButton variant="ghost" @click="close">Cancel</UButton>
        <UButton color="primary" :disabled="!trimmed" icon="i-lucide-camera" @click="go">
          Create
        </UButton>
      </div>
    </template>
  </UModal>
</template>
