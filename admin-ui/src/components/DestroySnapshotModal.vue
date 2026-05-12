<script setup lang="ts">
import { computed, ref, watch } from 'vue'

const props = defineProps<{
  open: boolean
  snapshotName: string | null
}>()

const emit = defineEmits<{
  (e: 'update:open', v: boolean): void
  (e: 'confirm', name: string): void
}>()

const typed = ref('')

watch(
  () => props.open,
  (o) => {
    if (o) typed.value = ''
  },
)

const armed = computed(() => typed.value === (props.snapshotName ?? ''))

function close() {
  emit('update:open', false)
}

function go() {
  if (props.snapshotName && armed.value) {
    emit('confirm', props.snapshotName)
    close()
  }
}
</script>

<template>
  <UModal
    :open="open"
    title="Destroy snapshot?"
    :description="`This permanently removes the snapshot from the pool. It cannot be undone.`"
    @update:open="emit('update:open', $event)"
  >
    <template #body>
      <div class="space-y-3 text-sm">
        <div>
          <span class="text-gray-500">Target:</span>
          <code class="ml-1 font-mono text-error-600 break-all">{{ snapshotName }}</code>
        </div>
        <div>
          Type the full snapshot name to enable the Destroy button.
        </div>
        <UInput
          v-model="typed"
          :placeholder="snapshotName ?? ''"
          autofocus
          class="font-mono"
          @keydown.enter="go"
        />
      </div>
    </template>
    <template #footer>
      <div class="flex justify-end gap-2 w-full">
        <UButton variant="ghost" @click="close">Cancel</UButton>
        <UButton color="error" :disabled="!armed" icon="i-lucide-trash-2" @click="go">
          Destroy
        </UButton>
      </div>
    </template>
  </UModal>
</template>
