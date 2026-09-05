<script setup lang="ts">
import { computed, ref, watch } from 'vue'

const props = defineProps<{
  dataset: string
  snapshots: string[]
  host: string
  loading: boolean
}>()
const open = defineModel<boolean>('open', { default: false })
const emit = defineEmits<{ confirm: [] }>()
const typed = ref('')
watch(open, () => {
  typed.value = ''
})
const armed = computed(
  () => props.snapshots.length > 0 && typed.value === props.dataset && !props.loading,
)
function dismiss() {
  open.value = false
}
function confirm() {
  if (!armed.value) return
  emit('confirm')
}
</script>

<template>
  <UModal
    v-model:open="open"
    title="Destroy selected snapshots?"
    :description="`${snapshots.length} snapshots will be permanently removed on ${host}. This cannot be undone.`"
  >
    <template #body>
      <ul class="font-mono text-xs space-y-1 max-h-64 overflow-y-auto">
        <li v-for="tag in snapshots" :key="tag" class="break-all">{{ dataset }}@{{ tag }}</li>
      </ul>
      <p class="text-muted text-sm my-3">
        ZFS will refuse to remove snapshots protected by holds or other constraints.
      </p>
      <UFormField :label="`Type ${dataset} to confirm`">
        <UInput
          v-model="typed"
          class="w-full font-mono"
          autocomplete="off"
          @keydown.enter="confirm"
        />
      </UFormField>
    </template>
    <template #footer>
      <div class="flex justify-end gap-2 w-full">
        <UButton variant="ghost" @click="dismiss">Cancel</UButton>
        <UButton color="error" :disabled="!armed" :loading="loading" @click="confirm"
          >Destroy {{ snapshots.length }}</UButton
        >
      </div>
    </template>
  </UModal>
</template>
