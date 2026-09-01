<script setup lang="ts">
// One confirmation dialog for actions that cannot be undone by clicking
// again. The console previously guarded snapshot destroys with a modal
// but let "stop transfer" and "stop scrub" fire on a single click of an
// icon-only button — both throw away work in progress.

defineProps<{
  title: string
  description?: string
  /** Rendered monospace: the exact thing being acted on. */
  subject?: string
  confirmLabel: string
  confirmColor?: 'error' | 'warning' | 'primary'
  loading?: boolean
}>()

const open = defineModel<boolean>('open', { default: false })
const emit = defineEmits<{ confirm: [] }>()

function accept() {
  open.value = false
  emit('confirm')
}

function dismiss() {
  open.value = false
}
</script>

<template>
  <UModal v-model:open="open" :title="title">
    <template #body>
      <div class="space-y-3">
        <p v-if="description" class="text-sm text-muted">{{ description }}</p>
        <p v-if="subject" class="font-mono text-sm break-all">{{ subject }}</p>
      </div>
    </template>
    <template #footer>
      <div class="flex justify-end gap-2 w-full">
        <UButton color="neutral" variant="ghost" @click="dismiss">Cancel</UButton>
        <UButton :color="confirmColor ?? 'error'" :loading="loading" @click="accept">
          {{ confirmLabel }}
        </UButton>
      </div>
    </template>
  </UModal>
</template>
