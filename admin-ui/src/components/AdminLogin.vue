<script setup lang="ts">
import { ref } from 'vue'
import TernMark from './TernMark.vue'
import { useAuth } from '../composables/useAuth'

const token = ref('')
const submitting = ref(false)
const { error, login } = useAuth()

async function submit() {
  if (!token.value || submitting.value) return
  submitting.value = true
  const submitted = token.value
  token.value = ''
  await login(submitted)
  submitting.value = false
}
</script>

<template>
  <main class="min-h-screen grid place-items-center bg-default p-6">
    <UCard class="w-full max-w-sm" :ui="{ body: 'space-y-5' }">
      <div class="flex items-center gap-3">
        <TernMark class="size-9 shrink-0" />
        <div>
          <div class="font-mono font-semibold">arctern</div>
          <div class="text-sm text-muted">Administrator access</div>
        </div>
      </div>

      <form class="space-y-4" @submit.prevent="submit">
        <UFormField label="Admin token" required>
          <UInput
            v-model="token"
            type="password"
            autocomplete="current-password"
            autofocus
            icon="i-lucide-key-round"
            class="w-full"
          />
        </UFormField>
        <UAlert v-if="error" color="error" :title="error" icon="i-lucide-circle-x" />
        <UButton
          type="submit"
          block
          icon="i-lucide-log-in"
          :loading="submitting"
          :disabled="!token"
        >
          Sign in
        </UButton>
      </form>
    </UCard>
  </main>
</template>
