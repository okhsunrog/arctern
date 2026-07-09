<script setup lang="ts">
import AdminLogin from './components/AdminLogin.vue'
import AdminShell from './AdminShell.vue'
import { useAuth } from './composables/useAuth'

const { status, checkSession } = useAuth()
void checkSession()
</script>

<template>
  <UApp :toaster="{ position: 'bottom-right' }">
    <div v-if="status === 'checking'" class="min-h-screen grid place-items-center bg-default">
      <UIcon name="i-lucide-loader-circle" class="size-6 animate-spin text-muted" />
    </div>
    <AdminLogin v-else-if="status === 'anonymous'" />
    <AdminShell v-else />
  </UApp>
</template>
