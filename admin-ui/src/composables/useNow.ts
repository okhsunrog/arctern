import { createSharedComposable } from '@vueuse/core'
import { onScopeDispose, ref } from 'vue'

// One clock for the whole console. Each transfer slot used to start its
// own 1s interval, so a job with four parallel send slots ticked four
// timers that all computed the same second.
export const useNowSeconds = createSharedComposable(() => {
  const now = ref(Math.floor(Date.now() / 1000))
  const handle = setInterval(() => (now.value = Math.floor(Date.now() / 1000)), 1000)
  onScopeDispose(() => clearInterval(handle))
  return now
})
