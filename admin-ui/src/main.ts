import './assets/main.css'

import { createApp } from 'vue'
import { createPinia } from 'pinia'
import { PiniaColada } from '@pinia/colada'
import { PiniaColadaAutoRefetch } from '@pinia/colada-plugin-auto-refetch'
import { PiniaColadaRetry } from '@pinia/colada-plugin-retry'
import ui from '@nuxt/ui/vue-plugin'
import { addCollection } from '@iconify/vue'
import lucide from '@iconify-json/lucide/icons.json'
import App from './App.vue'
import { client } from './client/client.gen'
import { markUnauthenticated } from './composables/useAuth'
import { isRetryable } from './utils/errors'
import router from './router'

// The console must work fully offline (loopback-only daemon): register
// the icon set locally. Production keeps only referenced icons and Nuxt UI defaults.
addCollection(lucide)

client.interceptors.response.use((response) => {
  if (response.status === 401) markUnauthenticated()
  return response
})

// A redeploy replaces every hashed chunk; a tab still holding the old
// index.html then fails to lazy-load view chunks. Reload once so the
// browser picks up the fresh index.html instead of dead-ending on a
// blank route.
router.onError((error, to) => {
  const msg = error instanceof Error ? error.message : String(error)
  if (/dynamically imported module|import\(\) chunk|Failed to fetch/i.test(msg)) {
    const key = 'arctern-chunk-reload'
    if (!sessionStorage.getItem(key)) {
      sessionStorage.setItem(key, '1')
      location.assign(to.fullPath)
    }
  }
})
router.afterEach(() => sessionStorage.removeItem('arctern-chunk-reload'))

const app = createApp(App)

app.use(router)
app.use(createPinia())
app.use(PiniaColada, {
  queryOptions: {
    // Every view already renders from cache first; refetching on mount
    // is what keeps a stale panel honest after a long tab suspension.
    refetchOnMount: true,
    refetchOnWindowFocus: true,
  },
  plugins: [
    // Replaces the per-composable setInterval this console used to run.
    // Only queries with a live consumer are revalidated.
    PiniaColadaAutoRefetch({ autoRefetch: true }),
    // The peer proxy rides an SSH control channel that can blink during
    // a route failover; one silent retry beats a red banner.
    PiniaColadaRetry({
      retry: (failureCount, error) => failureCount < 1 && isRetryable(error),
      delay: 800,
    }),
  ],
})
app.use(ui)

app.mount('#app')
