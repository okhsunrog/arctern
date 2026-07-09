import './assets/main.css'

import { createApp } from 'vue'
import ui from '@nuxt/ui/vue-plugin'
import { addCollection } from '@iconify/vue'
import lucide from '@iconify-json/lucide/icons.json'
import App from './App.vue'
import { client } from './client/client.gen'
import { markUnauthenticated } from './composables/useAuth'
import router from './router'

// The console must work fully offline (loopback-only daemon): register
// the icon set locally, otherwise @iconify/vue fetches every icon from
// api.iconify.design at runtime. 85K gzipped for the whole collection.
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
app.use(ui)

app.mount('#app')
