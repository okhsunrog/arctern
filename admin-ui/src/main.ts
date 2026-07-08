import './assets/main.css'

import { createApp } from 'vue'
import ui from '@nuxt/ui/vue-plugin'
import App from './App.vue'
import router from './router'

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
