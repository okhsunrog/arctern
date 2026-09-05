import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import type { Plugin } from 'vite-plus'
import type { IconifyJSON } from '@iconify/vue'

function sources(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name)
    if (entry.isDirectory()) return sources(path)
    return /\.(?:vue|[cm]?[jt]s)$/.test(path) && !path.endsWith('.test.ts') ? [path] : []
  })
}

// Keep application icons and Nuxt UI's default controls available offline.
// Development retains the full collection so new icons work immediately with HMR.
export function offlineIcons(): Plugin {
  let root: string
  return {
    name: 'arctern-offline-icons',
    apply: 'build',
    enforce: 'pre',
    configResolved(config) {
      root = config.root
    },
    transform(code, id) {
      if (!id.replaceAll('\\', '/').endsWith('/@iconify-json/lucide/icons.json')) return
      const collection = JSON.parse(code) as IconifyJSON
      const selected: IconifyJSON = {
        prefix: collection.prefix,
        width: collection.width,
        height: collection.height,
        icons: {},
        aliases: {},
      }
      function include(name: string) {
        if (selected.icons[name] || selected.aliases![name]) return
        if (collection.icons[name]) selected.icons[name] = collection.icons[name]
        else if (collection.aliases?.[name]) {
          selected.aliases![name] = collection.aliases[name]
          include(collection.aliases[name].parent)
        } else throw new Error(`Unknown Lucide icon: ${name}`)
      }
      for (const path of [
        ...sources(join(root, 'src')),
        ...sources(join(root, 'node_modules/@nuxt/ui/dist')),
      ]) {
        this.addWatchFile(path)
        const source = readFileSync(path, 'utf8')
        for (const match of source.matchAll(/\bi-lucide-([a-z0-9-]+)/g)) include(match[1]!)
      }
      return { code: JSON.stringify(selected), map: null }
    },
  }
}
