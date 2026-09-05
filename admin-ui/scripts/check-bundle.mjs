import { readFileSync } from 'node:fs'
import { gzipSync } from 'node:zlib'

const html = readFileSync(new URL('../dist/index.html', import.meta.url), 'utf8')
const assets = new Set([...html.matchAll(/(?:src|href)="(\/assets\/[^"]+\.js)"/g)].map((m) => m[1]))
if (assets.size === 0) throw new Error('No initial JavaScript assets found')
let compressed = 0
for (const asset of assets) {
  compressed += gzipSync(readFileSync(new URL(`../dist${asset}`, import.meta.url))).length
}
const budget = 280_000
console.log(`Initial JavaScript: ${compressed} bytes gzip (budget: ${budget})`)
if (compressed > budget) throw new Error('Initial JavaScript exceeds the bundle budget')
