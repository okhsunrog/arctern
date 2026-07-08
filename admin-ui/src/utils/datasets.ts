import type { DatasetSummary } from '../client'

export interface DatasetRow {
  name: string
  depth: number
  // Last path segment in tree mode; full path while searching.
  label: string
  type: string
  used: number | null
  hasChildren: boolean
  collapsed: boolean
}

function usedOf(d: DatasetSummary): number | null {
  const n = Number(d.properties?.used ?? '')
  return Number.isFinite(n) ? n : null
}

/**
 * Flatten the daemon's flat dataset list into the rows the picker
 * renders. In tree mode rows are indented by path depth and hidden
 * under any collapsed ancestor; a non-empty `search` switches to a
 * flat, full-path match list that ignores collapse state.
 */
export function visibleDatasetRows(
  datasets: DatasetSummary[],
  collapsed: Set<string>,
  search: string,
): DatasetRow[] {
  const fs = datasets
    .filter((d) => d.dataset_type === 'filesystem' || d.dataset_type === 'volume')
    .slice()
    .sort((a, b) => a.name.localeCompare(b.name))

  const names = fs.map((d) => d.name)
  const hasChildren = (name: string) => names.some((n) => n.startsWith(`${name}/`))

  const q = search.trim().toLowerCase()
  if (q) {
    return fs
      .filter((d) => d.name.toLowerCase().includes(q))
      .map((d) => ({
        name: d.name,
        depth: 0,
        label: d.name,
        type: d.dataset_type,
        used: usedOf(d),
        hasChildren: false,
        collapsed: false,
      }))
  }

  const rows: DatasetRow[] = []
  for (const d of fs) {
    const parts = d.name.split('/')
    let hidden = false
    for (let i = 1; i < parts.length; i++) {
      if (collapsed.has(parts.slice(0, i).join('/'))) {
        hidden = true
        break
      }
    }
    if (hidden) continue
    rows.push({
      name: d.name,
      depth: parts.length - 1,
      label: parts[parts.length - 1],
      type: d.dataset_type,
      used: usedOf(d),
      hasChildren: hasChildren(d.name),
      collapsed: collapsed.has(d.name),
    })
  }
  return rows
}
