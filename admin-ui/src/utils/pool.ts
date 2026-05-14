// `zpool` prints sizes like "608G", "1.48T", "49.7M", "0B" — human-readable
// suffixes with one decimal. parseZpoolSize converts these to bytes so the
// UI can compute ratios (capacity bars) and totals.

const SUFFIXES: Record<string, number> = {
  B: 1,
  K: 1024,
  M: 1024 ** 2,
  G: 1024 ** 3,
  T: 1024 ** 4,
  P: 1024 ** 5,
  E: 1024 ** 6,
}

export function parseZpoolSize(s: string | null | undefined): number {
  if (!s) return 0
  const m = /^([\d.]+)([BKMGTPE])?$/.exec(s.trim())
  if (!m || !m[1]) return 0
  const n = Number.parseFloat(m[1])
  const suffix = m[2] ?? 'B'
  return n * (SUFFIXES[suffix] ?? 1)
}

export function poolUsedPercent(alloc: string, total: string): number {
  const a = parseZpoolSize(alloc)
  const t = parseZpoolSize(total)
  return t > 0 ? Math.round((a / t) * 100) : 0
}

export function poolStateColor(
  state: string,
): 'success' | 'warning' | 'error' | 'neutral' {
  switch (state) {
    case 'ONLINE':
      return 'success'
    case 'DEGRADED':
      return 'warning'
    case 'FAULTED':
    case 'UNAVAIL':
    case 'REMOVED':
      return 'error'
    default:
      return 'neutral'
  }
}
