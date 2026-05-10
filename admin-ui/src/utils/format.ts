export function formatRelative(rfc3339: string | null | undefined): string {
  if (!rfc3339) return '—'
  const t = Date.parse(rfc3339)
  if (Number.isNaN(t)) return rfc3339
  const diffMs = Date.now() - t
  const past = diffMs >= 0
  const ms = Math.abs(diffMs)
  const sec = Math.round(ms / 1000)
  const min = Math.round(sec / 60)
  const hr = Math.round(min / 60)
  const day = Math.round(hr / 24)
  let out: string
  if (sec < 60) out = `${sec}s`
  else if (min < 60) out = `${min}m`
  else if (hr < 48) out = `${hr}h`
  else out = `${day}d`
  return past ? `${out} ago` : `in ${out}`
}

export function formatTimestamp(rfc3339: string | null | undefined): string {
  if (!rfc3339) return '—'
  const d = new Date(rfc3339)
  if (Number.isNaN(d.getTime())) return rfc3339
  return d.toLocaleString()
}

export function formatBytes(n: number | null | undefined): string {
  if (n == null) return '—'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
  let i = 0
  let v = n
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(v >= 100 || i === 0 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`
}
