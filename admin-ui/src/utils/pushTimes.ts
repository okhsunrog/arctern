// Push jobs replicate on their own per-target schedule; the 15-minute
// scheduler tick underneath is an implementation detail. These helpers
// derive the two numbers an operator actually wants — when data last
// reached a peer, and when it will next do so — from TargetStatus.

import type { JobStatus, TargetStatus } from '../client'

export interface NextSync {
  kind: 'due' | 'at' | 'blocked' | 'manual'
  /** Unix seconds, for kind === 'at'. */
  at?: number
  /** Human reason, for kind === 'blocked'. */
  reason?: string
}

export function lastSync(job: JobStatus): number | null {
  let max: number | null = null
  for (const t of job.targets ?? []) {
    if (t.last_success != null && (max == null || t.last_success > max)) max = t.last_success
  }
  return max
}

function targetDueAt(t: TargetStatus): number {
  if (t.last_success == null || t.auto_interval_secs == null) return 0 // due immediately
  return t.last_success + t.auto_interval_secs
}

export function nextSync(job: JobStatus): NextSync {
  const now = Math.floor(Date.now() / 1000)
  const auto = (job.targets ?? []).filter((t) => t.mode === 'auto')
  if (auto.length === 0) return { kind: 'manual' }
  let best: TargetStatus | null = null
  let bestAt = Number.POSITIVE_INFINITY
  for (const t of auto) {
    const at = targetDueAt(t)
    if (at < bestAt) {
      bestAt = at
      best = t
    }
  }
  if (!best) return { kind: 'manual' }
  if (bestAt > now) return { kind: 'at', at: bestAt }
  // Due — will it actually run?
  if (best.connected && best.route_auto) return { kind: 'due' }
  if (best.connected && !best.route_auto) {
    return {
      kind: 'blocked',
      reason: best.route ? `manual-only route (${best.route}) active` : 'manual-only route active',
    }
  }
  return { kind: 'blocked', reason: `${best.peer} unreachable` }
}

function fmtIn(s: number): string {
  if (s < 90) return `${Math.max(1, Math.round(s))}s`
  if (s < 5400) return `${Math.round(s / 60)}m`
  if (s < 129600) return `${Math.round(s / 3600)}h`
  return `${Math.round(s / 86400)}d`
}

export function formatLastSync(job: JobStatus): string {
  const ts = lastSync(job)
  if (ts == null) return 'never'
  const s = Math.max(0, Math.floor(Date.now() / 1000) - ts)
  return `${fmtIn(s)} ago`
}

export function formatNextSync(job: JobStatus): string {
  const n = nextSync(job)
  switch (n.kind) {
    case 'due':
      return 'due now'
    case 'at':
      return `in ~${fmtIn((n.at ?? 0) - Math.floor(Date.now() / 1000))}`
    case 'blocked':
      return n.reason ?? 'blocked'
    case 'manual':
      return 'on Send now'
  }
}
