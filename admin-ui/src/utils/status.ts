// The console's single status language: every domain state (job, peer,
// route, pool, vdev, log level, run outcome) maps here to one visual
// vocabulary — color token, icon, label, card rail class. Views never
// invent their own mapping.

import type { JobStatus, PeerReachability } from '../client'

export type StatusColor = 'success' | 'warning' | 'error' | 'info' | 'neutral'

export interface StatusView {
  color: StatusColor
  icon: string
  label: string
  /** Class for the card's left status rail. */
  rail: string
  /** True renders the dot with a live pulse. */
  pulse?: boolean
}

const RAIL: Record<StatusColor, string> = {
  success: 'rail rail-ok',
  warning: 'rail rail-warn',
  error: 'rail rail-err',
  info: 'rail rail-info',
  neutral: 'rail rail-idle',
}

function view(color: StatusColor, icon: string, label: string, pulse = false): StatusView {
  return { color, icon, label, rail: RAIL[color], pulse }
}

export function jobStatus(j: JobStatus): StatusView {
  // running/paused win: last_* describe the previous cycle and are
  // stale while a long send is in flight.
  if (j.paused) return view('neutral', 'i-lucide-circle-pause', 'paused')
  if (j.running) return view('info', 'i-lucide-loader', 'running', true)
  if (j.last_error) return view('error', 'i-lucide-circle-x', 'error')
  if (j.last_run) return view('success', 'i-lucide-circle-check', 'ok')
  return view('neutral', 'i-lucide-circle-dashed', 'idle')
}

export function runStatus(status: string): StatusView {
  switch (status) {
    case 'ok':
      return view('success', 'i-lucide-circle-check', 'ok')
    case 'error':
      return view('error', 'i-lucide-circle-x', 'error')
    case 'running':
      return view('info', 'i-lucide-loader', 'running', true)
    case 'cancelled':
      return view('neutral', 'i-lucide-circle-slash', 'cancelled')
    default:
      return view('neutral', 'i-lucide-circle-help', status)
  }
}

export function peerStatus(r: PeerReachability): StatusView {
  switch (r.kind) {
    case 'connected':
      return view('success', 'i-lucide-link', 'connected', true)
    case 'reconnecting':
      return view('warning', 'i-lucide-refresh-cw', 'reconnecting')
    case 'failed':
      return view('error', 'i-lucide-unlink', 'unreachable')
  }
}

export function routeHealthStatus(health: string): StatusView {
  switch (health) {
    case 'connected':
      return view('success', 'i-lucide-link', 'connected')
    case 'failed':
      return view('error', 'i-lucide-unlink', 'failed')
    default:
      return view('neutral', 'i-lucide-circle-dashed', 'standby')
  }
}

export function poolStatus(state: string): StatusView {
  switch (state) {
    case 'ONLINE':
      return view('success', 'i-lucide-hard-drive', 'ONLINE')
    case 'DEGRADED':
      return view('warning', 'i-lucide-triangle-alert', 'DEGRADED')
    case 'FAULTED':
    case 'UNAVAIL':
    case 'REMOVED':
      return view('error', 'i-lucide-octagon-alert', state)
    case 'OFFLINE':
      return view('neutral', 'i-lucide-power-off', 'OFFLINE')
    default:
      return view('neutral', 'i-lucide-circle-help', state)
  }
}

export function logLevelColor(level: string): StatusColor {
  if (level === 'ERROR') return 'error'
  if (level === 'WARN') return 'warning'
  if (level === 'INFO') return 'info'
  return 'neutral'
}

export function scanStatus(state: string | undefined): StatusView {
  switch (state) {
    case 'SCANNING':
      return view('info', 'i-lucide-radar', 'scanning', true)
    case 'FINISHED':
      return view('success', 'i-lucide-circle-check', 'finished')
    case 'CANCELED':
      return view('neutral', 'i-lucide-circle-slash', 'canceled')
    default:
      return view('neutral', 'i-lucide-circle-dashed', state ?? 'none')
  }
}
