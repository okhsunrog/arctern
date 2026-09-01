import { describe, expect, it } from 'vite-plus/test'
import type { JobStatus, TargetStatus } from '../client'
import { formatNextSync, lastSync, nextSync } from './pushTimes'

const HOUR = 3600
const now = () => Math.floor(Date.now() / 1000)

function target(overrides: Partial<TargetStatus> = {}): TargetStatus {
  return {
    peer: 'mira',
    mode: 'auto',
    connected: true,
    route: 'lan',
    route_auto: true,
    manual_queued: false,
    auto_interval_secs: HOUR,
    last_success: now() - 10,
    ...overrides,
  }
}

function job(targets: TargetStatus[]): JobStatus {
  return {
    name: 'push_to_mira',
    kind: 'push',
    last_run: null,
    next_run: null,
    last_error: null,
    running: false,
    paused: false,
    cancellable: false,
    transfers: [],
    targets,
  }
}

describe('nextSync', () => {
  it('counts down when the route permits scheduled replication', () => {
    expect(nextSync(job([target()])).kind).toBe('at')
  })

  it('is due once the interval has elapsed', () => {
    expect(nextSync(job([target({ last_success: now() - 2 * HOUR })])).kind).toBe('due')
  })

  // A manual-only route refuses scheduled sync whatever the clock says,
  // so counting down to one would promise something that cannot happen.
  it('reports the manual-only route before the clock, not after it', () => {
    const n = nextSync(job([target({ route: 'wg', route_auto: false })]))
    expect(n.kind).toBe('blocked')
    expect(n.reason).toContain('wg')
    expect(formatNextSync(job([target({ route: 'wg', route_auto: false })]))).toContain('wg')
  })

  // Unreachability is transient: the peer may reconnect before the
  // target comes due, so it only blocks a sync that is already owed.
  it('keeps counting down while a not-yet-due peer is unreachable', () => {
    expect(nextSync(job([target({ connected: false })])).kind).toBe('at')
  })

  it('reports an unreachable peer once the sync is owed', () => {
    const n = nextSync(job([target({ connected: false, last_success: now() - 2 * HOUR })]))
    expect(n.kind).toBe('blocked')
    expect(n.reason).toContain('unreachable')
  })

  it('has nothing to schedule when every target is manual', () => {
    expect(nextSync(job([target({ mode: 'manual' })])).kind).toBe('manual')
  })
})

describe('lastSync', () => {
  it('takes the most recent success across targets', () => {
    const recent = now() - 5
    expect(
      lastSync(
        job([target({ last_success: now() - 900 }), target({ peer: 'x', last_success: recent })]),
      ),
    ).toBe(recent)
  })

  it('is null before anything has ever synced', () => {
    expect(lastSync(job([target({ last_success: undefined })]))).toBeNull()
  })
})
