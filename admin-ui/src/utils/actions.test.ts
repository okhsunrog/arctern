import { describe, expect, it } from 'vite-plus/test'
import type { JobStatus, TargetStatus } from '../client'
import { pushOutcome, wakeOutcome } from './actions'

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

function job(overrides: Partial<JobStatus> = {}): JobStatus {
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
    targets: [target()],
    ...overrides,
  }
}

describe('wakeOutcome', () => {
  it('reports a plain success for kinds whose cycle always runs', () => {
    expect(wakeOutcome(job({ kind: 'snap', targets: [] }), 'snap_nova').tone).toBe('success')
  })

  // The original complaint: a manual-only active route means the woken
  // cycle selects nothing, yet the console reported "Woke up <job>".
  it('warns when the active route bars scheduled replication', () => {
    const out = wakeOutcome(
      job({ targets: [target({ route: 'wg', route_auto: false })] }),
      'push_to_mira',
    )
    expect(out.tone).toBe('warning')
    expect(out.description).toContain('manual-only route (wg)')
  })

  it('warns when no target has reached its auto interval', () => {
    const out = wakeOutcome(job(), 'push_to_mira')
    expect(out.tone).toBe('warning')
    expect(out.title).toContain('Nothing due')
  })

  it('confirms success when a target is actually due', () => {
    const out = wakeOutcome(
      job({ targets: [target({ last_success: now() - 2 * HOUR })] }),
      'push_to_mira',
    )
    expect(out.tone).toBe('success')
  })

  it('warns when every target is manual-only by policy', () => {
    const out = wakeOutcome(job({ targets: [target({ mode: 'manual' })] }), 'push_to_mira')
    expect(out.tone).toBe('warning')
    expect(out.description).toContain('send now')
  })

  it('reports the queued peers rather than the schedule', () => {
    const out = wakeOutcome(
      job({ targets: [target({ route_auto: false, manual_queued: true })] }),
      'push_to_mira',
    )
    expect(out.tone).toBe('success')
    expect(out.title).toContain('mira')
  })

  it('does not pretend a wake started anything while a cycle is running', () => {
    expect(wakeOutcome(job({ running: true }), 'push_to_mira').tone).toBe('warning')
  })
})

describe('pushOutcome', () => {
  it('promises seconds only when nothing is in flight', () => {
    expect(pushOutcome(job(), 'push_to_mira', 'mira').description).toContain('within seconds')
  })

  // The toast used to say "within seconds" mid-transfer, but the request
  // only drains on the next cycle — after the running send finishes.
  it('says the push waits for the running transfer', () => {
    const out = pushOutcome(job({ running: true }), 'push_to_mira', 'mira')
    expect(out.description).toContain('once it finishes')
    expect(out.description).not.toContain('within seconds')
  })
})

describe('pushOutcome during a cycle with no transfer', () => {
  // The planning window: the cycle is running but has registered no
  // transfer yet, so "mid-transfer" would name something that is not there.
  it('says a cycle is running rather than claiming a transfer', () => {
    const out = pushOutcome(job({ running: true, transfers: [] }), 'push_to_mira', 'mira')
    expect(out.description).toContain('already running a cycle')
    expect(out.description).not.toContain('mid-transfer')
  })

  it('still says mid-transfer when that peer is actually receiving', () => {
    const out = pushOutcome(
      job({
        running: true,
        transfers: [
          {
            dataset: 'novafs/arch0/data/root',
            peer: 'mira',
            kind: 'incremental',
            bytes_sent: 1,
            started_at: 0,
            phase: 'sending',
            phase_since: 0,
          },
        ],
      }),
      'push_to_mira',
      'mira',
    )
    expect(out.description).toContain('mid-transfer')
  })
})
