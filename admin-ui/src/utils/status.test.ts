import { describe, expect, it } from 'vite-plus/test'
import type { JobStatus, PushJobStatus } from '../client'
import { jobFailureMessage, jobStatus, runStatus } from './status'

function pushJob(overrides: Partial<PushJobStatus> = {}): JobStatus {
  return { name: 'push', kind: 'push', targets: [], ...overrides }
}

describe('job status outcomes', () => {
  it('keeps an operator cancellation neutral', () => {
    const job = pushJob({
      targets: [
        {
          peer: 'mira',
          mode: 'auto',
          connected: true,
          last_success: 100,
          last_attempt: 200,
          last_outcome: 'cancelled',
        },
      ],
    })

    expect(jobFailureMessage(job)).toBeNull()
    expect(jobStatus(job).color).toBe('success')
    expect(runStatus('cancelled').color).toBe('neutral')
  })

  it('never calls a dry-run job ok', () => {
    // A plan-only job used to show "ok" and "synced" after every cycle
    // while having replicated nothing.
    const job = pushJob({
      dry_run: true,
      last_run: '2026-09-05T10:00:00Z',
      targets: [
        {
          peer: 'mira',
          mode: 'auto',
          connected: true,
          last_attempt: 200,
          last_outcome: 'dry_run',
        },
      ],
    })

    expect(jobStatus(job).label).toBe('dry run')
    expect(jobStatus(job).color).toBe('neutral')
    expect(runStatus('dry_run').label).toBe('dry run')
    // Errors still outrank the dry-run label.
    expect(jobStatus(pushJob({ dry_run: true, last_error: 'plan: boom' })).color).toBe('error')
  })

  it('counts a target failure even when the job-level error is empty', () => {
    const job = pushJob({
      targets: [
        {
          peer: 'mira',
          mode: 'auto',
          connected: true,
          last_outcome: 'error',
          last_message: 'receiver closed the connection',
        },
      ],
    })

    expect(jobFailureMessage(job)).toBe('receiver closed the connection')
    expect(jobStatus(job).color).toBe('error')
  })

  it('does not count a previous target failure while that peer is retrying', () => {
    const job = pushJob({
      running: true,
      transfers: [
        {
          dataset: 'tank/home',
          peer: 'mira',
          kind: 'incremental',
          bytes_sent: 1024,
          started_at: 300,
        },
      ],
      targets: [
        {
          peer: 'mira',
          mode: 'auto',
          connected: true,
          last_attempt: 200,
          last_outcome: 'error',
          last_message: 'receiver closed the connection',
        },
      ],
    })

    expect(jobFailureMessage(job)).toBeNull()
    expect(jobStatus(job).label).toBe('running')
  })
})
