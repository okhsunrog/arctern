// What a job action will actually do, decided from the same status the
// card renders. The console used to report every accepted request as a
// success — including "Woke up push_to_mira" on a job whose only route
// is manual-only, where the woken cycle selects nothing and records an
// idle tick. An operator reading that toast has been told a lie.

import type { JobStatus, TargetStatus } from '../client'
import { nextSync } from './pushTimes'

export interface ActionOutcome {
  title: string
  description?: string
  /** success = something will happen; warning = accepted but inert. */
  tone: 'success' | 'warning'
}

/**
 * Wake pokes the scheduler. Snap and prune jobs always run a cycle on
 * the poke; push jobs run one only if a target is both due and reachable
 * over an auto-eligible route.
 */
export function wakeOutcome(job: JobStatus | undefined, name: string): ActionOutcome {
  if (!job) return { title: `Woke up ${name}`, tone: 'success' }
  if (job.kind === 'sink') {
    return {
      title: `${name} has no cycle to wake`,
      description: 'Sink jobs are event-driven — they react to incoming transfers.',
      tone: 'warning',
    }
  }
  if (job.kind !== 'push') return { title: `Woke up ${name}`, tone: 'success' }
  if (job.running) {
    return {
      title: `${name} is already replicating`,
      description: 'The scheduler was poked; the running cycle continues.',
      tone: 'warning',
    }
  }

  const queued = (job.targets ?? []).filter((t) => t.manual_queued).map((t) => t.peer)
  if (queued.length > 0) {
    return {
      title: `${name} will replicate to ${queued.join(', ')}`,
      tone: 'success',
    }
  }

  const next = nextSync(job)
  switch (next.kind) {
    case 'due':
      return { title: `${name} will replicate now`, tone: 'success' }
    case 'blocked':
      return {
        title: `Nothing scheduled for ${name}`,
        description: `Scheduler poked, but ${next.reason}. Use "send now" to replicate anyway.`,
        tone: 'warning',
      }
    case 'manual':
      return {
        title: `Nothing scheduled for ${name}`,
        description: 'Every target is manual-only. Use "send now" to replicate.',
        tone: 'warning',
      }
    case 'at':
      return {
        title: `Nothing due for ${name} yet`,
        description: 'Scheduler poked; no target has reached its auto interval.',
        tone: 'warning',
      }
  }
}

/**
 * A manual push is queued, not executed: the cycle loop drains the
 * request set when it next wakes. During a running cycle that is only
 * after the in-flight transfer finishes, which the old copy ("will
 * replicate within seconds") flatly denied.
 */
export function pushOutcome(job: JobStatus | undefined, name: string, peer: string): ActionOutcome {
  if (job?.running) {
    return {
      title: `Queued push to ${peer}`,
      description: `${name} is mid-transfer; this will start once it finishes.`,
      tone: 'success',
    }
  }
  return {
    title: `Queued push to ${peer}`,
    description: `${name} will replicate to ${peer} within seconds.`,
    tone: 'success',
  }
}

/** True while the daemon reports this peer's manual push as still queued. */
export function isQueued(target: TargetStatus): boolean {
  return target.manual_queued === true
}

/** A target currently receiving bytes, per the job's transfer list. */
export function isTransferring(job: JobStatus, target: TargetStatus): boolean {
  return (job.transfers ?? []).some((t) => t.peer === target.peer)
}
