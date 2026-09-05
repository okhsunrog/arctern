// What a job action will actually do, decided from the same status the
// card renders. The console used to report every accepted request as a
// success — including "Woke up push_to_mira" on a job whose only route
// is manual-only, where the woken cycle selects nothing and records an
// idle tick. An operator reading that toast has been told a lie.

import type { JobStatus, TargetStatus } from '../client'
import { asPushJob } from './jobs'
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
    // The running cycle already took its snapshot of the request set, so
    // this one drains on the next cycle either way — whether the job is
    // sending bytes or still deciding what to send.
    const doing = isTransferring(job, { peer } as TargetStatus)
      ? 'is mid-transfer'
      : 'is already running a cycle'
    return {
      title: `Queued push to ${peer}`,
      description: `${name} ${doing}; this will start once it finishes.`,
      tone: 'success',
    }
  }
  return {
    title: `Queued push to ${peer}`,
    description: `${name} will replicate to ${peer} within seconds.`,
    tone: 'success',
  }
}

/** A target currently receiving bytes, per the job's transfer list. */
export function isTransferring(job: JobStatus, target: TargetStatus): boolean {
  return (asPushJob(job)?.transfers ?? []).some((t) => t.peer === target.peer)
}

export type SendControl =
  /** Pressing it will queue a push. */
  | { kind: 'available'; tooltip: string }
  /** Nothing to offer: the thing the button asks for is already happening. */
  | { kind: 'hidden' }
  /** Normally available, blocked by something outside the operator's hands. */
  | { kind: 'disabled'; tooltip: string }

/**
 * Whether to offer "send now" for this target, and in what state.
 *
 * The distinction that matters: when the push is already under way or
 * already queued, the button is not merely unavailable — it is redundant,
 * because the badge beside it (and the progress bar above it) already say
 * so. A disabled button in `soft` variant is only slightly dimmed, so
 * leaving one there reads as a live call to action next to a running
 * transfer, which is exactly the contradiction this panel kept showing.
 * An unreachable peer is different: the action is normally available and
 * will be again, so the affordance stays, inert and explained.
 */
export function sendControl(job: JobStatus, target: TargetStatus): SendControl {
  if (isTransferring(job, target)) return { kind: 'hidden' }
  if (target.manual_queued) return { kind: 'hidden' }
  if (!target.connected) {
    return { kind: 'disabled', tooltip: `${target.peer} is unreachable` }
  }
  return { kind: 'available', tooltip: `Replicate to ${target.peer} now` }
}
