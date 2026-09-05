// `JobStatus` is a union discriminated by `kind`; only the push variant
// carries transfers, targets and pause state. Narrow once here so views
// and helpers do not each re-derive what "is a push job" means.

import type { JobStatus, PushJobStatus } from '../client'

export type PushJob = PushJobStatus & { kind: 'push' }

export function isPushJob(job: JobStatus): job is PushJob {
  return job.kind === 'push'
}

/** The push variant, or null for snap/prune jobs. */
export function asPushJob(job: JobStatus | undefined): PushJob | null {
  return job && isPushJob(job) ? job : null
}
