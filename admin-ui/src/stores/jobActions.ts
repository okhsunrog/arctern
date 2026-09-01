import { defineStore } from 'pinia'
import { reactive } from 'vue'

// Which job actions are in flight, shared by every `useJobs()` consumer.
// Held here rather than per composable instance so that triggering an
// action from the command palette greys out the matching button on the
// card behind it — the shell and the view are separate instances, and a
// per-instance set left each blind to the other's requests.

export type JobAction = 'wake' | 'cancel' | 'pause' | 'resume' | 'push'

/**
 * Keys are host-scoped on purpose: the same job name exists on this
 * daemon and on a peer's console, and acting on one must not disable the
 * other's button. The parts are NUL-joined because a config-derived job
 * or peer name can contain very nearly anything else.
 */
export function jobActionKey(
  action: JobAction,
  scope: string,
  name: string,
  peer?: string,
): string {
  return [action, scope, name, peer ?? ''].join('\u0000')
}

export const useJobActions = defineStore('job-actions', () => {
  const inFlight = reactive(new Set<string>())
  return { inFlight }
})
