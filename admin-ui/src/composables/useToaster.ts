import { useToast } from '@nuxt/ui/composables'
import type { ActionOutcome } from '../utils/actions'
import { apiErrorMessage } from '../utils/errors'

// Every operator-visible outcome goes through here so the console has
// one voice. The important rule it encodes: a request the daemon
// accepted is not automatically a success — an accepted wake that
// schedules nothing is reported as a warning, not a green check.

export function useToaster() {
  const toast = useToast()

  function report(outcome: ActionOutcome) {
    toast.add({
      title: outcome.title,
      description: outcome.description,
      color: outcome.tone === 'warning' ? 'warning' : 'success',
      icon: outcome.tone === 'warning' ? 'i-lucide-info' : 'i-lucide-circle-check',
      duration: outcome.tone === 'warning' ? 6000 : 2500,
    })
  }

  function success(title: string, description?: string) {
    report({ title, description, tone: 'success' })
  }

  function failure(title: string, error: unknown) {
    toast.add({
      title,
      description: apiErrorMessage(error),
      color: 'error',
      icon: 'i-lucide-circle-x',
    })
  }

  return { report, success, failure }
}
