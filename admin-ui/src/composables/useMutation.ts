// `useToast` is auto-imported by the Nuxt UI vite plugin (see
// auto-imports.d.ts).

// Every mutating call in the console goes through here: the daemon's
// error body (ApiErrorBody { error, message }) surfaces as a toast
// instead of dying silently, and successes confirm themselves.

interface CallResult {
  error?: unknown
}

export function apiErrorMessage(e: unknown): string {
  if (e && typeof e === 'object') {
    const body = e as { message?: unknown; error?: unknown }
    if (typeof body.message === 'string' && body.message) {
      return typeof body.error === 'string' && body.error
        ? `${body.error}: ${body.message}`
        : body.message
    }
  }
  if (e instanceof Error) return e.message
  return String(e)
}

export function useMutation() {
  const toast = useToast()

  /** Run a mutating API call; toast the outcome. Returns true on success. */
  async function mutate(
    title: string,
    call: () => Promise<CallResult>,
    opts: { successDescription?: string; silentSuccess?: boolean } = {},
  ): Promise<boolean> {
    let r: CallResult
    try {
      r = await call()
    } catch (e) {
      toast.add({
        title: `${title} failed`,
        description: apiErrorMessage(e),
        color: 'error',
        icon: 'i-lucide-circle-x',
      })
      return false
    }
    if (r.error) {
      toast.add({
        title: `${title} failed`,
        description: apiErrorMessage(r.error),
        color: 'error',
        icon: 'i-lucide-circle-x',
      })
      return false
    }
    if (!opts.silentSuccess) {
      toast.add({
        title,
        description: opts.successDescription,
        color: 'success',
        icon: 'i-lucide-circle-check',
        duration: 2500,
      })
    }
    return true
  }

  return { mutate, toast }
}
