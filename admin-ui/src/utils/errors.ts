// One error vocabulary for the whole console. The generated client
// returns `{ data?, error? }` rather than throwing, and the daemon's
// error body is `ApiErrorBody { error, message }` — every call site used
// to re-derive that, five slightly different ways.

/** Human message for anything the client or the network can hand back. */
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

/** The daemon's machine-readable error code (`snapshot_held`, …), when present. */
export function apiErrorCode(e: unknown): string | null {
  const body = e instanceof ApiCallError ? e.body : e
  if (body && typeof body === 'object') {
    const code = (body as { error?: unknown }).error
    if (typeof code === 'string' && code) return code
  }
  return null
}

/**
 * Turn a generated-client result into a promise that rejects on error.
 * Query libraries signal failure by throwing; the client signals it by
 * returning, so every query goes through here.
 */
export function unwrap<T>(r: { data?: T; error?: unknown; response?: Response }): T {
  if (r.error) throw new ApiCallError(r.error, r.response?.status)
  return r.data as T
}

/**
 * Carries the raw body so `apiErrorCode` still works after a throw, and
 * the HTTP status so retry policy can tell a flaky link from a refusal.
 */
export class ApiCallError extends Error {
  readonly body: unknown
  /** Undefined when the request never got a response at all. */
  readonly status: number | undefined
  constructor(body: unknown, status?: number) {
    super(apiErrorMessage(body))
    this.name = 'ApiCallError'
    this.body = body
    this.status = status
  }
}

/**
 * Whether re-issuing the request could plausibly succeed. A 4xx is the
 * daemon's considered answer — retrying a 404 for a pool that does not
 * exist only delays the error, and retrying a 401 fires a second request
 * after the session has already been marked anonymous. Server faults and
 * requests that never got a response (the peer proxy dropping during a
 * route failover) are the transient cases worth a second attempt.
 */
export function isRetryable(error: unknown): boolean {
  if (!(error instanceof ApiCallError)) return true
  if (error.status == null) return true
  return error.status >= 500
}
