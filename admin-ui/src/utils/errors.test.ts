import { describe, expect, it } from 'vite-plus/test'
import { ApiCallError, apiErrorCode, apiErrorMessage, isRetryable, unwrap } from './errors'

describe('unwrap', () => {
  it('returns data and keeps the status off the happy path', () => {
    expect(unwrap({ data: [1, 2] })).toEqual([1, 2])
  })

  it('throws with the daemon body and the HTTP status attached', () => {
    const call = () =>
      unwrap({
        error: { error: 'snapshot_held', message: 'held by 2 tags' },
        response: new Response(null, { status: 409 }),
      })
    expect(call).toThrow('snapshot_held: held by 2 tags')
    try {
      call()
    } catch (e) {
      expect((e as ApiCallError).status).toBe(409)
      expect(apiErrorCode(e)).toBe('snapshot_held')
    }
  })
})

describe('isRetryable', () => {
  // A 4xx is the daemon's considered answer; retrying only delays the
  // error, and a retried 401 fires after the session is already anonymous.
  it('does not retry client errors', () => {
    for (const status of [400, 401, 404, 409]) {
      expect(isRetryable(new ApiCallError({}, status))).toBe(false)
    }
  })

  it('retries server faults', () => {
    expect(isRetryable(new ApiCallError({}, 500))).toBe(true)
    expect(isRetryable(new ApiCallError({}, 502))).toBe(true)
  })

  // The peer proxy can drop mid route-failover without ever responding.
  it('retries when no response arrived at all', () => {
    expect(isRetryable(new ApiCallError({}, undefined))).toBe(true)
    expect(isRetryable(new TypeError('Failed to fetch'))).toBe(true)
  })
})

describe('apiErrorMessage', () => {
  it('joins the daemon code and message', () => {
    expect(apiErrorMessage({ error: 'bad_peer', message: 'not a target' })).toBe(
      'bad_peer: not a target',
    )
  })

  it('falls back to the message alone when there is no code', () => {
    expect(apiErrorMessage({ message: 'boom' })).toBe('boom')
  })

  it('handles plain errors and anything else', () => {
    expect(apiErrorMessage(new Error('network down'))).toBe('network down')
    expect(apiErrorMessage('nope')).toBe('nope')
  })
})
