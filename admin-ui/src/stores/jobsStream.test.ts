import { describe, expect, it } from 'vite-plus/test'
import { jobsStreamPath } from './jobsStream'

describe('jobsStreamPath', () => {
  it('uses the local live stream for the local console', () => {
    expect(jobsStreamPath('')).toBe('/api/v1/jobs/stream')
  })

  it('uses the dedicated peer live stream instead of the buffered proxy', () => {
    expect(jobsStreamPath('mira')).toBe('/api/v1/peers/mira/jobs/stream')
  })

  it('encodes peer names that are not URL-safe', () => {
    expect(jobsStreamPath('mira backup')).toBe('/api/v1/peers/mira%20backup/jobs/stream')
  })
})
