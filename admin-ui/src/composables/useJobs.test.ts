import { describe, expect, it } from 'vite-plus/test'
import { jobsStreamPath } from './useJobs'

describe('jobsStreamPath', () => {
  it('uses the local live stream for the local console', () => {
    expect(jobsStreamPath('')).toBe('/api/v1/jobs/stream')
  })

  it('uses the dedicated peer live stream instead of the buffered proxy', () => {
    expect(jobsStreamPath('/api/v1/peers/mira/proxy')).toBe('/api/v1/peers/mira/jobs/stream')
    expect(jobsStreamPath('/api/v1/peers/mira%20backup/proxy')).toBe(
      '/api/v1/peers/mira%20backup/jobs/stream',
    )
  })
})
