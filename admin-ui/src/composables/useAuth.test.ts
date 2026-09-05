import { afterEach, describe, expect, it, vi } from 'vite-plus/test'
import { checkSession, login, useAuth } from './useAuth'

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useAuth', () => {
  it('exchanges the administrator token for a session', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(login('admin-token')).resolves.toBe(true)
    expect(useAuth().status.value).toBe('authenticated')
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/auth/login', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ token: 'admin-token' }),
    })
  })

  it('surfaces a rejected login without authenticating', async () => {
    const response = new Response(JSON.stringify({ message: 'administrator login required' }), {
      status: 401,
      headers: { 'content-type': 'application/json' },
    })
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response))

    await expect(login('wrong')).resolves.toBe(false)
    expect(useAuth().status.value).toBe('anonymous')
    expect(useAuth().error.value).toBe('administrator login required')
  })

  it('restores an existing browser session', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)

    await checkSession()
    expect(useAuth().status.value).toBe('authenticated')
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/auth/session', { cache: 'no-store' })
  })
})

describe('logout', () => {
  it('keeps the session visible when logout is rejected', async () => {
    const { logout } = await import('./useAuth')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })))
    await checkSession()
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 503 })))
    expect(await logout()).toBe(false)
    expect(useAuth().status.value).toBe('authenticated')
    expect(useAuth().error.value).toContain('503')
  })

  it('reports a network failure and permits another logout attempt', async () => {
    const { logout } = await import('./useAuth')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })))
    await checkSession()
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('Network unavailable')))
    expect(await logout()).toBe(false)
    expect(useAuth().status.value).toBe('authenticated')
    expect(useAuth().error.value).toBe('Network unavailable')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })))
    expect(await logout()).toBe(true)
    expect(useAuth().status.value).toBe('anonymous')
    expect(useAuth().error.value).toBeNull()
  })
})
