import { readonly, ref } from 'vue'

type AuthStatus = 'checking' | 'authenticated' | 'anonymous'

const status = ref<AuthStatus>('checking')
const error = ref<string | null>(null)

async function responseMessage(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { message?: unknown }
    if (typeof body.message === 'string' && body.message) return body.message
  } catch {
    // A status line is enough when the response is not JSON.
  }
  return `Request failed (${response.status})`
}

export async function checkSession() {
  try {
    const response = await fetch('/api/v1/auth/session', { cache: 'no-store' })
    status.value = response.ok ? 'authenticated' : 'anonymous'
    error.value = null
  } catch (e) {
    status.value = 'anonymous'
    error.value = e instanceof Error ? e.message : String(e)
  }
}

export async function login(token: string): Promise<boolean> {
  error.value = null
  try {
    const response = await fetch('/api/v1/auth/login', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ token }),
    })
    if (!response.ok) {
      status.value = 'anonymous'
      error.value = await responseMessage(response)
      return false
    }
    status.value = 'authenticated'
    return true
  } catch (e) {
    status.value = 'anonymous'
    error.value = e instanceof Error ? e.message : String(e)
    return false
  }
}

export async function logout() {
  try {
    await fetch('/api/v1/auth/logout', { method: 'POST' })
  } finally {
    status.value = 'anonymous'
  }
}

export function markUnauthenticated() {
  status.value = 'anonymous'
}

export function useAuth() {
  return {
    status: readonly(status),
    error: readonly(error),
    checkSession,
    login,
    logout,
  }
}
