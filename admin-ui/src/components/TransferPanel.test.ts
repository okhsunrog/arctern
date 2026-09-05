// @vitest-environment happy-dom
import { describe, expect, it } from 'vite-plus/test'
import { mount } from '@vue/test-utils'
import type { PushJobStatus, TargetStatus, TransferInfo } from '../client'
import type { PushJob } from '../utils/jobs'
import TransferPanel from './TransferPanel.vue'

// Nuxt UI components are stubbed: what matters here is which controls the
// panel decides to render, not how Nuxt UI paints them.
const global = {
  stubs: {
    Badge: { template: '<span class="badge"><slot /></span>' },
    Tooltip: { props: ['text'], template: '<span class="tip" :data-tip="text"><slot /></span>' },
    Button: {
      props: ['disabled', 'loading', 'icon'],
      template: '<button class="btn" :disabled="disabled"><slot /></button>',
    },
    Progress: true,
    Icon: true,
    TransferSlot: true,
  },
}

function target(o: Partial<TargetStatus> = {}): TargetStatus {
  return {
    peer: 'mira',
    mode: 'auto',
    connected: true,
    route: 'lan',
    route_auto: true,
    manual_queued: false,
    auto_interval_secs: 3600,
    last_success: Math.floor(Date.now() / 1000) - 10,
    ...o,
  }
}

function transfer(peer = 'mira'): TransferInfo {
  return {
    dataset: 'novafs/arch0/data/home_new',
    peer,
    kind: 'incremental',
    bytes_sent: 26_500_000,
    total_bytes: 2_400_000_000,
    started_at: Math.floor(Date.now() / 1000) - 120,
    phase: 'sending',
    phase_since: Math.floor(Date.now() / 1000) - 120,
  }
}

function job(o: Partial<PushJobStatus> = {}): PushJob {
  return {
    name: 'push_to_mira',
    kind: 'push',
    last_run: null,
    next_run: null,
    last_error: null,
    running: false,
    paused: false,
    cancellable: false,
    transfers: [],
    targets: [target()],
    ...o,
  }
}

function sendButtons(wrapper: ReturnType<typeof mount>) {
  return wrapper.findAll('button.btn').filter((b) => /send now/i.test(b.text()))
}

describe('TransferPanel send control', () => {
  it('offers the button when the peer is idle and reachable', () => {
    const w = mount(TransferPanel, { props: { job: job() }, global })
    expect(sendButtons(w)).toHaveLength(1)
  })

  // The complaint that started all of this: a "Send now" call to action
  // sitting next to a live progress bar, where pressing it does nothing
  // to the transfer already running.
  it('does not render the button at all while replicating to that peer', () => {
    const w = mount(TransferPanel, {
      props: { job: job({ running: true, transfers: [transfer()] }) },
      global,
    })
    expect(w.findAll('button.btn')).toHaveLength(0)
    expect(w.text()).toContain('sending')
  })

  it('does not render the button once a push is already queued', () => {
    const w = mount(TransferPanel, {
      props: { job: job({ targets: [target({ manual_queued: true })] }) },
      global,
    })
    expect(w.findAll('button.btn')).toHaveLength(0)
    expect(w.text()).toContain('queued')
  })

  // A transfer to a DIFFERENT peer says nothing about this one.
  it('keeps the button for a peer that is not the one receiving', () => {
    const w = mount(TransferPanel, {
      props: {
        job: job({
          running: true,
          transfers: [transfer('elsewhere')],
          targets: [target(), target({ peer: 'elsewhere' })],
        }),
      },
      global,
    })
    const buttons = sendButtons(w)
    expect(buttons).toHaveLength(1)
    expect(buttons[0]!.attributes('disabled')).toBeUndefined()
  })

  // Unreachable is an external, transient condition rather than "already
  // doing it", so the affordance stays visible but inert and explains why.
  it('keeps the button disabled and explained when the peer is unreachable', () => {
    const w = mount(TransferPanel, {
      props: { job: job({ targets: [target({ connected: false })] }) },
      global,
    })
    const buttons = sendButtons(w)
    expect(buttons).toHaveLength(1)
    expect(buttons[0]!.attributes('disabled')).toBeDefined()
    expect(w.find('.tip').attributes('data-tip')).toMatch(/unreachable/i)
  })
})
