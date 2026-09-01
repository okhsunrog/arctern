<script setup lang="ts">
import { computed } from 'vue'
import type { JobStatus, TargetStatus } from '../client'
import { isTransferring, sendControl } from '../utils/actions'
import { formatAge, formatDuration } from '../utils/format'
import TransferSlot from './TransferSlot.vue'

const props = defineProps<{
  job: JobStatus
  onPushTo?: (name: string, peer: string) => void
  isPushing?: (name: string, peer: string) => boolean
}>()

function friendlyFailure(message?: string | null): string {
  if (!message) return 'No details were reported'
  if (/broken pipe/i.test(message)) return 'Receiver closed the connection before completion'
  if (/not connected/i.test(message)) return 'Peer is not connected'
  return message.replace(/^execute\s+[^:]+:\s*/i, '')
}

function targetOutcome(tg: TargetStatus): string | null {
  return tg.last_outcome ?? (tg.last_error ? 'error' : null)
}

function targetTone(tg: TargetStatus): string {
  if (isTransferring(props.job, tg) && targetOutcome(tg) === 'error') return 'text-info'
  if (targetOutcome(tg) === 'error') return 'text-error'
  return 'text-muted'
}

interface Badge {
  label: string
  color: 'info' | 'neutral' | 'success'
  title?: string
}

/**
 * What will happen to this target, in one word. Precedence matters: a
 * live transfer and a queued request are facts about right now, while
 * the mode/route pair only describes the schedule. Showing "auto paused"
 * next to a running progress bar — which is what this card used to do —
 * is technically true and completely unreadable.
 */
function modeBadge(tg: TargetStatus): Badge {
  if (isTransferring(props.job, tg)) {
    return { label: 'sending', color: 'success', title: 'Replicating to this peer right now.' }
  }
  if (tg.manual_queued) {
    return {
      label: 'queued',
      color: 'info',
      title: 'A manual push is queued and starts on the next cycle.',
    }
  }
  if (tg.mode !== 'auto') return { label: 'manual', color: 'neutral' }
  if (tg.connected && !tg.route_auto) {
    return {
      label: 'auto paused',
      color: 'neutral',
      title: 'Scheduled sync is suspended while the active route is manual-only.',
    }
  }
  return { label: 'auto', color: 'info' }
}

/** One human line per target: last sync + when the next one is due. */
function targetLine(tg: TargetStatus): string {
  if (targetOutcome(tg) === 'error') {
    const previous = `previous attempt failed ${formatAge(tg.last_attempt)}`
    const details = friendlyFailure(tg.last_message ?? tg.last_error)
    return isTransferring(props.job, tg)
      ? `Retrying now · ${previous}: ${details}`
      : `Failed ${formatAge(tg.last_attempt)} · ${details}`
  }
  if (targetOutcome(tg) === 'cancelled') {
    const previous = tg.last_success ? ` · last sync ${formatAge(tg.last_success)}` : ''
    return `Cancelled by operator ${formatAge(tg.last_attempt)}${previous}`
  }
  const synced = tg.last_success ? `synced ${formatAge(tg.last_success)}` : 'never synced'
  if (tg.manual_queued) return `${synced} · manual push queued`
  if (tg.mode !== 'auto') return synced
  if (tg.connected && !tg.route_auto) return `${synced} · route is manual-only`
  if (!tg.auto_interval_secs || !tg.last_success) return `${synced} · auto: every cycle`
  const due = tg.last_success + tg.auto_interval_secs - Math.floor(Date.now() / 1000)
  return due <= 0 ? `${synced} · auto: due now` : `${synced} · next auto in ~${formatDuration(due)}`
}

/** Detail line worth repeating per target, beyond the card's summary. */
function showDetail(tg: TargetStatus): boolean {
  return (
    (props.job.targets?.length ?? 0) > 1 ||
    (targetOutcome(tg) != null && targetOutcome(tg) !== 'ok') ||
    tg.manual_queued === true ||
    (tg.connected && !tg.route_auto)
  )
}

// Decided once per target per render. The template used to call
// modeBadge() three times and the send predicate twice for every row.
const rows = computed(() =>
  (props.job.targets ?? []).map((tg) => ({
    tg,
    badge: modeBadge(tg),
    send: sendControl(props.job, tg),
    line: targetLine(tg),
    detail: showDetail(tg),
  })),
)
</script>

<template>
  <div class="space-y-3">
    <!-- In-flight transfers, one block per parallel send slot -->
    <div v-if="job.transfers?.length" class="space-y-3">
      <TransferSlot
        v-for="t in job.transfers"
        :key="`${t.peer}:${t.dataset}`"
        :transfer="t"
        :show-peer="(job.targets?.length ?? 0) > 1"
      />
    </div>

    <!-- Per-target policy + manual trigger -->
    <div v-if="job.targets?.length" class="space-y-2">
      <div v-for="{ tg, badge, send, line, detail } in rows" :key="tg.peer" class="space-y-0.5">
        <div class="flex items-center gap-2 text-sm min-w-0">
          <span
            class="inline-block w-2 h-2 rounded-full shrink-0"
            :class="tg.connected ? 'bg-success pulse-dot' : 'bg-neutral-400 dark:bg-neutral-600'"
            :title="tg.connected ? 'reachable' : 'unreachable'"
          />
          <span class="font-medium font-mono truncate">{{ tg.peer }}</span>
          <UBadge
            v-if="tg.route"
            variant="outline"
            size="sm"
            color="neutral"
            class="shrink-0 whitespace-nowrap"
          >
            via {{ tg.route }}
          </UBadge>
          <UBadge
            variant="subtle"
            size="sm"
            :color="badge.color"
            :title="badge.title"
            class="shrink-0"
          >
            {{ badge.label }}
          </UBadge>
          <!-- Offered only when pressing it would do something. While the
               push is running or queued the badge above already says so,
               and a second control saying "send now" beside a live
               progress bar just contradicts it. -->
          <UTooltip v-if="send.kind !== 'hidden'" :text="send.tooltip" class="shrink-0 ms-auto">
            <UButton
              size="xs"
              variant="soft"
              icon="i-lucide-send"
              :loading="isPushing?.(job.name, tg.peer)"
              :disabled="send.kind === 'disabled' || isPushing?.(job.name, tg.peer)"
              @click="onPushTo?.(job.name, tg.peer)"
            >
              Send now
            </UButton>
          </UTooltip>
        </div>
        <!-- For single-target jobs the card-level Last/Next sync rows
             already say this; repeat per-target only when there is more
             than one target or something needs explaining. -->
        <div v-if="detail" class="text-xs ml-4 truncate" :class="targetTone(tg)" :title="line">
          {{ line }}
        </div>
      </div>
    </div>
  </div>
</template>
