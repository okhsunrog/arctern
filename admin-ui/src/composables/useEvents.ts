import {
  computed,
  ref,
  shallowRef,
  onScopeDispose,
  toValue,
  watch,
  type MaybeRefOrGetter,
} from 'vue'
import type { LogEvent } from '../client'
import { useEventsStream } from '../stores/eventsStream'

/**
 * The host's event tail. Dashboard and the Events view share one
 * connection per scope; each picks how much of the tail it renders.
 */
export function useEvents(scope: MaybeRefOrGetter<string> = '') {
  const store = useEventsStream()
  const paused = ref(false)
  const frozen = shallowRef<LogEvent[]>([])

  // Subscribing mutates the store, so it lives in a watcher rather than
  // in a computed — a getter must not have side effects on the state it
  // is being tracked against.
  let release: (() => void) | null = null
  watch(
    () => toValue(scope),
    (next) => {
      paused.value = false
      frozen.value = []
      release?.()
      release = store.subscribe(next)
    },
    { immediate: true },
  )
  onScopeDispose(() => release?.())

  const events = computed<LogEvent[]>(() =>
    paused.value ? frozen.value : (store.buffers[toValue(scope)] ?? []),
  )
  function togglePause() {
    frozen.value = paused.value ? [] : [...events.value]
    paused.value = !paused.value
  }

  return {
    events,
    connected: computed(() => store.connected[toValue(scope)] === true),
    error: computed(() =>
      store.connected[toValue(scope)] === false
        ? 'Live event updates interrupted. Reconnecting…'
        : null,
    ),
    paused: computed(() => paused.value),
    clear: () => {
      store.clear(toValue(scope))
      frozen.value = []
    },
    togglePause,
  }
}
