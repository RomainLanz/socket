import {
  computed,
  inject,
  onMounted,
  onScopeDispose,
  provide,
  shallowRef,
  toValue,
  watch,
  type ComputedRef,
  type InjectionKey,
  type MaybeRefOrGetter,
  type ShallowRef,
} from 'vue'
import type { Channel } from './channel.js'
import { buildChannelName, type Socket } from './socket.js'
import type { ConnectionState, EventHandler } from './types.js'
import {
  acquireChannel,
  acquireSocketOwnership,
  createChannelCallArgs,
  resolveChannelEventArgs,
  type FrameworkChannel,
  type FrameworkChannelEvent,
  type FrameworkChannelEventPayload,
  type FrameworkChannelParameters,
  type FrameworkChannelPattern,
} from './framework.js'

interface VueSocketContext<Registry> {
  socket: Socket<Registry>
  state: ShallowRef<ConnectionState>
}

type ChannelParameterInput<
  Registry,
  Pattern extends FrameworkChannelPattern<Registry>,
> = MaybeRefOrGetter<FrameworkChannelParameters<Registry, Pattern>>

type VueChannelCallArgs<Registry> = {
  [Pattern in FrameworkChannelPattern<Registry>]: FrameworkChannelParameters<
    Registry,
    Pattern
  > extends undefined
    ? [pattern: Pattern]
    : [pattern: Pattern, parameters: ChannelParameterInput<Registry, Pattern>]
}[FrameworkChannelPattern<Registry>]

type VueChannelEventArgs<
  Registry,
  Pattern extends FrameworkChannelPattern<Registry>,
  Event extends FrameworkChannelEvent<Registry, Pattern>,
> =
  FrameworkChannelParameters<Registry, Pattern> extends undefined
    ? [event: Event, handler: EventHandler<FrameworkChannelEventPayload<Registry, Pattern, Event>>]
    : [
        parameters: ChannelParameterInput<Registry, Pattern>,
        event: Event,
        handler: EventHandler<FrameworkChannelEventPayload<Registry, Pattern, Event>>,
      ]

export function createSocketComposables<Registry = never>() {
  const key = Symbol('socket') as InjectionKey<VueSocketContext<Registry>>

  function provideSocket(socket: Socket<Registry>, options: { owned?: boolean } = {}): void {
    const state = shallowRef(socket.state)
    const stopStateListener = socket.onStateChange((nextState) => {
      state.value = nextState
    })
    provide(key, { socket, state })

    let releaseOwnership: (() => void) | undefined
    if (options.owned) onMounted(() => (releaseOwnership = acquireSocketOwnership(socket)))
    onScopeDispose(() => {
      stopStateListener()
      releaseOwnership?.()
    })
  }

  function useContext(): VueSocketContext<Registry> {
    const context = inject(key)
    if (!context) throw new Error('provideSocket must be called by an ancestor')
    return context
  }

  function useSocket(): Socket<Registry> {
    return useContext().socket
  }

  function useSocketState(): {
    state: Readonly<ShallowRef<ConnectionState>>
    connected: ComputedRef<boolean>
  } {
    const state = useContext().state
    return { state, connected: computed(() => state.value === 'connected') }
  }

  function useChannel<const Args extends VueChannelCallArgs<Registry>>(
    ...args: Args
  ): Readonly<ShallowRef<FrameworkChannel<Registry, Args[0]> | null>>
  function useChannel(pattern: null): Readonly<ShallowRef<null>>
  function useChannel(
    ...args: [
      pattern: FrameworkChannelPattern<Registry> | null,
      parameterInput?: MaybeRefOrGetter<object | undefined>,
    ]
  ): Readonly<ShallowRef<Channel<any> | null>> {
    const [pattern, parameterInput] = args
    const socket = useSocket()
    const channel = shallowRef(null) as ShallowRef<Channel<any> | null>

    watch(
      () => (pattern === null ? null : toValue(parameterInput)),
      (parameters, _previousParameters, onCleanup) => {
        const nextName =
          pattern === null ? null : buildChannelName(pattern, parameters as object | undefined)
        if (nextName === null || pattern === null) {
          channel.value = null
          return
        }
        const channelArgs = createChannelCallArgs<Registry>(
          pattern,
          parameters as object | undefined
        )
        const acquired = acquireChannel(socket, ...channelArgs)
        channel.value = acquired.channel
        onCleanup(() => acquired.lease.release())
      },
      { immediate: true }
    )

    return channel
  }

  function useChannelEvent<
    const Pattern extends FrameworkChannelPattern<Registry>,
    Event extends FrameworkChannelEvent<Registry, Pattern>,
  >(pattern: Pattern | null, ...args: VueChannelEventArgs<Registry, Pattern, Event>): void {
    const socket = useSocket()
    const { parameterInput, event, handler } = resolveChannelEventArgs<
      ChannelParameterInput<Registry, Pattern>,
      Event,
      FrameworkChannelEventPayload<Registry, Pattern, Event>
    >(args)
    watch(
      () => (pattern === null ? null : toValue(parameterInput)),
      (parameters, _previousParameters, onCleanup) => {
        const nextName =
          pattern === null ? null : buildChannelName(pattern, parameters as object | undefined)
        if (nextName === null || pattern === null) return
        const channelArgs = createChannelCallArgs<Registry>(
          pattern,
          parameters as object | undefined
        )
        const { lease } = acquireChannel(socket, ...channelArgs)
        lease.listen(event, handler)
        onCleanup(() => lease.release())
      },
      { immediate: true }
    )
  }

  return { provideSocket, useSocket, useSocketState, useChannel, useChannelEvent }
}
