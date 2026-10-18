import {
  createContext,
  createElement,
  useContext,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from 'react'
import type { Channel } from './channel.js'
import { buildChannelName, type Socket } from './socket.js'
import type { ConnectionState, EventHandler } from './types.js'
import {
  acquireChannel,
  acquireSocketOwnership,
  createChannelCallArgs,
  resolveChannelEventArgs,
  type FrameworkChannel,
  type FrameworkChannelCallArgs,
  type FrameworkChannelEvent,
  type FrameworkChannelEventPayload,
  type FrameworkChannelParameters,
  type FrameworkChannelPattern,
  type FrameworkParameterizedChannelPattern,
  type FrameworkStaticChannelPattern,
} from './framework.js'

export interface SocketProviderProps<Registry> {
  socket: Socket<Registry>
  children?: ReactNode
  /** Connect on mount and disconnect on unmount. Use only for an exclusively owned socket. */
  owned?: boolean
}

export function createSocketHooks<Registry = never>() {
  const SocketContext = createContext<Socket<Registry> | null>(null)

  function useSocket(): Socket<Registry> {
    const socket = useContext(SocketContext)
    if (!socket) throw new Error('SocketProvider is missing')
    return socket
  }

  function SocketProvider({ socket, children, owned = false }: SocketProviderProps<Registry>) {
    useEffect(() => {
      if (!owned) return
      return acquireSocketOwnership(socket)
    }, [owned, socket])

    return createElement(SocketContext.Provider, { value: socket }, children)
  }

  function useSocketState(): ConnectionState {
    const socket = useSocket()
    return useSyncExternalStore(
      (notify) => socket.onStateChange(notify),
      () => socket.state,
      () => 'disconnected'
    )
  }

  function useChannel<const Args extends FrameworkChannelCallArgs<Registry>>(
    ...args: Args
  ): FrameworkChannel<Registry, Args[0]> | null
  function useChannel(pattern: null): null
  function useChannel(
    ...args: [pattern: FrameworkChannelPattern<Registry> | null, parameters?: object]
  ): Channel<any> | null {
    const [pattern, parameters] = args
    const socket = useSocket()
    const name = pattern === null ? null : buildChannelName(pattern, parameters)
    const [acquired, setAcquired] = useState<{
      socket: Socket<Registry>
      name: string
      channel: Channel<any>
    } | null>(null)

    useEffect(() => {
      if (pattern === null || name === null) {
        setAcquired(null)
        return
      }
      const channelArgs = createChannelCallArgs<Registry>(pattern, parameters)
      const { channel, lease } = acquireChannel(socket, ...channelArgs)
      setAcquired({ socket, name, channel })
      return () => lease.release()
    }, [name, pattern, socket])

    return acquired?.socket === socket && acquired.name === name ? acquired.channel : null
  }

  // `event` is a direct parameter with a `const` type so its literal, and the payload, are inferred
  function useChannelEvent<
    const Pattern extends FrameworkStaticChannelPattern<Registry>,
    const Event extends FrameworkChannelEvent<Registry, Pattern>,
  >(
    pattern: Pattern | null,
    event: Event,
    handler: EventHandler<FrameworkChannelEventPayload<Registry, Pattern, Event>>
  ): void
  function useChannelEvent<
    const Pattern extends FrameworkParameterizedChannelPattern<Registry>,
    const Event extends FrameworkChannelEvent<Registry, Pattern>,
  >(
    pattern: Pattern | null,
    parameters: FrameworkChannelParameters<Registry, Pattern>,
    event: Event,
    handler: EventHandler<FrameworkChannelEventPayload<Registry, Pattern, Event>>
  ): void
  function useChannelEvent(
    pattern: FrameworkChannelPattern<Registry> | null,
    ...args: unknown[]
  ): void {
    const socket = useSocket()
    const {
      parameterInput: parameters,
      event,
      handler,
    } = resolveChannelEventArgs<object, string, unknown>(args)
    const name = pattern === null ? null : buildChannelName(pattern, parameters)
    const handlerRef = useRef(handler)
    handlerRef.current = handler

    useEffect(() => {
      if (pattern === null || name === null) return
      const channelArgs = createChannelCallArgs<Registry>(pattern, parameters)
      const { lease } = acquireChannel(socket, ...channelArgs)
      lease.listen(event, (data) => handlerRef.current(data))
      return () => lease.release()
    }, [event, name, pattern, socket])
  }

  return { SocketProvider, useSocket, useSocketState, useChannel, useChannelEvent }
}
