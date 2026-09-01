import { Channel } from './channel.js'
import { ClientSocketSession } from './socket_session.js'
import { reportClientError } from './callback.js'
import { BindableChannelPattern } from '../bindable_channel_pattern.js'
import type {
  SocketOptions,
  ConnectionState,
  EventHandler,
  SocketClientTransport,
  ChannelContractFor,
  ChannelContract,
  ChannelCallArgs,
} from './types.js'

export const resolveChannel = Symbol('resolveChannel')

/**
 * Main Socket client.
 */
type IsUntypedRegistry<Registry> = unknown extends Registry
  ? true
  : [Registry] extends [undefined]
    ? true
    : false

type SocketChannelArgs<Registry> =
  IsUntypedRegistry<Registry> extends true ? [name: string] : ChannelCallArgs<Registry>

type ChannelResult<Registry, Pattern extends string> =
  IsUntypedRegistry<Registry> extends true
    ? Channel<ChannelContract>
    : Channel<ChannelContractFor<Registry, Pattern>>

type RuntimeChannelParameters = object

/** Constructs a concrete channel name from a supported generated pattern. @internal */
export function buildChannelName(
  pattern: string,
  parameters: RuntimeChannelParameters = {}
): string {
  const bindablePattern = BindableChannelPattern.parse(pattern)
  if (!bindablePattern) {
    throw new Error(`Unsupported generated channel pattern: ${pattern}`)
  }
  return bindablePattern.bind(parameters)
}

export class Socket<Registry = undefined> implements SocketClientTransport {
  #channels = new Map<string, Channel<any>>()
  #url: string
  #path: string
  #session: ClientSocketSession

  constructor(options: SocketOptions = {}) {
    const browserWindow = globalThis as typeof globalThis & {
      window?: { location?: { origin?: string } }
    }
    const browserOrigin =
      typeof globalThis === 'object' && browserWindow.window?.location?.origin
        ? browserWindow.window.location.origin
        : ''

    this.#url = options.url ?? browserOrigin
    this.#path = options.path ?? '/socket'
    this.#session = new ClientSocketSession({
      buildUrl: () => this.#buildUrl(),
      autoReconnect: options.autoReconnect,
      reconnectDelay: options.reconnectDelay,
      reconnectMaxDelay: options.reconnectMaxDelay,
      shouldReconnect: options.shouldReconnect,
      onConnected: () => {
        void this.#resubscribeChannels()
      },
    })
  }

  get state(): ConnectionState {
    return this.#session.state
  }

  get connected(): boolean {
    return this.#session.connected
  }

  connect(): Promise<void> {
    return this.#session.connect()
  }

  disconnect(): void {
    for (const channel of this.#channels.values()) {
      channel.unsubscribe().catch(() => {})
    }

    this.#session.disconnect()
  }

  channel<const Args extends SocketChannelArgs<Registry>>(
    ...args: Args
  ): ChannelResult<Registry, Args[0]> {
    const [pattern, parameters] = args as [string, object?]
    const name = args.length > 1 ? buildChannelName(pattern, parameters) : pattern
    return this[resolveChannel](name) as ChannelResult<Registry, Args[0]>
  }

  /** Resolves a channel for framework adapters after their public type checks. @internal */
  [resolveChannel](name: string): Channel<any> {
    if (!this.#channels.has(name)) {
      this.#channels.set(name, new Channel(name, this))
    }

    return this.#channels.get(name)!
  }

  async leave(name: string): Promise<void> {
    const channel = this.#channels.get(name)
    if (channel) {
      await channel.unsubscribe()
      if (this.#channels.get(name) === channel && !channel.subscribed) {
        this.#channels.delete(name)
      }
    }
  }

  onStateChange(handler: EventHandler<ConnectionState>): () => void {
    return this.#session.onStateChange(handler)
  }

  on<T = unknown>(event: string, handler: EventHandler<T>): () => void {
    return this.#session.on(event, handler)
  }

  off<T = unknown>(event: string, handler: EventHandler<T>): void {
    this.#session.off(event, handler)
  }

  send(message: Record<string, unknown>): void {
    this.#session.send(message)
  }

  sendRequest<T = unknown>(message: Record<string, unknown>, timeout = 5000): Promise<T> {
    return this.#session.sendRequest(message, timeout) as Promise<T>
  }

  #buildUrl(): string {
    const browserGlobal = globalThis as typeof globalThis & {
      location?: { origin?: string }
    }
    const url = new URL(this.#url || browserGlobal.location?.origin || 'http://localhost')
    if (url.protocol === 'http:') {
      url.protocol = 'ws:'
    } else if (url.protocol === 'https:') {
      url.protocol = 'wss:'
    } else if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
      throw new Error(`Unsupported socket URL protocol: ${url.protocol}`)
    }
    url.pathname = this.#path
    url.hash = ''

    return url.toString()
  }

  async #resubscribeChannels(): Promise<void> {
    for (const channel of this.#channels.values()) {
      if (channel.subscribed && !channel.active) {
        await channel.$resubscribe().catch(reportClientError)
      }
    }
  }
}
