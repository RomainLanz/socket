import { randomUUID } from 'node:crypto'
import { Bus } from '@boringnode/bus'
import type { Serializable, TransportFactory } from '@boringnode/bus/types/main'
import type { PresenceSocket } from './presence_manager.js'
import { PresenceSocketFrame, type SerializablePresenceSocket } from './presence_socket_frame.js'
import { parseDuration, type Duration } from './duration.js'

export type { SerializablePresenceSocket } from './presence_socket_frame.js'

const DEFAULT_CHANNEL = 'socket::broadcast'
const DEFAULT_PRESENCE_TIMEOUT = 100
export const DEFAULT_RETRY_QUEUE_MAX_SIZE = 1000

type ChannelEventMessage = {
  [key: string]: Serializable
  type: 'channel:event'
  origin: string
  channel: string
  event: string
  data: Serializable
  except: string[]
}

type BroadcastEventMessage = {
  [key: string]: Serializable
  type: 'broadcast:event'
  origin: string
  event: string
  data: Serializable
}

type PresenceSocketsRequestMessage = {
  [key: string]: Serializable
  type: 'presence:sockets:request'
  origin: string
  requestId: string
  channel: string
}

type PresenceSocketsResponseMessage = {
  [key: string]: Serializable
  type: 'presence:sockets:response'
  origin: string
  target: string
  requestId: string
  sockets: SerializablePresenceSocket[]
}

export type SocketBusMessage =
  | ChannelEventMessage
  | BroadcastEventMessage
  | PresenceSocketsRequestMessage
  | PresenceSocketsResponseMessage

export interface SocketTransportConfig {
  driver: TransportFactory
  channel?: string
  /**
   * How long to wait for other instances to answer a presence snapshot request.
   */
  presenceTimeout?: Duration
  /**
   * In-memory retry policy for transport publications that fail.
   *
   * The queue is enabled and limited to 1000 distinct messages by default.
   * Set maxSize to null only when an unbounded queue is explicitly desired.
   */
  retryQueue?: SocketRetryQueueConfig
}

export interface SocketRetryQueueConfig {
  enabled?: boolean
  removeDuplicates?: boolean
  maxSize?: number | null
  retryInterval?: Duration | false
}

function resolveRetryQueueConfig(config: SocketRetryQueueConfig | undefined) {
  const maxSize = config?.maxSize === undefined ? DEFAULT_RETRY_QUEUE_MAX_SIZE : config.maxSize

  if (maxSize !== null && (!Number.isSafeInteger(maxSize) || maxSize <= 0)) {
    throw new Error('transport.retryQueue.maxSize must be a positive integer or null')
  }

  return {
    enabled: true,
    maxSize,
    ...config,
  }
}

interface SocketBusHandlers {
  channel(message: Extract<SocketBusMessage, { type: 'channel:event' }>): void
  broadcast(message: Extract<SocketBusMessage, { type: 'broadcast:event' }>): void
  presenceSockets(channel: string): SerializablePresenceSocket[]
}

interface PendingPresenceRequest {
  sockets: PresenceSocketFrame[]
  resolve(sockets: PresenceSocket[]): void
  timeout: ReturnType<typeof setTimeout>
}

type ParsedPresenceSocketsResponseMessage = {
  type: 'presence:sockets:response'
  origin: string
  target: string
  requestId: string
  sockets: PresenceSocketFrame[]
}

type ParsedSocketBusMessage =
  | Exclude<SocketBusMessage, PresenceSocketsResponseMessage>
  | ParsedPresenceSocketsResponseMessage

function parseBusMessage(payload: unknown): ParsedSocketBusMessage | null {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null
  }

  const message = payload as Record<string, unknown>
  if (typeof message.origin !== 'string') {
    return null
  }

  switch (message.type) {
    case 'channel:event':
      return typeof message.channel === 'string' &&
        typeof message.event === 'string' &&
        Array.isArray(message.except) &&
        message.except.every((socketId) => typeof socketId === 'string')
        ? (message as ChannelEventMessage)
        : null
    case 'broadcast:event':
      return typeof message.event === 'string' ? (message as BroadcastEventMessage) : null
    case 'presence:sockets:request':
      return typeof message.requestId === 'string' && typeof message.channel === 'string'
        ? (message as PresenceSocketsRequestMessage)
        : null
    case 'presence:sockets:response':
      if (
        typeof message.target !== 'string' ||
        typeof message.requestId !== 'string' ||
        !Array.isArray(message.sockets)
      ) {
        return null
      }

      const sockets = Array.from(message.sockets, PresenceSocketFrame.fromTransport)
      return sockets.every((socket) => socket !== null)
        ? ({ ...message, sockets } as ParsedPresenceSocketsResponseMessage)
        : null
    default:
      return null
  }
}

export class SocketBus {
  #origin = randomUUID()
  #channel: string
  #bus: Bus
  #presenceTimeout: number
  #pendingPresenceRequests = new Map<string, PendingPresenceRequest>()

  constructor(
    transport: SocketTransportConfig,
    private handlers: SocketBusHandlers
  ) {
    this.#channel = transport.channel ?? DEFAULT_CHANNEL
    this.#presenceTimeout =
      parseDuration('transport.presenceTimeout', transport.presenceTimeout) ??
      DEFAULT_PRESENCE_TIMEOUT
    this.#bus = new Bus(transport.driver(), {
      retryQueue: resolveRetryQueueConfig(transport.retryQueue),
    })
  }

  async start(): Promise<void> {
    await this.#bus.subscribe(this.#channel, (payload) => {
      try {
        const message = parseBusMessage(payload)
        if (!message || message.origin === this.#origin) {
          return
        }

        switch (message.type) {
          case 'channel:event':
            this.handlers.channel(message)
            return
          case 'broadcast:event':
            this.handlers.broadcast(message)
            return
          case 'presence:sockets:request':
            void this.#respondToPresenceRequest(message).catch(() => {})
            return
          case 'presence:sockets:response':
            this.#handlePresenceResponse(message)
            return
        }
      } catch {
        return
      }
    })
  }

  publishChannel(channel: string, event: string, data: unknown, except?: string[]): void {
    const message: SocketBusMessage = {
      type: 'channel:event',
      origin: this.#origin,
      channel,
      event,
      data: data as Serializable,
      except: except ?? [],
    }

    void this.#bus.publish(this.#channel, message)
  }

  publishBroadcast(event: string, data: unknown): void {
    const message: BroadcastEventMessage = {
      type: 'broadcast:event',
      origin: this.#origin,
      event,
      data: data as Serializable,
    }
    void this.#bus.publish(this.#channel, message)
  }

  fetchPresenceSockets(channel: string): Promise<PresenceSocket[]> {
    const requestId = randomUUID()

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        const request = this.#pendingPresenceRequests.get(requestId)

        if (!request) {
          return
        }

        this.#pendingPresenceRequests.delete(requestId)
        resolve(request.sockets.map((socket) => socket.toSocket()))
      }, this.#presenceTimeout)

      this.#pendingPresenceRequests.set(requestId, {
        sockets: [],
        resolve,
        timeout,
      })

      const message: PresenceSocketsRequestMessage = {
        type: 'presence:sockets:request',
        origin: this.#origin,
        requestId,
        channel,
      }
      void this.#bus.publish(this.#channel, message)
    })
  }

  async close(): Promise<void> {
    for (const [requestId, request] of this.#pendingPresenceRequests) {
      clearTimeout(request.timeout)
      request.resolve(request.sockets.map((socket) => socket.toSocket()))
      this.#pendingPresenceRequests.delete(requestId)
    }

    await this.#bus.disconnect()
  }

  async #respondToPresenceRequest(
    message: Extract<SocketBusMessage, { type: 'presence:sockets:request' }>
  ): Promise<void> {
    const response: PresenceSocketsResponseMessage = {
      type: 'presence:sockets:response',
      origin: this.#origin,
      target: message.origin,
      requestId: message.requestId,
      sockets: this.handlers.presenceSockets(message.channel),
    }
    await this.#bus.publish(this.#channel, response as Serializable)
  }

  #handlePresenceResponse(message: ParsedPresenceSocketsResponseMessage): void {
    if (message.target !== this.#origin) {
      return
    }

    const request = this.#pendingPresenceRequests.get(message.requestId)

    if (!request) {
      return
    }

    request.sockets.push(...message.sockets)
  }
}
