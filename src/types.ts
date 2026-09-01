import type { WebSocket } from 'ws'
import type { IncomingMessage } from 'node:http'
import type { MiddlewareAsClass } from '@adonisjs/core/types/http'
import type { HttpContext } from '@adonisjs/core/http'
import type { PresenceData as ChannelPresenceData } from './presence_manager.js'
import type { SocketTransportConfig } from './socket_bus.js'
import type { Duration } from './duration.js'

export type { ChannelMessage, ChannelAck, ChannelServerEventsOf } from './shared_types.js'
export type { Duration } from './duration.js'
export type { SocketRetryQueueConfig, SocketTransportConfig } from './socket_bus.js'

export interface RawSocket {
  id: string
  data: Record<string, unknown>
  connection: WebSocket
  request: IncomingMessage
  httpContext: HttpContext
}

/**
 * Socket exposed to channels after the HTTP upgrade.
 */
export interface AuthenticatedSocket<User = unknown> {
  /**
   * Unique socket ID.
   */
  id: string

  /**
   * Authenticated user resolved during the HTTP upgrade.
   */
  user: User | undefined

  /**
   * Returns the authenticated user or throws a client-safe unauthorized error.
   */
  getUserOrFail(): User

  /**
   * Emits an event to this socket.
   */
  emit(event: string, data: unknown): void

  /**
   * Disconnects the socket.
   */
  disconnect(): void

  /**
   * Raw WebSocket connection metadata.
   */
  raw: RawSocket
}

/**
 * Short-lived context for preparing and authenticating a WebSocket upgrade.
 * Socket-lifetime mutable state belongs in AuthenticatedSocket.raw.data.
 */
export interface SocketUpgradeContext {
  httpContext: HttpContext
}

export type SocketAuthenticationResult<User = unknown> = User | false | null

export type SocketAuthenticationHandler<User = unknown> = (
  ctx: SocketUpgradeContext
) => SocketAuthenticationResult<User> | Promise<SocketAuthenticationResult<User>>

export type SocketOriginValues = boolean | string | string[]

export type SocketOriginResolver = (origin: string, httpContext: HttpContext) => SocketOriginValues

/**
 * AdonisJS HTTP middleware executed for the WebSocket upgrade request.
 */
export type SocketHttpMiddleware =
  | (() => Promise<{ default: MiddlewareAsClass }>)
  | MiddlewareAsClass

/**
 * Result of matching a channel.
 */
export interface ChannelMatch {
  channel: import('./base_channel.js').BaseChannelConstructor
  pattern: string
  params: Record<string, string>
}

/**
 * Channel options.
 */
export interface ChannelOptions {
  presence?: boolean
}

/**
 * Identity and channel-specific data returned for a presence member.
 */
export interface PresenceInfo {
  id: string
  data?: Record<string, unknown>
}

/**
 * Presence member exposed to snapshots and channel hooks.
 */
export interface PresenceMember {
  id: string
  joinedAt?: never
  [key: string]: unknown
}

/**
 * Context passed to middlewares.
 */
export interface MiddlewareContext<User = unknown> {
  socket: AuthenticatedSocket<User>
  channel: string
  params: Record<string, string>
  presenceData?: Record<string, unknown> | ChannelPresenceData
  setPresenceData(data: Record<string, unknown> | ChannelPresenceData): void
}

/**
 * Middleware function.
 */
export type MiddlewareFn<User = unknown> = (
  ctx: MiddlewareContext<User>,
  next: () => Promise<void>
) => Promise<void>

/**
 * Middleware class.
 */
export interface MiddlewareClass<User = unknown> {
  handle: MiddlewareFn<User>
}

/**
 * Constructor for a middleware resolved through the AdonisJS container.
 */
export interface MiddlewareConstructor<User = unknown> {
  new (...args: any[]): MiddlewareClass<User>
  readonly prototype: MiddlewareClass<User>
}

/**
 * Middleware, either a function, an instance, or a container-resolved class.
 */
export type Middleware<User = unknown> =
  | MiddlewareFn<User>
  | MiddlewareClass<User>
  | MiddlewareConstructor<User>

/**
 * Package configuration.
 */
export interface SocketConfig<User = unknown> {
  websocket?: {
    path?: string
    /**
     * Browser origin policy for opening a WebSocket connection.
     *
     * Uses the same value shapes as the AdonisJS CORS `origin` option. A resolver
     * receives the request origin and HTTP context and may return any static value.
     * Same-origin requests are allowed by default when this option is not defined.
     * Clients without an Origin header are always allowed.
     */
    origin?: SocketOriginValues | SocketOriginResolver
    /**
     * HTTP middleware executed only for the WebSocket upgrade request.
     *
     * Use this to initialize request-scoped services such as session or auth before
     * websocket.authenticate. Channel middleware should be declared on channel classes.
     */
    middleware?: SocketHttpMiddleware[]
    authenticate?: SocketAuthenticationHandler<User>
    /**
     * Sends a WebSocket ping every configured duration.
     *
     * When only pingTimeout is configured, the service uses a default 25s interval.
     */
    pingInterval?: Duration
    /**
     * Terminates connections that do not answer a ping within the configured duration.
     *
     * When only pingInterval is configured, the service uses a default 5s timeout.
     */
    pingTimeout?: Duration
    /**
     * Maximum inbound WebSocket message payload size in bytes.
     */
    maxPayload?: number
    /**
     * Maximum unresolved non-ping protocol messages per socket.
     */
    maxQueuedMessages?: number
    /**
     * Maximum inbound protocol messages accepted per rate limit interval.
     */
    maxMessagesPerInterval?: number
    /**
     * Duration of the inbound protocol message rate limit window.
     */
    messageRateInterval?: Duration
    /**
     * Maximum buffered outbound WebSocket bytes accepted before the service closes the socket.
     */
    maxBufferedAmount?: number
    /**
     * Maximum serialized size of one outbound WebSocket protocol message in bytes.
     */
    maxOutboundPayload?: number
    /**
     * Maximum number of active channel subscriptions retained by one socket.
     */
    maxSubscriptionsPerSocket?: number
    /**
     * Maximum accepted channel name length in characters.
     */
    maxChannelNameLength?: number
    /**
     * Maximum time spent waiting for socket handlers and subscription hooks during shutdown.
     * Internal socket state is forcibly released after this deadline.
     */
    shutdownTimeout?: Duration
  }
  /**
   * Optional transport used to synchronize broadcasts between SocketService instances.
   */
  transport?: SocketTransportConfig | null
}

export type SocketServiceStatus = 'starting' | 'ready' | 'stopping' | 'stopped' | 'failed'

export interface SocketHealthSnapshot {
  status: SocketServiceStatus
  ready: boolean
  connections: number
  channels: number
  lastError?: string
}

/**
 * Events emitted by the service.
 */
export interface SocketEvents<User = unknown> {
  connect: { socket: AuthenticatedSocket<User> }
  disconnect: { socket: AuthenticatedSocket<User>; reason: string }
  subscribe: { socket: AuthenticatedSocket<User>; channel: string }
  unsubscribe: { socket: AuthenticatedSocket<User>; channel: string }
}
