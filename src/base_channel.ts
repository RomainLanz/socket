import type {
  AuthenticatedSocket,
  ChannelOptions,
  PresenceInfo,
  PresenceMember,
  Middleware,
  MiddlewareConstructor,
  MiddlewareContext,
  MiddlewareFn,
} from './types.js'
import type { PresenceManager } from './presence_manager.js'
import { getDecoratedChannelHandlers } from './decorators.js'

interface ChannelBroadcaster {
  to(channel: string): {
    emit(event: string, data: unknown): void
    except(socketId: string): {
      emit(event: string, data: unknown): void
    }
  }
}

export type ChannelMiddlewareRunner = <User>(
  middleware: MiddlewareConstructor<User>,
  ctx: MiddlewareContext<User>,
  next: () => Promise<void>
) => Promise<void>

export type ChannelMiddlewareRunnerFactory = () => ChannelMiddlewareRunner

const runMiddlewareWithoutContainer: ChannelMiddlewareRunner = async (Middleware, ctx, next) => {
  await new Middleware().handle(ctx, next)
}

/**
 * Error whose message may intentionally be returned to a socket client.
 */
export class SocketResponseError extends Error {}

export class MissingPresenceInfoError extends SocketResponseError {
  constructor() {
    super('Presence channels must implement getPresenceInfo(socket)')
  }
}

/**
 * Abstract class used to define a channel.
 *
 * @example
 * ```ts
 * export default class ChatChannel extends BaseChannel {
 *   static pattern = 'chat/:roomId'
 *   static middlewares = [
 *     async (ctx, next) => {
 *       // Authorize access to the channel through middleware.
 *       // if (!Room.isMember(ctx.params.roomId, ctx.socket.user.id)) throw new Error('Forbidden')
 *       await next()
 *     },
 *   ]
 * }
 * ```
 *
 * @example With presence and decorated handlers
 * ```ts
 * import { onMessage } from '@rlanz/socket/decorators'
 *
 * export default class ChatChannel extends BaseChannel {
 *   static pattern = 'chat/:roomId'
 *   static options = { presence: true }
 *
 *   static middlewares = [
 *     async (ctx, next) => {
 *       if (!ctx.socket.user.isVerified) throw new Error('Not verified')
 *       await next()
 *     }
 *   ]
 *
 *   @onMessage('chat:send')
 *   async handleSend(socket: AuthenticatedSocket<User>, data: unknown) {
 *     // Handle chat:send event
 *   }
 *
 *   @onMessage('chat:typing')
 *   async handleTyping(socket: AuthenticatedSocket<User>, data: unknown) {
 *     // Handle chat:typing event
 *   }
 * }
 * ```
 */
export class BaseChannel<User = unknown, ServerEvents = Record<never, never>> {
  /** Type-only server event declarations consumed by generated browser client types. */
  declare readonly $serverEvents: ServerEvents
  /**
   * URL-like channel pattern, for example: 'chat/:roomId'.
   */
  static pattern: string

  /**
   * Channel options.
   */
  static options?: ChannelOptions

  /**
   * Middlewares to run before subscription.
   */
  static middlewares?: Middleware<any>[]

  /**
   * Injected socket service.
   */
  protected socket!: ChannelBroadcaster

  /**
   * Full channel name, for example: 'chat/general'.
   */
  protected channelName!: string

  /**
   * Parameters extracted from the pattern, for example: { roomId: 'general' }.
   */
  protected params!: Record<string, string>

  /**
   * Injected presence manager when presence is enabled.
   */
  protected presence!: PresenceManager

  /**
   * Injects dependencies.
   */
  $setContext(
    socket: ChannelBroadcaster,
    channelName: string,
    params: Record<string, string>,
    presence?: PresenceManager
  ): void {
    this.socket = socket
    this.channelName = channelName
    this.params = params
    if (presence) {
      this.presence = presence
    }
  }

  /**
   * Emits to every channel member.
   */
  protected broadcast<K extends keyof ServerEvents & string>(
    event: K,
    data: ServerEvents[K]
  ): void {
    this.socket.to(this.channelName).emit(event, data)
  }

  /**
   * Emits to every channel member except one socket.
   */
  protected broadcastExcept<K extends keyof ServerEvents & string>(
    socketId: string,
    event: K,
    data: ServerEvents[K]
  ): void {
    this.socket.to(this.channelName).except(socketId).emit(event, data)
  }

  /**
   * Runs the middleware chain.
   *
   * middleware[0] -> middleware[1] -> ... -> subscribe
   */
  async $runMiddlewares(
    ctx: MiddlewareContext<User>,
    runClassMiddleware: ChannelMiddlewareRunner = runMiddlewareWithoutContainer
  ): Promise<void> {
    const middlewares = (this.constructor as typeof BaseChannel).middlewares || []

    let index = 0

    const dispatch = async (): Promise<void> => {
      if (index >= middlewares.length) return

      const middleware = middlewares[index++]
      let nextCalled = false
      const next = async (): Promise<void> => {
        if (nextCalled) return
        nextCalled = true
        await dispatch()
      }

      if (typeof middleware === 'function' && middleware.prototype?.handle) {
        await runClassMiddleware(middleware as MiddlewareConstructor<User>, ctx, next)
      } else if (typeof middleware === 'function') {
        await (middleware as MiddlewareFn<User>)(ctx, next)
      } else {
        await middleware.handle(ctx, next)
      }
    }

    await dispatch()
  }

  /**
   * Returns member information for presence.
   * Implement when options.presence = true.
   */
  getPresenceInfo?(socket: AuthenticatedSocket<User>): PresenceInfo

  /**
   * Hook called when a socket joins the channel.
   */
  onJoin?(socket: AuthenticatedSocket<User>, ...params: string[]): Promise<void>

  /**
   * Hook called when a socket leaves the channel.
   */
  onLeave?(socket: AuthenticatedSocket<User>, ...params: string[]): Promise<void>

  /**
   * Hook called when a member joins a presence channel.
   */
  onMemberJoin?(
    socket: AuthenticatedSocket<User>,
    member: PresenceMember,
    ...params: string[]
  ): Promise<void>

  /**
   * Hook called when a member leaves a presence channel.
   */
  onMemberLeave?(
    socket: AuthenticatedSocket<User>,
    member: PresenceMember,
    ...params: string[]
  ): Promise<void>

  /**
   * Dispatches a message to the matching handler.
   * Called by SocketService. Do not override.
   */
  async $handleMessage(
    socket: AuthenticatedSocket<User>,
    event: string,
    data: unknown
  ): Promise<unknown> {
    const handlers = getDecoratedChannelHandlers<User>(this)
    const handler = handlers[event]

    if (typeof handler === 'function') {
      return Reflect.apply(handler, this, [socket, data])
    }

    throw new SocketResponseError(`Unknown channel event: ${event}`)
  }

  /**
   * Runs the generic join hook. ChannelSubscriptions owns subscription lifecycle.
   */
  async $handleJoin(socket: AuthenticatedSocket<User>, ...params: string[]): Promise<void> {
    await this.onJoin?.(socket, ...params)
  }

  /**
   * Runs the generic leave hook. ChannelSubscriptions owns subscription lifecycle.
   */
  async $handleLeave(socket: AuthenticatedSocket<User>, ...params: string[]): Promise<void> {
    await this.onLeave?.(socket, ...params)
  }

  $getPresenceInfo(socket: AuthenticatedSocket<User>): PresenceInfo {
    if (!this.getPresenceInfo) {
      throw new MissingPresenceInfoError()
    }

    return this.getPresenceInfo(socket)
  }
}

/** Constructor for any BaseChannel specialization, including container-injected dependencies. */
export interface BaseChannelConstructor {
  new (...args: any[]): BaseChannel<any, any>
  readonly name: string
  pattern: string
  options?: ChannelOptions
  middlewares?: Middleware<any>[]
}
