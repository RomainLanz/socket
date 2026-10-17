import type { Logger } from '@adonisjs/logger'
import { SocketResponseError } from './base_channel.js'
import type { BaseChannel } from './base_channel.js'
import {
  ChannelSubscriptionStorage,
  type StoredChannelSubscription,
} from './channel_subscription_storage.js'
import type { ChannelRouter } from './channel_router.js'
import type { PresenceData, PresenceManager, PresenceUser } from './presence_manager.js'
import { PendingWork } from './pending_work.js'
import type { SocketService } from './socket_service.js'
import type { AuthenticatedSocket, ChannelAck, ChannelMessage } from './types.js'

interface AuthorizedChannel<User> {
  instance: BaseChannel<User>
  params: Record<string, string>
  paramValues: string[]
  presenceData: Record<string, unknown> | PresenceData | null
}

export type ChannelSubscribeAck =
  | {
      ok: true
      presenceData?: Record<string, unknown> | PresenceData | null
      error?: never
    }
  | { ok: false; error: string; presenceData?: never }

export interface ChannelSubscribeResult {
  ack: ChannelSubscribeAck
  created: boolean
}

const SOCKET_CLOSED_ERROR = 'Socket closed'

export interface ChannelSubscriptionLimits {
  maxSubscriptionsPerSocket: number
  maxChannelNameLength: number
}

/**
 * One subscribe of a socket on a channel, from its join hooks until its
 * leave hooks ran. Every teardown goes through #detach, which releases the
 * attempt at once and runs its leave hooks exactly once:
 *
 *   joining ──join hooks done──▶ active
 *      │                           │
 *      └────────── #detach ────────┴──▶ released ──▶ leave hooks
 */
interface SubscriptionAttempt<User> {
  socket: AuthenticatedSocket<User>
  subscription: StoredChannelSubscription
  state: 'joining' | 'active' | 'released'
  /** Settles once onMemberJoin ran or was not needed; false if it failed. */
  memberJoined: PromiseWithResolvers<boolean>
  /** Settles once onJoin ran; false if the join failed. */
  joined: PromiseWithResolvers<boolean>
  /** The leave hooks, set when the attempt is released. */
  cleanup?: Promise<void>
}

/**
 * Durable channel subscriptions for sockets.
 */
export class ChannelSubscriptions<User = unknown> {
  #storage = new ChannelSubscriptionStorage()
  // socket id -> channel name -> attempt not released yet
  #attempts = new Map<string, Map<string, SubscriptionAttempt<User>>>()
  #leaveHooks = new PendingWork()
  #closedSockets = new WeakSet<AuthenticatedSocket<User>>()

  static #serializePresenceData(presenceData: PresenceData): Record<string, unknown> {
    return {
      ...presenceData,
      users: presenceData.users.map((user) => ({
        ...user,
        joinedAt: user.joinedAt.toISOString(),
      })),
    }
  }

  constructor(
    private socketService: SocketService<User>,
    private channelRouter: ChannelRouter,
    private logger: Logger,
    private presenceManager?: PresenceManager,
    private limits?: ChannelSubscriptionLimits
  ) {}

  setPresenceManager(presenceManager: PresenceManager): void {
    this.presenceManager = presenceManager
  }

  async subscribe(
    socket: AuthenticatedSocket<User>,
    channelName: string
  ): Promise<ChannelSubscribeResult> {
    return this.#subscribe(socket, channelName)
  }

  async #subscribe(
    socket: AuthenticatedSocket<User>,
    channelName: string
  ): Promise<ChannelSubscribeResult> {
    const existing = await this.#refreshExistingSubscription(socket, channelName)
    if (existing) {
      return existing
    }

    if (this.limits && channelName.length > this.limits.maxChannelNameLength) {
      return {
        ack: { ok: false, error: 'Channel name is too long' },
        created: false,
      }
    }

    if (
      this.limits &&
      this.#storage.channelNamesFor(socket.id).length >= this.limits.maxSubscriptionsPerSocket
    ) {
      return {
        ack: { ok: false, error: 'Socket subscription limit exceeded' },
        created: false,
      }
    }

    const result = await this.channelRouter.authorize(socket, channelName)

    if (!result.success) {
      if ('cause' in result && !(result.cause instanceof SocketResponseError)) {
        this.#warn(`failed to authorize channel "${channelName}": %s`, result.cause)
      }

      return {
        ack: { ok: false, error: result.error },
        created: false,
      }
    }

    return this.#completeSubscription(socket, channelName, result)
  }

  async #completeSubscription(
    socket: AuthenticatedSocket<User>,
    channelName: string,
    result: AuthorizedChannel<User>
  ): Promise<ChannelSubscribeResult> {
    if (this.#closedSockets.has(socket)) {
      return { ack: { ok: false, error: SOCKET_CLOSED_ERROR }, created: false }
    }

    const attempt: SubscriptionAttempt<User> = {
      socket,
      subscription: { channelName, instance: result.instance, paramValues: result.paramValues },
      state: 'joining',
      memberJoined: Promise.withResolvers(),
      joined: Promise.withResolvers(),
    }
    const attempts = this.#attempts.get(socket.id) ?? new Map()
    this.#attempts.set(socket.id, attempts.set(channelName, attempt))
    result.instance.$setContext(
      this.socketService,
      channelName,
      result.params,
      this.presenceManager
    )

    let presenceData: Record<string, unknown> | PresenceData | null = result.presenceData
    let presenceSnapshot: PresenceData | null = null
    let serializationError: unknown

    try {
      if (this.#usesPresence(result.instance)) {
        const presenceInfo = result.instance.$getPresenceInfo(socket)
        const joinedUser = this.presenceManager!.join(channelName, socket.raw, presenceInfo)
        const { joinedAt: _joinedAt, ...member } = joinedUser

        presenceSnapshot = await this.presenceManager!.snapshot(channelName)
        presenceData = presenceSnapshot
        const visibleUser = presenceSnapshot.users.find((user) => user.id === presenceInfo.id)
        const wasPresent = Boolean(visibleUser && visibleUser !== joinedUser)

        try {
          JSON.stringify(presenceData)
        } catch (error) {
          serializationError = error
          throw error
        }

        if (!wasPresent) {
          await result.instance.onMemberJoin?.(socket, member, ...result.paramValues)
        }
        attempt.memberJoined.resolve(true)
        await result.instance.onJoin?.(socket, ...result.paramValues)

        // Members may have joined or left while the hooks ran. Each local
        // member's data was validated by its own join and remote members
        // arrive as JSON, so the fresh snapshot needs no serialization check.
        presenceSnapshot = await this.presenceManager!.snapshot(channelName)
        presenceData = presenceSnapshot
      } else {
        try {
          JSON.stringify(presenceData)
        } catch (error) {
          serializationError = error
          throw error
        }

        await result.instance.onJoin?.(socket, ...result.paramValues)
      }
      attempt.joined.resolve(true)

      // The socket closed while the join hooks ran; #detach runs its leave.
      if (attempt.state === 'released') {
        return { ack: { ok: false, error: SOCKET_CLOSED_ERROR }, created: false }
      }

      attempt.state = 'active'
      this.#storage.set(socket.id, channelName, attempt.subscription)

      if (presenceSnapshot) {
        this.socketService
          .to(channelName)
          .except(socket.id)
          .emit('presence:update', ChannelSubscriptions.#serializePresenceData(presenceSnapshot))
      }
    } catch (error) {
      attempt.memberJoined.resolve(false)
      attempt.joined.resolve(false)
      await this.#detach(attempt)

      if (serializationError) {
        this.#warn(
          `failed to serialize subscription response for channel "${channelName}": %s`,
          serializationError
        )
      } else if (!(error instanceof SocketResponseError)) {
        this.#warn(`failed to join channel "${channelName}": %s`, error)
      }

      return {
        ack: {
          ok: false,
          error: serializationError
            ? 'Subscription response is not serializable'
            : error instanceof SocketResponseError
              ? error.message
              : 'Join failed',
        },
        created: false,
      }
    }

    return {
      ack: { ok: true, presenceData },
      created: true,
    }
  }

  async leave(socket: AuthenticatedSocket<User>, channelName: string): Promise<boolean> {
    const attempt = this.#attempts.get(socket.id)?.get(channelName)
    if (attempt?.state !== 'active') {
      return false
    }

    await this.#detach(attempt)
    return true
  }

  /**
   * Releases a closed socket. Its subscriptions and presence are gone when
   * this returns, so callers can emit disconnect right away. Leave hooks run
   * afterwards, also for joins still completing; the returned promise
   * settles once they finished.
   */
  release(socket: AuthenticatedSocket<User>): Promise<void> {
    this.#closedSockets.add(socket)
    const attempts = [...(this.#attempts.get(socket.id)?.values() ?? [])]
    return Promise.all(attempts.map((attempt) => this.#detach(attempt))).then(() => {})
  }

  /**
   * Settles once the leave hooks started so far finished, or returns null
   * when none is running. Used by shutdown.
   */
  drain(): Promise<void> | null {
    return this.#leaveHooks.settled()
  }

  /**
   * Releases an attempt at once: it leaves storage and presence, and its
   * leave hooks start on the next turn, after the caller's own work such as
   * disconnect listeners. Returns those hooks.
   */
  #detach(attempt: SubscriptionAttempt<User>): Promise<void> {
    if (attempt.cleanup) return attempt.cleanup

    const { socket, subscription } = attempt
    const duringJoin = attempt.state === 'joining'
    attempt.state = 'released'
    this.#storage.delete(socket.id, subscription.channelName)
    // Decided now, so of several sockets of one member leaving together
    // only the last one removed runs onMemberLeave.
    const lastLocalUser = this.presenceManager?.leave(subscription.channelName, socket.raw)

    const attempts = this.#attempts.get(socket.id)
    if (attempts?.get(subscription.channelName) === attempt) {
      attempts.delete(subscription.channelName)
      if (attempts.size === 0) this.#attempts.delete(socket.id)
    }

    attempt.cleanup = new Promise<void>((resolve) => setImmediate(resolve)).then(() =>
      this.#cleanupAfterLeave(attempt, lastLocalUser, duringJoin)
    )
    this.#leaveHooks.track(attempt.cleanup)
    return attempt.cleanup
  }

  /**
   * Runs the leave hooks of a released attempt, each paired with its join
   * hook: onMemberLeave waits for onMemberJoin, which may finish long before
   * a slow onJoin, and onLeave runs once onJoin succeeded. The presence:update
   * waits for neither. `lastLocalUser` is set when the removal took the
   * member's last local socket.
   */
  async #cleanupAfterLeave(
    attempt: SubscriptionAttempt<User>,
    lastLocalUser: PresenceUser | undefined,
    duringJoin: boolean
  ): Promise<void> {
    const { socket, subscription } = attempt
    const { channelName } = subscription

    if (this.#usesPresence(subscription.instance)) {
      try {
        let presenceData = await this.presenceManager!.snapshot(channelName)
        this.socketService
          .to(channelName)
          .emit('presence:update', ChannelSubscriptions.#serializePresenceData(presenceData))

        if (lastLocalUser && (await attempt.memberJoined.promise)) {
          // The member may have rejoined while its onMemberJoin was running.
          // Skipping the leave then is the lesser evil: that member gets two
          // onMemberJoin and one onMemberLeave, instead of a leave while present.
          if (duringJoin) presenceData = await this.presenceManager!.snapshot(channelName)
          if (!presenceData.users.some((user) => user.id === lastLocalUser.id)) {
            const { joinedAt: _joinedAt, ...member } = lastLocalUser
            await subscription.instance.onMemberLeave?.(socket, member, ...subscription.paramValues)
          }
        }
      } catch (error) {
        this.#warn(`failed to update presence while leaving channel "${channelName}": %s`, error)
      }
    }

    if (!(await attempt.joined.promise)) return

    try {
      await subscription.instance.onLeave?.(socket, ...subscription.paramValues)
    } catch (error) {
      this.#warn(`failed to leave channel "${channelName}": %s`, error)
    }
  }

  async handleMessage(
    socket: AuthenticatedSocket<User>,
    payload: ChannelMessage
  ): Promise<ChannelAck> {
    const subscription = this.#storage.get(socket.id, payload.channel)

    if (!subscription) {
      return { ok: false, error: 'Not subscribed' }
    }

    try {
      const result = await subscription.instance.$handleMessage(socket, payload.event, payload.data)

      try {
        JSON.stringify(result)
      } catch (error) {
        this.#warn(
          `failed to serialize handler response for channel "${payload.channel}": %s`,
          error
        )
        return { ok: false, error: 'Handler response is not serializable' }
      }

      return { ok: true, data: result }
    } catch (error) {
      if (!(error instanceof SocketResponseError)) {
        this.#warn(`channel handler failed for "${payload.channel}": %s`, error)
      }

      return {
        ok: false,
        error: error instanceof SocketResponseError ? error.message : 'Handler error',
      }
    }
  }

  relayWhisper(socket: AuthenticatedSocket<User>, payload: ChannelMessage): ChannelAck {
    if (!this.#storage.has(socket.id, payload.channel)) {
      return { ok: false, error: 'Not subscribed' }
    }

    this.socketService
      .to(payload.channel)
      .except(socket.id)
      .emit(`client:${payload.event}`, payload.data)

    return { ok: true }
  }

  clear(): void {
    this.#storage.clear()
    this.#attempts.clear()
    this.#leaveHooks = new PendingWork()
  }

  getSocketIds(channelName: string): string[] {
    return this.#storage.socketIdsFor(channelName)
  }

  subscriptionCountFor(socketId: string): number {
    return this.#storage.countForSocket(socketId)
  }

  get channelsCount(): number {
    return this.#storage.channelsCount
  }

  #warn(message: string, error: unknown): void {
    try {
      this.logger.warn(message, error)
    } catch {
      // Logging must never change the socket protocol outcome.
    }
  }

  #usesPresence(instance: BaseChannel<any, any>): boolean {
    return Boolean(
      (instance.constructor as typeof BaseChannel).options?.presence && this.presenceManager
    )
  }

  async #refreshExistingSubscription(
    socket: AuthenticatedSocket<User>,
    channelName: string
  ): Promise<ChannelSubscribeResult | null> {
    if (!this.#storage.has(socket.id, channelName)) {
      return null
    }

    let presenceData: PresenceData | undefined

    try {
      presenceData = await this.#getPresenceData(socket, channelName)
    } catch (error) {
      this.#warn(`failed to refresh subscription for channel "${channelName}": %s`, error)
      return {
        ack: { ok: false, error: 'Subscribe failed' },
        created: false,
      }
    }

    try {
      JSON.stringify(presenceData)
    } catch (error) {
      this.#warn(
        `failed to serialize subscription response for channel "${channelName}": %s`,
        error
      )
      return {
        ack: { ok: false, error: 'Subscription response is not serializable' },
        created: false,
      }
    }

    return {
      ack: {
        ok: true,
        presenceData,
      },
      created: false,
    }
  }

  async #getPresenceData(
    socket: AuthenticatedSocket<User>,
    channelName: string
  ): Promise<PresenceData | undefined> {
    if (!this.presenceManager?.hasLocal(channelName, socket.id)) {
      return undefined
    }

    return this.presenceManager.snapshot(channelName)
  }
}
