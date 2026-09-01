import type { Logger } from '@adonisjs/logger'
import { SocketResponseError } from './base_channel.js'
import type { BaseChannel } from './base_channel.js'
import {
  ChannelSubscriptionStorage,
  type StoredChannelSubscription,
} from './channel_subscription_storage.js'
import type { ChannelRouter } from './channel_router.js'
import type { PresenceData, PresenceManager } from './presence_manager.js'
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

export interface ChannelSubscriptionLimits {
  maxSubscriptionsPerSocket: number
  maxChannelNameLength: number
}

/**
 * Durable channel subscriptions for sockets.
 */
export class ChannelSubscriptions<User = unknown> {
  #storage = new ChannelSubscriptionStorage()
  #channelOperations = new Map<string, Promise<void>>()

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

    if (this.#usesPresence(result.instance)) {
      return this.#withChannelOperation(`${socket.id}\0${channelName}`, async () => {
        const concurrentSubscription = await this.#refreshExistingSubscription(socket, channelName)
        if (concurrentSubscription) {
          return concurrentSubscription
        }

        return this.#completeSubscription(socket, channelName, result)
      })
    }

    return this.#completeSubscription(socket, channelName, result)
  }

  async #completeSubscription(
    socket: AuthenticatedSocket<User>,
    channelName: string,
    result: AuthorizedChannel<User>
  ): Promise<ChannelSubscribeResult> {
    result.instance.$setContext(
      this.socketService,
      channelName,
      result.params,
      this.presenceManager
    )

    let presenceData: Record<string, unknown> | PresenceData | null = result.presenceData
    let presenceSnapshot: PresenceData | null = null
    let presenceJoined = false
    let serializationError: unknown

    try {
      if (this.#usesPresence(result.instance)) {
        const presenceInfo = result.instance.$getPresenceInfo(socket)
        const joinedUser = this.presenceManager!.join(channelName, socket.raw, presenceInfo)
        const { joinedAt: _joinedAt, ...member } = joinedUser
        presenceJoined = true

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
        await result.instance.onJoin?.(socket, ...result.paramValues)
      } else {
        try {
          JSON.stringify(presenceData)
        } catch (error) {
          serializationError = error
          throw error
        }

        await result.instance.onJoin?.(socket, ...result.paramValues)
      }

      this.#storage.set(socket.id, channelName, {
        channelName,
        instance: result.instance,
        paramValues: result.paramValues,
      })

      if (presenceSnapshot) {
        this.socketService
          .to(channelName)
          .except(socket.id)
          .emit('presence:update', ChannelSubscriptions.#serializePresenceData(presenceSnapshot))
      }
    } catch (error) {
      if (presenceJoined) {
        this.presenceManager!.leave(channelName, socket.raw)

        const rollbackData = await this.presenceManager!.snapshot(channelName).catch(() => null)
        if (rollbackData) {
          this.socketService
            .to(channelName)
            .emit('presence:update', ChannelSubscriptions.#serializePresenceData(rollbackData))
        }
      }

      this.#storage.delete(socket.id, channelName)

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
    const subscription = this.#storage.get(socket.id, channelName)
    if (!subscription) {
      return false
    }

    this.#storage.delete(socket.id, channelName)

    if (this.#usesPresence(subscription.instance)) {
      await this.#withChannelOperation(`${socket.id}\0${channelName}`, () =>
        this.#cleanupAfterLeave(socket, channelName, subscription)
      )
    } else {
      await this.#cleanupAfterLeave(socket, channelName, subscription)
    }

    return true
  }

  async #cleanupAfterLeave(
    socket: AuthenticatedSocket<User>,
    channelName: string,
    subscription: StoredChannelSubscription
  ): Promise<void> {
    if (this.#usesPresence(subscription.instance)) {
      const presenceUser = this.presenceManager!.getLocalUser(channelName, socket.id)
      this.presenceManager!.leave(channelName, socket.raw)

      try {
        const presenceData = await this.presenceManager!.snapshot(channelName)

        this.socketService
          .to(channelName)
          .emit('presence:update', ChannelSubscriptions.#serializePresenceData(presenceData))

        if (presenceUser && !presenceData.users.some((user) => user.id === presenceUser.id)) {
          const { joinedAt: _joinedAt, ...member } = presenceUser
          await subscription.instance.onMemberLeave?.(socket, member, ...subscription.paramValues)
        }
      } catch (error) {
        this.#warn(`failed to update presence while leaving channel "${channelName}": %s`, error)
      }
    }

    try {
      await subscription.instance.onLeave?.(socket, ...subscription.paramValues)
    } catch (error) {
      this.#warn(`failed to leave channel "${channelName}": %s`, error)
    }
  }

  async leaveAll(socket: AuthenticatedSocket<User>): Promise<void> {
    for (const channelName of this.#storage.channelNamesFor(socket.id)) {
      await this.leave(socket, channelName)
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

  deleteSocket(socketId: string): void {
    this.#storage.deleteSocket(socketId)
  }

  clear(): void {
    this.#storage.clear()
    this.#channelOperations.clear()
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

  async #withChannelOperation<Result>(
    operationKey: string,
    operation: () => Promise<Result>
  ): Promise<Result> {
    const previous = this.#channelOperations.get(operationKey) ?? Promise.resolve()
    const release = Promise.withResolvers<void>()
    const queued = previous.then(() => release.promise)
    this.#channelOperations.set(operationKey, queued)

    await previous
    try {
      return await operation()
    } finally {
      release.resolve()
      if (this.#channelOperations.get(operationKey) === queued) {
        this.#channelOperations.delete(operationKey)
      }
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
