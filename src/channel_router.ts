import { RouteTable } from '@boringnode/route-matcher'
import {
  SocketResponseError,
  type BaseChannel,
  type BaseChannelConstructor,
  type ChannelMiddlewareRunnerFactory,
} from './base_channel.js'
import { ChannelPattern, normalizeChannelParams } from './channel_pattern.js'
import type { AuthenticatedSocket, ChannelMatch, MiddlewareContext } from './types.js'

interface RegisteredChannel {
  pattern: ChannelPattern
  channel: BaseChannelConstructor
}

export type ChannelFactory = (
  channel: BaseChannelConstructor
) => BaseChannel<any> | Promise<BaseChannel<any>>

/**
 * Matches channel names against registered patterns.
 *
 * Pattern: chat/:roomId
 * Channel: chat/general
 *
 * chat / :roomId
 *   |      |
 * chat / general  => { roomId: "general" }
 */
export class ChannelRouter {
  /**
   * Registered channels keyed by their pattern.
   */
  #channels = new Map<string, RegisteredChannel>()
  #routes = new RouteTable<RegisteredChannel>({ precedence: 'specificity' })

  constructor(
    private makeChannel: ChannelFactory = (channel) => new channel(),
    private createMiddlewareRunner?: ChannelMiddlewareRunnerFactory
  ) {}

  /**
   * Registers a channel.
   */
  register(channel: BaseChannelConstructor): void {
    const patternValue = channel.pattern

    if (!patternValue) {
      throw new Error(`Channel ${channel.name} must define a static pattern`)
    }

    if (this.#channels.has(patternValue)) {
      throw new Error(`Channel pattern "${patternValue}" is already registered`)
    }

    const pattern = ChannelPattern.from(patternValue)
    const registered = { pattern, channel }

    this.#routes.add(pattern.tokens, registered)
    this.#channels.set(pattern.value, registered)
  }

  /**
   * Matches a channel name against registered patterns.
   */
  match(channelName: string): ChannelMatch | null {
    const matched = this.#routes.match(channelName, true)

    if (!matched) {
      return null
    }

    return {
      channel: matched.value.channel,
      pattern: matched.value.pattern.value,
      params: normalizeChannelParams(matched.params),
    }
  }

  /**
   * Authorizes a socket before it joins a channel.
   */
  async authorize<User = unknown>(socket: AuthenticatedSocket<User>, channelName: string) {
    const matched = this.match(channelName)

    if (!matched) {
      return {
        success: false,
        error: 'Channel not found',
      } as const
    }

    const instance = (await this.makeChannel(matched.channel)) as BaseChannel<User>
    const paramValues = Object.values(matched.params)

    const ctx: MiddlewareContext<User> = {
      socket,
      channel: channelName,
      params: matched.params,
      presenceData: undefined,
      setPresenceData(data) {
        ctx.presenceData = data
      },
    }

    // Run authorization middlewares.
    try {
      await instance.$runMiddlewares(ctx, this.createMiddlewareRunner?.())
    } catch (error) {
      return {
        success: false,
        error: error instanceof SocketResponseError ? error.message : 'Authorization failed',
        cause: error,
      } as const
    }

    return {
      success: true,
      instance,
      params: matched.params,
      paramValues,
      presenceData: ctx.presenceData ?? null,
    } as const
  }

  /**
   * Lists all registered channels.
   */
  list() {
    return [...this.#channels.values()].map(({ pattern, channel }) => ({
      pattern: pattern.value,
      name: channel.name,
      options: channel.options,
    }))
  }

  /**
   * Number of registered channels.
   */
  get size(): number {
    return this.#channels.size
  }
}
