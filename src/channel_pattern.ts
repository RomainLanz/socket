import {
  matchRouteTokens,
  parseRoute,
  type RouteParams,
  type RouteToken,
} from '@boringnode/route-matcher'

type MatchParams = Record<string, string>

export function normalizeChannelParams(params: RouteParams): MatchParams {
  return Object.fromEntries(
    Object.entries(params).map(([name, value]) => [
      name,
      Array.isArray(value) ? value.join('/') : value,
    ])
  )
}

export class ChannelPattern {
  readonly tokens: RouteToken[]

  private constructor(readonly value: string) {
    this.tokens = parseRoute(value)
  }

  /**
   * Parse one runtime channel pattern.
   */
  static from(value: string): ChannelPattern {
    return new ChannelPattern(value)
  }

  /**
   * Match one concrete channel name.
   */
  match(channelName: string): MatchParams | null {
    const params = matchRouteTokens(channelName, [this.tokens], true)
    return params === null ? null : normalizeChannelParams(params)
  }
}
