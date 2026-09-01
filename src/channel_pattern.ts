import type { BaseChannelConstructor } from './base_channel.js'
import { ChannelPatternSyntax } from './channel_pattern_syntax.js'

type MatchParams = Record<string, string>

export interface ChannelPatternMatch {
  pattern: ChannelPattern
  channel: BaseChannelConstructor
  params: MatchParams
}

interface MatchCandidate {
  pattern: ChannelPattern
  channel: BaseChannelConstructor
}

export class ChannelPattern {
  private readonly syntax: ChannelPatternSyntax

  private constructor(readonly value: string) {
    this.syntax = ChannelPatternSyntax.from(value)
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
    return this.syntax.match(channelName)
  }

  /**
   * Return the most specific matching candidate. Static segments have the highest score.
   * Parameters score above optional parameters. Wildcards have the lowest score.
   * Insertion order is preserved for ties.
   */
  static firstMatch(
    channelName: string,
    candidates: Iterable<MatchCandidate>
  ): ChannelPatternMatch | null {
    const sorted = [...candidates].sort((left, right) => {
      return ChannelPattern.#compareSpecificity(left.pattern, right.pattern)
    })
    const matched = ChannelPatternSyntax.firstMatch(
      sorted.map(({ pattern }) => pattern.syntax),
      channelName
    )
    if (!matched) return null

    const { pattern, channel } = sorted[matched.index]
    return { pattern, channel, params: matched.params }
  }

  static #compareSpecificity(left: ChannelPattern, right: ChannelPattern): number {
    return left.syntax.compareSpecificity(right.syntax)
  }
}
