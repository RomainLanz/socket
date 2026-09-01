/**
 * Pattern parsing and matching semantics are adapted from @poppinss/matchit.
 * Copyright 2020 Harminder Virk and contributors, used under the MIT license.
 * See the repository LICENSE.md for the license text.
 */

const SEPARATOR = '/'
const SLASH_CODE = 47
const COLON_CODE = 58
const ASTERISK_CODE = 42
const QUESTION_MARK_CODE = 63

type ChannelPatternSegment =
  | { readonly kind: 'static'; readonly value: string }
  | {
      readonly kind: 'parameter'
      readonly name: string
      readonly optional: boolean
      readonly suffix: string
    }
  | { readonly kind: 'wildcard'; readonly name: string }

export interface ChannelPatternSyntaxMatch {
  readonly index: number
  readonly params: Record<string, string>
}

/**
 * Immutable parsed syntax shared by runtime matching and generated binding.
 */
export class ChannelPatternSyntax {
  readonly canonicalPattern: string
  protected readonly segments: readonly ChannelPatternSegment[]
  readonly #specificity: readonly number[]

  protected constructor(value: string) {
    const canonicalPattern = ChannelPatternSyntax.#normalize(value)
    if (canonicalPattern === SEPARATOR) {
      this.canonicalPattern = canonicalPattern
      this.segments = Object.freeze([Object.freeze({ kind: 'static', value: SEPARATOR })])
    } else {
      const segments: ChannelPatternSegment[] = []
      let remaining = canonicalPattern
      let index = -1
      let start = 0
      let length = remaining.length

      while (++index < length) {
        const character = remaining.charCodeAt(index)

        if (character === COLON_CODE) {
          // Read the parameter name, its optional marker, and its suffix.
          start = index + 1
          let optional = false
          let marker = 0
          let suffix = ''

          while (index < length && remaining.charCodeAt(index) !== SLASH_CODE) {
            const parameterCharacter = remaining.charCodeAt(index)
            if (parameterCharacter === QUESTION_MARK_CODE) {
              marker = index
              optional = true
            } else if (parameterCharacter === 46 && suffix.length === 0) {
              marker = index
              suffix = remaining.slice(index)
            }
            index++
          }

          segments.push({
            kind: 'parameter',
            name: remaining.slice(start, marker || index),
            optional,
            suffix,
          })
          remaining = remaining.slice(index)
          length -= index
          index = 0
          continue
        }

        if (character === ASTERISK_CODE) {
          // A wildcard consumes the rest of the pattern.
          segments.push({ kind: 'wildcard', name: remaining.slice(index) })
          continue
        }

        start = index
        while (index < length && remaining.charCodeAt(index) !== SLASH_CODE) {
          index++
        }
        segments.push({ kind: 'static', value: remaining.slice(start, index) })
        remaining = remaining.slice(index)
        length -= index
        index = 0
        start = 0
      }

      this.canonicalPattern = canonicalPattern
      this.segments = Object.freeze(segments.map((segment) => Object.freeze(segment)))
    }

    this.#specificity = Object.freeze(
      this.segments.map((segment) => {
        if (segment.kind === 'static') return 40
        if (segment.kind === 'wildcard') return 0
        return segment.optional ? 20 : 30
      })
    )
  }

  /**
   * Parse one pattern and return its immutable syntax.
   */
  static from(value: string): ChannelPatternSyntax {
    const syntax = new ChannelPatternSyntax(value)
    Object.freeze(syntax)
    return syntax
  }

  /**
   * Match one channel name and return its decoded parameters.
   */
  match(channelName: string): Record<string, string> | null {
    return this.#matchValues(ChannelPatternSyntax.#split(channelName))
  }

  /**
   * Compare this syntax with another syntax for runtime routing.
   * Return a negative value when this syntax is more specific.
   */
  compareSpecificity(other: ChannelPatternSyntax): number {
    const length = Math.max(this.#specificity.length, other.#specificity.length)

    for (let index = 0; index < length; index++) {
      const difference = (other.#specificity[index] ?? -1) - (this.#specificity[index] ?? -1)
      if (difference !== 0) {
        return difference
      }
    }

    return other.#specificity.length - this.#specificity.length
  }

  /**
   * Return the first matching syntax without splitting the channel name again.
   */
  static firstMatch(
    patterns: readonly ChannelPatternSyntax[],
    channelName: string
  ): ChannelPatternSyntaxMatch | null {
    const values = ChannelPatternSyntax.#split(channelName)
    for (const [index, pattern] of patterns.entries()) {
      const params = pattern.#matchValues(values)
      if (params) return { index, params }
    }
    return null
  }

  #matchValues(values: string[]): Record<string, string> | null {
    const finalSegment = this.segments.at(-1)

    // A wildcard can consume extra values. A final optional parameter can be absent.
    const compatibleLength =
      this.segments.length === values.length ||
      (this.segments.length < values.length && finalSegment?.kind === 'wildcard') ||
      (this.segments.length > values.length &&
        finalSegment?.kind === 'parameter' &&
        finalSegment.optional)

    if (
      !compatibleLength ||
      !this.segments.every((segment, index) =>
        ChannelPatternSyntax.#segmentMatches(segment, values[index])
      )
    ) {
      return null
    }

    const parameters: Record<string, string> = {}
    for (const [index, segment] of this.segments.entries()) {
      // The root separator does not produce a parameter.
      if (values[index] === SEPARATOR) continue

      if (segment.kind === 'wildcard') {
        // Decode each wildcard value before the values are joined again.
        if (segment.name === '*') {
          parameters[segment.name] = values
            .slice(index)
            .map(ChannelPatternSyntax.#decode)
            .join(SEPARATOR)
        }
        break
      }

      if (segment.kind === 'parameter' && values[index] !== undefined) {
        // Remove the static suffix before the parameter is decoded.
        parameters[segment.name] = ChannelPatternSyntax.#decode(
          values[index].replace(segment.suffix, '')
        )
      }
    }
    return parameters
  }

  static #normalize(value: string): string {
    if (value === SEPARATOR) return value

    // Remove one leading slash and one trailing slash. This keeps Matchit behavior.
    let normalized = value
    if (normalized.charCodeAt(0) === SLASH_CODE) {
      normalized = normalized.slice(1)
    }
    if (normalized.charCodeAt(normalized.length - 1) === SLASH_CODE) {
      normalized = normalized.slice(0, -1)
    }
    return normalized
  }

  static #split(value: string): string[] {
    const normalized = ChannelPatternSyntax.#normalize(value)
    return normalized === SEPARATOR ? [SEPARATOR] : normalized.split(SEPARATOR)
  }

  static #decode(value: string): string {
    try {
      return decodeURIComponent(value)
    } catch {
      // Keep invalid encoded input unchanged. Matchit uses the same fallback.
      return value
    }
  }

  static #segmentMatches(segment: ChannelPatternSegment, value: string | undefined): boolean {
    if (segment.kind === 'static') {
      return segment.value === value
    }
    if (value === SEPARATOR) {
      return segment.kind === 'wildcard' || segment.optional
    }
    if (value === '') {
      return segment.kind === 'wildcard' || segment.suffix === ''
    }
    if (value === undefined) {
      return segment.kind === 'wildcard' || segment.suffix === ''
    }
    return segment.kind === 'wildcard' || value.endsWith(segment.suffix)
  }
}
