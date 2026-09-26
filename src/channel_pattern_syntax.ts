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

/**
 * Parsed syntax used to validate generated channel bindings.
 */
export abstract class ChannelPatternSyntax {
  readonly canonicalPattern: string
  protected readonly segments: readonly ChannelPatternSegment[]

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
}
