import { parse as parseDurationExpression } from '@lukeed/ms'

/**
 * Duration can be specified as milliseconds or as a human-readable string.
 */
export type Duration = number | string

// @lukeed/ms returns undefined for a zero amount such as '0s', so a zero
// is recognised by parsing the same unit with an amount of 1.
const ZERO_DURATION = /^-?(?:0+(?:\.0+)?|\.0+)( *[a-z]*)$/i

/**
 * Converts a duration to milliseconds, or undefined when it does not parse.
 */
export function toMilliseconds(duration: Duration): number | undefined {
  if (typeof duration === 'number') {
    return duration
  }

  const zero = ZERO_DURATION.exec(duration)
  if (zero) {
    return parseDurationExpression(`1${zero[1]}`) === undefined ? undefined : 0
  }

  return parseDurationExpression(duration)
}

export function parseDuration(name: string, duration: Duration | undefined): number | undefined {
  if (duration === undefined) {
    return undefined
  }

  const milliseconds = toMilliseconds(duration)

  if (typeof milliseconds === 'undefined' || !Number.isFinite(milliseconds) || milliseconds <= 0) {
    throw new Error(`${name} must be a positive duration`)
  }

  return milliseconds
}
