import { parse as parseDurationExpression } from '@lukeed/ms'

/**
 * Duration can be specified as milliseconds or as a human-readable string.
 */
export type Duration = number | string

export function parseDuration(name: string, duration: Duration | undefined): number | undefined {
  if (duration === undefined) {
    return undefined
  }

  const milliseconds = typeof duration === 'number' ? duration : parseDurationExpression(duration)

  if (typeof milliseconds === 'undefined' || !Number.isFinite(milliseconds) || milliseconds <= 0) {
    throw new Error(`${name} must be a positive duration`)
  }

  return milliseconds
}
