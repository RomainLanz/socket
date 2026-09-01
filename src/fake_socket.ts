import { AssertionError } from 'node:assert'
import { isDeepStrictEqual } from 'node:util'
import type { SocketEmission, SocketEmissionSink, SocketEmissionTarget } from './broadcasting.js'

export type SocketFakeTarget = SocketEmissionTarget
export type SocketFakeEmission = SocketEmission

type SocketFakeMatcher<Value> = Value | ((value: Value) => boolean)

export interface SocketFakeEmissionQuery {
  data?: unknown | ((data: unknown) => boolean)
  except?: SocketFakeMatcher<string[] | undefined>
}

export interface SocketFakeCountQuery extends SocketFakeEmissionQuery {
  target?: SocketFakeTarget
  event?: string
  channel?: string
}

export class SocketFake implements SocketEmissionSink {
  #emissions: SocketFakeEmission[] = []
  #restore: () => void

  constructor(restore: () => void) {
    this.#restore = restore
  }

  get emissions(): SocketFakeEmission[] {
    return [...this.#emissions]
  }

  record(emission: SocketFakeEmission): void {
    this.#emissions.push({
      ...emission,
      ...('except' in emission && emission.except ? { except: [...emission.except] } : {}),
    })
  }

  dispatch(emission: SocketFakeEmission): void {
    this.record(emission)
  }

  clear(): void {
    this.#emissions = []
  }

  restore(): void {
    this.#restore()
  }

  [Symbol.dispose](): void {
    this.restore()
  }

  assertBroadcasted(event: string, query?: SocketFakeEmissionQuery): void {
    this.#assertEmitted(
      (emission) => emission.target === 'global' && emission.event === event,
      query,
      `Expected socket event "${event}" to have been broadcasted`
    )
  }

  assertNotBroadcasted(event: string, query?: SocketFakeEmissionQuery): void {
    this.#assertNotEmitted(
      (emission) => emission.target === 'global' && emission.event === event,
      query,
      `Expected socket event "${event}" not to have been broadcasted`
    )
  }

  assertEmittedTo(channel: string, event: string, query?: SocketFakeEmissionQuery): void {
    this.#assertEmitted(
      (emission) =>
        emission.target === 'channel' && emission.channel === channel && emission.event === event,
      query,
      `Expected socket event "${event}" to have been emitted to channel "${channel}"`
    )
  }

  assertNotEmittedTo(channel: string, event: string, query?: SocketFakeEmissionQuery): void {
    this.#assertNotEmitted(
      (emission) =>
        emission.target === 'channel' && emission.channel === channel && emission.event === event,
      query,
      `Expected socket event "${event}" not to have been emitted to channel "${channel}"`
    )
  }

  assertCount(count: number, query?: SocketFakeCountQuery): void {
    const actual = this.#matching(query).length

    if (actual !== count) {
      throw new AssertionError({
        message: `Expected ${count} socket event(s) to have been emitted, but found ${actual}`,
        actual,
        expected: count,
        operator: 'strictEqual',
      })
    }
  }

  #assertEmitted(
    predicate: (emission: SocketFakeEmission) => boolean,
    query: SocketFakeEmissionQuery | undefined,
    message: string
  ): void {
    const matching = this.#emissions.filter((emission) => {
      return predicate(emission) && this.#matchesQuery(emission, query)
    })

    if (matching.length === 0) {
      throw new AssertionError({
        message,
        actual: this.#emissions,
        expected: query,
        operator: 'includes',
      })
    }
  }

  #assertNotEmitted(
    predicate: (emission: SocketFakeEmission) => boolean,
    query: SocketFakeEmissionQuery | undefined,
    message: string
  ): void {
    const matching = this.#emissions.filter((emission) => {
      return predicate(emission) && this.#matchesQuery(emission, query)
    })

    if (matching.length > 0) {
      throw new AssertionError({
        message,
        actual: matching,
        expected: [],
        operator: 'doesNotInclude',
      })
    }
  }

  #matching(query: SocketFakeCountQuery | undefined): SocketFakeEmission[] {
    return this.#emissions.filter((emission) => {
      if (!query) return true

      if (query.target && emission.target !== query.target) {
        return false
      }

      if (query?.event && emission.event !== query.event) {
        return false
      }

      if (query.channel && (!('channel' in emission) || emission.channel !== query.channel)) {
        return false
      }

      return this.#matchesQuery(emission, query)
    })
  }

  #matchesQuery(emission: SocketFakeEmission, query: SocketFakeEmissionQuery | undefined): boolean {
    if (!query) {
      return true
    }

    if ('data' in query) {
      if (typeof query.data === 'function') {
        if (!query.data(emission.data)) {
          return false
        }
      } else if (!isDeepStrictEqual(emission.data, query.data)) {
        return false
      }
    }

    if ('except' in query) {
      if (typeof query.except === 'function') {
        if (!query.except('except' in emission ? emission.except : undefined)) {
          return false
        }
      } else if (
        !isDeepStrictEqual(('except' in emission ? emission.except : undefined) ?? [], query.except)
      ) {
        return false
      }
    }

    return true
  }
}
