import type { PresenceUser } from './types.js'

export interface ClientPresenceDiff {
  readonly joining: readonly PresenceUser[]
  readonly leaving: readonly PresenceUser[]
}

/**
 * Immutable, validated client view of one channel's presence.
 */
export class ClientPresenceSnapshot {
  readonly count: number
  readonly users: readonly PresenceUser[]
  readonly #usersById: ReadonlyMap<string, PresenceUser>

  private constructor(users: PresenceUser[]) {
    // Copy and freeze each user. Consumer code cannot change the snapshot.
    this.users = Object.freeze(users.map((user) => Object.freeze({ ...user }) as PresenceUser))
    this.count = this.users.length

    // Build the ID index once. Diff operations do not need a second channel state.
    this.#usersById = new Map(this.users.map((user) => [user.id, user]))
  }

  /**
   * Validate one transport snapshot for the expected channel.
   */
  static fromTransport(value: unknown, expectedChannel: string): ClientPresenceSnapshot | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return null
    }

    const data = value as Record<string, unknown>

    // The transport count must be an integer and must match the user list.
    if (
      data.channel !== expectedChannel ||
      !Array.isArray(data.users) ||
      !Number.isInteger(data.count) ||
      data.count !== data.users.length
    ) {
      return null
    }

    const users: PresenceUser[] = []
    const userIds = new Set<string>()
    for (const candidate of data.users) {
      // Each user needs a unique ID and one transport timestamp.
      if (
        !candidate ||
        typeof candidate !== 'object' ||
        Array.isArray(candidate) ||
        typeof candidate.id !== 'string' ||
        typeof candidate.joinedAt !== 'string' ||
        userIds.has(candidate.id)
      ) {
        return null
      }
      userIds.add(candidate.id)
      users.push(candidate as PresenceUser)
    }

    return new ClientPresenceSnapshot(users)
  }

  /**
   * Return users that joined or left since the previous snapshot.
   */
  diff(previous: ClientPresenceSnapshot | null): ClientPresenceDiff {
    if (!previous) {
      return { joining: this.users, leaving: [] }
    }

    // Keep the order from the snapshot that owns each user.
    return {
      joining: this.users.filter((user) => !previous.#usersById.has(user.id)),
      leaving: previous.users.filter((user) => !this.#usersById.has(user.id)),
    }
  }
}
