import type { PresenceInfo } from './types.js'

export const PRESENCE_DATA_KEY = '__rlanzSocketPresence'

type PresenceSocketData = Record<string, unknown> & {
  [PRESENCE_DATA_KEY]?: Record<string, PresenceUser>
}

export interface PresenceUser {
  id: string
  joinedAt: Date
  [key: string]: unknown
}

export interface PresenceData {
  channel: string
  users: PresenceUser[]
  count: number
}

export interface PresenceSocket {
  id: string
  data: PresenceSocketData
}

/**
 * Tracks user presence in channels.
 *
 * channel -> socketId -> PresenceUser
 */
export class PresenceManager {
  #presence = new Map<string, Map<string, PresenceUser>>()
  #fetchSockets: ((channel: string) => Promise<PresenceSocket[]>) | null = null

  /**
   * Sets the socket fetcher used to build distributed snapshots.
   */
  setSocketFetcher(fetchSockets: ((channel: string) => Promise<PresenceSocket[]>) | null): void {
    this.#fetchSockets = fetchSockets
  }

  /**
   * Adds a user to a channel.
   */
  join(channel: string, socket: PresenceSocket, member: PresenceInfo): PresenceUser {
    const user = {
      ...member.data,
      id: member.id,
      joinedAt: new Date(),
    }

    const users = this.#presence.get(channel) ?? new Map()
    users.set(socket.id, user)
    this.#presence.set(channel, users)
    this.#setSocketPresence(socket, channel, user)

    return user
  }

  /**
   * Removes a user from a channel.
   */
  leave(channel: string, socket: PresenceSocket): void {
    const channelPresence = this.#presence.get(channel)

    if (channelPresence) {
      channelPresence.delete(socket.id)

      if (channelPresence.size === 0) {
        this.#presence.delete(channel)
      }
    }

    this.#deleteSocketPresence(socket, channel)
  }

  /**
   * Removes a user from every channel.
   * Returns the list of channels the user left.
   */
  leaveAll(socket: PresenceSocket): string[] {
    const leftChannels: string[] = []

    for (const [channel, users] of this.#presence) {
      if (users.has(socket.id)) {
        users.delete(socket.id)
        leftChannels.push(channel)

        if (users.size === 0) {
          this.#presence.delete(channel)
        }

        this.#deleteSocketPresence(socket, channel)
      }
    }

    return leftChannels
  }

  /**
   * Gets presence data for a channel.
   */
  async snapshot(channel: string): Promise<PresenceData> {
    if (this.#fetchSockets) {
      return this.#getFromSockets(channel)
    }

    return this.#localSnapshot(channel)
  }

  /**
   * Gets the number of users in a channel.
   */
  async count(channel: string): Promise<number> {
    return (await this.snapshot(channel)).count
  }

  /**
   * Checks whether a socket is locally present in a channel.
   */
  hasLocal(channel: string, socketId: string): boolean {
    return this.#presence.get(channel)?.has(socketId) || false
  }

  /**
   * Gets the presence user retained for one local socket.
   */
  getLocalUser(channel: string, socketId: string): PresenceUser | undefined {
    return this.#presence.get(channel)?.get(socketId)
  }

  /**
   * Lists every local channel where a socket is present.
   */
  getLocalChannelsForSocket(socketId: string): string[] {
    const channels: string[] = []

    for (const [channel, users] of this.#presence) {
      if (users.has(socketId)) {
        channels.push(channel)
      }
    }

    return channels
  }

  /**
   * Lists sockets that are locally present in a channel.
   */
  getLocalSockets(channel: string): PresenceSocket[] {
    const users = this.#presence.get(channel)

    if (!users) {
      return []
    }

    return Array.from(users.entries()).map(([socketId, user]) => ({
      id: socketId,
      data: {
        [PRESENCE_DATA_KEY]: {
          [channel]: user,
        },
      },
    }))
  }

  #localSnapshot(channel: string): PresenceData {
    return this.#createSnapshot(channel, Array.from(this.#presence.get(channel)?.entries() ?? []))
  }

  async #getFromSockets(channel: string): Promise<PresenceData> {
    const sockets = await this.#fetchSockets!(channel)
    const users = sockets.flatMap((socket): Array<[string, PresenceUser]> => {
      const user = socket.data?.[PRESENCE_DATA_KEY]?.[channel]
      return user ? [[socket.id, user]] : []
    })

    return this.#createSnapshot(channel, users)
  }

  #createSnapshot(
    channel: string,
    connections: Array<[socketId: string, user: PresenceUser]>
  ): PresenceData {
    const usersById = new Map<string, [socketId: string, user: PresenceUser]>()

    for (const connection of connections) {
      const current = usersById.get(connection[1].id)
      if (!current || this.#compareConnections(connection, current) < 0) {
        usersById.set(connection[1].id, connection)
      }
    }

    const users = [...usersById.values()]
      .sort((left, right) => this.#compareConnections(left, right))
      .map(([, user]) => user)

    return {
      channel,
      users,
      count: users.length,
    }
  }

  #compareConnections(
    [leftSocketId, leftUser]: [string, PresenceUser],
    [rightSocketId, rightUser]: [string, PresenceUser]
  ): number {
    const joinedAtDifference = leftUser.joinedAt.getTime() - rightUser.joinedAt.getTime()
    return joinedAtDifference || leftSocketId.localeCompare(rightSocketId)
  }

  #setSocketPresence(socket: PresenceSocket, channel: string, user: PresenceUser): void {
    socket.data[PRESENCE_DATA_KEY] ??= {}
    socket.data[PRESENCE_DATA_KEY][channel] = user
  }

  #deleteSocketPresence(socket: PresenceSocket, channel: string): void {
    if (!socket.data[PRESENCE_DATA_KEY]) {
      return
    }

    delete socket.data[PRESENCE_DATA_KEY][channel]

    if (Object.keys(socket.data[PRESENCE_DATA_KEY]).length === 0) {
      delete socket.data[PRESENCE_DATA_KEY]
    }
  }
}
