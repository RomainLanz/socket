import type { Serializable } from '@boringnode/bus/types/main'
import { PRESENCE_DATA_KEY, type PresenceSocket, type PresenceUser } from './presence_manager.js'

type SerializablePresenceUser = {
  [key: string]: Serializable
  id: string
  joinedAt: string
}

export interface SerializablePresenceSocket {
  [key: string]: Serializable
  id: string
  data: {
    [key: string]: Serializable
  }
}

/**
 * Transport representation of one socket participating in distributed presence.
 */
export class PresenceSocketFrame {
  private constructor(
    private readonly transportValue: SerializablePresenceSocket,
    private readonly socketValue: PresenceSocket
  ) {}

  /**
   * Create one transport frame from a local socket presence entry.
   */
  static fromSocket(channel: string, socket: PresenceSocket): PresenceSocketFrame {
    const user = socket.data[PRESENCE_DATA_KEY]?.[channel]
    if (!user || Number.isNaN(user.joinedAt.getTime())) {
      throw new Error(`Socket "${socket.id}" has no valid presence for channel "${channel}"`)
    }

    const transport: SerializablePresenceSocket = {
      id: socket.id,
      data: {
        [PRESENCE_DATA_KEY]: {
          [channel]: {
            // Keep custom fields. Convert only the presence timestamp.
            ...user,
            joinedAt: user.joinedAt.toISOString(),
          } as SerializablePresenceUser,
        },
      },
    }

    return new PresenceSocketFrame(transport, socket)
  }

  /**
   * Validate one transport value and restore its presence timestamps.
   */
  static fromTransport(value: unknown): PresenceSocketFrame | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return null
    }

    const transport = value as Record<string, unknown>

    // A valid frame needs a socket ID and one object for socket data.
    if (
      typeof transport.id !== 'string' ||
      !transport.data ||
      typeof transport.data !== 'object' ||
      Array.isArray(transport.data)
    ) {
      return null
    }

    const data = transport.data as Record<string, unknown>
    const presence = data[PRESENCE_DATA_KEY]

    // A socket can have no presence entries.
    if (presence === undefined) {
      return new PresenceSocketFrame(transport as SerializablePresenceSocket, {
        id: transport.id,
        data: { ...data },
      })
    }
    if (!presence || typeof presence !== 'object' || Array.isArray(presence)) {
      return null
    }

    // Validate all members before this frame becomes a PresenceSocket.
    const restoredPresence: Record<string, PresenceUser> = {}
    for (const [channel, candidate] of Object.entries(presence)) {
      if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)) {
        return null
      }

      const user = candidate as Record<string, unknown>
      if (
        typeof user.id !== 'string' ||
        typeof user.joinedAt !== 'string' ||
        Number.isNaN(Date.parse(user.joinedAt))
      ) {
        return null
      }

      // Keep custom fields. Restore only the presence timestamp.
      restoredPresence[channel] = {
        ...user,
        id: user.id,
        joinedAt: new Date(user.joinedAt),
      }
    }

    return new PresenceSocketFrame(transport as SerializablePresenceSocket, {
      id: transport.id,
      data: {
        ...data,
        [PRESENCE_DATA_KEY]: restoredPresence,
      },
    })
  }

  /**
   * Return the validated transport value.
   */
  toTransport(): SerializablePresenceSocket {
    return this.transportValue
  }

  /**
   * Return the socket value with restored presence timestamps.
   */
  toSocket(): PresenceSocket {
    return this.socketValue
  }
}
