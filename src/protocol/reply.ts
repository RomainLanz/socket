import type { ChannelAck, ServerProtocolMessage } from '../shared_types.js'
import type { ChannelSubscribeResult } from '../channel_subscriptions.js'

const INVALID_SOCKET_MESSAGE = 'Invalid socket message'

export class Reply {
  #id: string | undefined
  #ack: ChannelAck

  private constructor(id: string | undefined, ack: ChannelAck) {
    this.#id = id
    this.#ack = ack
  }

  static ok(id: string | undefined, data?: unknown): Reply {
    return new Reply(id, { ok: true, data })
  }

  static error(id: string | undefined, error: string): Reply {
    return new Reply(id, { ok: false, error })
  }

  static invalidMessage(id: string | undefined): ServerProtocolMessage {
    if (id === undefined) {
      return { type: 'error', error: INVALID_SOCKET_MESSAGE }
    }

    return Reply.error(id, INVALID_SOCKET_MESSAGE).toFrame()
  }

  static fromSubscribeResult(
    id: string | undefined,
    result: ChannelSubscribeResult | undefined
  ): Reply {
    if (!result) {
      return Reply.error(id, 'Socket service is not initialized')
    }

    if (!result.ack.ok) {
      return Reply.error(id, result.ack.error)
    }

    const data = result.ack.presenceData
      ? {
          presenceData: result.ack.presenceData,
        }
      : undefined

    return Reply.ok(id, data)
  }

  static fromChannelAck(id: string | undefined, ack: ChannelAck): Reply {
    return ack.ok ? Reply.ok(id, ack.data) : Reply.error(id, ack.error)
  }

  toFrame(): Extract<ServerProtocolMessage, { type: 'ack' }> {
    if (!this.#ack.ok) {
      return {
        id: this.#id,
        type: 'ack',
        ok: false,
        error: this.#ack.error,
      }
    }

    return this.#ack.data === undefined
      ? { id: this.#id, type: 'ack', ok: true }
      : { id: this.#id, type: 'ack', ok: true, data: this.#ack.data }
  }
}
