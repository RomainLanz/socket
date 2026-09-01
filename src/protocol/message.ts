import type {
  ChannelMessage,
  ClientProtocolMessage,
  ServerProtocolMessage,
} from '../shared_types.js'
import { Reply } from './reply.js'

type ClientMessageType = ClientProtocolMessage['type']

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export abstract class Message {
  abstract readonly valid: boolean
  abstract readonly type: ClientMessageType | 'invalid'

  static fromTransport(raw: string): TransportMessage {
    try {
      return Message.fromParsedTransport(JSON.parse(raw))
    } catch {
      return new InvalidMessage(undefined)
    }
  }

  private static fromParsedTransport(value: unknown): TransportMessage {
    if (!isRecord(value)) {
      return new InvalidMessage(undefined)
    }

    const id = typeof value.id === 'string' ? value.id : undefined

    if ('id' in value && typeof value.id !== 'string') {
      return new InvalidMessage(undefined)
    }

    if (!isClientMessageType(value.type)) {
      return new InvalidMessage(id)
    }

    if (value.type === 'ping') {
      return new PingMessage(id)
    }

    if (value.type === 'subscribe' && typeof value.channel === 'string') {
      return new SubscribeMessage(id, value.channel)
    }

    if (value.type === 'unsubscribe' && typeof value.channel === 'string') {
      return new UnsubscribeMessage(id, value.channel)
    }

    if (
      (value.type === 'message' || value.type === 'whisper') &&
      typeof value.channel === 'string' &&
      typeof value.event === 'string'
    ) {
      return new ChannelProtocolMessage(id, value.type, value.channel, value.event, value.data)
    }

    return new InvalidMessage(id)
  }
}

export class InvalidMessage extends Message {
  readonly valid = false
  readonly type = 'invalid'

  constructor(readonly id: string | undefined) {
    super()
  }

  toRejectionFrame(): ServerProtocolMessage {
    return Reply.invalidMessage(this.id)
  }
}

export class PingMessage extends Message {
  readonly valid = true
  readonly type = 'ping'

  constructor(readonly id: string | undefined) {
    super()
  }
}

export class SubscribeMessage extends Message {
  readonly valid = true
  readonly type = 'subscribe'

  constructor(
    readonly id: string | undefined,
    readonly channel: string
  ) {
    super()
  }
}

export class UnsubscribeMessage extends Message {
  readonly valid = true
  readonly type = 'unsubscribe'

  constructor(
    readonly id: string | undefined,
    readonly channel: string
  ) {
    super()
  }
}

export class ChannelProtocolMessage extends Message {
  readonly valid = true

  constructor(
    readonly id: string | undefined,
    readonly type: 'message' | 'whisper',
    readonly channel: string,
    readonly event: string,
    readonly data: unknown
  ) {
    super()
  }

  toChannelMessage(): ChannelMessage {
    return {
      channel: this.channel,
      event: this.event,
      data: this.data,
    }
  }
}

export type TransportMessage =
  | InvalidMessage
  | PingMessage
  | SubscribeMessage
  | UnsubscribeMessage
  | ChannelProtocolMessage

function isClientMessageType(value: unknown): value is ClientMessageType {
  return (
    value === 'ping' ||
    value === 'subscribe' ||
    value === 'unsubscribe' ||
    value === 'message' ||
    value === 'whisper'
  )
}
