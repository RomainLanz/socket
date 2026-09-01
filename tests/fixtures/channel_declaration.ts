import { BaseChannel } from '../../index.js'
import { onMessage } from '../../src/decorators.js'
import type { AuthenticatedSocket } from '../../src/types.js'

type ServerEvents = {
  'chat:message': { id: string; body: string }
}

export default class DeclarationChannel extends BaseChannel<unknown, ServerEvents> {
  static pattern = 'chat/:roomId'

  @onMessage('chat:send')
  async sendMessage(
    _socket: AuthenticatedSocket,
    data: { body: string }
  ): Promise<{ accepted: boolean }> {
    return { accepted: data.body.length > 0 }
  }
}

export interface AppSocket {
  readonly channels: {
    readonly 'chat/:roomId': {
      readonly params: { readonly roomId: string | number }
      readonly channel: typeof DeclarationChannel
      readonly handlers: { readonly 'chat:send': 'sendMessage' }
    }
  }
}
