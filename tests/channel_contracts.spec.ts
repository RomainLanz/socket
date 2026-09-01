import { test } from '@japa/runner'
import { BaseChannel } from '../src/base_channel.js'
import { Channel } from '../src/client/channel.js'
import { Socket } from '../src/client/socket.js'
import { onMessage } from '../src/decorators.js'
import type {
  ChannelBroadcastPayload,
  ChannelClientEventPayload,
  ChannelContractFor,
  ChannelParameters,
  SocketClientTransport,
} from '../src/client/types.js'
import type { AuthenticatedSocket } from '../src/types.js'

interface Message {
  id: string
  body: string
}

interface Acknowledgement {
  accepted: boolean
}

type ChatServerEvents = {
  'chat:message': Message
}

class ChatChannel extends BaseChannel<unknown, ChatServerEvents> {
  static pattern = 'chat/:roomId'

  @onMessage('chat:send')
  async sendMessage(
    _socket: AuthenticatedSocket<unknown>,
    _data: { body: string }
  ): Promise<Acknowledgement> {
    return { accepted: true }
  }

  @onMessage('chat:ping')
  ping(_socket: AuthenticatedSocket<unknown>, _data: undefined): void {}

  publish(message: Message) {
    this.broadcast('chat:message', message)
    this.broadcastExcept('socket-1', 'chat:message', message)

    // @ts-expect-error server event payload comes from ServerEvents
    this.broadcast('chat:message', { body: 'missing id' })
    // @ts-expect-error unknown server event
    this.broadcast('chat:other', message)
  }
}

class ClientOnlyChannel extends BaseChannel {
  static pattern = 'client-only'

  @onMessage('chat:send')
  sendMessage(_socket: AuthenticatedSocket<unknown>, _data: { body: string }): Acknowledgement {
    return { accepted: true }
  }

  publish() {
    // @ts-expect-error channels must declare their server events
    this.broadcast('anything', { remains: 'untyped' })
    // @ts-expect-error channels must declare their server events
    this.broadcastExcept('socket-1', 'anything', 1)
  }
}

class DecoratedChannel extends BaseChannel {
  static pattern = 'counter'

  @onMessage('counter:increment')
  async increment(
    _socket: AuthenticatedSocket<unknown>,
    data: { amount: number }
  ): Promise<{ total: number }> {
    return { total: data.amount }
  }

  @onMessage('counter:reset')
  reset(_socket: AuthenticatedSocket<unknown>): 'reset' {
    return 'reset'
  }

  @onMessage('counter:preview')
  preview(
    _socket: AuthenticatedSocket<unknown>,
    data?: { amount: number }
  ): { amount: number } | undefined {
    return data
  }
}

class OptionalChannel extends BaseChannel {
  static pattern = 'threads/:threadId?'

  @onMessage('thread:open')
  open(): 'opened' {
    return 'opened'
  }
}

class WildcardChannel extends BaseChannel {
  static pattern = 'files/*'

  @onMessage('file:open')
  open(): 'opened' {
    return 'opened'
  }
}

interface AppSocket {
  readonly channels: {
    readonly 'chat/:roomId': {
      readonly params: { readonly roomId: string | number }
      readonly channel: typeof ChatChannel
      readonly handlers: {
        readonly 'chat:send': 'sendMessage'
        readonly 'chat:ping': 'ping'
      }
    }
    readonly 'client-only': {
      readonly params: undefined
      readonly channel: typeof ClientOnlyChannel
      readonly handlers: { readonly 'chat:send': 'sendMessage' }
    }
    readonly 'counter': {
      readonly params: undefined
      readonly channel: typeof DecoratedChannel
      readonly handlers: {
        readonly 'counter:increment': 'increment'
        readonly 'counter:reset': 'reset'
        readonly 'counter:preview': 'preview'
      }
    }
    readonly 'threads/:threadId?': {
      readonly params: { readonly threadId?: string | number }
      readonly channel: typeof OptionalChannel
      readonly handlers: { readonly 'thread:open': 'open' }
    }
    readonly 'files/*': {
      readonly params: { readonly wildcard: string | number }
      readonly channel: typeof WildcardChannel
      readonly handlers: { readonly 'file:open': 'open' }
    }
  }
}

type IsNever<Value> = [Value] extends [never] ? true : false

const missingPatternIsRejected: IsNever<ChannelContractFor<AppSocket, 'missing'>> = true
const dynamicPatternIsRejected: IsNever<ChannelContractFor<AppSocket, string>> = true
const chatParameters: ChannelParameters<AppSocket, 'chat/:roomId'> = { roomId: 'general' }
const chatClientPayload: ChannelClientEventPayload<AppSocket, 'chat/:roomId', 'chat:send'> = {
  body: 'Hello',
}
const chatBroadcastPayload: ChannelBroadcastPayload<AppSocket, 'chat/:roomId', 'chat:message'> = {
  id: 'message-1',
  body: 'Hello',
}
// @ts-expect-error generated dynamic parameter names are enforced
const invalidChatParameters: ChannelParameters<AppSocket, 'chat/:roomId'> = { room: 'general' }
// @ts-expect-error broadcast payloads are inferred from the dynamic channel contract
const invalidChatBroadcastPayload: ChannelBroadcastPayload<
  AppSocket,
  'chat/:roomId',
  'chat:message'
> = { body: 'Hello' }
void [
  missingPatternIsRejected,
  dynamicPatternIsRejected,
  chatParameters,
  chatClientPayload,
  chatBroadcastPayload,
  invalidChatParameters,
  invalidChatBroadcastPayload,
]

function assertClientTypes(socket: Socket<AppSocket>, dynamicPattern: string) {
  const chat = socket.channel('chat/:roomId', { roomId: 'general' })
  chat.send('chat:send', { body: 'Hello' })
  chat.send('chat:ping')
  const ack: Promise<Acknowledgement> = chat.sendWithAck('chat:send', { body: 'Hello' })
  const pingAck: Promise<void> = chat.sendWithAck('chat:ping')
  chat.listen('chat:message', (message) => message.id)
  chat.stopListening('chat:message')
  void [ack, pingAck]

  // @ts-expect-error wrong client payload
  chat.send('chat:send', { text: 'Hello' })
  // @ts-expect-error unknown client event
  chat.send('chat:other', { body: 'Hello' })
  // @ts-expect-error unknown server event
  chat.listen('chat:other', () => {})
  // @ts-expect-error required pattern parameter is missing
  socket.channel('chat/:roomId')
  // @ts-expect-error concrete names are not registry keys
  socket.channel('chat/general')
  // @ts-expect-error dynamic patterns are not registry keys
  socket.channel(dynamicPattern, {})

  // @ts-expect-error optional patterns still use an explicit parameter object
  socket.channel('threads/:threadId?')
  socket.channel('threads/:threadId?', {}).send('thread:open')
  socket.channel('threads/:threadId?', { threadId: 42 }).send('thread:open')
  socket.channel('files/*', { wildcard: 'path/to/file' }).send('file:open')
  // @ts-expect-error wildcards require their explicit parameter
  socket.channel('files/*')

  const clientOnly = socket.channel('client-only')
  clientOnly.send('chat:send', { body: 'Hello' })
  // @ts-expect-error channels without declared server events cannot be listened to
  clientOnly.listen('anything', () => {})

  const counter = socket.channel('counter')
  // @ts-expect-error static patterns do not take a parameter object
  socket.channel('counter', {})
  const counterAck: Promise<{ total: number }> = counter.sendWithAck('counter:increment', {
    amount: 2,
  })
  const resetAck: Promise<'reset'> = counter.sendWithAck('counter:reset')
  counter.send('counter:preview')
  counter.send('counter:preview', { amount: 2 })
  const previewAck: Promise<{ amount: number } | undefined> = counter.sendWithAck('counter:preview')
  // @ts-expect-error decorator handler payload is inferred from parameter two
  counter.send('counter:increment', { amount: '2' })
  void [counterAck, resetAck, previewAck]
}

function assertUntypedClient(dynamicName: string) {
  const socket = new Socket()
  const channel = socket.channel(dynamicName)
  channel.send('anything', { stays: 'untyped' })
  channel.listen('anything', () => {})
}

function assertCorrelatedPatternTypes(
  socket: Socket<AppSocket>,
  pattern: 'client-only' | 'chat/:roomId'
) {
  // @ts-expect-error a static/dynamic union must be narrowed before parameters can be chosen safely
  socket.channel(pattern)
  // @ts-expect-error parameters cannot be paired with a pattern that might select a static channel
  socket.channel(pattern, { roomId: 'general' })

  if (pattern === 'chat/:roomId') {
    socket.channel(pattern, { roomId: 'general' }).listen('chat:message', (message) => message.id)
  } else {
    socket.channel(pattern).send('chat:send', { body: 'Hello' })
  }
}

void [assertClientTypes, assertUntypedClient, assertCorrelatedPatternTypes]

test.group('channel contracts', () => {
  test('dispatches decorated handlers and returns their acknowledgement', async ({ assert }) => {
    const result = await new ChatChannel().$handleMessage({} as AuthenticatedSocket, 'chat:send', {
      body: 'Hello',
    })

    assert.deepEqual(result, { accepted: true })
  })

  test('rejects unknown channel events', async ({ assert }) => {
    await assert.rejects(
      () => new ChatChannel().$handleMessage({} as AuthenticatedSocket, 'chat:unknown', undefined),
      'Unknown channel event: chat:unknown'
    )
  })

  test('constructs concrete names from explicit patterns and parameters', ({ assert }) => {
    const socket = new Socket<AppSocket>()

    assert.equal(socket.channel('chat/:roomId', { roomId: 'general' }).name, 'chat/general')
    assert.equal(socket.channel('threads/:threadId?', {}).name, 'threads')
    assert.equal(socket.channel('threads/:threadId?', { threadId: 42 }).name, 'threads/42')
    assert.equal(socket.channel('files/*', { wildcard: 'path/to/file' }).name, 'files/path/to/file')
  })

  test('keeps raw concrete names for an untyped socket', ({ assert }) => {
    const socket = new Socket()
    assert.equal(socket.channel('raw/concrete').name, 'raw/concrete')
    assert.equal(socket.channel('raw/*').name, 'raw/*')
    assert.equal(socket.channel(':literal').name, ':literal')
  })

  test('constructs an internal channel with an explicit contract', ({ assert }) => {
    const channel = new Channel<any>('chat/general', {} as SocketClientTransport)
    assert.equal(channel.name, 'chat/general')
  })
})
