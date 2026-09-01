import { test } from '@japa/runner'
import { StrictMode, createElement, useLayoutEffect } from 'react'
import { act, create, type ReactTestRenderer } from 'react-test-renderer'
import { computed } from 'vue'
import { BaseChannel } from '../src/base_channel.js'
import { Channel } from '../src/client/channel.js'
import { acquireChannel, acquireSocketOwnership } from '../src/client/framework.js'
import { createSocketHooks } from '../src/client/react.js'
import { resolveChannel, Socket } from '../src/client/socket.js'
import type { ConnectionState, EventHandler, SocketClientTransport } from '../src/client/types.js'
import { createSocketComposables } from '../src/client/vue.js'
import { onMessage } from '../src/decorators.js'

class ChatChannel extends BaseChannel<unknown, { message: { id: string; body: string } }> {
  @onMessage('send')
  send(_socket: unknown, _payload: { body: string }): { accepted: true } {
    return { accepted: true }
  }
}

class PersonPresenceChannel extends BaseChannel<unknown, { presence: { online: boolean } }> {
  @onMessage('ping')
  ping(_socket: unknown, _payload: { at: number }): { accepted: true } {
    return { accepted: true }
  }
}

interface AppSocket {
  readonly channels: {
    readonly 'chat/:room': {
      readonly params: { readonly room: string | number }
      readonly channel: typeof ChatChannel
      readonly handlers: { readonly send: 'send' }
    }
    readonly 'admin/people/:personType/:personId': {
      readonly params: {
        readonly personType: string | number
        readonly personId: string | number
      }
      readonly channel: typeof PersonPresenceChannel
      readonly handlers: { readonly ping: 'ping' }
    }
  }
}

const react = createSocketHooks<AppSocket>()
const vue = createSocketComposables<AppSocket>()

function assertFrameworkTypes() {
  const reactChannel = react.useChannel('chat/:room', { room: 'general' })!
  reactChannel.send('send', { body: 'hello' })
  react.useChannelEvent('chat/:room', { room: 'general' }, 'message', (message) => message.id)
  // @ts-expect-error generated channel patterns stay strict
  react.useChannel('missing')
  react.useChannelEvent(
    'chat/:room',
    { room: 'general' },
    'message',
    // @ts-expect-error server event payload is inferred from AppSocket
    (message: { missing: true }) => message
  )

  const vueChannel = vue.useChannel('chat/:room', () => ({ room: 'general' }))
  vueChannel.value?.send('send', { body: 'hello' })
  vue.useChannelEvent('chat/:room', { room: 'general' }, 'message', (message) => message.body)
  // @ts-expect-error generated channel patterns stay strict
  vue.useChannel('missing')

  const personParameters = computed(() => ({ personType: 'official', personId: '123' }))
  const personChannel = vue.useChannel('admin/people/:personType/:personId', personParameters)
  personChannel.value?.send('ping', { at: Date.now() })
  vue.useChannelEvent(
    'admin/people/:personType/:personId',
    personParameters,
    'presence',
    (presence) => presence.online
  )
  const reactPersonChannel = react.useChannel(
    'admin/people/:personType/:personId',
    personParameters.value
  )
  reactPersonChannel?.send('ping', { at: Date.now() })
  react.useChannelEvent(
    'admin/people/:personType/:personId',
    personParameters.value,
    'presence',
    (presence) => presence.online
  )
  vue.useChannelEvent(
    'admin/people/:personType/:personId',
    personParameters,
    'presence',
    // @ts-expect-error server event payload remains inferred for reactive parameters
    (presence: { missing: true }) => presence
  )
  // @ts-expect-error all required parameters must be provided
  vue.useChannel('admin/people/:personType/:personId', { personType: 'official' })
  // @ts-expect-error all required parameters must be provided
  react.useChannel('admin/people/:personType/:personId', { personType: 'official' })
}

function assertFrameworkPatternUnions(
  pattern: 'chat/:room' | 'admin/people/:personType/:personId'
) {
  // @ts-expect-error React parameters must stay correlated with the selected dynamic pattern
  react.useChannel(pattern, { room: 'general' })
  // @ts-expect-error Vue parameters must stay correlated with the selected dynamic pattern
  vue.useChannel(pattern, { room: 'general' })
}

void [assertFrameworkTypes, assertFrameworkPatternUnions]

class FakeChannel {
  active = false
  subscribed = false
  subscribeCalls = 0
  unsubscribeCalls = 0
  unsubscribeGate: Promise<void> | undefined
  managedSubscriptions = 0
  listeners = new Map<string, Set<EventHandler<any>>>()

  constructor(
    readonly name: string,
    private isConnected: () => boolean
  ) {}

  async subscribe(): Promise<this> {
    this.subscribeCalls++
    this.subscribed = true
    this.active = true
    return this
  }

  async unsubscribe(): Promise<void> {
    this.unsubscribeCalls++
    await this.unsubscribeGate
    this.subscribed = false
    this.active = false
    this.listeners.clear()
  }

  listen(event: string, handler: EventHandler<any>): this {
    const handlers = this.listeners.get(event) ?? new Set()
    handlers.add(handler)
    this.listeners.set(event, handlers)
    return this
  }

  $listen(event: string, handler: EventHandler<any>): void {
    this.listen(event, handler)
  }

  stopListening(event: string, handler?: EventHandler<any>): this {
    if (handler) this.listeners.get(event)?.delete(handler)
    else this.listeners.delete(event)
    return this
  }

  $stopListening(event: string, handler: EventHandler<any>): void {
    this.stopListening(event, handler)
  }

  emit(event: string, data: unknown): void {
    for (const handler of this.listeners.get(event) ?? []) handler(data)
  }

  $acquire(): { ready: Promise<FakeChannel>; release: () => Promise<void> } {
    this.managedSubscriptions++
    const ready = this.isConnected() ? this.subscribe() : Promise.resolve(this)
    let released = false
    return {
      ready,
      release: async () => {
        if (released) return
        released = true
        this.managedSubscriptions--
        if (this.managedSubscriptions === 0) await this.unsubscribe()
      },
    }
  }
}

class FakeSocket {
  connected = true
  state: ConnectionState = 'connected'
  connectCalls = 0
  disconnectCalls = 0
  channels = new Map<string, FakeChannel>()
  stateHandlers = new Set<EventHandler<ConnectionState>>()

  channel(name: string): FakeChannel {
    let channel = this.channels.get(name)
    if (!channel) {
      channel = new FakeChannel(name, () => this.connected)
      this.channels.set(name, channel)
    }
    return channel
  }

  [resolveChannel](name: string): FakeChannel {
    return this.channel(name)
  }

  onStateChange(handler: EventHandler<ConnectionState>): () => void {
    this.stateHandlers.add(handler)
    return () => this.stateHandlers.delete(handler)
  }

  async connect(): Promise<void> {
    this.connectCalls++
    this.setState('connected')
  }

  disconnect(): void {
    this.disconnectCalls++
    this.setState('disconnected')
  }

  setState(state: ConnectionState): void {
    this.state = state
    this.connected = state === 'connected'
    for (const handler of this.stateHandlers) handler(state)
    if (this.connected) {
      for (const channel of this.channels.values()) {
        if (channel.managedSubscriptions > 0 && !channel.active) void channel.subscribe()
      }
    }
  }
}

function managedSocket(fake: FakeSocket): Socket<AppSocket> {
  return fake as unknown as Socket<AppSocket>
}

async function flushTransitions(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0))
  await Promise.resolve()
}

class CoreTransport implements SocketClientTransport {
  connected = true
  subscribeCalls = 0
  unsubscribeCalls = 0
  subscribeReplies: Array<Promise<unknown>> = []
  unsubscribeReplies: Array<Promise<unknown>> = []
  listeners = new Map<string, Set<EventHandler<any>>>()

  sendRequest<T>(message: Record<string, unknown>): Promise<T> {
    if (message.type === 'subscribe') {
      this.subscribeCalls++
      return (this.subscribeReplies.shift() ?? Promise.resolve({ ok: true })) as Promise<T>
    }
    if (message.type === 'unsubscribe') {
      this.unsubscribeCalls++
      return (this.unsubscribeReplies.shift() ?? Promise.resolve({ ok: true })) as Promise<T>
    }
    return Promise.resolve({ ok: true }) as Promise<T>
  }

  send(): void {}

  on<T>(event: string, handler: EventHandler<T>): void {
    const handlers = this.listeners.get(event) ?? new Set()
    handlers.add(handler)
    this.listeners.set(event, handlers)
  }

  off<T>(event: string, handler: EventHandler<T>): void {
    this.listeners.get(event)?.delete(handler)
  }

  emit(event: string, data: unknown): void {
    for (const handler of this.listeners.get(event) ?? []) handler(data)
  }
}

class CoreSocket {
  readonly channelInstance: Channel<any>

  constructor(readonly transport: CoreTransport) {
    this.channelInstance = new Channel<any>('chat/general', transport)
  }

  get connected(): boolean {
    return this.transport.connected
  }

  channel(): Channel<any> {
    return this.channelInstance
  }

  [resolveChannel](): Channel<any> {
    return this.channelInstance
  }
}

function coreSocket(fake: CoreSocket): Socket<AppSocket> {
  return fake as unknown as Socket<AppSocket>
}

class BrowserWebSocket {
  static readonly OPEN = 1
  readyState = 0
  readonly requests: string[] = []
  autoReplyUnsubscribe = true
  #pendingUnsubscribes: string[] = []
  #listeners = new Map<string, Set<(event: { data?: string }) => void>>()

  addEventListener(event: string, listener: (event: { data?: string }) => void): void {
    const listeners = this.#listeners.get(event) ?? new Set()
    listeners.add(listener)
    this.#listeners.set(event, listeners)
  }

  removeEventListener(event: string, listener: (event: { data?: string }) => void): void {
    this.#listeners.get(event)?.delete(listener)
  }

  open(): void {
    this.readyState = BrowserWebSocket.OPEN
    this.#emit('open', {})
  }

  close(): void {
    this.readyState = 3
  }

  send(raw: string): void {
    const message = JSON.parse(raw) as { id: string; type: string }
    this.requests.push(message.type)
    if (
      message.type === 'subscribe' ||
      (message.type === 'unsubscribe' && this.autoReplyUnsubscribe)
    ) {
      queueMicrotask(() => {
        this.#emit('message', {
          data: JSON.stringify({ id: message.id, type: 'ack', ok: true }),
        })
      })
    } else if (message.type === 'unsubscribe') {
      this.#pendingUnsubscribes.push(message.id)
    }
  }

  replyToUnsubscribe(): void {
    const id = this.#pendingUnsubscribes.shift()
    if (id) this.#emit('message', { data: JSON.stringify({ id, type: 'ack', ok: true }) })
  }

  serverEvent(channel: string, event: string, data: unknown): void {
    this.#emit('message', {
      data: JSON.stringify({ type: 'event', channel, event, data }),
    })
  }

  #emit(event: string, payload: { data?: string }): void {
    for (const listener of this.#listeners.get(event) ?? []) listener(payload)
  }
}

test.group('framework channel lifecycle', () => {
  test('shares subscriptions and listeners until the final lease is released', async ({
    assert,
  }) => {
    const fake = new FakeSocket()
    const first = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    const second = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    const channel = fake.channel('chat/general')
    const messages: string[] = []

    first.lease.listen('message', (message: { body: string }) =>
      messages.push(`first:${message.body}`)
    )
    second.lease.listen('message', (message: { body: string }) =>
      messages.push(`second:${message.body}`)
    )
    await flushTransitions()

    assert.equal(channel.subscribeCalls, 1)
    channel.emit('message', { body: 'one' })
    assert.deepEqual(messages, ['first:one', 'second:one'])

    first.lease.release()
    await flushTransitions()
    assert.equal(channel.unsubscribeCalls, 0)
    channel.emit('message', { body: 'two' })
    assert.deepEqual(messages, ['first:one', 'second:one', 'second:two'])

    second.lease.release()
    await flushTransitions()
    assert.equal(channel.unsubscribeCalls, 1)
    assert.equal(fake.stateHandlers.size, 0)
  })

  test('coalesces teardown and reacquisition in the same turn', async ({ assert }) => {
    const fake = new FakeSocket()
    const first = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    await flushTransitions()

    first.lease.release()
    const second = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    await flushTransitions()

    const channel = fake.channel('chat/general')
    assert.equal(channel.subscribeCalls, 1)
    assert.equal(channel.unsubscribeCalls, 0)

    second.lease.release()
    await flushTransitions()
  })

  test('waits for a borrowed socket to connect before subscribing', async ({ assert }) => {
    const fake = new FakeSocket()
    fake.setState('disconnected')
    const acquired = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    await flushTransitions()
    assert.equal(fake.channel('chat/general').subscribeCalls, 0)

    fake.setState('connected')
    await flushTransitions()
    assert.equal(fake.channel('chat/general').subscribeCalls, 1)

    acquired.lease.release()
    await flushTransitions()
  })

  test('connects owned sockets without disconnecting strict-mode remounts', async ({ assert }) => {
    const fake = new FakeSocket()
    const socket = managedSocket(fake)
    const firstRelease = acquireSocketOwnership(socket)
    await Promise.resolve()
    assert.equal(fake.connectCalls, 1)

    firstRelease()
    const secondRelease = acquireSocketOwnership(socket)
    await Promise.resolve()
    assert.equal(fake.disconnectCalls, 0)
    assert.equal(fake.connectCalls, 1)

    secondRelease()
    await Promise.resolve()
    assert.equal(fake.disconnectCalls, 1)
  })

  test('serializes reacquisition while the final unsubscribe is pending', async ({ assert }) => {
    const fake = new FakeSocket()
    const channel = fake.channel('chat/general')
    const first = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    await flushTransitions()

    const deferred = Promise.withResolvers<void>()
    channel.unsubscribeGate = deferred.promise
    first.lease.release()
    await flushTransitions()
    assert.equal(channel.unsubscribeCalls, 1)

    const second = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    deferred.resolve()
    await flushTransitions()
    assert.equal(channel.subscribeCalls, 2)

    const third = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    second.lease.release()
    await flushTransitions()
    assert.equal(channel.unsubscribeCalls, 1)

    channel.unsubscribeGate = undefined
    third.lease.release()
    await flushTransitions()
    assert.equal(channel.unsubscribeCalls, 2)
  })

  test('keeps equal listener functions independently leased', async ({ assert }) => {
    const fake = new FakeSocket()
    const first = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    const second = acquireChannel(managedSocket(fake), 'chat/:room', { room: 'general' })
    let calls = 0
    const handler = () => calls++
    first.lease.listen('message', handler)
    second.lease.listen('message', handler)
    await flushTransitions()

    const channel = fake.channel('chat/general')
    channel.emit('message', {})
    assert.equal(calls, 2)
    first.lease.release()
    channel.emit('message', {})
    assert.equal(calls, 3)

    second.lease.release()
    await flushTransitions()
  })

  test('preserves a direct subscription acquired before a framework lease', async ({ assert }) => {
    const transport = new CoreTransport()
    const socket = new CoreSocket(transport)
    const channel = socket.channelInstance
    const received: string[] = []
    channel.listen('message', (message: string) => received.push(`direct:${message}`))
    await channel.subscribe()

    const framework = acquireChannel(coreSocket(socket), 'chat/:room', { room: 'general' })
    framework.lease.listen('message', (message: string) => received.push(`framework:${message}`))
    await flushTransitions()
    framework.lease.release()
    await flushTransitions()

    assert.isTrue(channel.subscribed)
    assert.isTrue(channel.active)
    assert.equal(transport.unsubscribeCalls, 0)
    transport.emit('channel:chat/general:event', { event: 'message', data: 'still-live' })
    assert.deepEqual(received, ['direct:still-live'])
    await channel.unsubscribe()
  })

  test('preserves a direct subscription acquired after a framework lease', async ({ assert }) => {
    const transport = new CoreTransport()
    const socket = new CoreSocket(transport)
    const framework = acquireChannel(coreSocket(socket), 'chat/:room', { room: 'general' })
    await flushTransitions()

    const channel = socket.channelInstance
    let directCalls = 0
    channel.listen('message', () => directCalls++)
    await channel.subscribe()
    framework.lease.release()
    await flushTransitions()

    assert.isTrue(channel.subscribed)
    assert.equal(transport.unsubscribeCalls, 0)
    transport.emit('channel:chat/general:event', { event: 'message', data: null })
    assert.equal(directCalls, 1)
    await channel.unsubscribe()
  })

  test('cancels a pending subscribe promptly and permits reacquisition', async ({ assert }) => {
    const transport = new CoreTransport()
    const firstReply = Promise.withResolvers<unknown>()
    transport.subscribeReplies.push(firstReply.promise)
    const socket = new CoreSocket(transport)
    const first = acquireChannel(coreSocket(socket), 'chat/:room', { room: 'general' })
    assert.equal(transport.subscribeCalls, 1)

    first.lease.release()
    await Promise.resolve()
    await Promise.resolve()
    assert.equal(transport.unsubscribeCalls, 1)
    assert.isFalse(socket.channelInstance.subscribed)

    const second = acquireChannel(coreSocket(socket), 'chat/:room', { room: 'general' })
    await flushTransitions()
    assert.equal(transport.subscribeCalls, 2)
    assert.strictEqual(second.channel, socket.channelInstance)
    assert.isTrue(socket.channelInstance.active)

    firstReply.resolve({ ok: true })
    second.lease.release()
    await flushTransitions()
    assert.equal(transport.unsubscribeCalls, 2)
  })

  test('preserves a direct listener acquired during a pending unsubscribe', async ({ assert }) => {
    const transport = new CoreTransport()
    const unsubscribeReply = Promise.withResolvers<unknown>()
    transport.unsubscribeReplies.push(unsubscribeReply.promise)
    const socket = new CoreSocket(transport)
    const framework = acquireChannel(coreSocket(socket), 'chat/:room', { room: 'general' })
    await flushTransitions()

    framework.lease.release()
    await Promise.resolve()
    assert.equal(transport.unsubscribeCalls, 1)

    let received = 0
    socket.channelInstance.listen('message', () => received++)
    const directSubscribe = socket.channelInstance.subscribe()
    unsubscribeReply.resolve({ ok: true })
    await directSubscribe
    transport.emit('channel:chat/general:event', { event: 'message', data: null })
    assert.equal(received, 1)

    await socket.channelInstance.unsubscribe()
  })

  test('keeps managed channel identity through disconnect, reconnect, and final release', async ({
    assert,
    cleanup,
  }) => {
    const runtime = globalThis as unknown as { WebSocket: unknown }
    const OriginalWebSocket = runtime.WebSocket
    const browserSockets: BrowserWebSocket[] = []
    runtime.WebSocket = class extends BrowserWebSocket {
      constructor() {
        super()
        browserSockets.push(this)
      }
    }
    cleanup(() => {
      runtime.WebSocket = OriginalWebSocket
    })

    const socket = new Socket<AppSocket>({ url: 'ws://example.test', autoReconnect: false })
    const acquired = acquireChannel(socket, 'chat/:room', { room: 'general' })
    const channel = socket.channel('chat/:room', { room: 'general' })
    assert.strictEqual(acquired.channel, channel)

    const firstConnection = socket.connect()
    browserSockets[0].open()
    await firstConnection
    await flushTransitions()
    assert.isTrue(channel.active)

    socket.disconnect()
    assert.strictEqual(socket.channel('chat/:room', { room: 'general' }), channel)
    assert.isFalse(channel.active)

    const secondConnection = socket.connect()
    browserSockets[1].open()
    await secondConnection
    await flushTransitions()
    assert.strictEqual(socket.channel('chat/:room', { room: 'general' }), acquired.channel)
    assert.isTrue(channel.active)

    acquired.lease.release()
    await flushTransitions()
    assert.deepEqual(
      browserSockets.flatMap((browser) => browser.requests),
      ['subscribe', 'subscribe', 'unsubscribe']
    )
    assert.isFalse(channel.subscribed)
    socket.disconnect()
  })

  test('keeps a reacquired framework listener across disconnect during unsubscribe', async ({
    assert,
    cleanup,
  }) => {
    const runtime = globalThis as unknown as { WebSocket: unknown }
    const OriginalWebSocket = runtime.WebSocket
    const browserSockets: BrowserWebSocket[] = []
    runtime.WebSocket = class extends BrowserWebSocket {
      constructor() {
        super()
        browserSockets.push(this)
      }
    }
    cleanup(() => {
      runtime.WebSocket = OriginalWebSocket
    })

    const socket = new Socket<AppSocket>({ url: 'ws://example.test', autoReconnect: false })
    const first = acquireChannel(socket, 'chat/:room', { room: 'general' })
    const firstConnection = socket.connect()
    browserSockets[0].open()
    await firstConnection
    await flushTransitions()

    browserSockets[0].autoReplyUnsubscribe = false
    first.lease.release()
    await Promise.resolve()
    const second = acquireChannel(socket, 'chat/:room', { room: 'general' })
    let received = 0
    second.lease.listen('message', () => received++)
    socket.disconnect()
    await flushTransitions()

    const secondConnection = socket.connect()
    browserSockets[1].open()
    await secondConnection
    await flushTransitions()
    browserSockets[1].serverEvent('chat/general', 'message', null)
    assert.equal(received, 1)

    second.lease.release()
    await flushTransitions()
    socket.disconnect()
  })

  test('clears direct intent on disconnect while preserving managed identity', async ({
    assert,
    cleanup,
  }) => {
    const runtime = globalThis as unknown as { WebSocket: unknown }
    const OriginalWebSocket = runtime.WebSocket
    const browserSockets: BrowserWebSocket[] = []
    runtime.WebSocket = class extends BrowserWebSocket {
      constructor() {
        super()
        browserSockets.push(this)
      }
    }
    cleanup(() => {
      runtime.WebSocket = OriginalWebSocket
    })

    const socket = new Socket<AppSocket>({ url: 'ws://example.test', autoReconnect: false })
    const framework = acquireChannel(socket, 'chat/:room', { room: 'general' })
    const channel = socket.channel('chat/:room', { room: 'general' })
    const firstConnection = socket.connect()
    browserSockets[0].open()
    await firstConnection
    await flushTransitions()
    let directCalls = 0
    let frameworkCalls = 0
    channel.listen('message', () => directCalls++)
    framework.lease.listen('message', () => frameworkCalls++)
    await channel.subscribe()

    socket.disconnect()
    assert.strictEqual(socket.channel('chat/:room', { room: 'general' }), channel)

    const secondConnection = socket.connect()
    browserSockets[1].open()
    await secondConnection
    await flushTransitions()
    browserSockets[1].serverEvent('chat/general', 'message', null)
    assert.equal(directCalls, 0)
    assert.equal(frameworkCalls, 1)
    assert.deepEqual(browserSockets[1].requests, ['subscribe'])

    framework.lease.release()
    await flushTransitions()
    assert.isFalse(channel.subscribed)
    socket.disconnect()
  })

  test('keeps a directly reacquired channel cached while leave is pending', async ({
    assert,
    cleanup,
  }) => {
    const runtime = globalThis as unknown as { WebSocket: unknown }
    const OriginalWebSocket = runtime.WebSocket
    const browserSockets: BrowserWebSocket[] = []
    runtime.WebSocket = class extends BrowserWebSocket {
      constructor() {
        super()
        browserSockets.push(this)
      }
    }
    cleanup(() => {
      runtime.WebSocket = OriginalWebSocket
    })

    const socket = new Socket<AppSocket>({ url: 'ws://example.test', autoReconnect: false })
    const connection = socket.connect()
    browserSockets[0].open()
    await connection
    const channel = socket.channel('chat/:room', { room: 'general' })
    await channel.subscribe()

    browserSockets[0].autoReplyUnsubscribe = false
    const leaving = socket.leave('chat/general')
    await Promise.resolve()
    let received = 0
    channel.listen('message', () => received++)
    const resubscribing = channel.subscribe()
    browserSockets[0].replyToUnsubscribe()
    await Promise.all([leaving, resubscribing])

    assert.strictEqual(socket.channel('chat/:room', { room: 'general' }), channel)
    assert.isTrue(channel.subscribed)
    assert.isTrue(channel.active)
    browserSockets[0].serverEvent('chat/general', 'message', null)
    assert.equal(received, 1)
    assert.deepEqual(browserSockets[0].requests, ['subscribe', 'unsubscribe', 'subscribe'])

    browserSockets[0].autoReplyUnsubscribe = true
    await channel.unsubscribe()
    socket.disconnect()
  })

  test('React publishes only the channel acquired after a pending leave settles', async ({
    assert,
    cleanup,
  }) => {
    const runtime = globalThis as unknown as {
      WebSocket: unknown
      IS_REACT_ACT_ENVIRONMENT?: boolean
    }
    const OriginalWebSocket = runtime.WebSocket
    const originalActEnvironment = runtime.IS_REACT_ACT_ENVIRONMENT
    const browserSockets: BrowserWebSocket[] = []
    runtime.WebSocket = class extends BrowserWebSocket {
      constructor() {
        super()
        browserSockets.push(this)
      }
    }
    runtime.IS_REACT_ACT_ENVIRONMENT = true
    cleanup(() => {
      runtime.WebSocket = OriginalWebSocket
      runtime.IS_REACT_ACT_ENVIRONMENT = originalActEnvironment
    })

    const socket = new Socket<AppSocket>({ url: 'ws://example.test', autoReconnect: false })
    const connection = socket.connect()
    browserSockets[0].open()
    await connection
    const staleChannel = socket.channel('chat/:room', { room: 'general' })
    await staleChannel.subscribe()
    browserSockets[0].autoReplyUnsubscribe = false
    const leaving = socket.leave('chat/general')
    await Promise.resolve()

    const renderedChannels: Array<Channel<any> | null> = []
    function Probe() {
      renderedChannels.push(react.useChannel('chat/:room', { room: 'general' }))
      return null
    }
    function SettleLeave() {
      useLayoutEffect(() => browserSockets[0].replyToUnsubscribe(), [])
      return null
    }

    // @ts-expect-error Scheduler's testing entrypoint does not publish declarations.
    const Scheduler = (await import('scheduler/unstable_mock.js')) as {
      unstable_flushUntilNextPaint(): void
      unstable_flushAllWithoutAsserting(): void
    }
    runtime.IS_REACT_ACT_ENVIRONMENT = false
    const renderer: ReactTestRenderer = create(
      createElement(
        StrictMode,
        null,
        createElement(
          react.SocketProvider,
          { socket },
          createElement(Probe),
          createElement(SettleLeave)
        )
      )
    )
    Scheduler.unstable_flushUntilNextPaint()
    await leaving
    assert.notStrictEqual(socket.channel('chat/:room', { room: 'general' }), staleChannel)

    runtime.IS_REACT_ACT_ENVIRONMENT = true
    await act(async () => Scheduler.unstable_flushAllWithoutAsserting())
    await flushTransitions()

    const published = renderedChannels.at(-1)
    assert.isNull(renderedChannels[0])
    assert.isNotNull(published)
    assert.notStrictEqual(published, staleChannel)
    assert.strictEqual(published, socket.channel('chat/:room', { room: 'general' }))
    assert.isTrue(published!.subscribed)

    browserSockets[0].autoReplyUnsubscribe = true
    await act(async () => renderer.unmount())
    await flushTransitions()
    assert.isFalse(published!.subscribed)
    socket.disconnect()
  })
})
