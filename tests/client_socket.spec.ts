import { createServer, type Server as HttpServer } from 'node:http'
import { test } from '@japa/runner'
import { WebSocket, WebSocketServer } from 'ws'
import { BaseChannel } from '../src/base_channel.js'
import { ChannelRouter } from '../src/channel_router.js'
import { resolveChannel, Socket as ClientSocket } from '../src/client/socket.js'
import { Channel } from '../src/client/channel.js'
import type {
  ChannelAck,
  ChannelContract,
  PresenceData,
  SocketClientTransport,
  SubscribeResult,
} from '../src/client/types.js'
import { SocketService } from './helpers/socket_service.js'

async function listen(httpServer: HttpServer): Promise<number> {
  await new Promise<void>((resolve) => {
    httpServer.listen(0, '127.0.0.1', resolve)
  })

  const address = httpServer.address()
  if (!address || typeof address === 'string') {
    throw new Error('Unable to resolve test server port')
  }

  return address.port
}

async function closeHttpServer(httpServer: HttpServer): Promise<void> {
  if (!httpServer.listening) return

  await new Promise<void>((resolve, reject) => {
    httpServer.close((error) => {
      if (error) {
        reject(error)
      } else {
        resolve()
      }
    })
  })
}

async function closeWsServer(wsServer: WebSocketServer): Promise<void> {
  for (const client of wsServer.clients) {
    client.terminate()
  }

  await new Promise<void>((resolve) => {
    wsServer.close(() => resolve())
  })
}

async function waitFor(assertion: () => boolean, timeout = 5000): Promise<void> {
  const startedAt = Date.now()

  while (Date.now() - startedAt < timeout) {
    if (assertion()) return
    await new Promise((resolve) => setTimeout(resolve, 25))
  }

  throw new Error('Timed out waiting for assertion')
}

interface SubscribeReply {
  ok(data?: Record<string, unknown>): void
  fail(error: string): void
  close(): void
}

function createSocketServer(
  httpServer: HttpServer,
  onSubscribe: (channelName: string, reply: SubscribeReply) => void,
  onMessage?: (event: string) => ChannelAck,
  onWhisper?: (message: Record<string, unknown>) => void
) {
  const wsServer = new WebSocketServer({ server: httpServer, path: '/socket' })

  wsServer.on('connection', (socket) => {
    socket.on('message', (raw) => {
      const message = JSON.parse(raw.toString())

      if (message.type === 'subscribe') {
        onSubscribe(message.channel, {
          ok(data) {
            socket.send(JSON.stringify({ id: message.id, type: 'ack', ok: true, data }))
          },
          fail(error) {
            socket.send(JSON.stringify({ id: message.id, type: 'ack', ok: false, error }))
          },
          close() {
            socket.close()
          },
        })
      }

      if (message.type === 'message') {
        socket.send(
          JSON.stringify({
            id: message.id,
            type: 'ack',
            ...(onMessage?.(message.event) ?? { ok: true }),
          })
        )
      }

      if (message.type === 'whisper') {
        onWhisper?.(message)
        socket.send(JSON.stringify({ id: message.id, type: 'ack', ok: true }))
      }

      if (message.type === 'unsubscribe') {
        socket.send(JSON.stringify({ id: message.id, type: 'ack', ok: true }))
      }
    })
  })

  return wsServer
}

function broadcastChannelEvent(
  wsServer: WebSocketServer,
  channel: string,
  event: string,
  data: unknown
) {
  for (const client of wsServer.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: 'event', channel, event, data }))
    }
  }
}

test.group('client socket', () => {
  test('preserves websocket URL schemes and rejects unsupported protocols', async ({
    assert,
    cleanup,
  }) => {
    const urls: string[] = []
    const sockets: Array<{
      emit(event: string): void
    }> = []
    const runtime = globalThis as unknown as { WebSocket: unknown }
    const OriginalWebSocket = runtime.WebSocket

    class FakeBrowserWebSocket {
      static readonly OPEN = 1
      readyState = 0
      #listeners = new Map<string, Set<() => void>>()

      constructor(url: string) {
        urls.push(url)
        sockets.push(this)
      }

      addEventListener(event: string, listener: () => void): void {
        const listeners = this.#listeners.get(event) ?? new Set()
        listeners.add(listener)
        this.#listeners.set(event, listeners)
      }

      removeEventListener(event: string, listener: () => void): void {
        this.#listeners.get(event)?.delete(listener)
      }

      close(): void {}
      send(): void {}

      emit(event: string): void {
        this.#listeners.get(event)?.forEach((listener) => listener())
      }
    }

    runtime.WebSocket = FakeBrowserWebSocket
    cleanup(() => {
      runtime.WebSocket = OriginalWebSocket
    })

    for (const [url, expected] of [
      ['http://example.com', 'ws://example.com/custom'],
      ['https://example.com', 'wss://example.com/custom'],
      ['ws://example.com', 'ws://example.com/custom'],
      ['wss://example.com', 'wss://example.com/custom'],
      ['https://example.com/original?token=abc#ignored', 'wss://example.com/custom?token=abc'],
    ]) {
      const client = new ClientSocket({ url, path: '/custom', autoReconnect: false })
      const connection = client.connect()
      sockets.at(-1)!.emit('open')
      await connection
      client.disconnect()
      assert.equal(urls.at(-1), expected)
    }

    const invalidClient = new ClientSocket({ url: 'ftp://example.com', autoReconnect: false })
    await assert.rejects(() => invalidClient.connect(), 'Unsupported socket URL protocol: ftp:')
  })

  test('ignores a successful subscribe response received after timeout', async ({ assert }) => {
    const request = Promise.withResolvers<SubscribeResult>()
    const transport: SocketClientTransport = {
      connected: true,
      sendRequest: () => request.promise as Promise<never>,
      send() {},
      on() {},
      off() {},
    }
    const channel = new Channel<ChannelContract>('chat/general', transport)

    await assert.rejects(
      () => channel.subscribe({ timeout: 5 }),
      'Subscribe timeout for channel: chat/general'
    )
    request.resolve({ ok: true })
    await new Promise((resolve) => setTimeout(resolve, 0))

    assert.isFalse(channel.active)
  })

  test('orders timeout compensation before a new subscription generation', async ({ assert }) => {
    class SlowChannel extends BaseChannel {
      static pattern = 'chat/:roomId'

      async onJoin() {
        await new Promise((resolve) => setTimeout(resolve, 100))
      }
    }

    const httpServer = createServer()
    const socketService = new SocketService()
    const serverEvents: string[] = []
    socketService.on('subscribe', () => {
      serverEvents.push('subscribe')
    })
    socketService.on('unsubscribe', () => {
      serverEvents.push('unsubscribe')
    })
    const router = new ChannelRouter()
    router.register(SlowChannel)
    await socketService.boot(httpServer, {}, router, { warn() {}, info() {} } as any)
    const port = await listen(httpServer)
    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
      autoReconnect: false,
    })

    try {
      await client.connect()
      const channel = client.channel('chat/general')
      await assert.rejects(
        () => channel.subscribe({ timeout: 10 }),
        'Subscribe timeout for channel: chat/general'
      )

      await channel.subscribe()
      await waitFor(() => serverEvents.length === 3)

      assert.deepEqual(serverEvents, ['subscribe', 'unsubscribe', 'subscribe'])
      assert.equal(socketService.health().channels, 1)
      assert.isTrue(channel.active)

      await channel.unsubscribe()

      assert.deepEqual(serverEvents, ['subscribe', 'unsubscribe', 'subscribe', 'unsubscribe'])
      assert.equal(socketService.health().channels, 0)
      assert.isFalse(channel.active)
    } finally {
      client.disconnect()
      await socketService.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('shares an in-flight connection between concurrent connect calls', async ({ assert }) => {
    const httpServer = createServer()
    const wsServer = new WebSocketServer({ server: httpServer, path: '/socket' })
    const connections: WebSocket[] = []

    wsServer.on('connection', (socket) => {
      connections.push(socket)
    })

    const port = await listen(httpServer)
    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
    })

    try {
      const firstConnect = client.connect()
      const secondConnect = client.connect()

      assert.equal(firstConnect, secondConnect)

      await Promise.all([firstConnect, secondConnect])
      await waitFor(() => connections.length === 1)

      assert.equal(connections.length, 1)
      assert.isTrue(client.connected)
    } finally {
      client.disconnect()
      await closeWsServer(wsServer)
      await closeHttpServer(httpServer)
    }
  })

  test('resubscribes subscribed channels after reconnect without duplicating initial subscribe', async ({
    assert,
  }) => {
    const subscribedChannels: string[] = []
    const firstHttpServer = createServer()
    const firstWsServer = createSocketServer(firstHttpServer, (channelName, reply) => {
      subscribedChannels.push(channelName)
      reply.ok()
    })
    const port = await listen(firstHttpServer)

    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
    })

    try {
      await client.connect()
      await client[resolveChannel]('chat/general').subscribe()

      assert.deepEqual(subscribedChannels, ['chat/general'])

      await closeWsServer(firstWsServer)
      await closeHttpServer(firstHttpServer)
      await waitFor(() => client.state === 'disconnected')

      const secondHttpServer = createServer()
      const secondWsServer = createSocketServer(secondHttpServer, (channelName, reply) => {
        subscribedChannels.push(channelName)
        reply.ok()
      })

      try {
        await new Promise<void>((resolve) => {
          secondHttpServer.listen(port, '127.0.0.1', resolve)
        })

        await waitFor(() => subscribedChannels.length === 2)

        assert.deepEqual(subscribedChannels, ['chat/general', 'chat/general'])
      } finally {
        client.disconnect()
        await closeWsServer(secondWsServer)
        await closeHttpServer(secondHttpServer)
      }
    } finally {
      client.disconnect()
      await closeWsServer(firstWsServer)
      await closeHttpServer(firstHttpServer)
    }
  }).timeout(10_000)

  test('does not reconnect when automatic reconnection is disabled', async ({ assert }) => {
    const firstHttpServer = createServer()
    const firstWsServer = createSocketServer(firstHttpServer, (_channelName, reply) => {
      reply.ok()
    })
    const port = await listen(firstHttpServer)

    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
      autoReconnect: false,
      reconnectDelay: 25,
    })

    try {
      await client.connect()

      await closeWsServer(firstWsServer)
      await closeHttpServer(firstHttpServer)
      await waitFor(() => client.state === 'disconnected')

      const secondHttpServer = createServer()
      const connections: WebSocket[] = []
      const secondWsServer = new WebSocketServer({ server: secondHttpServer, path: '/socket' })
      secondWsServer.on('connection', (socket) => {
        connections.push(socket)
      })

      try {
        await new Promise<void>((resolve) => {
          secondHttpServer.listen(port, '127.0.0.1', resolve)
        })
        await new Promise((resolve) => setTimeout(resolve, 100))

        assert.lengthOf(connections, 0)
      } finally {
        await closeWsServer(secondWsServer)
        await closeHttpServer(secondHttpServer)
      }
    } finally {
      client.disconnect()
      await closeWsServer(firstWsServer)
      await closeHttpServer(firstHttpServer)
    }
  }).timeout(10_000)

  test('does not reconnect after the server deliberately closes the socket', async ({ assert }) => {
    const httpServer = createServer()
    const socketService = new SocketService()
    let connections = 0
    socketService.on('connect', ({ socket }) => {
      connections += 1
      socket.disconnect()
    })
    await socketService.boot(httpServer, {}, new ChannelRouter(), { warn() {}, info() {} } as any)
    const port = await listen(httpServer)
    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
      reconnectDelay: 5,
      reconnectMaxDelay: 5,
    })

    try {
      await client.connect()
      await waitFor(() => client.state === 'disconnected')
      await new Promise((resolve) => setTimeout(resolve, 40))

      assert.equal(connections, 1)
      assert.equal(client.state, 'disconnected')
    } finally {
      client.disconnect()
      await socketService.close()
      await closeHttpServer(httpServer)
    }
  })

  test('allows the close policy to reconnect after a deliberate server close', async ({
    assert,
  }) => {
    const httpServer = createServer()
    const wsServer = new WebSocketServer({ server: httpServer, path: '/socket' })
    let connections = 0
    wsServer.on('connection', (socket) => {
      connections += 1
      if (connections === 1) socket.close(4000, 'maintenance')
    })
    const port = await listen(httpServer)
    const closes: Array<{ code: number; reason: string }> = []
    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
      reconnectDelay: 5,
      reconnectMaxDelay: 5,
      shouldReconnect(event) {
        closes.push({ code: event.code, reason: event.reason })
        return true
      },
    })

    try {
      await client.connect()
      await waitFor(() => connections === 2)

      assert.deepEqual(closes, [{ code: 4000, reason: 'maintenance' }])
      assert.isTrue(client.connected)
    } finally {
      client.disconnect()
      await closeWsServer(wsServer)
      await closeHttpServer(httpServer)
    }
  })

  test('keeps protocol-looking object channel ack payloads under data', async ({ assert }) => {
    const httpServer = createServer()
    const wsServer = createSocketServer(
      httpServer,
      (_channelName, reply) => {
        reply.ok()
      },
      () => ({
        ok: true,
        data: {
          ok: false,
          error: 'domain value',
        },
      })
    )
    const port = await listen(httpServer)

    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
    })

    try {
      await client.connect()
      const channel = await client[resolveChannel]('chat/general').subscribe()

      assert.deepEqual(await channel.sendWithAck('message'), {
        ok: false,
        error: 'domain value',
      })
    } finally {
      client.disconnect()
      await closeWsServer(wsServer)
      await closeHttpServer(httpServer)
    }
  })

  test('keeps presenceData channel ack payloads under data', async ({ assert }) => {
    const domainPayload = {
      presenceData: {
        channel: 'domain/value',
        users: [],
        count: 0,
      },
    }
    const httpServer = createServer()
    const wsServer = createSocketServer(
      httpServer,
      (_channelName, reply) => {
        reply.ok()
      },
      () => ({
        ok: true,
        data: domainPayload,
      })
    )
    const port = await listen(httpServer)

    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
    })

    try {
      await client.connect()
      const channel = await client[resolveChannel]('chat/general').subscribe()

      assert.deepEqual(await channel.sendWithAck('message'), domainPayload)
    } finally {
      client.disconnect()
      await closeWsServer(wsServer)
      await closeHttpServer(httpServer)
    }
  })

  test('sends client whispers through the channel protocol', async ({ assert }) => {
    const whispers: Record<string, unknown>[] = []
    const httpServer = createServer()
    const wsServer = createSocketServer(
      httpServer,
      (_channelName, reply) => {
        reply.ok()
      },
      undefined,
      (message) => {
        whispers.push(message)
      }
    )
    const port = await listen(httpServer)

    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
    })

    try {
      await client.connect()
      const channel = await client[resolveChannel]('chat/general').subscribe()

      channel.whisper('typing', { active: true })

      await waitFor(() => whispers.length === 1)

      assert.containSubset(whispers[0], {
        type: 'whisper',
        channel: 'chat/general',
        event: 'typing',
        data: { active: true },
      })
    } finally {
      client.disconnect()
      await closeWsServer(wsServer)
      await closeHttpServer(httpServer)
    }
  })

  test('applies subscribe presence payloads from ack data', async ({ assert }) => {
    const presenceData: PresenceData = {
      channel: 'presence/general',
      users: [
        {
          id: 'user-1',
          name: 'Ada',
          joinedAt: '2026-06-03T00:00:00.000Z',
        },
      ],
      count: 1,
    }
    const httpServer = createServer()
    const wsServer = createSocketServer(httpServer, (_channelName, reply) => {
      reply.ok({ presenceData })
    })
    const port = await listen(httpServer)

    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
    })
    const hereUsers: unknown[] = []

    try {
      await client.connect()
      const channel = await client[resolveChannel]('presence/general')
        .here((users) => {
          hereUsers.push(users)
        })
        .subscribe()

      assert.deepEqual(hereUsers, [presenceData.users])
      assert.deepEqual(channel.users, presenceData.users)
      assert.equal(channel.count, 1)
    } finally {
      client.disconnect()
      await closeWsServer(wsServer)
      await closeHttpServer(httpServer)
    }
  })

  test('ignores malformed presence updates without replacing channel state', async ({ assert }) => {
    const presenceData: PresenceData = {
      channel: 'presence/general',
      users: [
        {
          id: 'user-1',
          name: 'Ada',
          joinedAt: '2026-06-03T00:00:00.000Z',
        },
      ],
      count: 1,
    }
    const httpServer = createServer()
    const wsServer = createSocketServer(httpServer, (_channelName, reply) => {
      reply.ok({ presenceData })
    })
    const port = await listen(httpServer)
    const snapshots: string[][] = []
    const client = new ClientSocket({ url: `http://127.0.0.1:${port}` })
    const channel = client[resolveChannel]('presence/general').here((users) =>
      snapshots.push(users.map((user) => user.name as string))
    )

    try {
      await client.connect()
      await channel.subscribe()

      broadcastChannelEvent(wsServer, 'presence/general', 'presence:update', {
        channel: 'presence/general',
        users: null,
        count: 0,
      })
      broadcastChannelEvent(wsServer, 'presence/general', 'presence:update', {
        channel: 'presence/general',
        users: [{ name: 'Broken', joinedAt: '2026-06-03T00:00:00.000Z' }],
        count: 1,
      })
      await new Promise((resolve) => setTimeout(resolve, 25))

      assert.deepEqual(snapshots, [['Ada']])
      assert.deepEqual(channel.users, presenceData.users)
      assert.equal(channel.count, 1)
    } finally {
      client.disconnect()
      await closeWsServer(wsServer)
      await closeHttpServer(httpServer)
    }
  })

  test('keeps channel callbacks and subscription intent when a reconnect subscribe fails before a later reconnect succeeds', async ({
    assert,
  }) => {
    const initialPresence: PresenceData = {
      channel: 'presence/general',
      users: [{ id: '1', name: 'Ada', joinedAt: '2026-06-03T08:00:00.000Z' }],
      count: 1,
    }
    const joinedPresence: PresenceData = {
      channel: 'presence/general',
      users: [
        { id: '1', name: 'Ada', joinedAt: '2026-06-03T08:00:00.000Z' },
        { id: '2', name: 'Bert', joinedAt: '2026-06-03T08:01:00.000Z' },
      ],
      count: 2,
    }
    const leftPresence: PresenceData = {
      channel: 'presence/general',
      users: [{ id: '2', name: 'Bert', joinedAt: '2026-06-03T08:01:00.000Z' }],
      count: 1,
    }

    const subscribedChannels: string[] = []
    const messages: string[] = []
    const hereSnapshots: string[][] = []
    const joiningUsers: string[] = []
    const leavingUsers: string[] = []
    const originalConsoleError = console.error

    const firstHttpServer = createServer()
    const firstWsServer = createSocketServer(firstHttpServer, (channelName, reply) => {
      subscribedChannels.push(`first:${channelName}`)
      reply.ok({ presenceData: initialPresence })
    })
    const port = await listen(firstHttpServer)

    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
    })
    const channel = client[resolveChannel]('presence/general')
      .listen<{ text: string }>('chat:message', (message) => messages.push(message.text))
      .here((users) => hereSnapshots.push(users.map((user) => user.name as string)))
      .joining((user) => joiningUsers.push(user.name as string))
      .leaving((user) => leavingUsers.push(user.name as string))

    try {
      console.error = () => {}

      await client.connect()
      await channel.subscribe()

      assert.isTrue(channel.subscribed)
      assert.deepEqual(subscribedChannels, ['first:presence/general'])
      assert.deepEqual(hereSnapshots, [['Ada']])

      await closeWsServer(firstWsServer)
      await closeHttpServer(firstHttpServer)
      await waitFor(() => client.state === 'disconnected')

      let resolveFailedSubscribe!: () => void
      const failedSubscribe = new Promise<void>((resolve) => {
        resolveFailedSubscribe = resolve
      })
      const secondHttpServer = createServer()
      const secondWsServer = createSocketServer(secondHttpServer, (channelName, reply) => {
        subscribedChannels.push(`second:${channelName}`)
        reply.fail('temporary subscribe failure')
        resolveFailedSubscribe()
        setTimeout(() => reply.close(), 10)
      })

      try {
        await new Promise<void>((resolve) => {
          secondHttpServer.listen(port, '127.0.0.1', resolve)
        })
        await failedSubscribe
        await waitFor(() => client.state === 'disconnected')

        assert.isTrue(channel.subscribed)

        await closeWsServer(secondWsServer)
        await closeHttpServer(secondHttpServer)

        let resolveSuccessfulResubscribe!: () => void
        const successfulResubscribe = new Promise<void>((resolve) => {
          resolveSuccessfulResubscribe = resolve
        })
        const thirdHttpServer = createServer()
        const thirdWsServer = createSocketServer(thirdHttpServer, (channelName, reply) => {
          subscribedChannels.push(`third:${channelName}`)
          reply.ok({ presenceData: initialPresence })
          resolveSuccessfulResubscribe()
        })

        try {
          await new Promise<void>((resolve) => {
            thirdHttpServer.listen(port, '127.0.0.1', resolve)
          })
          await successfulResubscribe
          await waitFor(() => channel.active)

          broadcastChannelEvent(thirdWsServer, 'presence/general', 'chat:message', {
            text: 'callback survived',
          })
          broadcastChannelEvent(
            thirdWsServer,
            'presence/general',
            'presence:update',
            joinedPresence
          )
          broadcastChannelEvent(thirdWsServer, 'presence/general', 'presence:update', leftPresence)

          await waitFor(
            () =>
              messages.length === 1 && joiningUsers.includes('Bert') && leavingUsers.includes('Ada')
          )

          assert.deepEqual(subscribedChannels, [
            'first:presence/general',
            'second:presence/general',
            'third:presence/general',
          ])
          assert.deepEqual(messages, ['callback survived'])
          assert.deepEqual(joiningUsers, ['Bert'])
          assert.deepEqual(leavingUsers, ['Ada'])
          assert.deepEqual(hereSnapshots, [['Ada'], ['Ada'], ['Ada', 'Bert'], ['Bert']])
          assert.isTrue(channel.subscribed)
        } finally {
          client.disconnect()
          await closeWsServer(thirdWsServer)
          await closeHttpServer(thirdHttpServer)
        }
      } finally {
        await closeWsServer(secondWsServer)
        await closeHttpServer(secondHttpServer)
      }
    } finally {
      client.disconnect()
      console.error = originalConsoleError
      await closeWsServer(firstWsServer)
      await closeHttpServer(firstHttpServer)
    }
  }).timeout(15_000)

  test('stops channel callbacks after explicit unsubscribe', async ({ assert }) => {
    const initialPresence: PresenceData = {
      channel: 'presence/general',
      users: [{ id: '1', name: 'Ada', joinedAt: '2026-06-03T08:00:00.000Z' }],
      count: 1,
    }
    const joinedPresence: PresenceData = {
      channel: 'presence/general',
      users: [
        { id: '1', name: 'Ada', joinedAt: '2026-06-03T08:00:00.000Z' },
        { id: '2', name: 'Bert', joinedAt: '2026-06-03T08:01:00.000Z' },
      ],
      count: 2,
    }

    const messages: string[] = []
    const hereSnapshots: string[][] = []
    const joiningUsers: string[] = []

    const httpServer = createServer()
    const wsServer = createSocketServer(httpServer, (_channelName, reply) => {
      reply.ok({ presenceData: initialPresence })
    })
    const port = await listen(httpServer)
    const client = new ClientSocket({
      url: `http://127.0.0.1:${port}`,
    })
    const channel = client[resolveChannel]('presence/general')
      .listen<{ text: string }>('chat:message', (message) => messages.push(message.text))
      .here((users) => hereSnapshots.push(users.map((user) => user.name as string)))
      .joining((user) => joiningUsers.push(user.name as string))

    try {
      await client.connect()
      await channel.subscribe()
      await channel.unsubscribe()

      assert.isFalse(channel.subscribed)
      assert.isFalse(channel.active)

      broadcastChannelEvent(wsServer, 'presence/general', 'chat:message', {
        text: 'should not fire',
      })
      broadcastChannelEvent(wsServer, 'presence/general', 'presence:update', joinedPresence)
      await new Promise((resolve) => setTimeout(resolve, 50))

      assert.deepEqual(messages, [])
      assert.deepEqual(joiningUsers, [])
      assert.deepEqual(hereSnapshots, [['Ada']])
    } finally {
      client.disconnect()
      await closeWsServer(wsServer)
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)
})
