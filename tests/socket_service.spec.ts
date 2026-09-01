import { createServer, type Server as HttpServer } from 'node:http'
import { Socket as NetSocket } from 'node:net'
import { randomBytes, randomUUID } from 'node:crypto'
import { test } from '@japa/runner'
import { memory } from '@boringnode/bus/transports/memory'
import { WebSocket } from 'ws'
import { BaseChannel, SocketResponseError } from '../src/base_channel.js'
import { ChannelRouter } from '../src/channel_router.js'
import { onMessage } from '../src/decorators.js'
import { PresenceManager } from '../src/presence_manager.js'
import { SocketService } from './helpers/socket_service.js'
import { broadcastChannel } from '../src/tracing_channels.js'
import type { SocketTransportConfig } from '../src/socket_bus.js'
import type { SocketConfig } from '../src/types.js'
import type { SocketBroadcastMessage } from '../src/types/tracing_channels.js'

type SocketAuthenticate = NonNullable<NonNullable<SocketConfig['websocket']>['authenticate']>

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

async function waitFor(assertion: () => boolean, timeout = 5000): Promise<void> {
  const startedAt = Date.now()

  while (Date.now() - startedAt < timeout) {
    if (assertion()) return
    await new Promise((resolve) => setTimeout(resolve, 25))
  }

  throw new Error('Timed out waiting for assertion')
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

async function mustResolve(promise: Promise<void>, timeout = 250): Promise<void> {
  let timeoutId: ReturnType<typeof setTimeout>

  try {
    await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error('Promise did not resolve')), timeout)
      }),
    ])
  } finally {
    clearTimeout(timeoutId!)
  }
}

async function connectClient(port: number): Promise<WebSocket> {
  const client = new WebSocket(`ws://127.0.0.1:${port}/socket`)

  await new Promise<void>((resolve, reject) => {
    client.once('open', resolve)
    client.once('error', reject)
  })

  return client
}

async function upgradeRawWebSocket(port: number, path = '/socket', headers: string[] = []) {
  const client = new NetSocket()
  const key = randomBytes(16).toString('base64')

  await new Promise<void>((resolve, reject) => {
    client.once('error', reject)
    client.connect(port, '127.0.0.1', resolve)
  })

  client.write(
    [
      `GET ${path} HTTP/1.1`,
      `Host: 127.0.0.1:${port}`,
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Key: ${key}`,
      'Sec-WebSocket-Version: 13',
      ...headers,
      '',
      '',
    ].join('\r\n')
  )

  const response = await new Promise<string>((resolve, reject) => {
    let response = ''
    const timeout = setTimeout(() => {
      client.off('data', onData)
      reject(new Error('Timed out waiting for raw WebSocket handshake'))
    }, 1000)

    function onData(chunk: Buffer) {
      response += chunk.toString('latin1')
      if (!response.includes('\r\n\r\n')) {
        return
      }

      clearTimeout(timeout)
      client.off('data', onData)
      resolve(response)
    }

    client.on('data', onData)
  })

  return { client, response }
}

async function connectRawWebSocket(port: number): Promise<NetSocket> {
  const { client, response } = await upgradeRawWebSocket(port)

  if (!response.startsWith('HTTP/1.1 101')) {
    client.destroy()
    throw new Error(`Unexpected raw WebSocket handshake: ${response}`)
  }

  return client
}

async function assertUpgradeRejectedByAuthentication(
  assert: { isTrue(value: boolean): void; equal(actual: unknown, expected: unknown): void },
  authenticate: SocketAuthenticate
): Promise<void> {
  const httpServer = createServer()
  const socket = new SocketService()

  await socket.boot(httpServer, { websocket: { authenticate } }, new ChannelRouter(), makeLogger())
  const port = await listen(httpServer)
  const { client, response } = await upgradeRawWebSocket(port)

  try {
    assert.isTrue(response.startsWith('HTTP/1.1 401 Unauthorized'))
    assert.equal(socket.connectionsCount, 0)
  } finally {
    client.destroy()
    await socket.close()
    await closeHttpServer(httpServer)
  }
}

function listenForEvents(client: WebSocket, event: string, received: unknown[]): void {
  client.on('message', (raw) => {
    const frame = JSON.parse(raw.toString())
    if (frame.type === 'event' && frame.event === event) {
      received.push(frame.data)
    }
  })
}

async function sendAndWaitForAck(
  client: WebSocket,
  message: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const id = randomUUID()

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      client.off('message', onMessage)
      reject(new Error(`Timed out waiting for ack ${id}`))
    }, 1000)

    function onMessage(raw: Buffer) {
      const frame = JSON.parse(raw.toString())

      if (frame.id !== id) {
        return
      }

      clearTimeout(timeout)
      client.off('message', onMessage)
      resolve(frame)
    }

    client.on('message', onMessage)
    client.send(JSON.stringify({ id, ...message }))
  })
}

async function waitForFrame(
  client: WebSocket,
  predicate: (frame: Record<string, unknown>) => boolean,
  timeout = 1000
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      client.off('message', onMessage)
      reject(new Error('Timed out waiting for frame'))
    }, timeout)

    function onMessage(raw: Buffer) {
      const frame = JSON.parse(raw.toString())

      if (!predicate(frame)) {
        return
      }

      clearTimeout(timeoutId)
      client.off('message', onMessage)
      resolve(frame)
    }

    client.on('message', onMessage)
  })
}

async function waitForClose(
  client: WebSocket,
  timeout = 1000
): Promise<{ code: number; reason: string }> {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      client.off('close', onClose)
      reject(new Error('Timed out waiting for socket close'))
    }, timeout)

    function onClose(code: number, reason: Buffer) {
      clearTimeout(timeoutId)
      client.off('close', onClose)
      resolve({ code, reason: reason.toString() })
    }

    client.on('close', onClose)
  })
}

function setBufferedAmount(connection: WebSocket, bufferedAmount: number): void {
  Object.defineProperty(connection, 'bufferedAmount', {
    value: bufferedAmount,
    configurable: true,
  })
}

function collectBroadcastTraces(): {
  messages: SocketBroadcastMessage[]
  unsubscribe(): void
} {
  const messages: SocketBroadcastMessage[] = []
  const handler = {
    start() {},
    end(message: SocketBroadcastMessage) {
      messages.push({ ...message })
    },
    asyncStart() {},
    asyncEnd() {},
    error() {},
  }

  broadcastChannel.subscribe(handler as any)

  return {
    messages,
    unsubscribe() {
      broadcastChannel.unsubscribe(handler as any)
    },
  }
}

function makeLogger() {
  return {
    warnings: [] as unknown[][],
    warn(...args: unknown[]) {
      this.warnings.push(args)
    },
    info() {},
  } as any
}

function makeTransportConfig(channel = `socket:test:${randomUUID()}`): SocketTransportConfig {
  return {
    driver: memory(),
    channel,
  }
}

test.group('socket service', () => {
  test('boots idempotently without leaking upgrade listeners', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    const logger = makeLogger()

    await Promise.all([
      socket.boot(httpServer, {}, router, logger),
      socket.boot(httpServer, {}, router, logger),
    ])

    assert.equal(httpServer.listenerCount('upgrade'), 1)

    await socket.close()

    assert.equal(httpServer.listenerCount('upgrade'), 0)
  })

  test('close wins a race with an in-progress boot', async ({ assert }) => {
    const startEntered = Promise.withResolvers<void>()
    const releaseStart = Promise.withResolvers<void>()
    const driver = () => {
      const transport = {
        setId() {
          return transport
        },
        onReconnect() {},
        publish() {},
        async subscribe() {
          startEntered.resolve()
          await releaseStart.promise
        },
        async unsubscribe() {},
        async disconnect() {},
      }
      return transport
    }
    const httpServer = createServer()
    const socket = new SocketService()
    const boot = socket.boot(
      httpServer,
      { transport: { driver: driver as any } },
      new ChannelRouter(),
      makeLogger()
    )
    await startEntered.promise

    const close = socket.close()
    releaseStart.resolve()
    await Promise.allSettled([boot, close])

    assert.equal(socket.status, 'stopped')
    assert.equal(httpServer.listenerCount('upgrade'), 0)
    assert.equal(socket.connectionsCount, 0)
  })

  test('forces cleanup when boot does not finish before the shutdown deadline', async ({
    assert,
  }) => {
    const startEntered = Promise.withResolvers<void>()
    const driver = () => {
      const transport = {
        setId() {
          return transport
        },
        onReconnect() {},
        publish() {},
        async subscribe() {
          startEntered.resolve()
          await new Promise(() => {})
        },
        async unsubscribe() {},
        async disconnect() {},
      }
      return transport
    }
    const httpServer = createServer()
    const socket = new SocketService()
    void socket
      .boot(
        httpServer,
        {
          websocket: { shutdownTimeout: 20 },
          transport: { driver: driver as any },
        },
        new ChannelRouter(),
        makeLogger()
      )
      .catch(() => {})
    await startEntered.promise

    await mustResolve(socket.close())
    await mustResolve(socket.boot(httpServer, {}, new ChannelRouter(), makeLogger()))

    assert.equal(socket.status, 'ready')
    assert.equal(httpServer.listenerCount('upgrade'), 1)
    await socket.close()
    await closeHttpServer(httpServer)
  })

  test('closes a stale transport that finishes booting after forced shutdown', async ({
    assert,
  }) => {
    const startEntered = Promise.withResolvers<void>()
    const releaseStart = Promise.withResolvers<void>()
    let transportActive = false
    let disconnectCalls = 0
    const driver = () => {
      const transport = {
        setId() {
          return transport
        },
        onReconnect() {},
        publish() {},
        async subscribe() {
          startEntered.resolve()
          await releaseStart.promise
          transportActive = true
        },
        async unsubscribe() {},
        async disconnect() {
          transportActive = false
          disconnectCalls += 1
        },
      }
      return transport
    }
    const httpServer = createServer()
    const socket = new SocketService()
    const staleBoot = socket
      .boot(
        httpServer,
        {
          websocket: { shutdownTimeout: 20 },
          transport: { driver: driver as any },
        },
        new ChannelRouter(),
        makeLogger()
      )
      .catch(() => {})
    await startEntered.promise

    await socket.close()
    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())
    releaseStart.resolve()
    await staleBoot

    try {
      assert.isFalse(transportActive)
      assert.equal(disconnectCalls, 2)
      assert.equal(socket.status, 'ready')
      assert.equal(httpServer.listenerCount('upgrade'), 1)
    } finally {
      await socket.close()
      await closeHttpServer(httpServer)
    }
  })

  test('requires a failed live runtime to close before rebooting', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    const logger = makeLogger()
    await socket.boot(httpServer, {}, router, logger)
    const server = socket.server

    socket.markFailed(new Error('Runtime failed'))

    assert.equal(socket.status, 'failed')
    assert.strictEqual(socket.server, server)
    await assert.rejects(
      () => socket.boot(httpServer, {}, router, logger),
      'Socket service must be closed before rebooting'
    )
    assert.equal(httpServer.listenerCount('upgrade'), 1)

    await socket.close()
    await socket.boot(httpServer, {}, router, logger)
    assert.equal(socket.status, 'ready')
    assert.equal(httpServer.listenerCount('upgrade'), 1)

    await socket.close()
    await closeHttpServer(httpServer)
  })

  test('keeps close failures idempotent and allows reboot after cleanup', async ({ assert }) => {
    const driver = () => {
      const transport = {
        setId() {
          return transport
        },
        onReconnect() {},
        publish() {},
        async subscribe() {},
        async unsubscribe() {},
        async disconnect() {
          throw new Error('Transport close failed')
        },
      }
      return transport
    }
    const httpServer = createServer()
    const socket = new SocketService()
    await socket.boot(
      httpServer,
      { transport: { driver: driver as any } },
      new ChannelRouter(),
      makeLogger()
    )

    const close = socket.close()
    await assert.rejects(() => close, 'Transport close failed')
    await assert.rejects(() => socket.close(), 'Transport close failed')
    assert.equal(socket.status, 'failed')
    assert.equal(socket.health().lastError, 'Transport close failed')
    assert.equal(httpServer.listenerCount('upgrade'), 0)

    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())
    assert.equal(socket.status, 'ready')
    await socket.close()
    await closeHttpServer(httpServer)
  })

  test('resolves close before the service boots', async ({ assert }) => {
    const socket = new SocketService()

    await mustResolve(socket.close())

    assert.equal(socket.connectionsCount, 0)
  })

  test('resolves close after the service has already closed', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()

    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())

    await mustResolve(socket.close())
    await mustResolve(socket.close())

    assert.equal(socket.connectionsCount, 0)
    await closeHttpServer(httpServer)
  })

  test('resolves close when a peer does not complete the WebSocket close handshake', async ({
    assert,
  }) => {
    const httpServer = createServer()
    const socket = new SocketService()

    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())
    const port = await listen(httpServer)
    const client = await connectRawWebSocket(port)

    try {
      await mustResolve(socket.close())

      assert.equal(socket.connectionsCount, 0)
    } finally {
      client.destroy()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  })

  test('rejects an upgrade whose authentication finishes during shutdown', async ({ assert }) => {
    let releaseAuthentication!: () => void
    let markAuthenticationStarted!: () => void
    const authenticationStarted = new Promise<void>((resolve) => {
      markAuthenticationStarted = resolve
    })
    const authenticationBlocked = new Promise<void>((resolve) => {
      releaseAuthentication = resolve
    })
    const httpServer = createServer()
    const socket = new SocketService()

    await socket.boot(
      httpServer,
      {
        websocket: {
          async authenticate() {
            markAuthenticationStarted()
            await authenticationBlocked
            return { id: 'user-1' }
          },
        },
      },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = new WebSocket(`ws://127.0.0.1:${port}/socket`)
    const statusCode = new Promise<number | undefined>((resolve, reject) => {
      client.once('unexpected-response', (_request, response) => {
        response.resume()
        resolve(response.statusCode)
      })
      client.once('open', () => reject(new Error('WebSocket unexpectedly connected')))
      client.once('error', () => {})
    })

    try {
      await authenticationStarted
      const closing = socket.close()
      releaseAuthentication()

      assert.notEqual(await statusCode, 101)
      await mustResolve(closing)
      assert.equal(socket.connectionsCount, 0)
    } finally {
      releaseAuthentication()
      client.terminate()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('finalizes presence subscriptions before close resolves', async ({ assert }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'presence/:roomId'
      static options = { presence: true }
      static leaveCount = 0
      static memberLeaveCount = 0

      getPresenceInfo() {
        return { id: 'user-1', data: { name: 'Ada' } }
      }

      async onMemberLeave() {
        await sleep(10)
        PresenceChannel.memberLeaveCount += 1
      }

      async onLeave() {
        await sleep(10)
        PresenceChannel.leaveCount += 1
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const presence = new PresenceManager()
    const router = new ChannelRouter()
    router.register(PresenceChannel as any)
    socket.setPresenceManager(presence)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'presence/general',
      })

      await socket.close()

      assert.equal(PresenceChannel.memberLeaveCount, 1)
      assert.equal(PresenceChannel.leaveCount, 1)
      assert.equal(await presence.count('presence/general'), 0)
    } finally {
      client.terminate()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  })

  test('waits for in-flight subscriptions before finalizing sockets', async ({ assert }) => {
    let releaseJoin!: () => void
    let markJoinStarted!: () => void
    const joinStarted = new Promise<void>((resolve) => {
      markJoinStarted = resolve
    })
    const joinBlocked = new Promise<void>((resolve) => {
      releaseJoin = resolve
    })

    class SlowPresenceChannel extends BaseChannel {
      static pattern = 'slow-presence/:roomId'
      static options = { presence: true }
      static leaveCount = 0

      getPresenceInfo() {
        return { id: 'user-1', data: { name: 'Ada' } }
      }

      async onJoin() {
        markJoinStarted()
        await joinBlocked
      }

      async onLeave() {
        SlowPresenceChannel.leaveCount += 1
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const presence = new PresenceManager()
    const router = new ChannelRouter()
    router.register(SlowPresenceChannel as any)
    socket.setPresenceManager(presence)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      void sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'slow-presence/general',
      }).catch(() => {})
      await joinStarted

      let closed = false
      const closing = socket.close().then(() => {
        closed = true
      })
      await sleep(20)
      assert.isFalse(closed)

      releaseJoin()
      await closing

      assert.equal(SlowPresenceChannel.leaveCount, 1)
      assert.equal(await presence.count('slow-presence/general'), 0)
    } finally {
      releaseJoin()
      client.terminate()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  })

  test('forces socket cleanup when shutdown finalizers exceed their deadline', async ({
    assert,
  }) => {
    const handlerStarted = Promise.withResolvers<void>()

    class HangingChannel extends BaseChannel {
      static pattern = 'hanging/:roomId'

      @onMessage('hang')
      async hang() {
        handlerStarted.resolve()
        await new Promise(() => {})
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(HangingChannel as any)
    await socket.boot(httpServer, { websocket: { shutdownTimeout: 20 } }, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      await sendAndWaitForAck(client, { type: 'subscribe', channel: 'hanging/general' })
      client.send(
        JSON.stringify({
          id: 'hanging-handler',
          type: 'message',
          channel: 'hanging/general',
          event: 'hang',
        })
      )
      await handlerStarted.promise

      await mustResolve(socket.close())

      assert.equal(socket.status, 'stopped')
      assert.equal(socket.connectionsCount, 0)
      assert.equal(socket.health().channels, 0)
      assert.equal(httpServer.listenerCount('upgrade'), 0)
    } finally {
      client.terminate()
      await closeHttpServer(httpServer)
    }
  })

  test('resolves close when the transport does not disconnect before the shutdown deadline', async ({
    assert,
  }) => {
    const disconnectStarted = Promise.withResolvers<void>()
    const driver = () => {
      const transport = {
        setId() {
          return transport
        },
        onReconnect() {},
        publish() {},
        async subscribe() {},
        async unsubscribe() {},
        async disconnect() {
          disconnectStarted.resolve()
          await new Promise(() => {})
        },
      }
      return transport
    }
    const httpServer = createServer()
    const socket = new SocketService()
    await socket.boot(
      httpServer,
      {
        websocket: { shutdownTimeout: 20 },
        transport: { driver: driver as any },
      },
      new ChannelRouter(),
      makeLogger()
    )

    const closing = socket.close()
    await disconnectStarted.promise
    await mustResolve(closing)

    assert.equal(socket.status, 'stopped')
    assert.equal(httpServer.listenerCount('upgrade'), 0)
    await closeHttpServer(httpServer)
  })

  test('boots without requiring distributed presence fetches', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    const presence = new PresenceManager()

    socket.setPresenceManager(presence)

    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())

    try {
      assert.deepEqual(await presence.snapshot('presence/general'), {
        channel: 'presence/general',
        users: [],
        count: 0,
      })
    } finally {
      await socket.close()
      httpServer.close()
    }
  })

  test('uses local presence snapshots after the distributed bus closes', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    const presence = new PresenceManager()
    socket.setPresenceManager(presence)

    await socket.boot(
      httpServer,
      { transport: makeTransportConfig() },
      new ChannelRouter(),
      makeLogger()
    )
    await socket.close()

    assert.deepEqual(await presence.snapshot('presence/general'), {
      channel: 'presence/general',
      users: [],
      count: 0,
    })
    await closeHttpServer(httpServer)
  })

  test('boots using only websocket configuration', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    const logger = makeLogger()

    await socket.boot(
      httpServer,
      {
        websocket: {
          path: '/socket',
        },
      },
      new ChannelRouter(),
      logger
    )

    await socket.close()
    httpServer.close()

    assert.lengthOf(logger.warnings, 0)
  })

  test('leaves unmatched upgrade paths for other upgrade listeners', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()

    await socket.boot(
      httpServer,
      {
        websocket: {
          path: '/socket',
        },
      },
      new ChannelRouter(),
      makeLogger()
    )
    httpServer.on('upgrade', (request, rawSocket) => {
      const url = new URL(request.url ?? '/', 'ws://localhost')
      if (url.pathname !== '/other') {
        return
      }

      rawSocket.write('HTTP/1.1 418 Other Upgrade\r\n\r\n')
      rawSocket.destroy()
    })
    const port = await listen(httpServer)
    const { client, response } = await upgradeRawWebSocket(port, '/other')

    try {
      assert.isTrue(response.startsWith('HTTP/1.1 418 Other Upgrade'))
      assert.equal(socket.connectionsCount, 0)
    } finally {
      client.destroy()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('authenticates a socket during the HTTP upgrade', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService<{ id: string }>()
    let connectedSocketId: string | null = null

    socket.on('connect', ({ socket: connectedSocket }) => {
      connectedSocketId = connectedSocket.id
    })

    await socket.boot(
      httpServer,
      {
        websocket: {
          authenticate() {
            return { id: 'user-1' }
          },
        },
      },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = new WebSocket(`ws://127.0.0.1:${port}/socket`, {
      headers: { 'x-request-id': 'req-1' },
    })

    try {
      await new Promise<void>((resolve, reject) => {
        client.once('open', resolve)
        client.once('error', reject)
      })

      const connectedSocket = socket.getSocket(connectedSocketId!)!

      assert.deepEqual(connectedSocket.user, { id: 'user-1' })
      assert.deepEqual(connectedSocket.getUserOrFail(), { id: 'user-1' })
      assert.deepEqual(connectedSocket.raw.data, {})
      assert.isDefined(connectedSocket.raw.httpContext)

      connectedSocket.user = undefined
      let error: unknown
      try {
        connectedSocket.getUserOrFail()
      } catch (cause) {
        error = cause
      }
      assert.instanceOf(error, SocketResponseError)
      assert.equal((error as Error).message, 'Unauthorized')
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects cross-origin browser upgrades before authentication', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    let authenticationCalls = 0

    await socket.boot(
      httpServer,
      {
        websocket: {
          authenticate() {
            authenticationCalls += 1
            return { id: 'user-1' }
          },
        },
      },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const { client, response } = await upgradeRawWebSocket(port, '/socket', [
      'Origin: https://attacker.example',
    ])

    try {
      assert.isTrue(response.startsWith('HTTP/1.1 403 Forbidden'))
      assert.equal(authenticationCalls, 0)
      assert.equal(socket.connectionsCount, 0)
    } finally {
      client.destroy()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('uses a configured origin policy instead of the same-origin default', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()

    await socket.boot(
      httpServer,
      { websocket: { origin: ['https://app.example'] } },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const sameOrigin = await upgradeRawWebSocket(port, '/socket', [
      `Origin: http://127.0.0.1:${port}`,
    ])
    const configuredOrigin = await upgradeRawWebSocket(port, '/socket', [
      'Origin: https://app.example',
    ])
    const withoutOrigin = await upgradeRawWebSocket(port)

    try {
      assert.isTrue(sameOrigin.response.startsWith('HTTP/1.1 403 Forbidden'))
      assert.isTrue(configuredOrigin.response.startsWith('HTTP/1.1 101 Switching Protocols'))
      assert.isTrue(withoutOrigin.response.startsWith('HTTP/1.1 101 Switching Protocols'))
    } finally {
      for (const { client } of [sameOrigin, configuredOrigin, withoutOrigin]) {
        client.destroy()
      }
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('supports the AdonisJS CORS origin value shapes', async ({ assert }) => {
    const cases: Array<{
      policy: NonNullable<NonNullable<SocketConfig['websocket']>['origin']>
      requestOrigin: string
      status: string
    }> = [
      { policy: true, requestOrigin: 'https://any.example', status: '101 Switching Protocols' },
      { policy: false, requestOrigin: 'https://any.example', status: '403 Forbidden' },
      { policy: '*', requestOrigin: 'https://any.example', status: '101 Switching Protocols' },
      {
        policy: 'https://one.example,https://two.example',
        requestOrigin: 'https://two.example',
        status: '101 Switching Protocols',
      },
      {
        policy: ['https://app.example'],
        requestOrigin: 'https://app.example',
        status: '101 Switching Protocols',
      },
    ]

    for (const testCase of cases) {
      const httpServer = createServer()
      const socket = new SocketService()

      await socket.boot(
        httpServer,
        { websocket: { origin: testCase.policy } },
        new ChannelRouter(),
        makeLogger()
      )
      const port = await listen(httpServer)
      const { client, response } = await upgradeRawWebSocket(port, '/socket', [
        `Origin: ${testCase.requestOrigin}`,
      ])

      try {
        assert.isTrue(response.startsWith(`HTTP/1.1 ${testCase.status}`))
      } finally {
        client.destroy()
        await socket.close()
        await closeHttpServer(httpServer)
      }
    }
  }).timeout(10_000)

  test('resolves allowed origins from the request origin and HTTP context', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    let receivedOrigin: string | undefined
    let receivedContext: unknown
    let authenticationCalls = 0

    await socket.boot(
      httpServer,
      {
        websocket: {
          origin(origin, httpContext) {
            receivedOrigin = origin
            receivedContext = httpContext
            return origin === 'https://app.example'
          },
          authenticate() {
            authenticationCalls += 1
            return { id: 'user-1' }
          },
        },
      },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const accepted = await upgradeRawWebSocket(port, '/socket', ['Origin: https://app.example'])
    const rejected = await upgradeRawWebSocket(port, '/socket', [
      'Origin: https://attacker.example',
    ])

    try {
      assert.isTrue(accepted.response.startsWith('HTTP/1.1 101 Switching Protocols'))
      assert.isTrue(rejected.response.startsWith('HTTP/1.1 403 Forbidden'))
      assert.equal(receivedOrigin, 'https://attacker.example')
      assert.isDefined(receivedContext)
      assert.equal(authenticationCalls, 1)
    } finally {
      accepted.client.destroy()
      rejected.client.destroy()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('reports rejected lifecycle listeners without failing the socket service', async ({
    assert,
  }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    const logger = makeLogger()

    socket.on('connect', async () => {
      throw new Error('connect listener failed')
    })

    await socket.boot(httpServer, {}, new ChannelRouter(), logger)
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      await waitFor(() => logger.warnings.length === 1)
      assert.equal(socket.status, 'ready')
      assert.match(String(logger.warnings[0][0]), /listener failed/)
      assert.equal((logger.warnings[0][2] as Error).message, 'connect listener failed')
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects the HTTP upgrade when authentication fails', async ({ assert }) => {
    await assertUpgradeRejectedByAuthentication(assert, () => false)
  }).timeout(10_000)

  test('rejects the HTTP upgrade when authentication returns undefined', async ({ assert }) => {
    await assertUpgradeRejectedByAuthentication(assert, (() => undefined) as SocketAuthenticate)
  }).timeout(10_000)

  test('rejects the HTTP upgrade when authentication throws', async ({ assert }) => {
    await assertUpgradeRejectedByAuthentication(assert, () => {
      throw new Error('Unauthorized')
    })
  }).timeout(10_000)

  test('logs authentication exceptions without exposing them to the client', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    const logger = makeLogger()
    const authenticationError = new Error('authentication backend secret')

    await socket.boot(
      httpServer,
      {
        websocket: {
          authenticate() {
            throw authenticationError
          },
        },
      },
      new ChannelRouter(),
      logger
    )
    const port = await listen(httpServer)
    const { client, response } = await upgradeRawWebSocket(port)

    try {
      const warnings = logger.warnings as unknown[][]
      assert.isTrue(response.startsWith('HTTP/1.1 401 Unauthorized'))
      assert.isTrue(warnings.some((warning) => warning.includes(authenticationError)))
      assert.notInclude(response, authenticationError.message)
    } finally {
      client.destroy()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('terminates sockets that miss the configured heartbeat pong', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    let disconnected = false

    socket.on('disconnect', () => {
      disconnected = true
    })

    await socket.boot(
      httpServer,
      {
        websocket: {
          pingInterval: '10ms',
          pingTimeout: '20ms',
        },
      },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = await connectRawWebSocket(port)

    try {
      await waitFor(() => disconnected && socket.connectionsCount === 0, 1000)

      assert.isTrue(client.destroyed)
    } finally {
      client.destroy()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('validates heartbeat durations when booting the service', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()

    await assert.rejects(
      () =>
        socket.boot(
          httpServer,
          {
            websocket: {
              pingTimeout: 'nope',
            },
          },
          new ChannelRouter(),
          makeLogger()
        ),
      'websocket.pingTimeout must be a positive duration'
    )

    assert.equal(socket.status, 'failed')
    assert.equal(socket.health().lastError, 'websocket.pingTimeout must be a positive duration')

    await socket.close()
    await closeHttpServer(httpServer)
  })

  test('validates websocket message limits when booting the service', async ({ assert }) => {
    const cases: Array<[SocketConfig['websocket'], string]> = [
      [{ maxPayload: 0 }, 'websocket.maxPayload must be a positive integer'],
      [{ maxQueuedMessages: 0 }, 'websocket.maxQueuedMessages must be a positive integer'],
      [
        { maxMessagesPerInterval: 0 },
        'websocket.maxMessagesPerInterval must be a positive integer',
      ],
      [
        { messageRateInterval: 'nope' },
        'websocket.messageRateInterval must be a positive duration',
      ],
      [{ maxBufferedAmount: 0 }, 'websocket.maxBufferedAmount must be a positive integer'],
      [{ maxOutboundPayload: 0 }, 'websocket.maxOutboundPayload must be a positive integer'],
      [
        { maxSubscriptionsPerSocket: 0 },
        'websocket.maxSubscriptionsPerSocket must be a positive integer',
      ],
      [{ maxChannelNameLength: 0 }, 'websocket.maxChannelNameLength must be a positive integer'],
    ]

    for (const [websocket, message] of cases) {
      const httpServer = createServer()
      const socket = new SocketService()

      await assert.rejects(
        () => socket.boot(httpServer, { websocket }, new ChannelRouter(), makeLogger()),
        message
      )

      await socket.close()
      await closeHttpServer(httpServer)
    }
  })

  test('cleans heartbeat timers atomically when the service closes', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    let missedPings = 0

    socket.on('connect', ({ socket: connectedSocket }) => {
      connectedSocket.raw.connection.ping = (() => {
        missedPings += 1
      }) as WebSocket['ping']
    })

    await socket.boot(
      httpServer,
      {
        websocket: {
          pingInterval: 10,
          pingTimeout: 1000,
        },
      },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      await waitFor(() => missedPings > 0, 1000)

      await mustResolve(socket.close())

      assert.equal(socket.connectionsCount, 0)
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects malformed subscribe payloads without creating a subscription', async ({
    assert,
  }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)
    const received: unknown[] = []

    listenForEvents(client, 'notice', received)

    try {
      const ack = await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 42,
      })

      assert.equal(ack.type, 'ack')
      assert.equal(ack.ok, false)
      assert.equal(ack.error, 'Invalid socket message')

      socket.to('chat/general').emit('notice', { text: 'should not arrive' })
      await sleep(75)

      assert.lengthOf(received, 0)
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects subscriptions beyond configured count and channel name limits', async ({
    assert,
  }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(
      httpServer,
      { websocket: { maxSubscriptionsPerSocket: 1, maxChannelNameLength: 12 } },
      router,
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      assert.equal(
        (await sendAndWaitForAck(client, { type: 'subscribe', channel: 'chat/first' })).ok,
        true
      )
      assert.deepInclude(
        await sendAndWaitForAck(client, { type: 'subscribe', channel: 'chat/second' }),
        { ok: false, error: 'Socket subscription limit exceeded' }
      )
      assert.deepInclude(
        await sendAndWaitForAck(client, { type: 'subscribe', channel: 'chat/name-too-long' }),
        { ok: false, error: 'Channel name is too long' }
      )
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rolls back subscriptions whose presence ack cannot be serialized', async ({ assert }) => {
    let joinHooks = 0

    class PresenceChannel extends BaseChannel {
      static pattern = 'presence/:roomId'
      static options = { presence: true }

      getPresenceInfo() {
        return { id: 'user-1', data: { name: 'Ada', secretCounter: 1n } }
      }

      async onMemberJoin() {
        joinHooks += 1
      }

      async onJoin() {
        joinHooks += 1
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    const presence = new PresenceManager()
    let subscribed = false
    router.register(PresenceChannel as any)
    socket.setPresenceManager(presence)
    socket.on('subscribe', () => {
      subscribed = true
    })

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      const ack = await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'presence/general',
      })

      assert.deepInclude(ack, {
        type: 'ack',
        ok: false,
        error: 'Subscription response is not serializable',
      })
      assert.isFalse(subscribed)
      assert.equal(joinHooks, 0)
      assert.equal(await presence.count('presence/general'), 0)
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('returns a correlated failure when a channel handler response cannot be serialized', async ({
    assert,
  }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'

      @onMessage('counter')
      counter() {
        return { counter: 1n }
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      assert.equal(
        (await sendAndWaitForAck(client, { type: 'subscribe', channel: 'chat/general' })).ok,
        true
      )

      const ack = await sendAndWaitForAck(client, {
        type: 'message',
        channel: 'chat/general',
        event: 'counter',
      })

      assert.deepInclude(ack, {
        type: 'ack',
        ok: false,
        error: 'Handler response is not serializable',
      })
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('only exposes intentional authorization and handler errors', async ({ assert }) => {
    class PublicChannel extends BaseChannel {
      static pattern = 'public/:roomId'
      static middlewares = [() => Promise.reject(new SocketResponseError('Visible denial'))]
      @onMessage('fail')
      fail() {
        return Promise.reject(new SocketResponseError('Visible handler failure'))
      }
    }

    class PrivateAuthorizationChannel extends BaseChannel {
      static pattern = 'private-auth/:roomId'
      static middlewares = [() => Promise.reject(new Error('database password leaked'))]
    }

    class PrivateHandlerChannel extends BaseChannel {
      static pattern = 'private-handler/:roomId'
      @onMessage('fail')
      fail() {
        return Promise.reject(new Error('internal token leaked'))
      }
    }

    class BrokenChannel extends BaseChannel {
      static pattern = 'broken/:roomId'

      constructor() {
        super()
        throw new Error('constructor secret leaked')
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    const logger = makeLogger()
    router.register(PublicChannel as any)
    router.register(PrivateAuthorizationChannel as any)
    router.register(PrivateHandlerChannel as any)
    router.register(BrokenChannel as any)

    await socket.boot(httpServer, {}, router, logger)
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      assert.deepInclude(
        await sendAndWaitForAck(client, { type: 'subscribe', channel: 'public/general' }),
        { ok: false, error: 'Visible denial' }
      )
      assert.deepInclude(
        await sendAndWaitForAck(client, {
          type: 'subscribe',
          channel: 'private-auth/general',
        }),
        { ok: false, error: 'Authorization failed' }
      )

      PublicChannel.middlewares = []
      assert.equal(
        (await sendAndWaitForAck(client, { type: 'subscribe', channel: 'public/general' })).ok,
        true
      )
      assert.deepInclude(
        await sendAndWaitForAck(client, {
          type: 'message',
          channel: 'public/general',
          event: 'fail',
        }),
        { ok: false, error: 'Visible handler failure' }
      )

      assert.equal(
        (
          await sendAndWaitForAck(client, {
            type: 'subscribe',
            channel: 'private-handler/general',
          })
        ).ok,
        true
      )
      assert.deepInclude(
        await sendAndWaitForAck(client, {
          type: 'message',
          channel: 'private-handler/general',
          event: 'fail',
        }),
        { ok: false, error: 'Handler error' }
      )
      assert.deepInclude(
        await sendAndWaitForAck(client, { type: 'subscribe', channel: 'broken/general' }),
        { ok: false, error: 'Unexpected socket error' }
      )

      const warnings = logger.warnings as unknown[][]
      assert.isTrue(
        warnings.some((warning) =>
          warning.some(
            (value) => value instanceof Error && value.message === 'database password leaked'
          )
        )
      )
      assert.isTrue(
        warnings.some((warning) =>
          warning.some(
            (value) => value instanceof Error && value.message === 'internal token leaked'
          )
        )
      )
      assert.isTrue(
        warnings.some((warning) =>
          warning.some(
            (value) => value instanceof Error && value.message === 'constructor secret leaked'
          )
        )
      )
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects malformed unsubscribe payloads without leaving the subscription', async ({
    assert,
  }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)
    const received: unknown[] = []

    listenForEvents(client, 'notice', received)

    try {
      const subscribeAck = await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'chat/general',
      })

      assert.equal(subscribeAck.ok, true)

      const unsubscribeAck = await sendAndWaitForAck(client, {
        type: 'unsubscribe',
        channel: null,
      })

      assert.equal(unsubscribeAck.type, 'ack')
      assert.equal(unsubscribeAck.ok, false)
      assert.equal(unsubscribeAck.error, 'Invalid socket message')

      socket.to('chat/general').emit('notice', { text: 'still subscribed' })
      await waitFor(() => received.length === 1)

      assert.deepEqual(received, [{ text: 'still subscribed' }])
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('emits unsubscribe only when a subscription is removed', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    const unsubscribedChannels: string[] = []
    router.register(ChatChannel as any)
    socket.on('unsubscribe', ({ channel }) => {
      unsubscribedChannels.push(channel)
    })

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      const subscribeAck = await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'chat/general',
      })
      const firstAck = await sendAndWaitForAck(client, {
        type: 'unsubscribe',
        channel: 'chat/general',
      })
      const duplicateAck = await sendAndWaitForAck(client, {
        type: 'unsubscribe',
        channel: 'chat/general',
      })

      assert.equal(subscribeAck.ok, true)
      assert.deepInclude(firstAck, { type: 'ack', ok: true })
      assert.deepInclude(duplicateAck, { type: 'ack', ok: true })
      await waitFor(() => unsubscribedChannels.length === 1)
      assert.deepEqual(unsubscribedChannels, ['chat/general'])
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects malformed channel message payloads without calling channel handlers', async ({
    assert,
  }) => {
    let handledMessages = 0

    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
      @onMessage('chat:message')
      async handleMessage() {
        handledMessages += 1
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      const subscribeAck = await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'chat/general',
      })

      assert.equal(subscribeAck.ok, true)

      const messageAck = await sendAndWaitForAck(client, {
        type: 'message',
        channel: 'chat/general',
        event: 42,
      })

      assert.equal(messageAck.type, 'ack')
      assert.equal(messageAck.ok, false)
      assert.equal(messageAck.error, 'Invalid socket message')

      await sleep(75)

      assert.equal(handledMessages, 0)
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('closes sockets that send payloads above the configured limit', async ({ assert }) => {
    let handledMessages = 0

    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
      @onMessage('chat:message')
      async handleMessage() {
        handledMessages += 1
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(
      httpServer,
      {
        websocket: {
          maxPayload: 256,
        },
      },
      router,
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      const subscribeAck = await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'chat/general',
      })

      assert.equal(subscribeAck.ok, true)

      const closed = waitForClose(client)
      client.send(
        JSON.stringify({
          id: 'too-large',
          type: 'message',
          channel: 'chat/general',
          event: 'chat:message',
          data: 'x'.repeat(512),
        })
      )

      const close = await closed
      assert.equal(close.code, 1009)
      await sleep(75)
      assert.equal(handledMessages, 0)
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('closes sockets that exceed the configured queued message limit', async ({ assert }) => {
    let handledMessages = 0
    let handlerStarted = false
    let releaseHandler: (() => void) | undefined

    class SlowChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
      @onMessage('chat:slow')
      async handleSlow() {
        handledMessages += 1
        handlerStarted = true
        await new Promise<void>((resolve) => {
          releaseHandler = resolve
        })
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(SlowChannel as any)

    await socket.boot(
      httpServer,
      {
        websocket: {
          maxQueuedMessages: 1,
        },
      },
      router,
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      const subscribeAck = await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'chat/general',
      })

      assert.equal(subscribeAck.ok, true)

      client.send(
        JSON.stringify({
          id: 'slow-message',
          type: 'message',
          channel: 'chat/general',
          event: 'chat:slow',
        })
      )
      await waitFor(() => handlerStarted)

      const closed = waitForClose(client)
      client.send(
        JSON.stringify({
          id: 'overflow-message',
          type: 'message',
          channel: 'chat/general',
          event: 'chat:overflow',
        })
      )

      const close = await closed
      assert.equal(close.code, 1008)
      releaseHandler?.()
      await sleep(75)
      assert.equal(handledMessages, 1)
    } finally {
      releaseHandler?.()
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('closes sockets that exceed the configured inbound message rate', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()

    await socket.boot(
      httpServer,
      {
        websocket: {
          maxMessagesPerInterval: 2,
          messageRateInterval: '1s',
        },
      },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      const firstPong = waitForFrame(client, (frame) => frame.id === 'rate-1')
      client.send(JSON.stringify({ id: 'rate-1', type: 'ping' }))
      assert.deepEqual(await firstPong, { id: 'rate-1', type: 'pong' })

      const secondPong = waitForFrame(client, (frame) => frame.id === 'rate-2')
      client.send(JSON.stringify({ id: 'rate-2', type: 'ping' }))
      assert.deepEqual(await secondPong, { id: 'rate-2', type: 'pong' })

      const closed = waitForClose(client)
      client.send(JSON.stringify({ id: 'rate-3', type: 'ping' }))

      const close = await closed
      assert.equal(close.code, 1008)
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('responds to protocol pings while channel messages are queued', async ({ assert }) => {
    let handlerStarted = false
    let releaseHandler: (() => void) | undefined

    class SlowChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
      @onMessage('chat:slow')
      async handleSlow() {
        handlerStarted = true
        await new Promise<void>((resolve) => {
          releaseHandler = resolve
        })
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(SlowChannel as any)

    await socket.boot(
      httpServer,
      {
        websocket: {
          maxQueuedMessages: 1,
        },
      },
      router,
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      const subscribeAck = await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'chat/general',
      })

      assert.equal(subscribeAck.ok, true)

      const slowAck = waitForFrame(client, (frame) => frame.id === 'slow-message')
      client.send(
        JSON.stringify({
          id: 'slow-message',
          type: 'message',
          channel: 'chat/general',
          event: 'chat:slow',
        })
      )

      await waitFor(() => handlerStarted)

      const pong = waitForFrame(client, (frame) => frame.id === 'ping-while-queued')
      client.send(JSON.stringify({ id: 'ping-while-queued', type: 'ping' }))

      assert.deepEqual(await pong, {
        id: 'ping-while-queued',
        type: 'pong',
      })

      releaseHandler?.()
      assert.equal((await slowAck).ok, true)
    } finally {
      releaseHandler?.()
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('emits to sockets subscribed to a channel and stops after unsubscribe', async ({
    assert,
  }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const firstClient = await connectClient(port)
    const secondClient = await connectClient(port)
    const firstReceived: unknown[] = []
    const secondReceived: unknown[] = []

    listenForEvents(firstClient, 'notice', firstReceived)
    listenForEvents(secondClient, 'notice', secondReceived)

    try {
      const firstSubscribeAck = await sendAndWaitForAck(firstClient, {
        type: 'subscribe',
        channel: 'chat/general',
      })
      const secondSubscribeAck = await sendAndWaitForAck(secondClient, {
        type: 'subscribe',
        channel: 'chat/general',
      })

      assert.equal(firstSubscribeAck.type, 'ack')
      assert.equal(firstSubscribeAck.ok, true)
      assert.equal(secondSubscribeAck.type, 'ack')
      assert.equal(secondSubscribeAck.ok, true)

      socket.to('chat/general').emit('notice', { text: 'hello' })
      await waitFor(() => firstReceived.length === 1 && secondReceived.length === 1)

      const firstUnsubscribeAck = await sendAndWaitForAck(firstClient, {
        type: 'unsubscribe',
        channel: 'chat/general',
      })

      assert.equal(firstUnsubscribeAck.type, 'ack')
      assert.equal(firstUnsubscribeAck.ok, true)

      socket.to('chat/general').emit('notice', { text: 'still here' })
      await waitFor(() => firstReceived.length === 1 && secondReceived.length === 2)

      assert.deepEqual(firstReceived, [{ text: 'hello' }])
      assert.deepEqual(secondReceived, [{ text: 'hello' }, { text: 'still here' }])
    } finally {
      firstClient.close()
      secondClient.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('closes slow channel broadcast recipients and keeps delivery traces accurate', async ({
    assert,
  }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    const connectedSocketIds: string[] = []
    const traces = collectBroadcastTraces()
    router.register(ChatChannel as any)

    socket.on('connect', ({ socket: connectedSocket }) => {
      connectedSocketIds.push(connectedSocket.id)
    })

    await socket.boot(httpServer, { websocket: { maxBufferedAmount: 1024 } }, router, makeLogger())
    const port = await listen(httpServer)
    const slowClient = await connectClient(port)
    const healthyClient = await connectClient(port)
    const slowReceived: unknown[] = []
    const healthyReceived: unknown[] = []

    listenForEvents(slowClient, 'notice', slowReceived)
    listenForEvents(healthyClient, 'notice', healthyReceived)

    try {
      await sendAndWaitForAck(slowClient, {
        type: 'subscribe',
        channel: 'chat/general',
      })
      await sendAndWaitForAck(healthyClient, {
        type: 'subscribe',
        channel: 'chat/general',
      })
      await waitFor(() => connectedSocketIds.length === 2)

      setBufferedAmount(socket.getSocket(connectedSocketIds[0])!.raw.connection, 1025)
      const slowClose = waitForClose(slowClient)

      socket.to('chat/general').emit('notice', { text: 'hello' })

      await waitFor(() => healthyReceived.length === 1)
      const closed = await slowClose

      assert.equal(closed.code, 1008)
      assert.equal(closed.reason, 'Socket outbound buffer limit exceeded')
      assert.deepEqual(slowReceived, [])
      assert.deepEqual(healthyReceived, [{ text: 'hello' }])
      assert.equal(traces.messages.at(-1)?.delivered, 1)
    } finally {
      traces.unsubscribe()
      slowClient.close()
      healthyClient.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('closes slow global broadcast recipients', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    const connectedSocketIds: string[] = []

    socket.on('connect', ({ socket: connectedSocket }) => {
      connectedSocketIds.push(connectedSocket.id)
    })

    await socket.boot(
      httpServer,
      { websocket: { maxBufferedAmount: 1024 } },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const slowClient = await connectClient(port)
    const healthyClient = await connectClient(port)
    const slowReceived: unknown[] = []
    const healthyReceived: unknown[] = []

    listenForEvents(slowClient, 'maintenance', slowReceived)
    listenForEvents(healthyClient, 'maintenance', healthyReceived)

    try {
      await waitFor(() => connectedSocketIds.length === 2)
      setBufferedAmount(socket.getSocket(connectedSocketIds[0])!.raw.connection, 1025)
      const slowClose = waitForClose(slowClient)

      socket.broadcast('maintenance', { active: true })

      await waitFor(() => healthyReceived.length === 1)
      const closed = await slowClose

      assert.equal(closed.code, 1008)
      assert.equal(closed.reason, 'Socket outbound buffer limit exceeded')
      assert.deepEqual(slowReceived, [])
      assert.deepEqual(healthyReceived, [{ active: true }])
    } finally {
      slowClient.close()
      healthyClient.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('closes slow sockets when emitting directly to a socket', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()
    let connectedSocketId: string | null = null

    socket.on('connect', ({ socket: connectedSocket }) => {
      connectedSocketId = connectedSocket.id
    })

    await socket.boot(
      httpServer,
      { websocket: { maxBufferedAmount: 1024 } },
      new ChannelRouter(),
      makeLogger()
    )
    const port = await listen(httpServer)
    const client = await connectClient(port)
    const received: unknown[] = []

    listenForEvents(client, 'direct', received)

    try {
      await waitFor(() => connectedSocketId !== null)
      const connectedSocket = socket.getSocket(connectedSocketId!)!
      setBufferedAmount(connectedSocket.raw.connection, 1025)
      const close = waitForClose(client)

      connectedSocket.emit('direct', { ok: true })

      const closed = await close

      assert.equal(closed.code, 1008)
      assert.equal(closed.reason, 'Socket outbound buffer limit exceeded')
      assert.deepEqual(received, [])
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('applies outbound backpressure to channel acknowledgements', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'

      @onMessage('reply')
      reply() {
        return { ok: true }
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    let connectedSocketId: string | undefined
    router.register(ChatChannel as any)
    socket.on('connect', ({ socket: connectedSocket }) => {
      connectedSocketId = connectedSocket.id
    })
    await socket.boot(httpServer, { websocket: { maxBufferedAmount: 1024 } }, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      assert.equal(
        (await sendAndWaitForAck(client, { type: 'subscribe', channel: 'chat/general' })).ok,
        true
      )
      setBufferedAmount(socket.getSocket(connectedSocketId!)!.raw.connection, 1025)
      const close = waitForClose(client)

      client.send(
        JSON.stringify({
          id: 'slow-ack',
          type: 'message',
          channel: 'chat/general',
          event: 'reply',
        })
      )

      const closed = await close
      assert.equal(closed.code, 1008)
      assert.equal(closed.reason, 'Socket outbound buffer limit exceeded')
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('closes a socket instead of sending an oversized acknowledgement', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'

      @onMessage('reply')
      reply() {
        return { content: 'x'.repeat(1024) }
      }
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)
    await socket.boot(httpServer, { websocket: { maxOutboundPayload: 256 } }, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      assert.equal(
        (await sendAndWaitForAck(client, { type: 'subscribe', channel: 'chat/general' })).ok,
        true
      )
      const close = waitForClose(client)

      client.send(
        JSON.stringify({
          id: 'oversized-ack',
          type: 'message',
          channel: 'chat/general',
          event: 'reply',
        })
      )

      const closed = await close
      assert.equal(closed.code, 1009)
      assert.equal(closed.reason, 'Socket outbound message too big')
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('does not publish a broadcast before validating its WebSocket frame', async ({ assert }) => {
    const driver = memory()()
    const publish = driver.publish.bind(driver)
    let publishCalls = 0
    driver.publish = (async (...args: Parameters<typeof publish>) => {
      publishCalls += 1
      return publish(...args)
    }) as typeof driver.publish

    const httpServer = createServer()
    const socket = new SocketService()
    await socket.boot(
      httpServer,
      {
        transport: {
          driver: () => driver,
          channel: `socket:test:${randomUUID()}`,
        },
      },
      new ChannelRouter(),
      makeLogger()
    )

    try {
      assert.throws(
        () => socket.broadcast('invalid', { counter: 1n }),
        'Do not know how to serialize a BigInt'
      )
      await sleep(25)
      assert.equal(publishCalls, 0)
    } finally {
      await socket.close()
      await closeHttpServer(httpServer)
    }
  })

  test('coordinates concurrent presence joins without serializing distributed snapshots', async ({
    assert,
  }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'presence/:roomId'
      static options = { presence: true }

      getPresenceInfo(socket: any) {
        return { id: socket.id, data: { name: socket.id } }
      }
    }

    const firstSnapshot = Promise.withResolvers<void>()
    const pendingSnapshots: Array<() => void> = []
    let releaseImmediately = false
    const presence = new PresenceManager()
    presence.setSocketFetcher((channel) => {
      if (releaseImmediately) return Promise.resolve(presence.getLocalSockets(channel))

      return new Promise((resolve) => {
        pendingSnapshots.push(() => resolve(presence.getLocalSockets(channel)))
        firstSnapshot.resolve()
      })
    })

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(PresenceChannel as any)
    socket.setPresenceManager(presence)
    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const clients = await Promise.all(Array.from({ length: 5 }, () => connectClient(port)))
    const subscriptions = clients.map((client) =>
      sendAndWaitForAck(client, { type: 'subscribe', channel: 'presence/general' })
    )

    try {
      await firstSnapshot.promise
      for (let turn = 0; turn < 10; turn++) {
        await new Promise<void>((resolve) => setImmediate(resolve))
      }

      const snapshotsStartedTogether = pendingSnapshots.length
      releaseImmediately = true
      pendingSnapshots.splice(0).forEach((release) => release())
      const acknowledgements = await Promise.all(subscriptions)

      assert.equal(snapshotsStartedTogether, 5)
      assert.isTrue(acknowledgements.every((ack) => ack.ok))
      assert.equal(socket.health().channels, 1)
    } finally {
      releaseImmediately = true
      pendingSnapshots.splice(0).forEach((release) => release())
      await Promise.allSettled(subscriptions)
      clients.forEach((client) => client.close())
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('synchronizes channel broadcasts across socket instances', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const transport = makeTransportConfig()
    const firstHttpServer = createServer()
    const secondHttpServer = createServer()
    const firstSocket = new SocketService()
    const secondSocket = new SocketService()
    const firstRouter = new ChannelRouter()
    const secondRouter = new ChannelRouter()
    firstRouter.register(ChatChannel as any)
    secondRouter.register(ChatChannel as any)

    await firstSocket.boot(firstHttpServer, { transport }, firstRouter, makeLogger())
    await secondSocket.boot(secondHttpServer, { transport }, secondRouter, makeLogger())
    const firstPort = await listen(firstHttpServer)
    const secondPort = await listen(secondHttpServer)
    const firstClient = await connectClient(firstPort)
    const secondClient = await connectClient(secondPort)
    const firstReceived: unknown[] = []
    const secondReceived: unknown[] = []

    listenForEvents(firstClient, 'notice', firstReceived)
    listenForEvents(secondClient, 'notice', secondReceived)

    try {
      await sendAndWaitForAck(firstClient, {
        type: 'subscribe',
        channel: 'chat/general',
      })
      await sendAndWaitForAck(secondClient, {
        type: 'subscribe',
        channel: 'chat/general',
      })

      firstSocket.to('chat/general').emit('notice', { text: 'hello cluster' })

      await waitFor(() => firstReceived.length === 1 && secondReceived.length === 1)

      assert.deepEqual(firstReceived, [{ text: 'hello cluster' }])
      assert.deepEqual(secondReceived, [{ text: 'hello cluster' }])
    } finally {
      firstClient.close()
      secondClient.close()
      await firstSocket.close()
      await secondSocket.close()
      await closeHttpServer(firstHttpServer)
      await closeHttpServer(secondHttpServer)
    }
  }).timeout(10_000)

  test('synchronizes channel broadcasts with exclusions across socket instances', async ({
    assert,
  }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const transport = makeTransportConfig()
    const firstHttpServer = createServer()
    const secondHttpServer = createServer()
    const firstSocket = new SocketService()
    const secondSocket = new SocketService()
    const firstRouter = new ChannelRouter()
    const secondRouter = new ChannelRouter()
    let secondSocketId: string | null = null
    firstRouter.register(ChatChannel as any)
    secondRouter.register(ChatChannel as any)

    secondSocket.on('connect', ({ socket }) => {
      secondSocketId = socket.id
    })

    await firstSocket.boot(firstHttpServer, { transport }, firstRouter, makeLogger())
    await secondSocket.boot(secondHttpServer, { transport }, secondRouter, makeLogger())
    const firstPort = await listen(firstHttpServer)
    const secondPort = await listen(secondHttpServer)
    const firstClient = await connectClient(firstPort)
    const secondClient = await connectClient(secondPort)
    const firstReceived: unknown[] = []
    const secondReceived: unknown[] = []

    listenForEvents(firstClient, 'notice', firstReceived)
    listenForEvents(secondClient, 'notice', secondReceived)

    try {
      await sendAndWaitForAck(firstClient, {
        type: 'subscribe',
        channel: 'chat/general',
      })
      await sendAndWaitForAck(secondClient, {
        type: 'subscribe',
        channel: 'chat/general',
      })
      await waitFor(() => secondSocketId !== null)

      firstSocket.to('chat/general').except(secondSocketId!).emit('notice', { text: 'filtered' })
      await waitFor(() => firstReceived.length === 1)
      await sleep(75)

      assert.deepEqual(firstReceived, [{ text: 'filtered' }])
      assert.lengthOf(secondReceived, 0)
    } finally {
      firstClient.close()
      secondClient.close()
      await firstSocket.close()
      await secondSocket.close()
      await closeHttpServer(firstHttpServer)
      await closeHttpServer(secondHttpServer)
    }
  }).timeout(10_000)

  test('synchronizes global broadcasts across socket instances', async ({ assert }) => {
    const transport = makeTransportConfig()
    const firstHttpServer = createServer()
    const secondHttpServer = createServer()
    const firstSocket = new SocketService()
    const secondSocket = new SocketService()

    await firstSocket.boot(firstHttpServer, { transport }, new ChannelRouter(), makeLogger())
    await secondSocket.boot(secondHttpServer, { transport }, new ChannelRouter(), makeLogger())
    const firstPort = await listen(firstHttpServer)
    const secondPort = await listen(secondHttpServer)
    const firstClient = await connectClient(firstPort)
    const secondClient = await connectClient(secondPort)
    const firstReceived: unknown[] = []
    const secondReceived: unknown[] = []

    listenForEvents(firstClient, 'maintenance', firstReceived)
    listenForEvents(secondClient, 'maintenance', secondReceived)

    try {
      firstSocket.broadcast('maintenance', { active: true })

      await waitFor(() => firstReceived.length === 1 && secondReceived.length === 1)

      assert.deepEqual(firstReceived, [{ active: true }])
      assert.deepEqual(secondReceived, [{ active: true }])
    } finally {
      firstClient.close()
      secondClient.close()
      await firstSocket.close()
      await secondSocket.close()
      await closeHttpServer(firstHttpServer)
      await closeHttpServer(secondHttpServer)
    }
  }).timeout(10_000)

  test('synchronizes presence snapshots across socket instances', async ({ assert }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'presence/:roomId'
      static options = { presence: true }

      getPresenceInfo(socket: any) {
        return {
          id: socket.id,
          data: {
            name: socket.id,
            activity: {
              lastSeen: new Date('2026-07-31T12:00:00.000Z'),
            },
          },
        }
      }
    }

    const transport = makeTransportConfig()
    transport.presenceTimeout = 20
    const firstHttpServer = createServer()
    const secondHttpServer = createServer()
    const firstSocket = new SocketService()
    const secondSocket = new SocketService()
    const firstPresence = new PresenceManager()
    const secondPresence = new PresenceManager()
    const firstRouter = new ChannelRouter()
    const secondRouter = new ChannelRouter()
    firstRouter.register(PresenceChannel as any)
    secondRouter.register(PresenceChannel as any)
    firstSocket.setPresenceManager(firstPresence)
    secondSocket.setPresenceManager(secondPresence)

    await firstSocket.boot(firstHttpServer, { transport }, firstRouter, makeLogger())
    await secondSocket.boot(secondHttpServer, { transport }, secondRouter, makeLogger())
    const firstPort = await listen(firstHttpServer)
    const secondPort = await listen(secondHttpServer)
    const firstClient = await connectClient(firstPort)
    const secondClient = await connectClient(secondPort)
    const firstUpdates: any[] = []

    listenForEvents(firstClient, 'presence:update', firstUpdates)

    try {
      const firstAck = await sendAndWaitForAck(firstClient, {
        type: 'subscribe',
        channel: 'presence/general',
      })

      assert.equal((firstAck.data as any).presenceData.count, 1)

      const secondAck = await sendAndWaitForAck(secondClient, {
        type: 'subscribe',
        channel: 'presence/general',
      })

      await waitFor(() => firstUpdates.length === 1)

      assert.equal((secondAck.data as any).presenceData.count, 2)
      assert.deepEqual((secondAck.data as any).presenceData.users[0].activity, {
        lastSeen: '2026-07-31T12:00:00.000Z',
      })
      assert.equal(firstUpdates[0].count, 2)
      assert.lengthOf(firstUpdates[0].users, 2)
      assert.deepEqual(firstUpdates[0].users[0].activity, {
        lastSeen: '2026-07-31T12:00:00.000Z',
      })

      await sendAndWaitForAck(secondClient, {
        type: 'unsubscribe',
        channel: 'presence/general',
      })

      await waitFor(() => firstUpdates.length === 2)

      assert.equal(firstUpdates[1].count, 1)
      assert.lengthOf(firstUpdates[1].users, 1)
    } finally {
      firstClient.close()
      secondClient.close()
      await firstSocket.close()
      await secondSocket.close()
      await closeHttpServer(firstHttpServer)
      await closeHttpServer(secondHttpServer)
    }
  }).timeout(10_000)
})
