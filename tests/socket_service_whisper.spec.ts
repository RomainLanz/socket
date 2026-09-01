import { createServer, type Server as HttpServer } from 'node:http'
import { randomUUID } from 'node:crypto'
import { test } from '@japa/runner'
import { WebSocket } from 'ws'
import { BaseChannel } from '../src/base_channel.js'
import { ChannelRouter } from '../src/channel_router.js'
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

async function waitFor(assertion: () => boolean, timeout = 5000): Promise<void> {
  const startedAt = Date.now()

  while (Date.now() - startedAt < timeout) {
    if (assertion()) return
    await new Promise((resolve) => setTimeout(resolve, 25))
  }

  throw new Error('Timed out waiting for assertion')
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

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

function setBufferedAmount(connection: WebSocket, bufferedAmount: number): void {
  Object.defineProperty(connection, 'bufferedAmount', {
    value: bufferedAmount,
    configurable: true,
  })
}

async function connectClient(port: number): Promise<WebSocket> {
  const client = new WebSocket(`ws://127.0.0.1:${port}/socket`)

  await new Promise<void>((resolve, reject) => {
    client.once('open', resolve)
    client.once('error', reject)
  })

  return client
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

function makeLogger() {
  return {
    warnings: [] as unknown[][],
    warn(...args: unknown[]) {
      this.warnings.push(args)
    },
    info() {},
  } as any
}

test.group('socket service whispers', () => {
  test('relays client whispers to other subscribed channel members only', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const sender = await connectClient(port)
    const peer = await connectClient(port)
    const outsider = await connectClient(port)
    const senderReceived: unknown[] = []
    const peerReceived: unknown[] = []
    const outsiderReceived: unknown[] = []

    listenForEvents(sender, 'client:typing', senderReceived)
    listenForEvents(peer, 'client:typing', peerReceived)
    listenForEvents(outsider, 'client:typing', outsiderReceived)

    try {
      assert.equal(
        (
          await sendAndWaitForAck(sender, {
            type: 'subscribe',
            channel: 'chat/general',
          })
        ).ok,
        true
      )
      assert.equal(
        (
          await sendAndWaitForAck(peer, {
            type: 'subscribe',
            channel: 'chat/general',
          })
        ).ok,
        true
      )

      const whisperAck = await sendAndWaitForAck(sender, {
        type: 'whisper',
        channel: 'chat/general',
        event: 'typing',
        data: { active: true },
      })

      assert.equal(whisperAck.ok, true)
      await waitFor(() => peerReceived.length === 1)
      await sleep(75)

      assert.deepEqual(senderReceived, [])
      assert.deepEqual(peerReceived, [{ active: true }])
      assert.deepEqual(outsiderReceived, [])
    } finally {
      sender.close()
      peer.close()
      outsider.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('closes slow whisper recipients without dropping the sender ack', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    const connectedSocketIds: string[] = []
    router.register(ChatChannel as any)

    socket.on('connect', ({ socket: connectedSocket }) => {
      connectedSocketIds.push(connectedSocket.id)
    })

    await socket.boot(httpServer, { websocket: { maxBufferedAmount: 1024 } }, router, makeLogger())
    const port = await listen(httpServer)
    const sender = await connectClient(port)
    const peer = await connectClient(port)
    const senderReceived: unknown[] = []
    const peerReceived: unknown[] = []

    listenForEvents(sender, 'client:typing', senderReceived)
    listenForEvents(peer, 'client:typing', peerReceived)

    try {
      assert.equal(
        (
          await sendAndWaitForAck(sender, {
            type: 'subscribe',
            channel: 'chat/general',
          })
        ).ok,
        true
      )
      assert.equal(
        (
          await sendAndWaitForAck(peer, {
            type: 'subscribe',
            channel: 'chat/general',
          })
        ).ok,
        true
      )
      await waitFor(() => connectedSocketIds.length === 2)

      setBufferedAmount(socket.getSocket(connectedSocketIds[1])!.raw.connection, 1025)
      const peerClose = waitForClose(peer)

      const whisperAck = await sendAndWaitForAck(sender, {
        type: 'whisper',
        channel: 'chat/general',
        event: 'typing',
        data: { active: true },
      })
      const closed = await peerClose
      await sleep(75)

      assert.equal(whisperAck.ok, true)
      assert.equal(closed.code, 1008)
      assert.equal(closed.reason, 'Socket outbound buffer limit exceeded')
      assert.deepEqual(senderReceived, [])
      assert.deepEqual(peerReceived, [])
    } finally {
      sender.close()
      peer.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects whispers from sockets that are not subscribed to the channel', async ({
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

    try {
      const whisperAck = await sendAndWaitForAck(client, {
        type: 'whisper',
        channel: 'chat/general',
        event: 'typing',
        data: { active: true },
      })

      assert.equal(whisperAck.ok, false)
      assert.equal(whisperAck.error, 'Not subscribed')
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)
})
