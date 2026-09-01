import { test } from '@japa/runner'
import { ClientSocketSession } from '../src/client/socket_session.js'

type FakeWebSocketListener = (event: { data?: unknown }) => void

class FakeWebSocket {
  static readonly OPEN = 1

  readonly sent: string[] = []
  readonly url: string
  readyState = FakeWebSocket.OPEN
  #listeners = new Map<string, Set<FakeWebSocketListener>>()

  constructor(url: string) {
    this.url = url
  }

  addEventListener(event: string, listener: FakeWebSocketListener): void {
    if (!this.#listeners.has(event)) {
      this.#listeners.set(event, new Set())
    }

    this.#listeners.get(event)!.add(listener)
  }

  removeEventListener(event: string, listener: FakeWebSocketListener): void {
    this.#listeners.get(event)?.delete(listener)
  }

  send(data: string): void {
    this.sent.push(data)
  }

  close(): void {
    this.readyState = 3
    this.emit('close')
  }

  emit(event: string, data?: unknown): void {
    this.#listeners.get(event)?.forEach((listener) => listener({ data }))
  }
}

test.group('client socket session', () => {
  test('correlates ack frames and routes server event frames without a WebSocket server', async ({
    assert,
  }) => {
    const sockets: FakeWebSocket[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })

    const presenceUpdates: unknown[] = []
    const channelEvents: unknown[] = []
    const socketEvents: unknown[] = []

    session.on('channel:chat/general:presence:update', (data) => {
      presenceUpdates.push(data)
    })
    session.on('channel:chat/general:event', (data) => {
      channelEvents.push(data)
    })
    session.on('server:notice', (data) => {
      socketEvents.push(data)
    })

    const connect = session.connect()
    sockets[0].emit('open')
    await connect

    const request = session.sendRequest(
      { type: 'message', channel: 'chat/general', event: 'ask' },
      100
    )

    assert.deepEqual(JSON.parse(sockets[0].sent[0]), {
      id: '1',
      type: 'message',
      channel: 'chat/general',
      event: 'ask',
    })

    sockets[0].emit('message', JSON.stringify({ id: '1', type: 'ack' }))
    sockets[0].emit('message', JSON.stringify({ id: '1', type: 'ack', ok: 'yes' }))
    sockets[0].emit(
      'message',
      JSON.stringify({ id: '1', type: 'ack', ok: true, error: 'contradictory' })
    )
    sockets[0].emit(
      'message',
      JSON.stringify({ id: '1', type: 'ack', ok: true, data: { value: 42 } })
    )
    assert.deepEqual(await request, { ok: true, data: { value: 42 } })

    const requestWithId = session.sendRequest({ id: 'caller-id', type: 'ping' }, 100)
    assert.deepEqual(JSON.parse(sockets[0].sent[1]), { id: '2', type: 'ping' })
    sockets[0].emit('message', JSON.stringify({ id: '2', type: 'ack', ok: true }))
    assert.deepEqual(await requestWithId, { ok: true, data: undefined })

    const failedRequest = session.sendRequest({ type: 'ping' }, 100)
    sockets[0].emit(
      'message',
      JSON.stringify({ id: '3', type: 'ack', ok: false, error: 'failed', data: null })
    )
    sockets[0].emit('message', JSON.stringify({ id: '3', type: 'ack', ok: false }))
    sockets[0].emit('message', JSON.stringify({ id: '3', type: 'ack', ok: false, error: 42 }))
    sockets[0].emit('message', JSON.stringify({ id: '3', type: 'ack', ok: false, error: 'failed' }))
    assert.deepEqual(await failedRequest, { ok: false, error: 'failed' })

    sockets[0].emit(
      'message',
      JSON.stringify({
        type: 'event',
        channel: 'chat/general',
        event: 'presence:update',
        data: { users: [] },
      })
    )
    sockets[0].emit(
      'message',
      JSON.stringify({
        type: 'event',
        channel: 'chat/general',
        event: 'chat:message',
        data: { text: 'hello' },
      })
    )
    sockets[0].emit(
      'message',
      JSON.stringify({ type: 'event', event: 'server:notice', data: { ok: true } })
    )
    sockets[0].emit('message', 'not json')
    sockets[0].emit('message', JSON.stringify({ type: 'event', event: 42, data: 'ignored' }))
    sockets[0].emit(
      'message',
      JSON.stringify({ type: 'event', channel: 42, event: 'server:notice', data: 'ignored' })
    )

    assert.deepEqual(presenceUpdates, [{ users: [] }])
    assert.deepEqual(channelEvents, [{ event: 'chat:message', data: { text: 'hello' } }])
    assert.deepEqual(socketEvents, [{ ok: true }])
  })

  test('isolates callback errors from connection state transitions', async ({
    assert,
    cleanup,
  }) => {
    const sockets: FakeWebSocket[] = []
    const reportedErrors: unknown[] = []
    const runtime = globalThis as typeof globalThis & { reportError?: (error: unknown) => void }
    const previousReportError = runtime.reportError
    runtime.reportError = (error) => reportedErrors.push(error)
    cleanup(() => {
      runtime.reportError = previousReportError
    })

    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })
    const observedStates: string[] = []

    session.onStateChange(() => {
      throw new Error('state callback failed')
    })
    session.onStateChange((state) => {
      observedStates.push(state)
    })
    session.on('connect', () => {
      throw new Error('connect callback failed')
    })

    const connection = session.connect()
    sockets[0].emit('open')
    await connection

    assert.isTrue(session.connected)
    assert.deepEqual(observedStates, ['connecting', 'connected'])
    assert.deepEqual(
      reportedErrors.map((error) => (error as Error).message),
      ['state callback failed', 'state callback failed', 'connect callback failed']
    )
  })

  test('consumes and reports rejected callback promises without blocking later callbacks', async ({
    assert,
    cleanup,
  }) => {
    const sockets: FakeWebSocket[] = []
    const reportedErrors: unknown[] = []
    const runtime = globalThis as typeof globalThis & { reportError?: (error: unknown) => void }
    const previousReportError = runtime.reportError
    runtime.reportError = (error) => reportedErrors.push(error)
    cleanup(() => {
      runtime.reportError = previousReportError
    })

    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })
    const calls: string[] = []
    session.on('notice', async () => {
      calls.push('rejecting')
      throw new Error('async callback failed')
    })
    session.on('notice', () => calls.push('following'))

    const connection = session.connect()
    sockets[0].emit('open')
    await connection
    sockets[0].emit('message', JSON.stringify({ type: 'event', event: 'notice' }))
    await Promise.resolve()
    await Promise.resolve()

    assert.deepEqual(calls, ['rejecting', 'following'])
    assert.deepEqual(
      reportedErrors.map((error) => (error as Error).message),
      ['async callback failed']
    )
  })

  test('allows retrying after WebSocket creation throws', async ({ assert }) => {
    const sockets: FakeWebSocket[] = []
    let attempts = 0
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      createWebSocket(url) {
        attempts += 1
        if (attempts === 1) {
          throw new Error('WebSocket unavailable')
        }

        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })

    await assert.rejects(() => session.connect(), 'WebSocket unavailable')
    assert.equal(session.state, 'disconnected')

    const connection = session.connect()
    sockets[0].emit('open')
    await connection

    assert.isTrue(session.connected)
    assert.equal(attempts, 2)
  })

  test('allows reconnecting immediately after a manual disconnect', async ({ assert }) => {
    const sockets: FakeWebSocket[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })

    const firstConnection = session.connect()
    sockets[0].emit('open')
    await firstConnection

    session.disconnect()
    const secondConnection = session.connect()

    assert.equal(sockets.length, 2)
    sockets[0].emit('close')
    assert.equal(session.state, 'connecting')

    sockets[1].emit('open')
    await secondConnection
    assert.isTrue(session.connected)

    const notices: unknown[] = []
    session.on('notice', (data) => notices.push(data))
    sockets[0].emit('message', JSON.stringify({ type: 'event', event: 'notice', data: 'stale' }))
    sockets[1].emit('message', JSON.stringify({ type: 'event', event: 'notice', data: 'current' }))
    assert.deepEqual(notices, ['current'])
  })

  test('stops opening when a connected state callback disconnects reentrantly', async ({
    assert,
  }) => {
    const sockets: FakeWebSocket[] = []
    let connectedCalls = 0
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
      onConnected() {
        connectedCalls += 1
      },
    })
    const events: string[] = []
    session.on('connect', () => events.push('connect'))
    session.onStateChange((state) => {
      if (state === 'connected' && sockets.length === 1) {
        session.disconnect()
      }
    })

    const firstConnection = session.connect()
    sockets[0].emit('open')

    await assert.rejects(() => firstConnection, 'Socket disconnected')
    assert.equal(session.state, 'disconnected')
    assert.isFalse(session.connected)
    assert.deepEqual(events, [])
    assert.equal(connectedCalls, 0)

    const secondConnection = session.connect()
    assert.equal(sockets.length, 2)
    sockets[1].emit('open')
    await secondConnection

    assert.isTrue(session.connected)
    assert.deepEqual(events, ['connect'])
    assert.equal(connectedCalls, 1)
  })

  test('stops before creating a WebSocket when a connecting callback disconnects', async ({
    assert,
  }) => {
    const sockets: FakeWebSocket[] = []
    const states: string[] = []
    const events: string[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })
    session.on('disconnect', () => events.push('disconnect'))
    session.onStateChange((state) => {
      states.push(state)
      if (state === 'connecting') session.disconnect()
    })

    await assert.rejects(() => session.connect(), 'Socket disconnected')

    assert.deepEqual(states, ['connecting', 'disconnected'])
    assert.deepEqual(events, ['disconnect'])
    assert.lengthOf(sockets, 0)
  })

  test('replaces a failed attempt even when its close event has not arrived', async ({
    assert,
  }) => {
    const sockets: FakeWebSocket[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      reconnectDelay: 0,
      reconnectMaxDelay: 0,
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })

    const failed = session.connect()
    sockets[0].emit('error')
    await assert.rejects(() => failed, 'WebSocket connection failed')
    assert.equal(session.state, 'disconnected')

    const replacement = session.connect()
    assert.lengthOf(sockets, 2)
    assert.equal(session.state, 'connecting')

    sockets[0].emit('open')
    sockets[0].emit('close')
    assert.equal(session.state, 'connecting')

    sockets[1].emit('open')
    await replacement
    assert.isTrue(session.connected)
  })

  test('continues automatic retries after an error without a close event', async ({ assert }) => {
    const sockets: FakeWebSocket[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      reconnectDelay: 0,
      reconnectMaxDelay: 0,
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })

    const initial = session.connect()
    sockets[0].emit('open')
    await initial
    sockets[0].emit('close')
    await new Promise((resolve) => setTimeout(resolve, 5))

    assert.lengthOf(sockets, 2)
    sockets[1].emit('error')
    await new Promise((resolve) => setTimeout(resolve, 5))

    assert.lengthOf(sockets, 3)
    assert.equal(session.state, 'connecting')
    session.disconnect()
  })

  test('cancels a scheduled retry when connect is called manually', async ({ assert }) => {
    const sockets: FakeWebSocket[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      reconnectDelay: 10,
      reconnectMaxDelay: 10,
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })

    const initial = session.connect()
    sockets[0].emit('open')
    await initial
    sockets[0].emit('close')

    const manual = session.connect()
    await new Promise((resolve) => setTimeout(resolve, 20))
    assert.lengthOf(sockets, 2)

    sockets[1].emit('open')
    await manual
  })

  test('does not schedule a retry over a synchronous reconnect callback', async ({ assert }) => {
    const sockets: FakeWebSocket[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      reconnectDelay: 10,
      reconnectMaxDelay: 10,
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })

    const initial = session.connect()
    sockets[0].emit('open')
    await initial
    session.onStateChange((state) => {
      if (state === 'disconnected') void session.connect()
    })

    sockets[0].emit('close')
    await new Promise((resolve) => setTimeout(resolve, 20))
    assert.lengthOf(sockets, 2)
    assert.equal(session.state, 'connecting')

    sockets[1].emit('open')
    assert.isTrue(session.connected)
  })

  test('preserves retry backoff when a connected callback closes reentrantly', async ({
    assert,
    cleanup,
  }) => {
    const sockets: FakeWebSocket[] = []
    const retryTimers: Array<{ delay: number; run(): void }> = []
    const originalSetTimeout = globalThis.setTimeout
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      reconnectDelay: 1,
      reconnectMaxDelay: 10,
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })
    cleanup(() => {
      globalThis.setTimeout = originalSetTimeout
    })
    globalThis.setTimeout = ((handler: () => void, timeout?: number) => {
      retryTimers.push({ delay: Number(timeout), run: handler })
      return Object.create(null) as ReturnType<typeof setTimeout>
    }) as typeof setTimeout
    session.onStateChange((state) => {
      if (state === 'connected' && sockets.length === 3) {
        sockets[2].emit('close')
      }
    })

    const initial = session.connect()
    sockets[0].emit('open')
    await initial
    sockets[0].emit('close')
    retryTimers[0].run()
    sockets[1].emit('close')
    retryTimers[1].run()
    sockets[2].emit('open')

    assert.deepEqual(
      retryTimers.map((timer) => timer.delay),
      [1, 2, 4]
    )
  })

  test('emits one disconnect transition across repeated disconnect calls', async ({ assert }) => {
    const sockets: FakeWebSocket[] = []
    const states: string[] = []
    const events: string[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })
    session.onStateChange((state) => states.push(state))
    session.on('disconnect', () => events.push('disconnect'))

    const connection = session.connect()
    sockets[0].emit('open')
    await connection
    session.disconnect()
    session.disconnect()

    assert.deepEqual(states, ['connecting', 'connected', 'disconnected'])
    assert.deepEqual(events, ['disconnect'])
  })

  test('does not let a failed reconnect tear down a newer connection attempt', async ({
    assert,
  }) => {
    const sockets: FakeWebSocket[] = []
    const session = new ClientSocketSession({
      buildUrl: () => 'ws://localhost/socket',
      reconnectDelay: 0,
      reconnectMaxDelay: 0,
      createWebSocket(url) {
        const socket = new FakeWebSocket(url)
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
    })

    const initialConnection = session.connect()
    sockets[0].emit('open')
    await initialConnection

    session.onStateChange((state) => {
      if (state === 'disconnected' && sockets.length === 2) {
        void session.connect()
      }
    })

    sockets[0].emit('close')
    await new Promise((resolve) => setTimeout(resolve, 5))
    assert.equal(sockets.length, 2)

    sockets[1].emit('error')
    await Promise.resolve()

    assert.equal(sockets.length, 3)
    assert.equal(session.state, 'connecting')
    sockets[2].emit('open')
    assert.isTrue(session.connected)
  })
})
