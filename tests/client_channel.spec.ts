import { test } from '@japa/runner'
import { Channel } from '../src/client/channel.js'
import type { ChannelContract, EventHandler, SocketClientTransport } from '../src/client/types.js'

class FakeTransport implements SocketClientTransport {
  connected = true
  handlers = new Map<string, Set<EventHandler<unknown>>>()

  on<T = unknown>(event: string, handler: EventHandler<T>): () => void {
    const handlers = this.handlers.get(event) ?? new Set()
    handlers.add(handler as EventHandler<unknown>)
    this.handlers.set(event, handlers)
    return () => this.off(event, handler)
  }

  off<T = unknown>(event: string, handler: EventHandler<T>): void {
    this.handlers.get(event)?.delete(handler as EventHandler<unknown>)
  }

  send(): void {}

  async sendRequest<T = unknown>(): Promise<T> {
    return {
      ok: true,
      data: {
        presenceData: {
          channel: 'presence/general',
          users: [{ id: '1', name: 'Ada', joinedAt: '2026-06-03T08:00:00.000Z' }],
          count: 1,
        },
      },
    } as T
  }

  emit(event: string, data: unknown): void {
    this.handlers.get(event)?.forEach((handler) => handler(data))
  }
}

test.group('client channel callback isolation', () => {
  test('rejects presence snapshots with duplicate user IDs', async ({ assert }) => {
    const transport = new FakeTransport()
    const joining: string[] = []
    const channel = new Channel<ChannelContract>('presence/general', transport).joining((user) => {
      joining.push(user.name as string)
    })

    await channel.subscribe()
    transport.emit('channel:presence/general:presence:update', {
      channel: 'presence/general',
      users: [
        { id: '1', name: 'Ada', joinedAt: '2026-06-03T08:00:00.000Z' },
        { id: '2', name: 'Bert', joinedAt: '2026-06-03T08:01:00.000Z' },
        { id: '2', name: 'Bert duplicate', joinedAt: '2026-06-03T08:02:00.000Z' },
      ],
      count: 3,
    })

    assert.equal(channel.count, 1)
    assert.deepEqual(
      channel.users.map((user) => user.name),
      ['Ada']
    )
    assert.deepEqual(joining, [])
  })

  test('accepts presence members without a name', async ({ assert }) => {
    const transport = new FakeTransport()
    const channel = new Channel<ChannelContract>('presence/general', transport)

    await channel.subscribe()
    transport.emit('channel:presence/general:presence:update', {
      channel: 'presence/general',
      users: [{ id: '2', joinedAt: '2026-06-03T08:01:00.000Z' }],
      count: 1,
    })

    assert.deepEqual(channel.users, [{ id: '2', joinedAt: '2026-06-03T08:01:00.000Z' }])
    assert.equal(channel.count, 1)
  })

  test('ignores snapshots for another channel or with an inconsistent count', async ({
    assert,
  }) => {
    const transport = new FakeTransport()
    const channel = new Channel<ChannelContract>('presence/general', transport)

    await channel.subscribe()
    transport.emit('channel:presence/general:presence:update', {
      channel: 'presence/other',
      users: [{ id: '2', joinedAt: '2026-06-03T08:01:00.000Z' }],
      count: 1,
    })
    transport.emit('channel:presence/general:presence:update', {
      channel: 'presence/general',
      users: [],
      count: 1,
    })

    assert.equal(channel.count, 1)
    assert.deepEqual(
      channel.users.map((user) => user.id),
      ['1']
    )
  })

  test('isolates presence and event callbacks after committing presence state', async ({
    assert,
    cleanup,
  }) => {
    const runtime = globalThis as typeof globalThis & { reportError?: (error: unknown) => void }
    const previousReportError = runtime.reportError
    const reportedErrors: unknown[] = []
    runtime.reportError = (error) => reportedErrors.push(error)
    cleanup(() => {
      runtime.reportError = previousReportError
    })

    const transport = new FakeTransport()
    const order: string[] = []
    const channel = new Channel<ChannelContract>('presence/general', transport)
      .here((users) => {
        order.push(`here:${users.length}`)
        if (users.length === 1) throw new Error('here failed')
      })
      .joining(async () => {
        order.push(`joining:${channel.count}`)
        throw new Error('joining failed')
      })
      .leaving(() => {
        order.push(`leaving:${channel.count}`)
        throw new Error('leaving failed')
      })
      .listen('message', () => {
        order.push('first listener')
        throw new Error('listener failed')
      })
      .listen('message', () => order.push('second listener'))

    await channel.subscribe()
    assert.isTrue(channel.active)

    transport.emit('channel:presence/general:presence:update', {
      channel: 'presence/general',
      users: [{ id: '2', name: 'Bert', joinedAt: '2026-06-03T08:01:00.000Z' }],
      count: 1,
    })
    transport.emit('channel:presence/general:event', { event: 'message', data: {} })
    await Promise.resolve()
    await Promise.resolve()

    assert.deepEqual(
      channel.users.map((user) => user.name),
      ['Bert']
    )
    assert.deepEqual(order, [
      'here:1',
      'joining:1',
      'leaving:1',
      'here:1',
      'first listener',
      'second listener',
    ])
    assert.deepEqual(
      reportedErrors.map((error) => (error as Error).message),
      ['here failed', 'leaving failed', 'here failed', 'listener failed', 'joining failed']
    )
  })

  test('isolates callbacks when reading the global error reporter throws', async ({
    assert,
    cleanup,
  }) => {
    const previousDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'reportError')
    Object.defineProperty(globalThis, 'reportError', {
      configurable: true,
      get() {
        throw new Error('reporter getter failed')
      },
    })
    cleanup(() => {
      if (previousDescriptor) {
        Object.defineProperty(globalThis, 'reportError', previousDescriptor)
      } else {
        Reflect.deleteProperty(globalThis, 'reportError')
      }
    })

    const transport = new FakeTransport()
    const calls: string[] = []
    const channel = new Channel<ChannelContract>('presence/general', transport)
      .listen('message', () => {
        calls.push('failing listener')
        throw new Error('listener failed')
      })
      .listen('message', () => calls.push('following listener'))
    await channel.subscribe()

    transport.emit('channel:presence/general:event', { event: 'message', data: {} })

    assert.deepEqual(calls, ['failing listener', 'following listener'])
  })
})
