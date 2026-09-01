import { test } from '@japa/runner'
import { Bus } from '@boringnode/bus'
import { memory } from '@boringnode/bus/transports/memory'
import { PRESENCE_DATA_KEY } from '../src/presence_manager.js'
import { SocketBus } from '../src/socket_bus.js'
import { DEFAULT_RETRY_QUEUE_MAX_SIZE } from '../src/socket_bus.js'

test.group('SocketBus', () => {
  test('rejects invalid retry queue limits', ({ assert }) => {
    assert.throws(
      () =>
        new SocketBus(
          { driver: memory(), retryQueue: { maxSize: 0 } },
          { channel() {}, broadcast() {}, presenceSockets: () => [] }
        ),
      'transport.retryQueue.maxSize must be a positive integer or null'
    )
  })

  test('bounds failed transport publications by default', async ({ assert }) => {
    let reconnect: (() => Promise<void> | void) | undefined
    let transportAvailable = false
    const delivered: unknown[] = []
    const driver = () => {
      const transport = {
        setId() {
          return transport
        },
        onReconnect(callback: () => Promise<void> | void) {
          reconnect = callback
        },
        async publish(_channel: string, message: unknown) {
          if (!transportAvailable) {
            throw new Error('transport unavailable')
          }
          delivered.push(message)
        },
        async subscribe() {},
        async unsubscribe() {},
        async disconnect() {},
      }

      return transport
    }
    const bus = new SocketBus(
      { driver: driver as any },
      { channel() {}, broadcast() {}, presenceSockets: () => [] }
    )

    for (let index = 0; index <= DEFAULT_RETRY_QUEUE_MAX_SIZE; index++) {
      bus.publishBroadcast(`event:${index}`, null)
    }
    await new Promise<void>((resolve) => setImmediate(resolve))

    transportAvailable = true
    await reconnect?.()

    assert.lengthOf(delivered, DEFAULT_RETRY_QUEUE_MAX_SIZE)
    assert.equal((delivered[0] as any).event, 'event:1')
    assert.equal(
      (delivered[DEFAULT_RETRY_QUEUE_MAX_SIZE - 1] as any).event,
      `event:${DEFAULT_RETRY_QUEUE_MAX_SIZE}`
    )

    await bus.close()
  })

  test('passes an explicit retry queue limit to the transport bus', async ({ assert }) => {
    let reconnect: (() => Promise<void> | void) | undefined
    let transportAvailable = false
    const delivered: unknown[] = []
    const driver = () => {
      const transport = {
        setId() {
          return transport
        },
        onReconnect(callback: () => Promise<void> | void) {
          reconnect = callback
        },
        async publish(_channel: string, message: unknown) {
          if (!transportAvailable) {
            throw new Error('transport unavailable')
          }
          delivered.push(message)
        },
        async subscribe() {},
        async unsubscribe() {},
        async disconnect() {},
      }

      return transport
    }
    const bus = new SocketBus(
      { driver: driver as any, retryQueue: { maxSize: 2 } },
      { channel() {}, broadcast() {}, presenceSockets: () => [] }
    )

    bus.publishBroadcast('first', null)
    bus.publishBroadcast('second', null)
    bus.publishBroadcast('third', null)
    await new Promise<void>((resolve) => setImmediate(resolve))

    transportAvailable = true
    await reconnect?.()

    assert.deepEqual(
      delivered.map((message) => (message as any).event),
      ['second', 'third']
    )

    await bus.close()
  })

  test('accepts nested serializable payloads and repeated references', async () => {
    const shared = { enabled: true }
    const bus = new SocketBus(
      { driver: memory() },
      { channel() {}, broadcast() {}, presenceSockets: () => [] }
    )

    bus.publishBroadcast('valid', { values: [null, 'value', 42, false, shared], shared })
    await bus.close()
  })

  test('ignores malformed routing messages', async ({ assert, cleanup }) => {
    const channel = `socket-bus-malformed-${crypto.randomUUID()}`
    const driver = memory()
    let received = 0
    const bus = new SocketBus(
      { driver, channel },
      {
        channel() {
          received++
        },
        broadcast() {},
        presenceSockets: () => [],
      }
    )
    const publisher = new Bus(driver())
    cleanup(async () => {
      await Promise.all([bus.close(), publisher.disconnect()])
    })
    await bus.start()

    await publisher.publish(channel, null)
    await publisher.publish(channel, {
      type: 'channel:event',
      origin: 'other',
      channel: 42,
      event: 'message',
      data: null,
      except: [],
    } as any)
    await new Promise((resolve) => setTimeout(resolve, 10))

    assert.equal(received, 0)
  })

  test('ignores malformed presence responses without leaving requests pending', async ({
    assert,
    cleanup,
  }) => {
    const channel = `socket-bus-malformed-presence-${crypto.randomUUID()}`
    const driver = memory()
    const responder = new SocketBus(
      { driver, channel },
      {
        channel() {},
        broadcast() {},
        presenceSockets: () => new Array(1),
      }
    )
    const requester = new SocketBus(
      { driver, channel, presenceTimeout: 10 },
      { channel() {}, broadcast() {}, presenceSockets: () => [] }
    )
    cleanup(async () => {
      await Promise.all([responder.close(), requester.close()])
    })
    await responder.start()
    await requester.start()

    assert.deepEqual(await requester.fetchPresenceSockets('chat'), [])
  })

  test('restores presence dates and custom fields', async ({ assert, cleanup }) => {
    const channel = `socket-bus-presence-${crypto.randomUUID()}`
    const driver = memory()
    const responder = new SocketBus(
      { driver, channel },
      {
        channel() {},
        broadcast() {},
        presenceSockets: () => [
          {
            id: 'socket-1',
            data: {
              [PRESENCE_DATA_KEY]: {
                chat: {
                  id: 'user-1',
                  joinedAt: '2026-07-31T12:00:00.000Z',
                  role: 'admin',
                },
              },
            },
          },
        ],
      }
    )
    const requester = new SocketBus(
      { driver, channel, presenceTimeout: 20 },
      { channel() {}, broadcast() {}, presenceSockets: () => [] }
    )
    cleanup(async () => {
      await Promise.all([responder.close(), requester.close()])
    })
    await responder.start()
    await requester.start()

    const sockets = await requester.fetchPresenceSockets('chat')
    const user = sockets[0].data[PRESENCE_DATA_KEY]?.chat

    assert.instanceOf(user?.joinedAt, Date)
    assert.equal(user?.joinedAt.toISOString(), '2026-07-31T12:00:00.000Z')
    assert.equal(user?.role, 'admin')
    assert.notProperty(user!, 'name')
  })
})
