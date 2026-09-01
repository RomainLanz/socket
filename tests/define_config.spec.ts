import { test } from '@japa/runner'
import { memory } from '@boringnode/bus/transports/memory'
import { authenticateWithAdonisAuth, defineConfig } from '../index.js'
import { makeHttpContext } from './helpers/http_context.js'

test.group('defineConfig', () => {
  test('should return the given config without normalizing it', ({ assert }) => {
    const config = defineConfig({
      websocket: {
        path: '/realtime',
        pingInterval: '25s',
        pingTimeout: '5s',
      },
    })

    assert.equal(config.websocket?.path, '/realtime')
    assert.equal(config.websocket?.pingInterval, '25s')
    assert.equal(config.websocket?.pingTimeout, '5s')
  })

  test('should not set transport defaults', ({ assert }) => {
    const config = defineConfig({
      transport: {
        driver: () => null as any,
      } as any,
    })

    assert.isUndefined(config.transport?.channel)
  })

  test('should not override explicit transport channel', ({ assert }) => {
    const config = defineConfig({
      transport: {
        driver: () => null as any,
        channel: 'custom',
      } as any,
    })

    assert.equal(config.transport?.channel, 'custom')
  })

  test('should ignore null transport', ({ assert }) => {
    const config = defineConfig({
      transport: null,
    })

    assert.isNull(config.transport)
  })

  test('should parse transport presence timeout at the socket bus boundary', async ({ assert }) => {
    const { SocketBus } = await import('../src/socket_bus.js')
    let bus: InstanceType<typeof SocketBus>

    assert.doesNotThrow(() => {
      bus = new SocketBus(
        {
          driver: memory(),
          presenceTimeout: '100ms',
        },
        {
          channel() {},
          broadcast() {},
          presenceSockets() {
            return []
          },
        }
      )
    })

    await bus!.start()
    await bus!.close()
  })

  test('should validate transport presence timeout at the socket bus boundary', async ({
    assert,
  }) => {
    const { SocketBus } = await import('../src/socket_bus.js')

    assert.throws(
      () =>
        new SocketBus(
          {
            driver: memory(),
            presenceTimeout: -1,
          },
          {
            channel() {},
            broadcast() {},
            presenceSockets() {
              return []
            },
          }
        ),
      'transport.presenceTimeout must be a positive duration'
    )
  })

  test('should authenticate with AdonisJS auth by default', async ({ assert }) => {
    const user = { id: 'user-1' }
    let authenticated = false

    const authenticate = authenticateWithAdonisAuth<typeof user>()
    const httpContext = makeHttpContext()
    Reflect.set(httpContext, 'auth', {
      authenticateUsing() {
        authenticated = true
      },
      getUserOrFail() {
        return user
      },
    })

    const result = await authenticate({
      httpContext,
    })

    assert.isTrue(authenticated)
    assert.equal(result, user)
  })
})
