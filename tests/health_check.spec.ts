import { createServer } from 'node:http'
import { test } from '@japa/runner'
import { HealthChecks } from '@adonisjs/core/health'
import { ChannelRouter } from '../src/channel_router.js'
import { SocketHealthCheck } from '../src/health_check.js'
import { SocketService } from './helpers/socket_service.js'

async function closeHttpServer(httpServer: ReturnType<typeof createServer>): Promise<void> {
  if (!httpServer.listening) return

  await new Promise<void>((resolve, reject) => {
    httpServer.close((error) => {
      if (error) reject(error)
      else resolve()
    })
  })
}

function makeLogger() {
  return {
    warn() {},
    info() {},
  } as any
}

test.group('socket health check', () => {
  test('reports the socket service as unhealthy before boot', async ({ assert }) => {
    const socket = new SocketService()
    const result = await new SocketHealthCheck(socket).run()

    assert.equal(result.status, 'error')
    assert.equal(result.message, 'Socket service is stopped')
    assert.containSubset(result.meta, {
      status: 'stopped',
      ready: false,
      connections: 0,
      channels: 0,
    })
  })

  test('reports the socket service as ready after boot', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()

    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())

    try {
      const result = await new SocketHealthCheck(socket).run()

      assert.equal(result.status, 'ok')
      assert.equal(result.message, 'Socket service is ready')
      assert.containSubset(result.meta, {
        status: 'ready',
        ready: true,
        connections: 0,
        channels: 0,
      })
    } finally {
      await socket.close()
      await closeHttpServer(httpServer)
    }
  })

  test('integrates with AdonisJS HealthChecks readiness report', async ({ assert }) => {
    const socket = new SocketService()
    const healthChecks = new HealthChecks().register([new SocketHealthCheck(socket)])

    const unhealthyReport = await healthChecks.run()

    assert.isFalse(unhealthyReport.isHealthy)
    assert.equal(unhealthyReport.status, 'error')

    const httpServer = createServer()
    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())

    try {
      const healthyReport = await healthChecks.run()

      assert.isTrue(healthyReport.isHealthy)
      assert.equal(healthyReport.status, 'ok')
      assert.equal(healthyReport.checks[0].name, 'Socket health check')
    } finally {
      await socket.close()
      await closeHttpServer(httpServer)
    }
  })

  test('reports the socket service as unhealthy after shutdown', async ({ assert }) => {
    const httpServer = createServer()
    const socket = new SocketService()

    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())
    await socket.close()

    try {
      const result = await new SocketHealthCheck(socket).run()

      assert.equal(result.status, 'error')
      assert.containSubset(result.meta, {
        status: 'stopped',
        ready: false,
      })
    } finally {
      await closeHttpServer(httpServer)
    }
  })

  test('reports boot failures with the last error', async ({ assert }) => {
    const socket = new SocketService()
    const error = new Error('Unable to bind socket server')

    socket.markFailed(error)

    const result = await new SocketHealthCheck(socket).run()

    assert.equal(result.status, 'error')
    assert.containSubset(result.meta, {
      status: 'failed',
      ready: false,
      lastError: 'Unable to bind socket server',
    })
  })
})
