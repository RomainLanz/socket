import { createServer, type Server as HttpServer } from 'node:http'
import { randomUUID } from 'node:crypto'
import diagnosticsChannel from 'node:diagnostics_channel'
import { test } from '@japa/runner'
import { WebSocket } from 'ws'
import { SpanKind, SpanStatusCode, trace } from '@opentelemetry/api'
import { registerInstrumentations } from '@opentelemetry/instrumentation'
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics'
import packageJson from '../package.json' with { type: 'json' }
import { BaseChannel } from '../src/base_channel.js'
import { ChannelRouter } from '../src/channel_router.js'
import { onMessage } from '../src/decorators.js'
import { SocketInstrumentation } from '../src/otel.js'
import { SocketService } from './helpers/socket_service.js'
import { getFinishedSpans, resetSpans, setupTracing } from './helpers/setup_tracing.js'

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
      if (error) reject(error)
      else resolve()
    })
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
    warn() {},
    info() {},
  } as any
}

test.group('socket instrumentation', (group) => {
  group.each.setup(() => {
    setupTracing()
    resetSpans()
  })

  test('subscribes when registered through the standard instrumentation API', ({ assert }) => {
    const instrumentation = new SocketInstrumentation()
    const unregister = registerInstrumentations({ instrumentations: [instrumentation] })

    try {
      assert.isTrue(diagnosticsChannel.tracingChannel('rlanz.socket.connect').hasSubscribers)
    } finally {
      unregister()
    }
  })

  test('records metrics with a meter provider installed after construction', async ({ assert }) => {
    const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE)
    const reader = new PeriodicExportingMetricReader({
      exporter,
      exportIntervalMillis: 60_000,
    })
    const meterProvider = new MeterProvider({ readers: [reader] })
    const instrumentation = new SocketInstrumentation()
    instrumentation.setMeterProvider(meterProvider)
    const httpServer = createServer()
    const socket = new SocketService()
    await socket.boot(httpServer, {}, new ChannelRouter(), makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      await meterProvider.forceFlush()

      const metric = exporter
        .getMetrics()
        .flatMap((resource) => resource.scopeMetrics)
        .flatMap((scope) => scope.metrics)
        .find((item) => item.descriptor.name === 'rlanz_socket_connections_total')

      assert.equal(metric?.dataPoints[0]?.value, 1)
    } finally {
      instrumentation.disable()
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
      await meterProvider.shutdown()
    }
  })

  test('uses the package version as its instrumentation version', ({ assert }) => {
    const instrumentation = new SocketInstrumentation()

    try {
      assert.equal(instrumentation.instrumentationVersion, packageJson.version)
    } finally {
      instrumentation.disable()
    }
  })

  test('makes the operation span active while the operation runs', async ({ assert }) => {
    let activeSpanId: string | undefined

    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'

      @onMessage('chat:send')
      sendMessage() {
        activeSpanId = trace.getActiveSpan()?.spanContext().spanId
      }
    }

    const instrumentation = new SocketInstrumentation()
    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)
    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      await sendAndWaitForAck(client, { type: 'subscribe', channel: 'chat/general' })
      await sendAndWaitForAck(client, {
        type: 'message',
        channel: 'chat/general',
        event: 'chat:send',
      })

      const operationSpan = getFinishedSpans().find(
        (span) => span.name === 'socket message chat/general'
      )
      assert.equal(activeSpanId, operationSpan?.spanContext().spanId)
    } finally {
      instrumentation.disable()
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('creates spans for socket operations', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'

      @onMessage('chat:send')
      sendMessage() {
        return { delivered: true }
      }
    }

    const instrumentation = new SocketInstrumentation()
    instrumentation.enable()
    instrumentation.manuallyRegister()

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      await sendAndWaitForAck(client, {
        type: 'subscribe',
        channel: 'chat/general',
      })

      await sendAndWaitForAck(client, {
        type: 'message',
        channel: 'chat/general',
        event: 'chat:send',
        data: { body: 'hello' },
      })

      socket.to('chat/general').emit('chat:notice', { body: 'server' })

      const spans = getFinishedSpans()
      const connectSpan = spans.find((span) => span.name === 'socket connect')
      const subscribeSpan = spans.find((span) => span.name === 'socket subscribe chat/general')
      const messageSpan = spans.find((span) => span.name === 'socket message chat/general')
      const broadcastSpan = spans.find((span) => span.name === 'socket broadcast channel')

      assert.isDefined(connectSpan)
      assert.equal(connectSpan!.kind, SpanKind.SERVER)
      assert.equal(connectSpan!.attributes['network.transport'], 'websocket')

      assert.isDefined(subscribeSpan)
      assert.equal(subscribeSpan!.status.code, SpanStatusCode.OK)
      assert.equal(subscribeSpan!.attributes['messaging.destination.name'], 'chat/general')
      assert.equal(subscribeSpan!.attributes['socket.subscription.created'], true)

      assert.isDefined(messageSpan)
      assert.equal(messageSpan!.status.code, SpanStatusCode.OK)
      assert.equal(messageSpan!.attributes['messaging.event.name'], 'chat:send')

      assert.isDefined(broadcastSpan)
      assert.equal(broadcastSpan!.kind, SpanKind.PRODUCER)
      assert.equal(broadcastSpan!.attributes['socket.broadcast.target'], 'channel')
      assert.equal(broadcastSpan!.attributes['socket.broadcast.delivered'], 1)
    } finally {
      instrumentation.disable()
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('marks failed channel messages as errors', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const instrumentation = new SocketInstrumentation()
    instrumentation.enable()
    instrumentation.manuallyRegister()

    const httpServer = createServer()
    const socket = new SocketService()
    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    await socket.boot(httpServer, {}, router, makeLogger())
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      await sendAndWaitForAck(client, {
        type: 'message',
        channel: 'chat/general',
        event: 'chat:send',
      })

      const messageSpan = getFinishedSpans().find(
        (span) => span.name === 'socket message chat/general'
      )

      assert.isDefined(messageSpan)
      assert.equal(messageSpan!.status.code, SpanStatusCode.ERROR)
      assert.equal(messageSpan!.status.message, 'Not subscribed')
    } finally {
      instrumentation.disable()
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)
})
