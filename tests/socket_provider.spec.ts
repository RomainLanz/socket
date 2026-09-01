import 'reflect-metadata'
import { createServer, type Server as HttpServer } from 'node:http'
import { inject } from '@adonisjs/core'
import { AppFactory } from '@adonisjs/core/factories/app'
import type { ContainerBindings } from '@adonisjs/core/types'
import { test } from '@japa/runner'
import { WebSocket } from 'ws'
import SocketProvider from '../providers/socket_provider.js'
import { BaseChannel } from '../src/base_channel.js'
import { HttpContext } from '@adonisjs/core/http'
import { HttpContextFactory } from '@adonisjs/core/factories/http'
import type { SocketConfig } from '../src/types.js'

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

async function connectClient(port: number): Promise<WebSocket> {
  const client = new WebSocket(`ws://127.0.0.1:${port}/socket`)

  await new Promise<void>((resolve, reject) => {
    client.once('open', resolve)
    client.once('error', reject)
  })

  return client
}

function makeLogger() {
  return {
    child() {
      return this
    },
    info() {},
    warn() {},
  } as any
}

function makeApp(
  httpServer: HttpServer,
  config: SocketConfig,
  adonisServer = makeServer(httpServer),
  logger = makeLogger()
) {
  const singletons = new Map<string, { factory: () => unknown; value?: unknown }>()

  const container = {
    singleton(key: string, factory: () => unknown) {
      singletons.set(key, { factory })
    },

    make(key: string | (new (...args: any[]) => unknown)) {
      if (typeof key === 'function') {
        return new key()
      }

      if (key === 'logger') {
        return logger
      }

      if (key === 'server') {
        return adonisServer
      }

      const binding = singletons.get(key)
      if (!binding) {
        throw new Error(`Missing container binding: ${key}`)
      }

      binding.value ??= binding.factory()

      return binding.value
    },

    createResolver() {
      return {}
    },
  }

  return {
    container,
    config: {
      get(key: string, fallback: unknown) {
        return key === 'socket' ? config : fallback
      },
    },
    makePath(path: string) {
      return path
    },
    async import() {
      return { socketChannels: [] }
    },
  } as any
}

function makeServer(httpServer: HttpServer) {
  return {
    getNodeServer() {
      return httpServer
    },

    createRequest(request: unknown) {
      return request
    },

    createResponse(_request: unknown, response: unknown) {
      return response
    },

    createHttpContext(request: unknown, response: unknown, containerResolver: unknown) {
      void request
      void response
      void containerResolver
      return new HttpContextFactory().create()
    },

    pipeline(middleware: any[]) {
      let onError: (error: unknown) => void = (error) => {
        throw error
      }
      let onFinal = async () => {}

      return {
        errorHandler(handler: (error: unknown) => void) {
          onError = handler
          return this
        },

        finalHandler(handler: () => Promise<void>) {
          onFinal = handler
          return this
        },

        async run(ctx: unknown) {
          let index = 0

          const next = async (): Promise<void> => {
            const Middleware = middleware[index++]

            if (!Middleware) {
              await onFinal()
              return
            }

            await new Middleware().handle(ctx, next)
          }

          try {
            await next()
          } catch (error) {
            onError(error)
          }
        },
      }
    },
  }
}

test.group('socket provider', () => {
  test('registers the generated manifest', async ({ assert }) => {
    class GeneratedChannel extends BaseChannel {
      static pattern = 'generated'
    }

    const generatedApp = makeApp(createServer(), {})
    generatedApp.import = async () => ({ socketChannels: [GeneratedChannel] })
    const generatedProvider = new SocketProvider(generatedApp)
    generatedProvider.register()
    await generatedProvider.boot()
    const router = await generatedApp.container.make('socket.router')
    assert.equal(router.match('generated')?.channel, GeneratedChannel)
  })

  test('resolves generated channel instances through the AdonisJS container', async ({
    assert,
  }) => {
    const dependency = { name: 'container dependency' }

    class GeneratedChannel extends BaseChannel {
      static pattern = 'generated'

      constructor(readonly injectedDependency: typeof dependency) {
        super()
      }
    }

    const app = makeApp(createServer(), {})
    const originalMake = app.container.make.bind(app.container)
    app.container.make = (key: string | (new (...args: any[]) => unknown)) => {
      if (key === GeneratedChannel) {
        return new GeneratedChannel(dependency)
      }

      return originalMake(key)
    }
    app.import = async () => ({ socketChannels: [GeneratedChannel] })

    const provider = new SocketProvider(app)
    provider.register()
    await provider.boot()

    const router = await app.container.make('socket.router')
    const result = await router.authorize({} as any, 'generated')

    assert.isTrue(result.success)
    if (result.success) {
      assert.strictEqual(result.instance.injectedDependency, dependency)
    }
  })

  test('injects channel middleware dependencies through the AdonisJS container', async ({
    assert,
  }) => {
    class MiddlewareDependency {
      constructor(readonly resolver: object) {}
    }
    const resolvedDependencies: MiddlewareDependency[] = []

    // Applications emit this metadata through tsconfig.app; the package tsconfig does not.
    @inject()
    @Reflect.metadata('design:paramtypes', [MiddlewareDependency])
    class FirstInjectedMiddleware {
      constructor(readonly dependency: MiddlewareDependency) {}

      async handle(_ctx: unknown, next: () => Promise<void>) {
        resolvedDependencies.push(this.dependency)
        await next()
      }
    }

    @inject()
    @Reflect.metadata('design:paramtypes', [MiddlewareDependency])
    class SecondInjectedMiddleware {
      constructor(readonly dependency: MiddlewareDependency) {}

      async handle() {
        resolvedDependencies.push(this.dependency)
      }
    }

    class GeneratedChannel extends BaseChannel {
      static pattern = 'generated'
      static middlewares = [FirstInjectedMiddleware, SecondInjectedMiddleware]
    }

    const app = new AppFactory<ContainerBindings>()
      .merge({
        importer: async () => ({ socketChannels: [GeneratedChannel] }),
      })
      .create(new URL('../', import.meta.url))
    await app.init()
    app.container.bindValue('logger', makeLogger())
    app.container.bind(MiddlewareDependency, (resolver) => {
      return new MiddlewareDependency(resolver)
    })

    const provider = new SocketProvider(app)
    provider.register()
    await provider.boot()

    const router = await app.container.make('socket.router')
    const result = await router.authorize({} as any, 'generated')

    if (!result.success) throw result.cause
    assert.isTrue(result.success)
    assert.lengthOf(resolvedDependencies, 2)
    assert.instanceOf(resolvedDependencies[0], MiddlewareDependency)
    assert.strictEqual(resolvedDependencies[0].resolver, resolvedDependencies[1].resolver)
  })

  test('does not mask generated manifest import or export failures', async ({ assert }) => {
    const importFailure = Object.assign(new Error('missing channel dependency'), {
      code: 'ERR_MODULE_NOT_FOUND',
    })
    const failingApp = makeApp(createServer(), {})
    failingApp.import = async () => {
      throw importFailure
    }
    const failingProvider = new SocketProvider(failingApp)
    failingProvider.register()
    await assert.rejects(() => failingProvider.boot(), 'Failed to load generated socket channels')

    const malformedApp = makeApp(createServer(), {})
    malformedApp.import = async () => ({})
    const malformedProvider = new SocketProvider(malformedApp)
    malformedProvider.register()
    await assert.rejects(
      () => malformedProvider.boot(),
      'Generated socket channel manifest must export a socketChannels array'
    )
  })

  test('creates and retains exactly one context without auth or middleware', async ({ assert }) => {
    const httpServer = createServer()
    const server = makeServer(httpServer)
    const originalCreate = server.createHttpContext
    let createdContexts = 0
    let createdContext: ReturnType<typeof originalCreate> | undefined
    server.createHttpContext = (...args) => {
      createdContexts += 1
      createdContext = originalCreate(...args)
      return createdContext
    }
    const app = makeApp(httpServer, {}, server)
    const provider = new SocketProvider(app)
    provider.register()
    await provider.boot()
    await provider.ready()
    const socket = await app.container.make('socket')
    let connectedContext: unknown
    socket.on('connect', ({ socket: connectedSocket }: any) => {
      connectedContext = connectedSocket.raw.httpContext
    })
    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      assert.equal(createdContexts, 1)
      assert.instanceOf(createdContext, HttpContext)
      assert.strictEqual(connectedContext, createdContext)
    } finally {
      client.close()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('fails boot for generated channels with invalid exports', async ({ assert }) => {
    class NotAChannel {
      static pattern = 'invalid'
    }
    const app = makeApp(createServer(), {})
    app.import = async () => ({ socketChannels: [NotAChannel] })
    const provider = new SocketProvider(app)
    provider.register()
    await assert.rejects(
      () => provider.boot(),
      'Socket channel generated channel at index 0 must default export a BaseChannel with a pattern'
    )
  })

  test('runs lazy upgrade middleware before authenticating the socket', async ({ assert }) => {
    const httpServer = createServer()
    let authenticatedContext: HttpContext | undefined
    const order: string[] = []

    class LazyUpgradeMiddleware {
      async handle(ctx: HttpContext, next: () => Promise<void>) {
        order.push('middleware:before')
        Reflect.set(ctx, 'lazyMiddlewareRan', true)
        await next()
        order.push('middleware:after')
      }
    }

    const config: SocketConfig = {
      websocket: {
        middleware: [async () => ({ default: LazyUpgradeMiddleware })],
        authenticate({ httpContext }) {
          order.push('authenticate')
          authenticatedContext = httpContext

          if (!Reflect.get(authenticatedContext, 'lazyMiddlewareRan')) {
            return false
          }

          return {
            user: { id: 1 },
          }
        },
      },
    }
    const server = makeServer(httpServer)
    const originalCreate = server.createHttpContext
    server.createHttpContext = (...args) => {
      order.push('context')
      return originalCreate(...args)
    }
    const app = makeApp(httpServer, config, server)

    const provider = new SocketProvider(app)
    provider.register()
    await provider.boot()
    await provider.ready()

    const port = await listen(httpServer)
    const client = await connectClient(port)

    try {
      assert.deepEqual(order, ['context', 'middleware:before', 'authenticate', 'middleware:after'])
      assert.isTrue(authenticatedContext && Reflect.get(authenticatedContext, 'lazyMiddlewareRan'))
    } finally {
      client.close()
      await app.container.make('socket').close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects middleware short-circuits without authenticating', async ({ assert }) => {
    const httpServer = createServer()
    let authenticated = false

    class ShortCircuitMiddleware {
      async handle() {}
    }

    const app = makeApp(httpServer, {
      websocket: {
        middleware: [ShortCircuitMiddleware],
        authenticate() {
          authenticated = true
          return { id: 'user-1' }
        },
      },
    })
    const provider = new SocketProvider(app)
    provider.register()
    await provider.boot()
    await provider.ready()
    const socket = await app.container.make('socket')
    const port = await listen(httpServer)
    const client = new WebSocket(`ws://127.0.0.1:${port}/socket`)

    try {
      const statusCode = await new Promise<number | undefined>((resolve, reject) => {
        client.once('unexpected-response', (_request, response) => resolve(response.statusCode))
        client.once('open', () => reject(new Error('WebSocket unexpectedly connected')))
        client.once('error', () => {})
      })

      assert.equal(statusCode, 500)
      assert.isFalse(authenticated)
      assert.equal(socket.connectionsCount, 0)
    } finally {
      client.terminate()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('treats middleware unwind failures as internal server errors', async ({ assert }) => {
    const httpServer = createServer()
    let authenticated = false

    class FailingMiddleware {
      async handle(_ctx: HttpContext, next: () => Promise<void>) {
        await next()
        throw new Error('commit failed')
      }
    }

    const app = makeApp(httpServer, {
      websocket: {
        middleware: [FailingMiddleware],
        authenticate() {
          authenticated = true
          return { id: 'user-1' }
        },
      },
    })
    const provider = new SocketProvider(app)
    provider.register()
    await provider.boot()
    await provider.ready()
    const socket = await app.container.make('socket')
    const port = await listen(httpServer)
    const client = new WebSocket(`ws://127.0.0.1:${port}/socket`)

    try {
      const statusCode = await new Promise<number | undefined>((resolve, reject) => {
        client.once('unexpected-response', (_request, response) => resolve(response.statusCode))
        client.once('open', () => reject(new Error('WebSocket unexpectedly connected')))
        client.once('error', () => {})
      })

      assert.equal(statusCode, 500)
      assert.isTrue(authenticated)
      assert.equal(socket.connectionsCount, 0)
    } finally {
      client.terminate()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects context creation failures as internal server errors', async ({ assert }) => {
    const httpServer = createServer()
    const server = makeServer(httpServer)
    server.createHttpContext = () => {
      throw new Error('context failed')
    }
    const app = makeApp(httpServer, {}, server)
    const provider = new SocketProvider(app)
    provider.register()
    await provider.boot()
    await provider.ready()
    const socket = await app.container.make('socket')
    const port = await listen(httpServer)
    const client = new WebSocket(`ws://127.0.0.1:${port}/socket`)

    try {
      const statusCode = await new Promise<number | undefined>((resolve, reject) => {
        client.once('unexpected-response', (_request, response) => resolve(response.statusCode))
        client.once('open', () => reject(new Error('WebSocket unexpectedly connected')))
        client.once('error', () => {})
      })

      assert.equal(statusCode, 500)
      assert.equal(socket.connectionsCount, 0)
    } finally {
      client.terminate()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)

  test('rejects upgrade failures even when the logger throws', async ({ assert }) => {
    const httpServer = createServer()
    const server = makeServer(httpServer)
    server.createHttpContext = () => {
      throw new Error('context failed')
    }
    const logger = {
      child() {
        return this
      },
      info() {},
      warn() {
        throw new Error('logger failed')
      },
    } as any
    const app = makeApp(httpServer, {}, server, logger)
    const provider = new SocketProvider(app)
    provider.register()
    await provider.boot()
    await provider.ready()
    const socket = await app.container.make('socket')
    const port = await listen(httpServer)
    const client = new WebSocket(`ws://127.0.0.1:${port}/socket`)

    try {
      const statusCode = await new Promise<number | undefined>((resolve, reject) => {
        client.once('unexpected-response', (_request, response) => resolve(response.statusCode))
        client.once('open', () => reject(new Error('WebSocket unexpectedly connected')))
        client.once('error', () => {})
      })

      assert.equal(statusCode, 500)
      assert.equal(socket.connectionsCount, 0)
    } finally {
      client.terminate()
      await socket.close()
      await closeHttpServer(httpServer)
    }
  }).timeout(10_000)
})
