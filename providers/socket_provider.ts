import type { ApplicationService } from '@adonisjs/core/types'
import type { HttpContext, Server as AdonisServer } from '@adonisjs/core/http'
import type { Logger } from '@adonisjs/logger'
import type { MiddlewareAsClass } from '@adonisjs/core/types/http'
import { ServerResponse } from 'node:http'
import { SocketService } from '../src/socket_service.js'
import { ChannelRouter } from '../src/channel_router.js'
import { BaseChannel } from '../src/base_channel.js'
import { PresenceManager } from '../src/presence_manager.js'
import type { SocketUpgradeContextRunner } from '../src/socket_upgrader.js'
import type { SocketConfig, SocketHttpMiddleware } from '../src/types.js'

declare module '@adonisjs/core/types' {
  interface ContainerBindings {
    'socket': SocketService
    'socket.router': ChannelRouter
    'socket.presence': PresenceManager
  }
}

export default class SocketProvider {
  constructor(protected app: ApplicationService) {}

  /**
   * Registers the bindings in the container.
   */
  register(): void {
    this.app.container.singleton('socket.router', () => {
      return new ChannelRouter(
        (channel) => this.app.container.make(channel),
        () => {
          const resolver = this.app.container.createResolver()

          return async (Middleware, ctx, next) => {
            const instance = await resolver.make(Middleware)
            await resolver.call(instance, 'handle', [ctx, next])
          }
        }
      )
    })

    this.app.container.singleton('socket.presence', () => {
      return new PresenceManager()
    })

    this.app.container.singleton('socket', () => {
      return new SocketService()
    })
  }

  /**
   * Boots the provider.
   */
  async boot(): Promise<void> {
    const router = await this.app.container.make('socket.router')

    // Discover and register channel classes.
    await this.#registerChannels(router)
  }

  /**
   * Called when the app is ready and the HTTP server is running.
   */
  async ready(): Promise<void> {
    const socket = await this.app.container.make('socket')
    const router = await this.app.container.make('socket.router')
    const presence = await this.app.container.make('socket.presence')
    const logger = await this.#resolveLogger()
    const config = this.app.config.get<SocketConfig>('socket', {})

    socket.setPresenceManager(presence)

    logger.info('router has %d channels registered', router.size)

    const server = await this.app.container.make('server')
    const httpServer = server.getNodeServer()

    if (!httpServer) {
      const error = new Error('HTTP server not available.')
      socket.markFailed(error)
      logger.warn('HTTP server not available; socket server was not started')
      return
    }

    try {
      await socket.boot(
        httpServer,
        config,
        router,
        logger,
        this.#createUpgradeContextRunner(config, server)
      )
      logger.info('server started')
    } catch (error) {
      socket.markFailed(error)
      throw error
    }
  }

  /**
   * Performs a clean shutdown.
   */
  async shutdown(): Promise<void> {
    const socket = await this.app.container.make('socket')
    await socket.close()
  }

  /**
   * Resolves the AdonisJS logger and creates a child logger for the socket context.
   */
  async #resolveLogger(): Promise<Logger> {
    const logger = await this.app.container.make('logger')
    return logger.child({ module: 'socket' })
  }

  /**
   * Creates an AdonisJS HTTP context for WebSocket upgrade requests.
   */
  #createUpgradeContextRunner(
    config: SocketConfig,
    server: AdonisServer
  ): SocketUpgradeContextRunner {
    const middleware = config.websocket?.middleware ?? []

    return async (request, handler, shortCircuitBeforeMiddleware) => {
      const response = new ServerResponse(request)
      const adonisRequest = server.createRequest(request, response)
      const adonisResponse = server.createResponse(request, response)
      const httpContext = server.createHttpContext(
        adonisRequest,
        adonisResponse,
        this.app.container.createResolver()
      )

      const earlyResult = await shortCircuitBeforeMiddleware?.(httpContext)
      if (earlyResult !== undefined) {
        return earlyResult
      }

      return this.#runUpgradeMiddleware(server, middleware, httpContext, handler)
    }
  }

  /**
   * Runs middleware attached to WebSocket upgrade requests.
   */
  async #runUpgradeMiddleware<Result>(
    server: AdonisServer,
    middleware: NonNullable<NonNullable<SocketConfig['websocket']>['middleware']>,
    httpContext: HttpContext,
    handler: (httpContext: HttpContext) => Promise<Result>
  ): Promise<Result> {
    if (middleware.length === 0) {
      return handler(httpContext)
    }

    const middlewareClasses: MiddlewareAsClass[] = await Promise.all(
      middleware.map(async (one): Promise<MiddlewareAsClass> => {
        if (one.prototype?.handle) {
          return one as MiddlewareAsClass
        }

        const moduleExports = await (one as Exclude<SocketHttpMiddleware, MiddlewareAsClass>)()
        return moduleExports.default
      })
    )

    let outcome: { value: Result } | undefined

    await server
      .pipeline(middlewareClasses)
      .errorHandler((error) => {
        throw error
      })
      .finalHandler(async () => {
        outcome = { value: await handler(httpContext) }
      })
      .run(httpContext)

    if (!outcome) {
      throw new Error('WebSocket upgrade middleware must call next()')
    }

    return outcome.value
  }

  /**
   * Scans and registers channel classes.
   */
  async #registerChannels(router: ChannelRouter): Promise<void> {
    const logger = await this.#resolveLogger()

    let generated: { socketChannels?: unknown }
    try {
      generated = await this.app.import('#generated/socket_channels')
    } catch (error) {
      throw new Error('Failed to load generated socket channels', { cause: error })
    }

    if (!Array.isArray(generated.socketChannels)) {
      throw new Error('Generated socket channel manifest must export a socketChannels array')
    }

    for (const [index, channelClass] of generated.socketChannels.entries()) {
      const source = `generated channel at index ${index}`
      if (
        typeof channelClass !== 'function' ||
        !(channelClass.prototype instanceof BaseChannel) ||
        !channelClass.pattern
      ) {
        throw new Error(`Socket channel ${source} must default export a BaseChannel with a pattern`)
      }

      try {
        router.register(channelClass)
      } catch (error) {
        throw new Error(`Failed to register socket channel from ${source}`, { cause: error })
      }

      logger.info('registered channel: %s', channelClass.pattern)
    }

    logger.info('total channels registered: %d', router.size)
  }
}
