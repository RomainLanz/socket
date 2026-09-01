import type { IncomingMessage } from 'node:http'
import type { Duplex } from 'node:stream'
import type { WebSocket, WebSocketServer } from 'ws'
import type { HttpContext } from '@adonisjs/core/http'
import type { SocketConfig, SocketUpgradeContext } from './types.js'

export const DEFAULT_WEBSOCKET_PATH = '/socket'

export interface AcceptedUpgrade<User> {
  httpContext: HttpContext
  user?: User
}

export type SocketUpgradeHandler = (request: IncomingMessage, socket: Duplex, head: Buffer) => void

type AcceptUpgrade<User> = (
  connection: WebSocket,
  request: IncomingMessage,
  upgrade: AcceptedUpgrade<User>
) => void

export type SocketUpgradeContextRunner = <Result>(
  request: IncomingMessage,
  handler: (httpContext: HttpContext) => Promise<Result>,
  shortCircuitBeforeMiddleware?: (
    httpContext: HttpContext
  ) => Result | undefined | Promise<Result | undefined>
) => Promise<Result>

type UpgradeResult<User> =
  | { accepted: AcceptedUpgrade<User> }
  | { rejected: 'authentication' | 'origin' }

export class SocketUpgrader<User = unknown> {
  #path: string

  constructor(
    private server: WebSocketServer,
    private config: SocketConfig<User>['websocket'] | undefined,
    private runWithHttpContext: SocketUpgradeContextRunner,
    private reportError: (message: string, error: unknown) => void = () => {}
  ) {
    this.#path = config?.path ?? DEFAULT_WEBSOCKET_PATH
  }

  async handle(
    request: IncomingMessage,
    socket: Duplex,
    head: Buffer,
    accept: AcceptUpgrade<User>
  ): Promise<boolean> {
    if (!this.#matchesPath(request)) {
      return false
    }

    const result = await this.#prepareUpgrade(request)

    if ('rejected' in result) {
      if (result.rejected === 'origin') {
        SocketUpgrader.reject(socket, 403, 'Forbidden')
        return true
      }

      SocketUpgrader.reject(socket, 401, 'Unauthorized')
      return true
    }

    this.server.handleUpgrade(request, socket, head, (connection) => {
      accept(connection, request, result.accepted)
    })

    return true
  }

  static reject(socket: Duplex, statusCode: number, reason: string): void {
    if (!socket.writableEnded) {
      socket.write(`HTTP/1.1 ${statusCode} ${reason}\r\n\r\n`)
    }

    socket.destroy()
  }

  #matchesPath(request: IncomingMessage): boolean {
    const url = new URL(request.url ?? '/', 'ws://localhost')
    return url.pathname === this.#path
  }

  #isOriginAllowed(request: IncomingMessage, httpContext: HttpContext): boolean {
    const origin = request.headers.origin
    if (!origin) {
      return true
    }

    if (this.config?.origin !== undefined) {
      return this.#matchesOriginPolicy(origin, httpContext)
    }

    const host = request.headers.host
    if (!host) {
      return false
    }

    const forwardedHeader = request.headers['x-forwarded-proto']
    const forwardedProtocol = (
      Array.isArray(forwardedHeader) ? forwardedHeader[0] : forwardedHeader
    )
      ?.split(',')[0]
      ?.trim()
    const protocol =
      forwardedProtocol === 'http' || forwardedProtocol === 'https'
        ? forwardedProtocol
        : (request.socket as typeof request.socket & { encrypted?: boolean }).encrypted
          ? 'https'
          : 'http'

    try {
      return origin === new URL(`${protocol}://${host}`).origin
    } catch {
      return false
    }
  }

  #matchesOriginPolicy(origin: string, httpContext: HttpContext): boolean {
    let originPolicy = this.config?.origin ?? false

    if (typeof originPolicy === 'function') {
      originPolicy = originPolicy(origin, httpContext)
    }

    if (originPolicy === true || originPolicy === '*') {
      return true
    }

    if (originPolicy === false) {
      return false
    }

    if (Array.isArray(originPolicy)) {
      return originPolicy.includes(origin)
    }

    return originPolicy.split(',').includes(origin)
  }

  async #prepareUpgrade(request: IncomingMessage): Promise<UpgradeResult<User>> {
    return this.runWithHttpContext<UpgradeResult<User>>(
      request,
      async (httpContext) => {
        const accepted = await this.#authenticate(httpContext)
        return accepted ? { accepted } : { rejected: 'authentication' }
      },
      (httpContext) => {
        return this.#isOriginAllowed(request, httpContext) ? undefined : { rejected: 'origin' }
      }
    )
  }

  async #authenticate(httpContext: HttpContext): Promise<AcceptedUpgrade<User> | null> {
    const config = this.config
    if (!config?.authenticate) {
      return { httpContext }
    }

    try {
      const ctx: SocketUpgradeContext = {
        httpContext,
      }

      const result = await config.authenticate(ctx)

      if (result === false || result === null || result === undefined) {
        return null
      }

      return {
        httpContext,
        user: result,
      }
    } catch (error) {
      try {
        this.reportError('socket authentication failed: %s', error)
      } catch {
        // Error reporting must never change an authentication rejection.
      }
      return null
    }
  }
}
