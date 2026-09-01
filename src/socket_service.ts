import { randomUUID } from 'node:crypto'
import type { IncomingMessage } from 'node:http'
import type { Server as HttpServer } from 'node:http'
import type { Logger } from '@adonisjs/logger'
import Emittery from 'emittery'
import { WebSocket, WebSocketServer } from 'ws'
import type { RawData } from 'ws'
import type {
  AuthenticatedSocket,
  SocketConfig,
  SocketEvents,
  ChannelAck,
  RawSocket,
  SocketHealthSnapshot,
  SocketServiceStatus,
} from './types.js'
import type { SocketUpgradeContextRunner } from './socket_upgrader.js'
import {
  SERVER_DISCONNECT_CODE,
  type ChannelMessage,
  type ServerProtocolMessage,
} from './shared_types.js'
import type { ChannelRouter } from './channel_router.js'
import type { PresenceManager } from './presence_manager.js'
import { ChannelSubscriptions, type ChannelSubscriptionLimits } from './channel_subscriptions.js'
import { Message, type TransportMessage } from './protocol/message.js'
import { MessageQueue } from './message_queue.js'
import { Reply } from './protocol/reply.js'
import type { SocketEmission, SocketEmissionSink } from './broadcasting.js'
import { SocketFake } from './fake_socket.js'
import { SocketBus } from './socket_bus.js'
import { PresenceSocketFrame, type SerializablePresenceSocket } from './presence_socket_frame.js'
import {
  SocketUpgrader,
  type AcceptedUpgrade,
  type SocketUpgradeHandler,
} from './socket_upgrader.js'
import { parseDuration, type Duration } from './duration.js'
import { SocketResponseError } from './base_channel.js'
import {
  broadcastChannel,
  channelMessageChannel,
  connectChannel,
  disconnectChannel,
  subscribeChannel,
  unsubscribeChannel,
} from './tracing_channels.js'
import type {
  SocketBroadcastMessage,
  SocketChannelMessage,
  SocketSubscribeMessage,
  SocketUnsubscribeMessage,
} from './types/tracing_channels.js'

const DEFAULT_PING_INTERVAL = 25_000
const DEFAULT_PING_TIMEOUT = 5_000
const DEFAULT_MAX_PAYLOAD = 1024 * 1024
const DEFAULT_MAX_QUEUED_MESSAGES = 100
const DEFAULT_MAX_MESSAGES_PER_INTERVAL = 1000
const DEFAULT_MESSAGE_RATE_INTERVAL = 1000
const DEFAULT_MAX_BUFFERED_AMOUNT = 16 * 1024 * 1024
const DEFAULT_MAX_OUTBOUND_PAYLOAD = 1024 * 1024
const DEFAULT_MAX_SUBSCRIPTIONS_PER_SOCKET = 100
const DEFAULT_MAX_CHANNEL_NAME_LENGTH = 255
const DEFAULT_SHUTDOWN_TIMEOUT = 5_000
const CLOSE_POLICY_VIOLATION = 1008
const CLOSE_MESSAGE_TOO_BIG = 1009
const CLOSE_OUTBOUND_BUFFER_LIMIT = 'Socket outbound buffer limit exceeded'
const CLOSE_OUTBOUND_MESSAGE_TOO_BIG = 'Socket outbound message too big'
const SERVER_DISCONNECT_REASON = 'Socket server disconnected'

interface InboundMessageConfig {
  maxPayload: number
  maxQueuedMessages: number
  maxMessagesPerInterval: number
  messageRateInterval: number
  subscriptionLimits: ChannelSubscriptionLimits
}

interface OutboundMessageConfig {
  maxBufferedAmount: number
  maxOutboundPayload: number
}

interface SocketMessageRate {
  count: number
  intervalStartedAt: number
}

async function settleBeforeDeadline<Result>(
  promise: Promise<Result>,
  deadline: number
): Promise<PromiseSettledResult<Result> | null> {
  const remaining = deadline - Date.now()
  if (remaining <= 0) {
    void promise.catch(() => {})
    return null
  }

  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise.then(
        (value): PromiseFulfilledResult<Result> => ({ status: 'fulfilled', value }),
        (reason): PromiseRejectedResult => ({ status: 'rejected', reason })
      ),
      new Promise<null>((resolve) => {
        timer = setTimeout(() => resolve(null), remaining)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function serializeFrame(frame: ServerProtocolMessage): string {
  return JSON.stringify(frame)
}

function sendFrame(
  connection: WebSocket,
  frame: ServerProtocolMessage,
  config: OutboundMessageConfig
): boolean {
  const serializedFrame = serializeFrame(frame)
  return sendSerializedFrameWithBackpressure(connection, serializedFrame, config)
}

function sendSerializedFrameWithBackpressure(
  connection: WebSocket,
  serializedFrame: string,
  config: OutboundMessageConfig
): boolean {
  if (connection.readyState !== WebSocket.OPEN) {
    return false
  }

  const payloadSize = Buffer.byteLength(serializedFrame)
  if (payloadSize > config.maxOutboundPayload) {
    closeConnection(connection, CLOSE_MESSAGE_TOO_BIG, CLOSE_OUTBOUND_MESSAGE_TOO_BIG)
    return false
  }

  if (connection.bufferedAmount + payloadSize > config.maxBufferedAmount) {
    closeConnection(connection, CLOSE_POLICY_VIOLATION, CLOSE_OUTBOUND_BUFFER_LIMIT)
    return false
  }

  connection.send(serializedFrame)
  return true
}

function parsePositiveInteger(name: string, value: number | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback
  }

  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`)
  }

  return value
}

function resolveHeartbeatConfig(
  config: { pingInterval?: Duration; pingTimeout?: Duration } | undefined
): { interval: number; timeout: number } | null {
  const pingInterval = parseDuration('websocket.pingInterval', config?.pingInterval)
  const pingTimeout = parseDuration('websocket.pingTimeout', config?.pingTimeout)

  if (pingInterval === undefined && pingTimeout === undefined) {
    return null
  }

  return {
    interval: pingInterval ?? DEFAULT_PING_INTERVAL,
    timeout: pingTimeout ?? DEFAULT_PING_TIMEOUT,
  }
}

function resolveInboundMessageConfig(
  config:
    | {
        maxPayload?: number
        maxQueuedMessages?: number
        maxMessagesPerInterval?: number
        messageRateInterval?: Duration
        maxSubscriptionsPerSocket?: number
        maxChannelNameLength?: number
      }
    | undefined
): InboundMessageConfig {
  const messageRateInterval =
    parseDuration('websocket.messageRateInterval', config?.messageRateInterval) ??
    DEFAULT_MESSAGE_RATE_INTERVAL

  return {
    maxPayload: parsePositiveInteger(
      'websocket.maxPayload',
      config?.maxPayload,
      DEFAULT_MAX_PAYLOAD
    ),
    maxQueuedMessages: parsePositiveInteger(
      'websocket.maxQueuedMessages',
      config?.maxQueuedMessages,
      DEFAULT_MAX_QUEUED_MESSAGES
    ),
    maxMessagesPerInterval: parsePositiveInteger(
      'websocket.maxMessagesPerInterval',
      config?.maxMessagesPerInterval,
      DEFAULT_MAX_MESSAGES_PER_INTERVAL
    ),
    messageRateInterval,
    subscriptionLimits: {
      maxSubscriptionsPerSocket: parsePositiveInteger(
        'websocket.maxSubscriptionsPerSocket',
        config?.maxSubscriptionsPerSocket,
        DEFAULT_MAX_SUBSCRIPTIONS_PER_SOCKET
      ),
      maxChannelNameLength: parsePositiveInteger(
        'websocket.maxChannelNameLength',
        config?.maxChannelNameLength,
        DEFAULT_MAX_CHANNEL_NAME_LENGTH
      ),
    },
  }
}

function resolveOutboundMessageConfig(
  config:
    | {
        maxBufferedAmount?: number
        maxOutboundPayload?: number
      }
    | undefined
): OutboundMessageConfig {
  return {
    maxBufferedAmount: parsePositiveInteger(
      'websocket.maxBufferedAmount',
      config?.maxBufferedAmount,
      DEFAULT_MAX_BUFFERED_AMOUNT
    ),
    maxOutboundPayload: parsePositiveInteger(
      'websocket.maxOutboundPayload',
      config?.maxOutboundPayload,
      DEFAULT_MAX_OUTBOUND_PAYLOAD
    ),
  }
}

function getRawDataByteLength(data: RawData): number {
  if (Array.isArray(data)) {
    return data.reduce((total, chunk) => total + chunk.byteLength, 0)
  }

  return data.byteLength
}

function closeConnection(connection: WebSocket, code: number, reason: string): void {
  if (connection.readyState === WebSocket.OPEN) {
    connection.close(code, reason)
  }
}

interface SocketServiceRuntime<User> {
  kind: 'runtime'
  server: WebSocketServer
  httpServer: HttpServer
  upgradeHandler: SocketUpgradeHandler
  channelSubscriptions: ChannelSubscriptions<User>
  bus: SocketBus | null
}

interface SocketServiceBootOperation<User> {
  kind: 'boot'
  promise: Promise<void>
  closeRequested: boolean
  channelSubscriptions?: ChannelSubscriptions<User>
  bus?: SocketBus
  busCloseBeforeStart?: Promise<void>
  busCloseAfterStart?: Promise<void>
  runtime?: SocketServiceRuntime<User>
}

type SocketServiceResources<User> = SocketServiceBootOperation<User> | SocketServiceRuntime<User>

type SocketServiceLifecycle<User> =
  | { status: 'stopped' }
  | { status: 'starting'; operation: SocketServiceBootOperation<User> }
  | { status: 'ready'; runtime: SocketServiceRuntime<User> }
  | {
      status: 'stopping'
      resources: SocketServiceResources<User>
      close: Promise<void>
    }
  | {
      status: 'failed'
      error: Error
      resources?: SocketServiceResources<User>
      close?: Promise<void>
    }

/**
 * Main service for managing WebSocket connections.
 */
export class SocketService<User = unknown> extends Emittery<SocketEvents<User>> {
  #sockets = new Map<string, AuthenticatedSocket<User>>()
  #presenceManager: PresenceManager | null = null
  #fake: SocketFake | null = null
  #deliverySink: SocketEmissionSink = {
    dispatch: (emission) => {
      this.#deliverBroadcast(emission)
    },
  }
  #broadcastSink: SocketEmissionSink = this.#deliverySink
  #lifecycle: SocketServiceLifecycle<User> = { status: 'stopped' }
  #heartbeatInterval: ReturnType<typeof setInterval> | null = null
  #heartbeatTimeouts = new Map<string, ReturnType<typeof setTimeout>>()
  #messageQueue = new MessageQueue()
  #messageRates = new Map<string, SocketMessageRate>()
  #socketFinalizations = new Map<string, Promise<void>>()
  #outboundMessageConfig = resolveOutboundMessageConfig(undefined)
  #shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT
  #logger: Logger | null = null

  /**
   * Accesses the underlying ws server.
   */
  get server(): WebSocketServer {
    const runtime = this.#runtime
    if (!runtime) {
      throw new Error('WebSocket server is not initialized. Call boot() first.')
    }

    return runtime.server
  }

  get #resources(): SocketServiceResources<User> | undefined {
    switch (this.#lifecycle.status) {
      case 'starting':
        return this.#lifecycle.operation
      case 'ready':
        return this.#lifecycle.runtime
      case 'stopping':
      case 'failed':
        return this.#lifecycle.resources
      case 'stopped':
        return undefined
    }
  }

  get #runtime(): SocketServiceRuntime<User> | undefined {
    if (this.#lifecycle.status === 'ready') return this.#lifecycle.runtime
    if (this.#lifecycle.status !== 'failed') return undefined

    const resources = this.#lifecycle.resources
    if (!resources) return undefined
    if (resources.kind === 'runtime') return resources
    return resources.runtime
  }

  get #channelSubscriptions(): ChannelSubscriptions<User> | undefined {
    const resources = this.#resources
    if (!resources) return undefined
    if (resources.kind === 'runtime') return resources.channelSubscriptions
    return resources.runtime?.channelSubscriptions ?? resources.channelSubscriptions
  }

  get #bus(): SocketBus | null | undefined {
    const resources = this.#resources
    if (!resources) return undefined
    if (resources.kind === 'runtime') return resources.bus
    return resources.runtime?.bus ?? resources.bus
  }

  #isStarting(operation: SocketServiceBootOperation<User>): boolean {
    return this.#lifecycle.status === 'starting' && this.#lifecycle.operation === operation
  }

  #closeBootBusAfterStart(operation: SocketServiceBootOperation<User>): Promise<void> {
    const bus = operation.bus
    if (!bus) return Promise.resolve()
    if (operation.busCloseAfterStart) return operation.busCloseAfterStart

    const close = bus.close()
    operation.busCloseAfterStart = close
    return close
  }

  setPresenceManager(presenceManager: PresenceManager): void {
    this.#presenceManager = presenceManager
    this.#channelSubscriptions?.setPresenceManager(presenceManager)
    this.#configurePresenceFetcher(this.#bus)
  }

  boot(
    httpServer: HttpServer,
    config: SocketConfig<User>,
    channelRouter: ChannelRouter,
    logger: Logger,
    runWithHttpContext: SocketUpgradeContextRunner
  ): Promise<void> {
    if (this.#lifecycle.status === 'ready') return Promise.resolve()
    if (this.#lifecycle.status === 'starting') return this.#lifecycle.operation.promise
    if (this.#lifecycle.status === 'stopping') {
      return Promise.reject(new Error('Socket service is stopping'))
    }
    if (this.#lifecycle.status === 'failed' && this.#lifecycle.resources) {
      return Promise.reject(new Error('Socket service must be closed before rebooting'))
    }

    this.#logger = logger
    const deferred = Promise.withResolvers<void>()
    const operation: SocketServiceBootOperation<User> = {
      kind: 'boot',
      promise: deferred.promise,
      closeRequested: false,
    }
    this.#lifecycle = { status: 'starting', operation }

    void this.#boot(httpServer, config, channelRouter, logger, runWithHttpContext, operation).then(
      deferred.resolve,
      deferred.reject
    )
    return operation.promise
  }

  async #boot(
    httpServer: HttpServer,
    config: SocketConfig<User>,
    channelRouter: ChannelRouter,
    logger: Logger,
    runWithHttpContext: SocketUpgradeContextRunner,
    operation: SocketServiceBootOperation<User>
  ): Promise<void> {
    try {
      const websocketConfig = config.websocket
      const heartbeatConfig = resolveHeartbeatConfig(websocketConfig)
      const inboundMessageConfig = resolveInboundMessageConfig(websocketConfig)
      this.#outboundMessageConfig = resolveOutboundMessageConfig(websocketConfig)
      this.#shutdownTimeout =
        parseDuration('websocket.shutdownTimeout', websocketConfig?.shutdownTimeout) ??
        DEFAULT_SHUTDOWN_TIMEOUT

      operation.channelSubscriptions = new ChannelSubscriptions(
        this,
        channelRouter,
        logger,
        this.#presenceManager ?? undefined,
        inboundMessageConfig.subscriptionLimits
      )

      if (config.transport) {
        operation.bus = new SocketBus(config.transport, {
          channel: (message) => {
            this.#traceBroadcast(
              {
                target: 'channel',
                channel: message.channel,
                event: message.event,
                via: 'bus',
                except: message.except,
              },
              () =>
                this.#emitToChannelLocally(
                  message.channel,
                  message.event,
                  message.data,
                  message.except
                )
            )
          },
          broadcast: (message) => {
            this.#traceBroadcast({ target: 'global', event: message.event, via: 'bus' }, () =>
              this.#broadcastLocally(message.event, message.data)
            )
          },
          presenceSockets: (channel) => {
            return this.#getLocalPresenceSockets(channel)
          },
        })

        await operation.bus.start()
        if (!this.#isStarting(operation)) {
          throw new Error('Socket service boot interrupted by shutdown')
        }
      }

      const websocketServer = new WebSocketServer({
        noServer: true,
        maxPayload: inboundMessageConfig.maxPayload,
      })

      const upgrader = new SocketUpgrader<User>(
        websocketServer,
        websocketConfig,
        runWithHttpContext,
        (message, error) => this.#warn(message, error)
      )

      const upgradeHandler: SocketUpgradeHandler = (request, socket, head) => {
        upgrader
          .handle(request, socket, head, (connection, upgradeRequest, accepted) => {
            this.#handleConnection(connection, upgradeRequest, accepted, inboundMessageConfig)
          })
          .catch((error) => {
            this.#warn('failed to upgrade socket connection: %s', error)
            SocketUpgrader.reject(socket, 500, 'Internal Server Error')
          })
      }
      const runtime: SocketServiceRuntime<User> = {
        kind: 'runtime',
        server: websocketServer,
        httpServer,
        upgradeHandler,
        channelSubscriptions: operation.channelSubscriptions,
        bus: operation.bus ?? null,
      }
      operation.runtime = runtime
      httpServer.on('upgrade', upgradeHandler)

      this.#startHeartbeat(heartbeatConfig)
      if (!this.#isStarting(operation)) {
        throw new Error('Socket service boot interrupted by shutdown')
      }

      this.#lifecycle = { status: 'ready', runtime }
      this.#configurePresenceFetcher(runtime.bus)
    } catch (error) {
      if (!this.#isStarting(operation)) {
        if (operation.closeRequested) {
          await this.#closeBootBusAfterStart(operation)
        }
        if (
          this.#lifecycle.status === 'failed' &&
          this.#lifecycle.resources === operation &&
          !this.#lifecycle.close
        ) {
          await this.close()
        }
        throw error
      }

      const failure = error instanceof Error ? error : new Error('Socket service boot failed')
      this.#lifecycle = { status: 'failed', error: failure, resources: operation }
      await this.close()
      throw error
    }
  }

  #handleConnection(
    connection: WebSocket,
    request: IncomingMessage,
    upgrade: AcceptedUpgrade<User>,
    inboundMessageConfig: InboundMessageConfig
  ): void {
    if (this.#lifecycle.status !== 'ready') {
      connection.terminate()
      return
    }

    const socket = this.#wrapSocket(connection, request, upgrade)

    connectChannel.traceSync(
      () => {
        this.#sockets.set(socket.id, socket)
        this.#emitLifecycleEvent('connect', { socket })
      },
      { socketId: socket.id }
    )

    connection.on('error', () => {})

    connection.on('message', (data) => {
      if (getRawDataByteLength(data) > inboundMessageConfig.maxPayload) {
        closeConnection(connection, CLOSE_MESSAGE_TOO_BIG, 'Message too big')
        return
      }

      if (!this.#acceptMessageRate(socket.id, inboundMessageConfig)) {
        closeConnection(connection, CLOSE_POLICY_VIOLATION, 'Socket message rate limit exceeded')
        return
      }

      const message = Message.fromTransport(data.toString())

      if (message.type === 'ping') {
        sendFrame(
          socket.raw.connection,
          { id: message.id, type: 'pong' },
          this.#outboundMessageConfig
        )
        return
      }

      const enqueued = this.#messageQueue.enqueue(
        socket.id,
        async () => {
          await this.#handleMessage(socket, message).catch((error) => {
            try {
              this.#logger?.warn('unexpected socket message failure: %s', error)
            } catch {
              // Logging must never prevent a protocol response.
            }

            sendFrame(
              connection,
              message.id === undefined
                ? { type: 'error', error: 'Unexpected socket error' }
                : Reply.error(message.id, 'Unexpected socket error').toFrame(),
              this.#outboundMessageConfig
            )
          })
        },
        { maxDepth: inboundMessageConfig.maxQueuedMessages }
      )

      if (!enqueued) {
        closeConnection(connection, CLOSE_POLICY_VIOLATION, 'Socket message queue limit exceeded')
      }
    })

    connection.on('pong', () => {
      this.#clearHeartbeatTimeout(socket.id)
    })

    connection.on('close', () => {
      void Promise.resolve()
        .then(() => this.#finalizeSocket(socket))
        .catch((error) => this.markFailed(error))
    })
  }

  #finalizeSocket(socket: AuthenticatedSocket<User>): Promise<void> {
    const pending = this.#socketFinalizations.get(socket.id)
    if (pending) {
      return pending
    }

    if (!this.#sockets.has(socket.id)) {
      return Promise.resolve()
    }

    const channelSubscriptions = this.#channelSubscriptions
    const finalization = disconnectChannel
      .tracePromise(
        async () => {
          this.#clearHeartbeatTimeout(socket.id)
          this.#messageRates.delete(socket.id)
          await this.#messageQueue.drain(socket.id)
          this.#messageQueue.delete(socket.id)
          await channelSubscriptions?.leaveAll(socket)
          channelSubscriptions?.deleteSocket(socket.id)
          this.#sockets.delete(socket.id)
          this.#emitLifecycleEvent('disconnect', { socket, reason: 'close' })
        },
        {
          socketId: socket.id,
          reason: 'close',
          subscriptions: channelSubscriptions?.subscriptionCountFor(socket.id) ?? 0,
        }
      )
      .finally(() => {
        this.#socketFinalizations.delete(socket.id)
      })

    this.#socketFinalizations.set(socket.id, finalization)
    return finalization
  }

  async #handleMessage(
    socket: AuthenticatedSocket<User>,
    message: TransportMessage
  ): Promise<void> {
    if (!message.valid) {
      sendFrame(socket.raw.connection, message.toRejectionFrame(), this.#outboundMessageConfig)
      return
    }

    switch (message.type) {
      case 'ping':
        return
      case 'subscribe': {
        const traceMessage: SocketSubscribeMessage = {
          socketId: socket.id,
          channel: message.channel,
        }

        await subscribeChannel.tracePromise(async () => {
          const result = await this.#channelSubscriptions?.subscribe(socket, message.channel)

          traceMessage.created = result?.created ?? false
          traceMessage.ok = result?.ack.ok ?? false
          if (!result?.ack.ok) {
            traceMessage.error = result?.ack.error ?? 'Socket service is not initialized'
          }

          if (result?.created) {
            this.#emitLifecycleEvent('subscribe', { socket, channel: message.channel })
          }

          sendFrame(
            socket.raw.connection,
            Reply.fromSubscribeResult(message.id, result).toFrame(),
            this.#outboundMessageConfig
          )
        }, traceMessage)
        return
      }
      case 'unsubscribe': {
        const traceMessage: SocketUnsubscribeMessage = {
          socketId: socket.id,
          channel: message.channel,
        }

        await unsubscribeChannel.tracePromise(async () => {
          traceMessage.removed =
            (await this.#channelSubscriptions?.leave(socket, message.channel)) ?? false
          traceMessage.ok = true
          if (traceMessage.removed) {
            this.#emitLifecycleEvent('unsubscribe', { socket, channel: message.channel })
          }
          sendFrame(
            socket.raw.connection,
            Reply.ok(message.id).toFrame(),
            this.#outboundMessageConfig
          )
        }, traceMessage)
        return
      }
      case 'message':
      case 'whisper': {
        await this.#handleChannelFrame(socket, message)
        return
      }
    }
  }

  async #handleChannelFrame(
    socket: AuthenticatedSocket<User>,
    message: Extract<TransportMessage, { type: 'message' | 'whisper' }>
  ): Promise<void> {
    const payload = message.toChannelMessage()
    const traceMessage: SocketChannelMessage = {
      socketId: socket.id,
      channel: payload.channel,
      event: payload.event,
    }

    await channelMessageChannel.tracePromise(async () => {
      const ack = await this.#handleChannelPayload(socket, message.type, payload)
      traceMessage.ok = ack.ok
      if (!ack.ok) {
        traceMessage.error = ack.error
      }

      sendFrame(
        socket.raw.connection,
        Reply.fromChannelAck(message.id, ack).toFrame(),
        this.#outboundMessageConfig
      )
    }, traceMessage)
  }

  async #handleChannelPayload(
    socket: AuthenticatedSocket<User>,
    type: 'message' | 'whisper',
    payload: ChannelMessage
  ): Promise<ChannelAck> {
    if (typeof payload.channel !== 'string' || typeof payload.event !== 'string') {
      return { ok: false, error: `Invalid channel ${type}` }
    }

    if (type === 'message') {
      return this.#channelSubscriptions!.handleMessage(socket, payload)
    }

    return this.#channelSubscriptions!.relayWhisper(socket, payload)
  }

  #wrapSocket(
    connection: WebSocket,
    request: IncomingMessage,
    upgrade: AcceptedUpgrade<User>
  ): AuthenticatedSocket<User> {
    const raw: RawSocket = {
      id: randomUUID(),
      data: {},
      connection,
      request,
      httpContext: upgrade.httpContext,
    }

    return {
      id: raw.id,
      user: upgrade.user,

      getUserOrFail() {
        if (this.user === undefined) {
          throw new SocketResponseError('Unauthorized')
        }

        return this.user
      },

      emit: (event, data) => {
        this.#sendSerializedEventFrame(connection, serializeFrame({ type: 'event', event, data }))
      },

      disconnect() {
        connection.close(SERVER_DISCONNECT_CODE, SERVER_DISCONNECT_REASON)
      },

      raw,
    }
  }

  to(channel: string) {
    return {
      emit: (event: string, data: unknown) => {
        this.#broadcastSink.dispatch({ target: 'channel', channel, event, data })
      },
      except: (socketId: string) => ({
        emit: (event: string, data: unknown) => {
          this.#broadcastSink.dispatch({
            target: 'channel',
            channel,
            event,
            data,
            except: [socketId],
          })
        },
      }),
    }
  }

  broadcast(event: string, data: unknown): void {
    this.#broadcastSink.dispatch({ target: 'global', event, data })
  }

  #deliverBroadcast(emission: SocketEmission): void {
    switch (emission.target) {
      case 'channel': {
        const channel = emission.channel
        const serializedFrame = serializeFrame({
          type: 'event',
          channel,
          event: emission.event,
          data: emission.data,
        })
        this.#bus?.publishChannel(channel, emission.event, emission.data, emission.except)

        const traceMessage: SocketBroadcastMessage = {
          target: 'channel',
          channel,
          event: emission.event,
          via: 'local',
        }
        if (emission.except) {
          traceMessage.except = emission.except
        }

        this.#traceBroadcast(traceMessage, () =>
          this.#emitToChannelLocally(
            channel,
            emission.event,
            emission.data,
            emission.except,
            serializedFrame
          )
        )
        return
      }
      case 'global': {
        const serializedFrame = serializeFrame({
          type: 'event',
          event: emission.event,
          data: emission.data,
        })
        this.#bus?.publishBroadcast(emission.event, emission.data)
        this.#traceBroadcast({ target: 'global', event: emission.event, via: 'local' }, () =>
          this.#broadcastLocally(emission.event, emission.data, serializedFrame)
        )
      }
    }
  }

  #traceBroadcast(message: SocketBroadcastMessage, deliver: () => number): void {
    broadcastChannel.traceSync(() => {
      message.delivered = deliver()
    }, message)
  }

  #sendSerializedEventFrame(connection: WebSocket, serializedFrame: string): boolean {
    return sendSerializedFrameWithBackpressure(
      connection,
      serializedFrame,
      this.#outboundMessageConfig
    )
  }

  #emitToChannelLocally(
    channel: string,
    event: string,
    data: unknown,
    except: string[] = [],
    serializedFrame = serializeFrame({ type: 'event', channel, event, data })
  ): number {
    let delivered = 0

    for (const socketId of this.#channelSubscriptions?.getSocketIds(channel) ?? []) {
      if (except.includes(socketId)) {
        continue
      }

      const socket = this.#sockets.get(socketId)
      if (socket && this.#sendSerializedEventFrame(socket.raw.connection, serializedFrame)) {
        delivered += 1
      }
    }

    return delivered
  }

  #broadcastLocally(
    event: string,
    data: unknown,
    serializedFrame = serializeFrame({ type: 'event', event, data })
  ): number {
    let delivered = 0

    for (const socket of this.#sockets.values()) {
      if (this.#sendSerializedEventFrame(socket.raw.connection, serializedFrame)) {
        delivered += 1
      }
    }

    return delivered
  }

  #configurePresenceFetcher(bus: SocketBus | null | undefined): void {
    if (!this.#presenceManager || !bus) {
      return
    }

    this.#presenceManager.setSocketFetcher(async (channel) => {
      return [
        ...this.#presenceManager!.getLocalSockets(channel),
        ...(await bus.fetchPresenceSockets(channel)),
      ]
    })
  }

  #getLocalPresenceSockets(channel: string): SerializablePresenceSocket[] {
    return (this.#presenceManager?.getLocalSockets(channel) ?? []).map((socket) => {
      return PresenceSocketFrame.fromSocket(channel, socket).toTransport()
    })
  }

  getSocket(socketId: string): AuthenticatedSocket<User> | undefined {
    return this.#sockets.get(socketId)
  }

  get connectionsCount(): number {
    return this.#sockets.size
  }

  get status(): SocketServiceStatus {
    return this.#lifecycle.status
  }

  get ready(): boolean {
    return this.#lifecycle.status === 'ready'
  }

  fake(): SocketFake {
    const fake = new SocketFake(() => {
      if (this.#fake === fake) {
        this.restore()
      }
    })

    this.#fake = fake
    this.#broadcastSink = fake
    return fake
  }

  restore(): void {
    this.#fake = null
    this.#broadcastSink = this.#deliverySink
  }

  markFailed(error: unknown): void {
    const failure = error instanceof Error ? error : new Error('Socket service failed')
    const lifecycle = this.#lifecycle

    if (lifecycle.status === 'stopped') {
      this.#lifecycle = { status: 'failed', error: failure }
      return
    }
    if (lifecycle.status === 'starting') {
      this.#lifecycle = { status: 'failed', error: failure, resources: lifecycle.operation }
      return
    }
    if (lifecycle.status === 'ready') {
      this.#lifecycle = { status: 'failed', error: failure, resources: lifecycle.runtime }
      return
    }

    this.#lifecycle = { ...lifecycle, status: 'failed', error: failure }
  }

  #warn(message: string, error: unknown): void {
    try {
      this.#logger?.warn(message, error)
    } catch {
      // Logging must never change socket behavior.
    }
  }

  #emitLifecycleEvent<EventName extends keyof SocketEvents<User>>(
    eventName: EventName,
    event: SocketEvents<User>[EventName]
  ): void {
    void this.emit(eventName, event).catch((error) => {
      try {
        this.#logger?.warn('socket %s listener failed: %s', eventName, error)
      } catch {
        // Lifecycle observers and their reporting must not affect the socket service.
      }
    })
  }

  health(): SocketHealthSnapshot {
    const error = this.#lifecycle.status === 'failed' ? this.#lifecycle.error : undefined
    return {
      status: this.#lifecycle.status,
      ready: this.ready,
      connections: this.#sockets.size,
      channels: this.#channelSubscriptions?.channelsCount ?? 0,
      ...(error ? { lastError: error.message } : {}),
    }
  }

  #startHeartbeat(config: { interval: number; timeout: number } | null): void {
    this.#stopHeartbeat()

    if (!config) {
      return
    }

    this.#heartbeatInterval = setInterval(() => {
      this.#pingSockets(config.timeout)
    }, config.interval)
  }

  #pingSockets(timeout: number): void {
    for (const socket of this.#sockets.values()) {
      const { connection } = socket.raw

      if (connection.readyState !== WebSocket.OPEN) {
        this.#clearHeartbeatTimeout(socket.id)
        continue
      }

      if (this.#heartbeatTimeouts.has(socket.id)) {
        continue
      }

      const timeoutId = setTimeout(() => {
        this.#heartbeatTimeouts.delete(socket.id)
        connection.terminate()
      }, timeout)

      this.#heartbeatTimeouts.set(socket.id, timeoutId)

      try {
        connection.ping()
      } catch {
        this.#clearHeartbeatTimeout(socket.id)
        connection.terminate()
      }
    }
  }

  #clearHeartbeatTimeout(socketId: string): void {
    const timeout = this.#heartbeatTimeouts.get(socketId)

    if (!timeout) {
      return
    }

    clearTimeout(timeout)
    this.#heartbeatTimeouts.delete(socketId)
  }

  #stopHeartbeat(): void {
    if (this.#heartbeatInterval) {
      clearInterval(this.#heartbeatInterval)
      this.#heartbeatInterval = null
    }

    for (const timeout of this.#heartbeatTimeouts.values()) {
      clearTimeout(timeout)
    }

    this.#heartbeatTimeouts.clear()
  }

  #acceptMessageRate(socketId: string, config: InboundMessageConfig): boolean {
    const now = Date.now()
    const rate = this.#messageRates.get(socketId)

    if (!rate || now - rate.intervalStartedAt >= config.messageRateInterval) {
      this.#messageRates.set(socketId, { count: 1, intervalStartedAt: now })
      return true
    }

    if (rate.count >= config.maxMessagesPerInterval) {
      return false
    }

    rate.count += 1
    return true
  }

  #cleanup(channelSubscriptions: ChannelSubscriptions<User> | undefined): void {
    this.#stopHeartbeat()
    this.#sockets.clear()
    this.#messageQueue.clear()
    this.#messageRates.clear()
    this.#socketFinalizations.clear()
    channelSubscriptions?.clear()
  }

  close(): Promise<void> {
    const lifecycle = this.#lifecycle
    if (lifecycle.status === 'stopped') return Promise.resolve()
    if (lifecycle.status === 'stopping') return lifecycle.close
    if (lifecycle.status === 'failed' && lifecycle.close) return lifecycle.close
    if (lifecycle.status === 'failed' && !lifecycle.resources) return Promise.resolve()

    if (lifecycle.status === 'starting') {
      return this.#startClose(lifecycle.operation, lifecycle.operation.promise)
    }
    if (lifecycle.status === 'ready') return this.#startClose(lifecycle.runtime)

    return this.#startClose(lifecycle.resources!, undefined, lifecycle.error)
  }

  #startClose(
    resources: SocketServiceResources<User>,
    pendingBoot?: Promise<void>,
    preservedError?: Error
  ): Promise<void> {
    const deferred = Promise.withResolvers<void>()
    const close = deferred.promise
    if (resources.kind === 'boot') resources.closeRequested = true
    this.#lifecycle = preservedError
      ? { status: 'failed', error: preservedError, resources, close }
      : { status: 'stopping', resources, close }

    void this.#close(resources, pendingBoot).then(
      () => {
        if (!('close' in this.#lifecycle) || this.#lifecycle.close !== close) return

        this.#lifecycle =
          this.#lifecycle.status === 'failed'
            ? { status: 'failed', error: this.#lifecycle.error, close }
            : { status: 'stopped' }
        deferred.resolve()
      },
      (error) => {
        const failure = error instanceof Error ? error : new Error('Socket service shutdown failed')
        if ('close' in this.#lifecycle && this.#lifecycle.close === close) {
          this.#lifecycle = { status: 'failed', error: failure, close }
        }
        deferred.reject(error)
      }
    )
    return close
  }

  async #waitForBootClose(pendingBoot: Promise<void>, deadline: number): Promise<void> {
    const result = await settleBeforeDeadline(pendingBoot, deadline)
    if (result === null) {
      this.#warn(
        'socket boot exceeded the %dms shutdown timeout; forcing cleanup',
        this.#shutdownTimeout
      )
    }
  }

  #startServerClose(server: WebSocketServer | undefined): Promise<unknown | null> {
    if (!server) return Promise.resolve(null)

    return new Promise((resolve) => {
      server.close((error) => resolve(error ?? null))
    })
  }

  async #closeSockets(deadline: number): Promise<unknown[]> {
    this.#stopHeartbeat()
    const sockets = [...this.#sockets.values()]
    for (const socket of sockets) {
      const { connection } = socket.raw
      connection.close(SERVER_DISCONNECT_CODE, SERVER_DISCONNECT_REASON)
      const terminateTimer = setTimeout(() => connection.terminate(), 50)
      connection.once('close', () => clearTimeout(terminateTimer))
    }

    const finalizations = sockets.map((socket) => {
      return Promise.resolve().then(() => this.#finalizeSocket(socket))
    })
    const results = await settleBeforeDeadline(Promise.allSettled(finalizations), deadline)
    if (results === null) {
      this.#warn(
        'socket finalization exceeded the %dms shutdown timeout; forcing cleanup',
        this.#shutdownTimeout
      )
      return []
    }
    if (results.status === 'rejected') return []

    const errors: unknown[] = []
    for (const result of results.value) {
      if (result.status === 'rejected') errors.push(result.reason)
    }
    return errors
  }

  async #closeBus(
    resources: SocketServiceResources<User>,
    bus: SocketBus | null | undefined,
    deadline: number
  ): Promise<unknown[]> {
    if (!bus) return []

    const close =
      resources.kind === 'boot'
        ? (resources.busCloseAfterStart ?? (resources.busCloseBeforeStart ??= bus.close()))
        : bus.close()
    const result = await settleBeforeDeadline(close, deadline)
    if (result === null) {
      this.#warn(
        'socket transport exceeded the %dms shutdown timeout; forcing cleanup',
        this.#shutdownTimeout
      )
      return []
    }
    return result.status === 'rejected' ? [result.reason] : []
  }

  async #finishServerClose(close: Promise<unknown | null>, deadline: number): Promise<unknown[]> {
    const result = await settleBeforeDeadline(close, deadline)
    if (result === null) {
      this.#warn(
        'socket server exceeded the %dms shutdown timeout; forcing cleanup',
        this.#shutdownTimeout
      )
      return []
    }
    if (result.status === 'rejected' || !result.value) return []
    return [result.value]
  }

  async #close(
    resources: SocketServiceResources<User>,
    pendingBoot?: Promise<void>
  ): Promise<void> {
    const deadline = Date.now() + this.#shutdownTimeout
    if (pendingBoot) await this.#waitForBootClose(pendingBoot, deadline)

    const runtime = resources.kind === 'runtime' ? resources : resources.runtime
    const channelSubscriptions =
      runtime?.channelSubscriptions ??
      (resources.kind === 'boot' ? resources.channelSubscriptions : undefined)
    const server = runtime?.server
    const bus = runtime?.bus ?? (resources.kind === 'boot' ? resources.bus : undefined)
    const httpServer = runtime?.httpServer
    const upgradeHandler = runtime?.upgradeHandler

    if (httpServer && upgradeHandler) {
      httpServer.off('upgrade', upgradeHandler)
    }

    const serverClose = this.#startServerClose(server)
    const errors = await this.#closeSockets(deadline)

    this.#cleanup(channelSubscriptions)
    this.#presenceManager?.setSocketFetcher(null)

    errors.push(...(await this.#closeBus(resources, bus, deadline)))
    errors.push(...(await this.#finishServerClose(serverClose, deadline)))

    if (errors.length > 0) {
      const error =
        errors.length === 1 && errors[0] instanceof Error
          ? errors[0]
          : new AggregateError(errors, 'Socket service shutdown failed')
      throw error
    }
  }
}
