import type { ConnectionState, EventHandler } from './types.js'
import { SERVER_DISCONNECT_CODE, type ServerProtocolMessage } from '../shared_types.js'
import { callClientHandler } from './callback.js'

type Callable = (...args: never[]) => unknown
type IncomingMessage = Extract<ServerProtocolMessage, { type: 'ack' | 'event' }>

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (error: Error) => void
  timer: ReturnType<typeof setTimeout>
}

interface ConnectDeferred extends PromiseWithResolvers<void> {}

interface SocketConnection {
  socket: WebSocket
}

type ClientSocketLifecycle =
  | { kind: 'disconnected'; reconnectAttempt: number }
  | { kind: 'waiting'; timer: ReturnType<typeof setTimeout>; reconnectAttempt: number }
  | { kind: 'opening'; deferred: ConnectDeferred; reconnectAttempt: number }
  | {
      kind: 'connecting'
      connection: SocketConnection
      deferred: ConnectDeferred
      reconnectAttempt: number
    }
  | {
      kind: 'opened'
      connection: SocketConnection
      deferred: ConnectDeferred
      reconnectAttempt: number
    }
  | { kind: 'connected'; connection: SocketConnection }
  | { kind: 'failed'; connection: SocketConnection; reconnectAttempt: number }

type DisconnectedLifecycle = Extract<ClientSocketLifecycle, { kind: 'disconnected' }>
type WaitingLifecycle = Extract<ClientSocketLifecycle, { kind: 'waiting' }>

interface ClientSocketSessionOptions {
  buildUrl: () => string
  createWebSocket?: (url: string) => WebSocket
  autoReconnect?: boolean
  reconnectDelay?: number
  reconnectMaxDelay?: number
  shouldReconnect?: (event: CloseEvent) => boolean
  onConnected?: () => void
}

export class ClientSocketSession {
  #lifecycle: ClientSocketLifecycle = { kind: 'disconnected', reconnectAttempt: 0 }
  #stateHandlers = new Set<Callable>()
  #eventHandlers = new Map<string, Set<Callable>>()
  #pendingRequests = new Map<string, PendingRequest>()
  #requestId = 0
  #buildUrl: () => string
  #createWebSocket: (url: string) => WebSocket
  #autoReconnect: boolean
  #reconnectDelay: number
  #reconnectMaxDelay: number
  #shouldReconnect: (event: CloseEvent) => boolean
  #onConnected?: () => void

  constructor(options: ClientSocketSessionOptions) {
    this.#buildUrl = options.buildUrl
    this.#createWebSocket = options.createWebSocket ?? ((url) => new WebSocket(url))
    this.#autoReconnect = options.autoReconnect ?? true
    this.#reconnectDelay = Math.max(0, options.reconnectDelay ?? 250)
    this.#reconnectMaxDelay = Math.max(this.#reconnectDelay, options.reconnectMaxDelay ?? 5000)
    this.#shouldReconnect =
      options.shouldReconnect ?? ((event) => event.code !== SERVER_DISCONNECT_CODE)
    this.#onConnected = options.onConnected
  }

  get state(): ConnectionState {
    switch (this.#lifecycle.kind) {
      case 'opening':
      case 'connecting':
        return 'connecting'
      case 'opened':
      case 'connected':
        return 'connected'
      case 'disconnected':
      case 'waiting':
      case 'failed':
        return 'disconnected'
    }
  }

  get connected(): boolean {
    return this.state === 'connected'
  }

  connect(): Promise<void> {
    if (this.#lifecycle.kind === 'connected') return Promise.resolve()
    if (this.#lifecycle.kind === 'opening' || this.#lifecycle.kind === 'connecting') {
      return this.#lifecycle.deferred.promise
    }
    if (this.#lifecycle.kind === 'opened') return this.#lifecycle.deferred.promise

    const reconnectAttempt = this.#reconnectAttempt
    const failedSocket =
      this.#lifecycle.kind === 'failed' ? this.#lifecycle.connection.socket : null
    if (this.#lifecycle.kind === 'waiting') clearTimeout(this.#lifecycle.timer)

    const connection = this.#startConnection(reconnectAttempt)
    failedSocket?.close()
    return connection
  }

  #startConnection(reconnectAttempt: number): Promise<void> {
    const deferred = Promise.withResolvers<void>()
    const opening: ClientSocketLifecycle = { kind: 'opening', deferred, reconnectAttempt }
    this.#setLifecycle(opening)
    if (this.#lifecycle !== opening) return deferred.promise

    let socket: WebSocket
    try {
      socket = this.#createWebSocket(this.#buildUrl())
    } catch (error) {
      this.#setLifecycle({ kind: 'disconnected', reconnectAttempt })
      deferred.reject(error)
      return deferred.promise
    }

    const connection = { socket }
    const connecting: ClientSocketLifecycle = {
      kind: 'connecting',
      connection,
      deferred,
      reconnectAttempt,
    }
    this.#lifecycle = connecting

    const cleanup = () => {
      socket.removeEventListener('open', handleOpen)
      socket.removeEventListener('error', handleError)
    }

    const handleOpen = () => {
      if (this.#lifecycle !== connecting) return

      cleanup()
      const opened: ClientSocketLifecycle = {
        kind: 'opened',
        connection,
        deferred,
        reconnectAttempt,
      }
      this.#setLifecycle(opened)
      if (!this.#isCurrent(opened)) return

      this.#lifecycle = { kind: 'connected', connection }
      this.#emitLocal('connect')
      if (this.#onConnected) this.#callHandler(this.#onConnected)
      deferred.resolve()
    }

    const handleError = () => {
      if (this.#lifecycle !== connecting) return

      cleanup()
      deferred.reject(new Error('WebSocket connection failed'))
      this.#setLifecycle({ kind: 'failed', connection, reconnectAttempt })
    }

    socket.addEventListener('open', handleOpen)
    socket.addEventListener('error', handleError)
    socket.addEventListener('message', (event) => {
      if (this.#owns(connection)) this.#handleIncoming(event.data)
    })
    socket.addEventListener('close', (event) => {
      cleanup()
      if (this.#owns(connection)) this.#handleClose(event)
    })

    return deferred.promise
  }

  disconnect(): void {
    const lifecycle = this.#lifecycle
    const shouldEmitDisconnect = this.state !== 'disconnected'
    if (lifecycle.kind === 'waiting') clearTimeout(lifecycle.timer)
    if (
      lifecycle.kind === 'opening' ||
      lifecycle.kind === 'connecting' ||
      lifecycle.kind === 'opened'
    ) {
      lifecycle.deferred.reject(new Error('Socket disconnected'))
    }

    this.#setLifecycle({ kind: 'disconnected', reconnectAttempt: 0 })
    if (shouldEmitDisconnect) {
      this.#emitLocal('disconnect')
    }
    if ('connection' in lifecycle) lifecycle.connection.socket.close()
    this.#rejectPending(new Error('Socket disconnected'))
  }

  onStateChange(handler: EventHandler<ConnectionState>): () => void {
    this.#stateHandlers.add(handler)

    return () => {
      this.#stateHandlers.delete(handler)
    }
  }

  on<T = unknown>(event: string, handler: EventHandler<T>): () => void {
    if (!this.#eventHandlers.has(event)) {
      this.#eventHandlers.set(event, new Set())
    }

    this.#eventHandlers.get(event)!.add(handler)

    return () => {
      this.off(event, handler)
    }
  }

  off<T = unknown>(event: string, handler: EventHandler<T>): void {
    this.#eventHandlers.get(event)?.delete(handler)
  }

  send(message: Record<string, unknown>): void {
    const socket = this.#connection?.socket
    if (socket?.readyState !== WebSocket.OPEN) {
      return
    }

    socket.send(JSON.stringify(message))
  }

  sendRequest(message: Record<string, unknown>, timeout = 5000): Promise<unknown> {
    const socket = this.#connection?.socket
    if (socket?.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error('Socket is not connected'))
    }

    const id = String(++this.#requestId)
    const payload = { ...message, id }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pendingRequests.delete(id)
        reject(new Error('Socket request timed out'))
      }, timeout)

      this.#pendingRequests.set(id, {
        resolve,
        reject,
        timer,
      })
      socket.send(JSON.stringify(payload))
    })
  }

  #handleIncoming(raw: unknown): void {
    const message = this.#parseMessage(raw)
    if (!message) {
      return
    }

    if (message.type === 'ack') {
      this.#resolveAck(message)
      return
    }

    if (message.type === 'event') {
      if (message.channel) {
        if (message.event === 'presence:update') {
          this.#emitLocal(`channel:${message.channel}:presence:update`, message.data)
          return
        }

        this.#emitLocal(`channel:${message.channel}:event`, {
          event: message.event,
          data: message.data,
        })
        return
      }

      this.#emitLocal(message.event, message.data)
    }
  }

  #parseMessage(raw: unknown): IncomingMessage | null {
    try {
      const data = typeof raw === 'string' ? raw : String(raw)
      const message = JSON.parse(data) as Record<string, unknown> | null

      if (!message || typeof message !== 'object' || Array.isArray(message)) {
        return null
      }

      if (
        message.type === 'ack' &&
        typeof message.id === 'string' &&
        ((message.ok === true && message.error === undefined) ||
          (message.ok === false && typeof message.error === 'string' && message.data === undefined))
      ) {
        return message as IncomingMessage
      }

      if (
        message.type === 'event' &&
        typeof message.event === 'string' &&
        (message.channel === undefined || typeof message.channel === 'string')
      ) {
        return message as IncomingMessage
      }

      return null
    } catch {
      return null
    }
  }

  #resolveAck(message: Extract<ServerProtocolMessage, { type: 'ack' }>): void {
    if (!message.id) {
      return
    }

    const pending = this.#pendingRequests.get(message.id)
    if (!pending) {
      return
    }

    clearTimeout(pending.timer)
    this.#pendingRequests.delete(message.id)

    if (message.ok) {
      pending.resolve({ ok: true, data: message.data })
    } else {
      pending.resolve({ ok: false, error: message.error })
    }
  }

  #handleClose(event: CloseEvent): void {
    const lifecycle = this.#lifecycle
    const reconnectAttempt = 'reconnectAttempt' in lifecycle ? lifecycle.reconnectAttempt : 0
    const shouldEmitDisconnect = this.state !== 'disconnected'
    if (lifecycle.kind === 'connecting') {
      lifecycle.deferred.reject(new Error('WebSocket connection failed'))
    }

    const disconnected: DisconnectedLifecycle = { kind: 'disconnected', reconnectAttempt }
    this.#setLifecycle(disconnected)
    if (shouldEmitDisconnect) {
      this.#emitLocal('disconnect')
    }
    this.#rejectPending(new Error('Socket disconnected'))

    if (this.#shouldReconnect(event) && this.#lifecycle === disconnected) {
      this.#scheduleReconnect(disconnected)
    }
  }

  #scheduleReconnect(disconnected: DisconnectedLifecycle): void {
    if (!this.#autoReconnect || this.#lifecycle !== disconnected) return

    const delay = Math.min(
      this.#reconnectDelay * 2 ** disconnected.reconnectAttempt,
      this.#reconnectMaxDelay
    )
    const waiting: WaitingLifecycle = {
      kind: 'waiting',
      reconnectAttempt: disconnected.reconnectAttempt + 1,
      timer: setTimeout(() => {
        if (this.#lifecycle !== waiting) return

        this.#startConnection(waiting.reconnectAttempt).catch(() => {
          if (this.#lifecycle.kind === 'failed') {
            const failed = this.#lifecycle
            const disconnected: DisconnectedLifecycle = {
              kind: 'disconnected',
              reconnectAttempt: failed.reconnectAttempt,
            }
            this.#setLifecycle(disconnected)
            failed.connection.socket.close()
            if (this.#isCurrent(disconnected)) this.#scheduleReconnect(disconnected)
            return
          }

          if (this.#lifecycle.kind === 'disconnected') {
            this.#scheduleReconnect(this.#lifecycle)
          }
        })
      }, delay),
    }
    this.#setLifecycle(waiting)
  }

  get #connection(): SocketConnection | undefined {
    const lifecycle = this.#lifecycle
    return 'connection' in lifecycle ? lifecycle.connection : undefined
  }

  get #reconnectAttempt(): number {
    const lifecycle = this.#lifecycle
    return 'reconnectAttempt' in lifecycle ? lifecycle.reconnectAttempt : 0
  }

  #owns(connection: SocketConnection): boolean {
    return this.#connection === connection
  }

  #isCurrent(lifecycle: ClientSocketLifecycle): boolean {
    return this.#lifecycle === lifecycle
  }

  #setLifecycle(lifecycle: ClientSocketLifecycle): void {
    const previousState = this.state
    this.#lifecycle = lifecycle
    const state = this.state

    if (previousState !== state) {
      this.#stateHandlers.forEach((handler) => this.#callHandler(handler, state))
    }
  }

  #rejectPending(error: Error): void {
    for (const [id, pending] of this.#pendingRequests) {
      clearTimeout(pending.timer)
      pending.reject(error)
      this.#pendingRequests.delete(id)
    }
  }

  #emitLocal(event: string, data?: unknown): void {
    this.#eventHandlers.get(event)?.forEach((handler) => this.#callHandler(handler, data))
  }

  #callHandler(handler: Callable, data?: unknown): void {
    callClientHandler(handler, data)
  }
}
