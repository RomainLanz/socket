import type {
  SocketClientTransport,
  SubscribeOptions,
  SubscribeResult,
  UnsubscribeOptions,
  PresenceUser,
  EventHandler,
  ChannelMessage,
  ChannelAck,
  ChannelContract,
} from './types.js'
import { callClientHandler } from './callback.js'
import { ClientPresenceSnapshot } from './presence_snapshot.js'

const CLIENT_EVENT_PREFIX = 'client:'

type Callable = (...args: never[]) => unknown

/**
 * Represents a subscribed channel.
 * Chainable API inspired by Laravel Echo.
 *
 * @example
 * ```ts
 * socket.channel('chat/:roomId', { roomId: 'general' })
 *   .here((users) => setOnlineUsers(users))
 *   .joining((user) => console.log(`${user.name} joined`))
 *   .leaving((user) => console.log(`${user.name} left`))
 *   .listen('chat:message', (msg) => addMessage(msg))
 *   .subscribe()
 * ```
 */
type ClientMap<C> = C extends ChannelContract<infer E, unknown> ? E : unknown
type ServerMap<C> = C extends ChannelContract<unknown, infer E> ? E : unknown
type IsUnknown<T> = unknown extends T ? true : false
type StringKey<T> = Extract<keyof T, string>
type EventDataArgs<Payload> = undefined extends Payload ? [data?: Payload] : [data: Payload]

interface PendingSubscribe<Contract> {
  promise: Promise<Channel<Contract>>
  resolve: (channel: Channel<Contract>) => void
  reject: (error: Error) => void
}

type RemoteSynchronization<Contract> =
  | { status: 'idle' }
  | { status: 'subscribing'; pending: PendingSubscribe<Contract>; connected: boolean }
  | { status: 'active' }
  | { status: 'unsubscribing'; promise: Promise<void> }
  | { status: 'waitingForReconnect' }
  | { status: 'subscriptionUncertain' }
  | { status: 'remoteSubscriptionUncertain' }

export class Channel<Contract> {
  /**
   * Channel name.
   */
  readonly name: string

  /**
   * Socket client transport.
   */
  #transport: SocketClientTransport

  /**
   * Whether the channel should stay subscribed.
   */
  #subscribed = false
  #remoteSynchronization: RemoteSynchronization<Contract> = { status: 'idle' }

  /**
   * Event handlers grouped by event type.
   */
  #eventHandlers = new Map<string, Set<Callable>>()
  #managedEventHandlers = new Map<string, Set<Callable>>()

  /**
   * Current presence data.
   */
  #presence: ClientPresenceSnapshot | null = null

  /**
   * Handler registered through .here().
   */
  #hereHandler: EventHandler<PresenceUser[]> | null = null

  /**
   * Handler registered through .joining().
   */
  #joiningHandler: EventHandler<PresenceUser> | null = null

  /**
   * Handler registered through .leaving().
   */
  #leavingHandler: EventHandler<PresenceUser> | null = null

  /**
   * Internal transport handlers to clean up on unsubscribe.
   */
  #boundPresenceHandler: ((data: unknown) => void) | null = null
  #boundEventHandler: ((data: unknown) => void) | null = null
  #boundDisconnectHandler: (() => void) | null = null
  #managedSubscriptions = 0

  constructor(name: string, transport: SocketClientTransport) {
    this.name = name
    this.#transport = transport
  }

  /**
   * Whether the channel should stay subscribed.
   */
  get subscribed(): boolean {
    return this.#subscribed || this.#managedSubscriptions > 0
  }

  /** Whether a framework lifecycle currently owns this channel. @internal */
  get $managed(): boolean {
    return this.#managedSubscriptions > 0
  }

  /**
   * Whether the channel is active on the server.
   */
  get active(): boolean {
    return this.#remoteSynchronization.status === 'active'
  }

  /**
   * List of present users.
   */
  get users(): PresenceUser[] {
    return [...(this.#presence?.users ?? [])]
  }

  /**
   * Number of present users.
   */
  get count(): number {
    return this.#presence?.count ?? 0
  }

  /**
   * Registers a handler called with the present users.
   * Called immediately on subscribe and after each presence change.
   */
  here(handler: EventHandler<PresenceUser[]>): this {
    this.#hereHandler = handler
    return this
  }

  /**
   * Registers a handler called when a user joins.
   */
  joining(handler: EventHandler<PresenceUser>): this {
    this.#joiningHandler = handler
    return this
  }

  /**
   * Registers a handler called when a user leaves.
   */
  leaving(handler: EventHandler<PresenceUser>): this {
    this.#leavingHandler = handler
    return this
  }

  /**
   * Listens for an event on this channel.
   */
  listen<T = unknown>(
    this: IsUnknown<ServerMap<Contract>> extends true ? Channel<Contract> : never,
    event: string,
    handler: EventHandler<T>
  ): this
  listen<Event extends StringKey<ServerMap<Contract>>>(
    this: Channel<Contract>,
    event: Event,
    handler: EventHandler<ServerMap<Contract>[Event]>
  ): this
  listen(event: string, handler: EventHandler<any>): this {
    const handlers = this.#eventHandlers.get(event) ?? new Set<Callable>()
    handlers.add(handler)
    this.#eventHandlers.set(event, handlers)
    return this
  }

  /**
   * Removes an event listener.
   */
  stopListening<T = unknown>(
    this: IsUnknown<ServerMap<Contract>> extends true ? Channel<Contract> : never,
    event: string,
    handler?: EventHandler<T>
  ): this
  stopListening<Event extends StringKey<ServerMap<Contract>>>(
    this: Channel<Contract>,
    event: Event,
    handler?: EventHandler<ServerMap<Contract>[Event]>
  ): this
  stopListening(event: string, handler?: EventHandler<any>): this {
    if (handler) {
      this.#eventHandlers.get(event)?.delete(handler)
    } else {
      this.#eventHandlers.delete(event)
    }
    return this
  }

  /** Registers a lifecycle-owned event handler. @internal */
  $listen(event: string, handler: EventHandler<any>): void {
    const handlers = this.#managedEventHandlers.get(event) ?? new Set<Callable>()
    handlers.add(handler)
    this.#managedEventHandlers.set(event, handlers)
  }

  /** Removes a lifecycle-owned event handler. @internal */
  $stopListening(event: string, handler: EventHandler<any>): void {
    const handlers = this.#managedEventHandlers.get(event)
    handlers?.delete(handler)
    if (handlers?.size === 0) this.#managedEventHandlers.delete(event)
  }

  /**
   * Listens for a client event whispered by another channel member.
   */
  listenForWhisper<T = unknown>(event: string, handler: EventHandler<T>): this {
    const handlers =
      this.#eventHandlers.get(`${CLIENT_EVENT_PREFIX}${event}`) ?? new Set<Callable>()
    handlers.add(handler)
    this.#eventHandlers.set(`${CLIENT_EVENT_PREFIX}${event}`, handlers)
    return this
  }

  /**
   * Removes a whispered client event listener.
   */
  stopListeningForWhisper<T = unknown>(event: string, handler?: EventHandler<T>): this {
    const name = `${CLIENT_EVENT_PREFIX}${event}`
    if (handler) this.#eventHandlers.get(name)?.delete(handler)
    else this.#eventHandlers.delete(name)
    return this
  }

  /**
   * Subscribes to the channel.
   *
   * client listeners -> subscribe ack -> active channel
   *        |                 |                 |
   *        v                 v                 v
   *     events ready     presence sync      send/listen
   */
  async subscribe(options: SubscribeOptions = {}): Promise<this> {
    this.#subscribed = true
    return this.#ensureSubscribed(options)
  }

  /** Acquires subscription intent without overriding a direct consumer. @internal */
  $acquire(options: SubscribeOptions = {}): {
    ready: Promise<Channel<Contract>>
    release: () => Promise<void>
  } {
    this.#managedSubscriptions++
    const ready = this.#transport.connected
      ? this.#ensureSubscribed(options)
      : Promise.resolve(this)
    let released = false

    return {
      ready,
      release: () => {
        if (released) {
          return this.#remoteSynchronization.status === 'unsubscribing'
            ? this.#remoteSynchronization.promise
            : Promise.resolve()
        }
        released = true
        this.#managedSubscriptions--
        return this.#unsubscribeIfUnused()
      },
    }
  }

  /** Restores desired subscriptions after the transport reconnects. @internal */
  $resubscribe(options: SubscribeOptions = {}): Promise<this> {
    return this.#ensureSubscribed(options)
  }

  async #ensureSubscribed(options: SubscribeOptions): Promise<this> {
    if (this.#remoteSynchronization.status === 'unsubscribing') {
      await this.#remoteSynchronization.promise
    }

    if (!this.subscribed) return this

    if (this.#remoteSynchronization.status === 'subscribing') {
      await this.#remoteSynchronization.pending.promise
      return this
    }

    if (this.#remoteSynchronization.status === 'active') {
      return this
    }

    const timeout = options.timeout ?? 5000

    // Attach listeners before subscribing so the first server events are not missed.
    if (
      this.#remoteSynchronization.status === 'idle' ||
      this.#remoteSynchronization.status === 'remoteSubscriptionUncertain'
    ) {
      this.#setupSocketListeners()
    }

    const deferred = Promise.withResolvers<Channel<Contract>>()
    const synchronization: RemoteSynchronization<Contract> = {
      status: 'subscribing',
      pending: deferred,
      connected: true,
    }
    this.#remoteSynchronization = synchronization

    const timer = setTimeout(() => {
      if (this.#remoteSynchronization !== synchronization) return
      this.#remoteSynchronization = { status: 'waitingForReconnect' }
      this.#resetConnectionState()
      synchronization.pending.reject(new Error(`Subscribe timeout for channel: ${this.name}`))
      void this.#transport
        .sendRequest({ type: 'unsubscribe', channel: this.name }, timeout)
        .catch(() => {})
    }, timeout)

    this.#transport
      .sendRequest<SubscribeResult>({ type: 'subscribe', channel: this.name }, timeout)
      .then((response) => {
        if (this.#remoteSynchronization !== synchronization) return
        clearTimeout(timer)

        if (response.ok) {
          this.#remoteSynchronization = { status: 'active' }

          const presenceData = ClientPresenceSnapshot.fromTransport(
            response.data?.presenceData,
            this.name
          )

          // Apply the initial presence snapshot.
          if (presenceData) {
            this.#handleInitialPresence(presenceData)
          }

          synchronization.pending.resolve(this)
        } else {
          this.#remoteSynchronization = { status: 'waitingForReconnect' }
          this.#resetConnectionState()
          synchronization.pending.reject(new Error(response.error))
        }
      })
      .catch((error) => {
        if (this.#remoteSynchronization !== synchronization) return
        clearTimeout(timer)
        this.#remoteSynchronization = synchronization.connected
          ? { status: 'subscriptionUncertain' }
          : { status: 'waitingForReconnect' }
        this.#resetConnectionState()
        synchronization.pending.reject(
          error instanceof Error ? error : new Error('Subscribe failed')
        )
      })

    await deferred.promise
    return this
  }

  /**
   * Unsubscribes from the channel.
   */
  async unsubscribe(options: UnsubscribeOptions = {}): Promise<void> {
    this.#subscribed = false
    this.#clearUserCallbacks()
    return this.#unsubscribeIfUnused(options)
  }

  #unsubscribeIfUnused(options: UnsubscribeOptions = {}): Promise<void> {
    if (this.subscribed) return Promise.resolve()
    if (this.#remoteSynchronization.status === 'unsubscribing') {
      return this.#remoteSynchronization.promise
    }

    const previousSynchronization = this.#remoteSynchronization
    const hadPendingSubscribe = previousSynchronization.status === 'subscribing'
    if (previousSynchronization.status === 'subscribing') {
      previousSynchronization.pending.reject(new Error(`Unsubscribed from channel: ${this.name}`))
    }

    const remoteSubscriptionPossible =
      previousSynchronization.status === 'active' ||
      previousSynchronization.status === 'subscriptionUncertain' ||
      previousSynchronization.status === 'remoteSubscriptionUncertain'

    if (!hadPendingSubscribe && !remoteSubscriptionPossible) {
      this.#cleanupTerminal()
      return Promise.resolve()
    }

    const timeout = options.timeout ?? 5000
    const deferred = Promise.withResolvers<void>()
    const synchronization: RemoteSynchronization<Contract> = {
      status: 'unsubscribing',
      promise: deferred.promise,
    }
    this.#remoteSynchronization = synchronization
    let remoteMayRemain = true
    let terminalSynchronization: RemoteSynchronization<Contract> | null = null
    const timer = setTimeout(() => deferred.resolve(), timeout)

    this.#transport
      .sendRequest({ type: 'unsubscribe', channel: this.name }, timeout)
      .then(() => {
        remoteMayRemain = false
        clearTimeout(timer)
        deferred.resolve()
        if (this.#remoteSynchronization === terminalSynchronization) {
          this.#remoteSynchronization = { status: 'idle' }
        }
      })
      .catch(() => {
        clearTimeout(timer)
        deferred.resolve()
      })

    const pending = deferred.promise
    void pending.finally(() => {
      if (this.#remoteSynchronization !== synchronization) return
      terminalSynchronization = remoteMayRemain
        ? { status: 'remoteSubscriptionUncertain' }
        : { status: 'idle' }
      this.#remoteSynchronization = terminalSynchronization
      this.#resetConnectionState()
      this.#detachTransportListeners()
      if (!this.subscribed) this.#clearCallbacks()
    })
    return pending
  }

  /**
   * Sends an event to the server.
   */
  send<T = unknown>(
    this: IsUnknown<ClientMap<Contract>> extends true ? Channel<Contract> : never,
    event: string,
    data?: T
  ): this
  send<Event extends StringKey<ClientMap<Contract>>>(
    this: Channel<Contract>,
    event: Event,
    ...[data]: ClientMap<Contract>[Event] extends { payload: infer Payload }
      ? EventDataArgs<Payload>
      : never
  ): this
  send(event: string, data?: unknown): this {
    if (!this.active) {
      console.warn(`Cannot send to channel ${this.name}: not subscribed`)
      return this
    }
    const payload: ChannelMessage = {
      channel: this.name,
      event,
      data,
    }
    this.#transport.send({ type: 'message', ...payload })
    return this
  }

  /**
   * Relays a client event to other members of this channel.
   */
  whisper<T = unknown>(event: string, data?: T): this {
    if (!this.active) {
      console.warn(`Cannot whisper to channel ${this.name}: not subscribed`)
      return this
    }
    const payload: ChannelMessage = {
      channel: this.name,
      event,
      data,
    }
    this.#transport.send({ type: 'whisper', ...payload })
    return this
  }

  /**
   * Sends an event and waits for a response.
   */
  sendWithAck<T = unknown, Result = unknown>(
    this: IsUnknown<ClientMap<Contract>> extends true ? Channel<Contract> : never,
    event: string,
    data?: T
  ): Promise<Result>
  sendWithAck<Event extends StringKey<ClientMap<Contract>>>(
    this: Channel<Contract>,
    event: Event,
    ...[data]: ClientMap<Contract>[Event] extends { payload: infer Payload }
      ? EventDataArgs<Payload>
      : never
  ): Promise<
    ClientMap<Contract>[Event] extends { ack: infer Acknowledgement } ? Acknowledgement : unknown
  >
  async sendWithAck(event: string, data?: unknown): Promise<unknown> {
    if (!this.active) {
      throw new Error(`Cannot send to channel ${this.name}: not subscribed`)
    }
    const payload: ChannelMessage = {
      channel: this.name,
      event,
      data,
    }
    const response = await this.#transport.sendRequest<ChannelAck>({
      type: 'message',
      ...payload,
    })
    if (!response?.ok) {
      throw new Error(response?.error ?? `Channel handler failed: ${event}`)
    }
    return response.data
  }

  /**
   * Configures transport listeners.
   */
  #setupSocketListeners(): void {
    // Handler for presence:update.
    this.#boundPresenceHandler = (data: unknown) => {
      const presenceData = ClientPresenceSnapshot.fromTransport(data, this.name)
      if (!presenceData) return
      this.#handlePresenceUpdate(presenceData)
    }
    this.#transport.on(`channel:${this.name}:presence:update`, this.#boundPresenceHandler)

    this.#boundEventHandler = (data: unknown) => {
      if (!data || typeof data !== 'object') return

      const payload = data as { event?: string; data?: unknown }
      if (!payload.event) return

      const handlers = this.#eventHandlers.get(payload.event)
      handlers?.forEach((handler) => callClientHandler(handler, payload.data))
      const managedHandlers = this.#managedEventHandlers.get(payload.event)
      managedHandlers?.forEach((handler) => callClientHandler(handler, payload.data))
    }
    this.#transport.on(`channel:${this.name}:event`, this.#boundEventHandler)

    // Mark the channel inactive when the socket disconnects.
    this.#boundDisconnectHandler = () => {
      if (this.#remoteSynchronization.status === 'subscribing') {
        this.#remoteSynchronization.connected = false
      } else if (
        this.#remoteSynchronization.status === 'active' ||
        this.#remoteSynchronization.status === 'subscriptionUncertain'
      ) {
        this.#remoteSynchronization = { status: 'waitingForReconnect' }
      } else if (this.#remoteSynchronization.status === 'remoteSubscriptionUncertain') {
        this.#remoteSynchronization = { status: 'idle' }
      }
      this.#resetConnectionState()
    }
    this.#transport.on('disconnect', this.#boundDisconnectHandler)
  }

  /**
   * Handles the initial presence snapshot.
   */
  #handleInitialPresence(data: ClientPresenceSnapshot): void {
    this.#presence = data

    // Call here() with all users.
    if (this.#hereHandler) {
      callClientHandler(this.#hereHandler, [...data.users])
    }
  }

  /**
   * Handles a presence update.
   *
   * previous IDs     current IDs
   *     |                |
   *     +-- diff in -----+--> joining / leaving callbacks
   */
  #handlePresenceUpdate(data: ClientPresenceSnapshot): void {
    const { joining, leaving } = data.diff(this.#presence)

    // Commit the validated update before invoking user code.
    this.#presence = data

    // Detect new users.
    for (const user of joining) {
      if (this.#joiningHandler) {
        callClientHandler(this.#joiningHandler, user)
      }
    }

    // Detect users that left.
    for (const user of leaving) {
      if (this.#leavingHandler) {
        callClientHandler(this.#leavingHandler, user)
      }
    }

    // Call here() with the updated list.
    if (this.#hereHandler) {
      callClientHandler(this.#hereHandler, [...data.users])
    }
  }

  #resetConnectionState(): void {
    this.#presence = null
  }

  #detachTransportListeners(): void {
    if (this.#boundPresenceHandler) {
      this.#transport.off(`channel:${this.name}:presence:update`, this.#boundPresenceHandler)
      this.#boundPresenceHandler = null
    }

    if (this.#boundEventHandler) {
      this.#transport.off(`channel:${this.name}:event`, this.#boundEventHandler)
      this.#boundEventHandler = null
    }

    if (this.#boundDisconnectHandler) {
      this.#transport.off('disconnect', this.#boundDisconnectHandler)
      this.#boundDisconnectHandler = null
    }
  }

  #clearUserCallbacks(): void {
    this.#eventHandlers.clear()
    this.#hereHandler = null
    this.#joiningHandler = null
    this.#leavingHandler = null
  }

  #clearCallbacks(): void {
    this.#clearUserCallbacks()
    this.#managedEventHandlers.clear()
  }

  /**
   * Performs terminal cleanup after the user leaves or the socket is disposed.
   */
  #cleanupTerminal(): void {
    this.#remoteSynchronization = { status: 'idle' }
    this.#resetConnectionState()
    this.#detachTransportListeners()
    this.#clearCallbacks()
  }
}
