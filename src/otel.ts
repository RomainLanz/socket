import {
  context,
  SpanKind,
  SpanStatusCode,
  trace,
  type Counter,
  type Span,
  type UpDownCounter,
} from '@opentelemetry/api'
import type { TracingChannelSubscribers } from 'node:diagnostics_channel'
import { InstrumentationBase } from '@opentelemetry/instrumentation'
import type { InstrumentationConfig } from '@opentelemetry/instrumentation'
import packageJson from '../package.json' with { type: 'json' }
import {
  broadcastChannel,
  channelMessageChannel,
  connectChannel,
  disconnectChannel,
  subscribeChannel,
  setTracingOperationRunner,
  unsubscribeChannel,
} from './tracing_channels.js'
import type {
  SocketBroadcastMessage,
  SocketChannelMessage,
  SocketConnectMessage,
  SocketDisconnectMessage,
  SocketSubscribeMessage,
  SocketUnsubscribeMessage,
} from './types/tracing_channels.js'

export interface SocketInstrumentationConfig extends InstrumentationConfig {
  /**
   * The messaging system identifier.
   *
   * @default '@rlanz/socket'
   */
  messagingSystem?: string
}

type AttributeValue = string | number | boolean

export class SocketInstrumentation extends InstrumentationBase<SocketInstrumentationConfig> {
  protected subscribed = false
  protected spans = new WeakMap<object, Span>()
  protected connectHandlers?: TracingChannelSubscribers<SocketConnectMessage>
  protected disconnectHandlers?: TracingChannelSubscribers<SocketDisconnectMessage>
  protected subscribeHandlers?: TracingChannelSubscribers<SocketSubscribeMessage>
  protected unsubscribeHandlers?: TracingChannelSubscribers<SocketUnsubscribeMessage>
  protected messageHandlers?: TracingChannelSubscribers<SocketChannelMessage>
  protected broadcastHandlers?: TracingChannelSubscribers<SocketBroadcastMessage>

  #connectionCounter!: Counter
  #disconnectionCounter!: Counter
  #activeConnections!: UpDownCounter
  #subscriptionCounter!: Counter
  #activeSubscriptions!: UpDownCounter
  #messageCounter!: Counter
  #broadcastCounter!: Counter
  #broadcastDeliveryCounter!: Counter

  constructor(config: SocketInstrumentationConfig = {}) {
    super('@rlanz/socket', packageJson.version, config)
    this._updateMetricInstruments()
    if (this.isEnabled()) this.enable()
  }

  get #messagingSystem(): string {
    return this.getConfig().messagingSystem ?? '@rlanz/socket'
  }

  protected init() {
    return undefined
  }

  protected _updateMetricInstruments() {
    if (this.subscribed === undefined) return

    this.#connectionCounter = this.meter.createCounter('rlanz_socket_connections_total', {
      description: 'Total number of accepted socket connections',
    })
    this.#disconnectionCounter = this.meter.createCounter('rlanz_socket_disconnections_total', {
      description: 'Total number of closed socket connections',
    })
    this.#activeConnections = this.meter.createUpDownCounter('rlanz_socket_connections_active', {
      description: 'Number of currently active socket connections',
    })
    this.#subscriptionCounter = this.meter.createCounter('rlanz_socket_subscriptions_total', {
      description: 'Total number of successful channel subscriptions',
    })
    this.#activeSubscriptions = this.meter.createUpDownCounter(
      'rlanz_socket_subscriptions_active',
      { description: 'Number of currently active channel subscriptions' }
    )
    this.#messageCounter = this.meter.createCounter('rlanz_socket_messages_total', {
      description: 'Total number of channel messages received from clients',
    })
    this.#broadcastCounter = this.meter.createCounter('rlanz_socket_broadcasts_total', {
      description: 'Total number of broadcast operations',
    })
    this.#broadcastDeliveryCounter = this.meter.createCounter(
      'rlanz_socket_broadcast_deliveries_total',
      { description: 'Total number of socket deliveries made by broadcast operations' }
    )
  }

  enable() {
    super.enable()
    // `InstrumentationBase` calls `enable()` from its constructor, before this
    // subclass's fields and private methods are installed on the instance.
    // During that premature call `this.subscribed` is still `undefined`, so the
    // guard prevents calling `#subscribe()` on a not-yet-branded instance.
    if (this.subscribed !== undefined) this.#subscribe()
  }

  disable() {
    if (this.subscribed !== undefined) this.#unsubscribe()
    super.disable()
  }

  manuallyRegister() {
    this.#subscribe()
  }

  #subscribe() {
    if (this.subscribed) return
    if (!this.isEnabled()) return

    this.subscribed = true

    this.connectHandlers = {
      start: (message) =>
        this.#startSpan(message, 'socket connect', SpanKind.SERVER, {
          'network.transport': 'websocket',
        }),
      end: (message) => this.#handleConnectEnd(message),
      asyncStart: () => {},
      asyncEnd: () => {},
      error: (message) => this.#recordError(message),
    }

    this.disconnectHandlers = {
      start: (message) =>
        this.#startSpan(message, 'socket disconnect', SpanKind.INTERNAL, {
          'socket.disconnect.reason': message.reason,
          'socket.disconnect.subscriptions': message.subscriptions ?? 0,
        }),
      end: () => {},
      asyncStart: () => {},
      asyncEnd: (message) => this.#handleDisconnectEnd(message),
      error: (message) => this.#recordError(message),
    }

    this.subscribeHandlers = {
      start: (message) =>
        this.#startSpan(message, `socket subscribe ${message.channel}`, SpanKind.SERVER, {
          'messaging.destination.name': message.channel,
          'messaging.operation.name': 'subscribe',
          'messaging.operation.type': 'receive',
        }),
      end: () => {},
      asyncStart: () => {},
      asyncEnd: (message) => this.#handleSubscribeEnd(message),
      error: (message) => this.#recordError(message),
    }

    this.unsubscribeHandlers = {
      start: (message) =>
        this.#startSpan(message, `socket unsubscribe ${message.channel}`, SpanKind.SERVER, {
          'messaging.destination.name': message.channel,
          'messaging.operation.name': 'unsubscribe',
          'messaging.operation.type': 'receive',
        }),
      end: () => {},
      asyncStart: () => {},
      asyncEnd: (message) => this.#handleUnsubscribeEnd(message),
      error: (message) => this.#recordError(message),
    }

    this.messageHandlers = {
      start: (message) =>
        this.#startSpan(message, `socket message ${message.channel}`, SpanKind.SERVER, {
          'messaging.destination.name': message.channel,
          'messaging.operation.name': 'process',
          'messaging.operation.type': 'receive',
          'messaging.event.name': message.event,
        }),
      end: () => {},
      asyncStart: () => {},
      asyncEnd: (message) => this.#handleMessageEnd(message),
      error: (message) => this.#recordError(message),
    }

    this.broadcastHandlers = {
      start: (message) =>
        this.#startSpan(message, `socket broadcast ${message.target}`, SpanKind.PRODUCER, {
          ...this.#broadcastAttributes(message),
        }),
      end: (message) => this.#handleBroadcastEnd(message),
      asyncStart: () => {},
      asyncEnd: () => {},
      error: (message) => this.#recordError(message),
    }

    connectChannel.subscribe(this.connectHandlers)
    disconnectChannel.subscribe(this.disconnectHandlers)
    subscribeChannel.subscribe(this.subscribeHandlers)
    unsubscribeChannel.subscribe(this.unsubscribeHandlers)
    channelMessageChannel.subscribe(this.messageHandlers)
    broadcastChannel.subscribe(this.broadcastHandlers)
  }

  #unsubscribe() {
    if (!this.subscribed) return

    if (this.connectHandlers) connectChannel.unsubscribe(this.connectHandlers)
    if (this.disconnectHandlers) disconnectChannel.unsubscribe(this.disconnectHandlers)
    if (this.subscribeHandlers) subscribeChannel.unsubscribe(this.subscribeHandlers)
    if (this.unsubscribeHandlers) unsubscribeChannel.unsubscribe(this.unsubscribeHandlers)
    if (this.messageHandlers) channelMessageChannel.unsubscribe(this.messageHandlers)
    if (this.broadcastHandlers) broadcastChannel.unsubscribe(this.broadcastHandlers)

    this.subscribed = false
    this.spans = new WeakMap()
  }

  #startSpan(
    message: object,
    name: string,
    kind: SpanKind,
    attributes: Record<string, AttributeValue> = {}
  ) {
    const span = this.tracer.startSpan(name, {
      kind,
      attributes: {
        'messaging.system': this.#messagingSystem,
        ...attributes,
      },
    })

    this.spans.set(message, span)
    const spanContext = trace.setSpan(context.active(), span)
    setTracingOperationRunner(message, (operation) => context.with(spanContext, operation))
  }

  #handleDisconnectEnd(message: SocketDisconnectMessage) {
    this.#disconnectionCounter.add(1, { 'socket.disconnect.reason': message.reason })
    this.#activeConnections.add(-1)
    if (message.subscriptions) {
      this.#activeSubscriptions.add(-message.subscriptions)
    }

    this.#endSpan(message)
  }

  #handleConnectEnd(message: SocketConnectMessage) {
    this.#connectionCounter.add(1)
    this.#activeConnections.add(1)
    this.#endSpan(message)
  }

  #handleSubscribeEnd(message: SocketSubscribeMessage) {
    const attributes = {
      'messaging.destination.name': message.channel,
      'socket.subscription.created': Boolean(message.created),
    }

    if (message.ok) {
      this.#subscriptionCounter.add(1, attributes)
      if (message.created) this.#activeSubscriptions.add(1, attributes)
    }

    this.#endSpanWithOutcome(message, attributes)
  }

  #handleUnsubscribeEnd(message: SocketUnsubscribeMessage) {
    const attributes = {
      'messaging.destination.name': message.channel,
      'socket.subscription.removed': Boolean(message.removed),
    }

    if (message.ok && message.removed) {
      this.#activeSubscriptions.add(-1, attributes)
    }

    this.#endSpanWithOutcome(message, attributes)
  }

  #handleMessageEnd(message: SocketChannelMessage) {
    const attributes = {
      'messaging.destination.name': message.channel,
      'messaging.event.name': message.event,
    }

    this.#messageCounter.add(1, {
      ...attributes,
      'socket.message.ok': Boolean(message.ok),
    })

    this.#endSpanWithOutcome(message, attributes)
  }

  #handleBroadcastEnd(message: SocketBroadcastMessage) {
    const attributes = this.#broadcastAttributes(message)

    this.#broadcastCounter.add(1, attributes)
    if (message.delivered !== undefined) {
      this.#broadcastDeliveryCounter.add(message.delivered, attributes)
    }

    this.#endSpan(message, {
      ...attributes,
      ...(message.delivered !== undefined
        ? { 'socket.broadcast.delivered': message.delivered }
        : {}),
    })
  }

  #recordError(message: object & { error: unknown }) {
    const span = this.spans.get(message)
    if (!span) return

    if (message.error instanceof Error) {
      span.recordException(message.error)
      span.setStatus({ code: SpanStatusCode.ERROR, message: message.error.message })
    } else {
      span.setStatus({ code: SpanStatusCode.ERROR })
    }

    span.end()
    this.spans.delete(message)
  }

  #endSpanWithOutcome(
    message: SocketSubscribeMessage | SocketUnsubscribeMessage | SocketChannelMessage,
    attributes: Record<string, AttributeValue>
  ) {
    this.#endSpan(message, attributes, message)
  }

  #endSpan(
    message: object,
    attributes: Record<string, AttributeValue> = {},
    outcome?: { ok?: boolean; error?: string }
  ) {
    const span = this.spans.get(message)
    if (!span) return

    span.setAttributes(attributes)

    if (outcome?.ok === false) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: outcome.error })
      if (outcome.error) {
        span.setAttribute('error.type', outcome.error)
      }
    } else {
      span.setStatus({ code: SpanStatusCode.OK })
    }

    span.end()
    this.spans.delete(message)
  }

  #broadcastAttributes(message: SocketBroadcastMessage): Record<string, AttributeValue> {
    const attributes: Record<string, AttributeValue> = {
      'messaging.operation.name': 'publish',
      'messaging.operation.type': 'send',
      'messaging.event.name': message.event,
      'socket.broadcast.target': message.target,
      'socket.broadcast.via': message.via,
    }

    if (message.channel) attributes['messaging.destination.name'] = message.channel
    if (message.except) attributes['socket.broadcast.except_count'] = message.except.length

    return attributes
  }
}
