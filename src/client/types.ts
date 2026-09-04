export type { ChannelMessage, ChannelAck, ChannelServerEventsOf } from '../shared_types.js'
import type { ChannelAck, ChannelServerEventsOf } from '../shared_types.js'

/** Type-only contract carried by a browser channel. */
export interface ChannelContract<ClientEvents = unknown, ServerEvents = unknown> {
  readonly clientEvents: ClientEvents
  readonly serverEvents: ServerEvents
}

type ChannelInstance<Entry> = Entry extends {
  readonly channel: abstract new (...args: any[]) => infer Instance
}
  ? Instance
  : never

type HandlerName<E, K> = E extends { readonly handlers: infer H }
  ? K extends keyof H
    ? H[K]
    : never
  : never

type HandlerAck<Entry, Event> =
  HandlerName<Entry, Event> extends keyof ChannelInstance<Entry>
    ? ChannelInstance<Entry>[HandlerName<Entry, Event>] extends (...args: any[]) => infer Result
      ? Awaited<Result>
      : unknown
    : unknown

type HandlerPayload<Entry, Event> =
  HandlerName<Entry, Event> extends keyof ChannelInstance<Entry>
    ? ChannelInstance<Entry>[HandlerName<Entry, Event>] extends (...args: any[]) => unknown
      ? Parameters<ChannelInstance<Entry>[HandlerName<Entry, Event>]> extends [
          unknown,
          ...infer Rest,
        ]
        ? Rest extends []
          ? undefined
          : Rest[0]
        : undefined
      : unknown
    : unknown

type ClientContracts<Entry> = Entry extends { readonly handlers: infer Handlers }
  ? {
      [Event in Extract<keyof Handlers, string>]: {
        payload: HandlerPayload<Entry, Event>
        ack: HandlerAck<Entry, Event>
      }
    }
  : unknown

type ContractOfEntry<Entry> = ChannelContract<
  ClientContracts<Entry>,
  ChannelServerEventsOf<ChannelInstance<Entry>>
>

type RegistryChannels<Registry> = Registry extends { readonly channels: infer Channels }
  ? Channels
  : never

type RegistryChannelEntry<
  Registry,
  Pattern extends ChannelPattern<Registry>,
> = RegistryChannels<Registry>[Pattern]

/** Channel patterns explicitly declared in a generated registry. */
export type ChannelPattern<Registry> = Extract<keyof RegistryChannels<Registry>, string>

/** Resolves an explicit generated channel pattern to its contract. */
export type ChannelContractFor<Registry, Pattern extends string> = string extends Pattern
  ? never
  : Pattern extends ChannelPattern<Registry>
    ? ContractOfEntry<RegistryChannelEntry<Registry, Pattern>>
    : never

export type ChannelParameterValue = string | number

/** Parameters generated for an explicit channel pattern. Static channels use `undefined`. */
export type ChannelParameters<Registry, Pattern extends ChannelPattern<Registry>> =
  RegistryChannelEntry<Registry, Pattern> extends { readonly params: infer Parameters }
    ? Parameters extends object | undefined
      ? Parameters
      : never
    : never

/** A correlated client call for one generated channel pattern. */
export type ChannelCallArgs<
  Registry,
  Pattern extends ChannelPattern<Registry> = ChannelPattern<Registry>,
> =
  Pattern extends ChannelPattern<Registry>
    ? ChannelParameters<Registry, Pattern> extends undefined
      ? [pattern: Pattern]
      : [pattern: Pattern, parameters: ChannelParameters<Registry, Pattern>]
    : never

type ClientEvents<Contract> =
  Contract extends ChannelContract<infer Events, unknown> ? Events : never

type ServerEvents<Contract> =
  Contract extends ChannelContract<unknown, infer Events> ? Events : never

/** Client events accepted by a generated channel. */
export type ChannelClientEvent<Registry, Pattern extends ChannelPattern<Registry>> = Extract<
  keyof ClientEvents<ChannelContractFor<Registry, Pattern>>,
  string
>

/** Payload sent from the client for a generated channel event. */
export type ChannelClientEventPayload<
  Registry,
  Pattern extends ChannelPattern<Registry>,
  Event extends ChannelClientEvent<Registry, Pattern>,
> = ClientEvents<ChannelContractFor<Registry, Pattern>>[Event] extends {
  payload: infer Payload
}
  ? Payload
  : never

/** Server broadcast events received by a generated channel. */
export type ChannelBroadcastEvent<Registry, Pattern extends ChannelPattern<Registry>> = Extract<
  keyof ServerEvents<ChannelContractFor<Registry, Pattern>>,
  string
>

/** Payload received from a server broadcast on a generated channel. */
export type ChannelBroadcastPayload<
  Registry,
  Pattern extends ChannelPattern<Registry>,
  Event extends ChannelBroadcastEvent<Registry, Pattern>,
> = ServerEvents<ChannelContractFor<Registry, Pattern>>[Event]

/** Details about a rejected automatic channel resubscription. */
export interface ResubscribeErrorContext {
  channel: string
  error: Error
}

/**
 * Connection options.
 */
export interface SocketOptions {
  /**
   * Server URL. Defaults to window.location.origin.
   */
  url?: string

  /**
   * WebSocket path. Defaults to /socket.
   */
  path?: string

  /**
   * Automatic reconnection. Defaults to true.
   */
  autoReconnect?: boolean

  /**
   * Initial automatic reconnection delay in ms. Defaults to 250.
   */
  reconnectDelay?: number

  /**
   * Maximum automatic reconnection delay in ms. Defaults to 5000.
   */
  reconnectMaxDelay?: number

  /**
   * Decides whether a remotely closed connection should reconnect.
   * By default, deliberate server disconnects are terminal and other closes reconnect.
   */
  shouldReconnect?: (event: CloseEvent) => boolean

  /**
   * Handles a rejected automatic channel resubscription.
   */
  onResubscribeError?: (context: ResubscribeErrorContext) => void
}

/**
 * Channel subscription options.
 */
export interface SubscribeOptions {
  /**
   * Timeout in ms. Defaults to 5000.
   */
  timeout?: number
}

/**
 * Channel unsubscription options.
 */
export interface UnsubscribeOptions {
  /**
   * Timeout in ms. Defaults to 5000.
   */
  timeout?: number
}

/**
 * Data returned by a successful channel subscription.
 */
export interface SubscribeAckData {
  presenceData?: PresenceData | null
}

/**
 * Subscription response.
 */
export type SubscribeResult = ChannelAck<SubscribeAckData>

/**
 * Presence data.
 */
export interface PresenceData {
  channel: string
  users: PresenceUser[]
  count: number
  [key: string]: unknown
}

/**
 * User in presence data.
 */
export interface PresenceUser {
  id: string
  joinedAt: string
  [key: string]: unknown
}

/**
 * Connection state.
 */
export type ConnectionState = 'disconnected' | 'connecting' | 'connected'

/**
 * Event handler.
 */
export type EventHandler<T = unknown> = (data: T) => void

/**
 * Internal client transport used by channels.
 */
export interface SocketClientTransport {
  connected: boolean
  sendRequest<T = unknown>(message: Record<string, unknown>, timeout?: number): Promise<T>
  send(message: Record<string, unknown>): void
  on<T = unknown>(event: string, handler: EventHandler<T>): void
  off<T = unknown>(event: string, handler: EventHandler<T>): void
}
