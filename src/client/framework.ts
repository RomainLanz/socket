import type { Channel } from './channel.js'
import { buildChannelName, resolveChannel, type Socket } from './socket.js'
import type {
  ChannelCallArgs,
  ChannelContract,
  ChannelContractFor,
  ChannelParameters,
  ChannelPattern,
  EventHandler,
} from './types.js'
import { callClientHandler } from './callback.js'

type AnySocket = Socket<any>
type ServerEvents<Contract> =
  Contract extends ChannelContract<unknown, infer Events> ? Events : unknown

type FrameworkChannelMember<Registry, Pattern extends string> =
  ChannelContractFor<Registry, Pattern> extends infer Contract
    ? [Contract] extends [never]
      ? never
      : Channel<Contract>
    : never

export type FrameworkChannel<Registry, Pattern extends string> = FrameworkChannelMember<
  Registry,
  Pattern
>

export type FrameworkChannelPattern<Registry> = ChannelPattern<Registry>
export type FrameworkChannelCallArgs<Registry> = ChannelCallArgs<Registry>
export type FrameworkChannelParameters<
  Registry,
  Pattern extends ChannelPattern<Registry>,
> = ChannelParameters<Registry, Pattern>

export function createChannelCallArgs<Registry>(
  pattern: ChannelPattern<Registry>,
  parameters?: object
): ChannelCallArgs<Registry> {
  return (parameters === undefined ? [pattern] : [pattern, parameters]) as ChannelCallArgs<Registry>
}

/** Resolves the shared positional arguments accepted by framework channel-event helpers. @internal */
export function resolveChannelEventArgs<ParameterInput, Event extends string, Payload>(
  args: readonly unknown[]
): {
  parameterInput: ParameterInput | undefined
  event: Event
  handler: EventHandler<Payload>
} {
  return {
    parameterInput: (args.length === 3 ? args[0] : undefined) as ParameterInput | undefined,
    event: args.at(-2) as Event,
    handler: args.at(-1) as EventHandler<Payload>,
  }
}

export type FrameworkChannelEvent<Registry, Pattern extends string> =
  FrameworkChannel<Registry, Pattern> extends Channel<infer Contract>
    ? Extract<keyof ServerEvents<Contract>, string>
    : never

export type FrameworkChannelEventPayload<
  Registry,
  Pattern extends string,
  Event extends FrameworkChannelEvent<Registry, Pattern>,
> =
  FrameworkChannel<Registry, Pattern> extends Channel<infer Contract>
    ? Event extends keyof ServerEvents<Contract>
      ? ServerEvents<Contract>[Event]
      : never
    : never

interface ManagedEvent {
  handlers: Set<EventHandler<any>>
  dispatch: EventHandler<any>
}

interface ManagedEntry {
  channel: Channel<any>
  socket: AnySocket
  leases: Set<ChannelLease>
  events: Map<string, ManagedEvent>
  lifecycle: { ready: Promise<Channel<any>>; release: () => Promise<void> } | undefined
}

const socketEntries = new WeakMap<AnySocket, Map<string, ManagedEntry>>()
const socketOwners = new WeakMap<AnySocket, { count: number; generation: number }>()

function reportSubscriptionError(error: unknown): void {
  queueMicrotask(() => {
    console.error(error)
  })
}

function attachEvents(entry: ManagedEntry): void {
  for (const [event, managed] of entry.events) {
    entry.channel.$listen(event, managed.dispatch)
  }
}

function getEntry(socket: AnySocket, name: string): ManagedEntry {
  let entries = socketEntries.get(socket)
  if (!entries) {
    entries = new Map()
    socketEntries.set(socket, entries)
  }

  const channel = socket[resolveChannel](name)
  let entry = entries.get(name)
  if (!entry || entry.channel !== channel) {
    entry = {
      channel,
      socket,
      leases: new Set(),
      events: new Map(),
      lifecycle: undefined,
    }
    entries.set(name, entry)
  }

  return entry
}

export class ChannelLease {
  readonly entry: ManagedEntry
  #released = false
  #listeners: Array<{ event: string; handler: EventHandler<any> }> = []

  constructor(entry: ManagedEntry) {
    this.entry = entry
  }

  listen(event: string, handler: EventHandler<any>): void {
    if (this.#released) return

    let managed = this.entry.events.get(event)
    if (!managed) {
      const handlers = new Set<EventHandler<any>>()
      managed = {
        handlers,
        dispatch: (data) => {
          for (const listener of handlers) callClientHandler(listener, data)
        },
      }
      this.entry.events.set(event, managed)
      this.entry.channel.$listen(event, managed.dispatch)
    }
    const registration: EventHandler<any> = (data) => handler(data)
    managed.handlers.add(registration)
    this.#listeners.push({ event, handler: registration })
  }

  release(): void {
    if (this.#released) return
    this.#released = true

    for (const { event, handler } of this.#listeners) {
      const managed = this.entry.events.get(event)
      managed?.handlers.delete(handler)
      if (managed?.handlers.size === 0) {
        this.entry.channel.$stopListening(event, managed.dispatch)
        this.entry.events.delete(event)
      }
    }
    this.#listeners = []
    this.entry.leases.delete(this)

    queueMicrotask(() => {
      if (this.entry.leases.size > 0) return
      const lifecycle = this.entry.lifecycle
      this.entry.lifecycle = undefined
      void lifecycle?.release().then(() => {
        if (this.entry.leases.size > 0 || this.entry.lifecycle) return
        const entries = socketEntries.get(this.entry.socket)
        if (entries?.get(this.entry.channel.name) === this.entry) {
          entries.delete(this.entry.channel.name)
        }
      })
    })
  }
}

export function acquireChannel<Registry, const Args extends ChannelCallArgs<Registry>>(
  socket: Socket<Registry>,
  ...args: Args
): { channel: FrameworkChannel<Registry, Args[0]>; lease: ChannelLease } {
  const [pattern, parameters] = args as [string, object?]
  const name = buildChannelName(pattern, parameters)
  const entry = getEntry(socket, name)
  const lease = new ChannelLease(entry)
  entry.leases.add(lease)

  if (!entry.lifecycle) {
    const lifecycle = entry.channel.$acquire()
    entry.lifecycle = lifecycle
    attachEvents(entry)
    void lifecycle.ready
      .then(() => {
        if (entry.lifecycle === lifecycle) attachEvents(entry)
      })
      .catch((error) => {
        if (entry.lifecycle === lifecycle && entry.leases.size > 0 && entry.socket.connected) {
          reportSubscriptionError(error)
        }
      })
  }

  return { channel: entry.channel as FrameworkChannel<Registry, Args[0]>, lease }
}

/** Connects an exclusively owned socket and coalesces development-mode remounts. */
export function acquireSocketOwnership(socket: AnySocket): () => void {
  let owner = socketOwners.get(socket)
  if (!owner) {
    owner = { count: 0, generation: 0 }
    socketOwners.set(socket, owner)
    socket.connect().catch(reportSubscriptionError)
  }
  owner.count++
  owner.generation++

  let released = false
  return () => {
    if (released) return
    released = true
    owner.count--
    const generation = ++owner.generation
    queueMicrotask(() => {
      if (owner.count === 0 && owner.generation === generation) {
        socket.disconnect()
        socketOwners.delete(socket)
      }
    })
  }
}
