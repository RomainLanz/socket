# @rlanz/socket

A WebSocket server and typed client for AdonisJS, powered by
[`ws`](https://github.com/websockets/ws).

> [!WARNING]
> This package is available for testing and early production use. It will move to
> `@adonisjs/socket` before its official release. Expect breaking changes and a migration.

`@rlanz/socket` provides:

- AdonisJS channels with middleware, lifecycle hooks, and dependency injection
- end-to-end types generated from server channel classes
- presence channels and client-to-client events
- automatic client reconnection and re-subscription
- cross-instance broadcasts through `@boringnode/bus`
- React and Vue adapters
- health checks, OpenTelemetry instrumentation, and test fakes

## Install

```sh
yarn add @rlanz/socket
node ace configure @rlanz/socket
```

The configure command registers the provider in the `web` environment and adds the Assembler hook
that discovers channels and generates client types.

```ts
// adonisrc.ts
export default defineConfig({
  providers: [
    {
      file: () => import('@rlanz/socket/provider'),
      environment: ['web'],
    },
  ],
  hooks: {
    init: [() => import('@rlanz/socket/assembler_hook')],
  },
})
```

The package requires Node.js 24 or newer and AdonisJS 7.

## Quick start

Create a channel in `app/channels`.

```ts
// app/channels/chat_channel.ts
import { BaseChannel } from '@rlanz/socket'
import { onMessage } from '@rlanz/socket/decorators'
import type { AuthenticatedSocket } from '@rlanz/socket/types'

type User = { id: string; name: string }
type Message = { id: string; text: string }

type ServerEvents = {
  'chat:message': Message
}

export default class ChatChannel extends BaseChannel<User, ServerEvents> {
  static pattern = 'chat/:roomId'

  @onMessage('chat:send')
  async sendMessage(
    socket: AuthenticatedSocket<User>,
    payload: { text: string }
  ): Promise<Message> {
    const message = { id: crypto.randomUUID(), text: payload.text }

    this.broadcastExcept(socket.id, 'chat:message', message)
    return message
  }
}
```

The Assembler hook generates an `AppSocket` type from this class. Give it to the browser client to
type channel parameters, event names, payloads, and acknowledgements.

```ts
import { Socket } from '@rlanz/socket/client'
import type { AppSocket } from '#generated/socket'

const socket = new Socket<AppSocket>()
const channel = socket.channel('chat/:roomId', { roomId: 'general' })

channel.listen('chat:message', (message) => {
  console.log(message.id)
})

await socket.connect()
await channel.subscribe()

const message = await channel.sendWithAck('chat:send', { text: 'Hello' })
console.log(message.id)
```

TypeScript rejects unknown channels, missing channel parameters, unknown events, invalid payloads,
and invalid server broadcasts.

## Configure the server

Create `config/socket.ts` when you need to change the defaults.

```ts
// config/socket.ts
import { defineConfig } from '@rlanz/socket'

export default defineConfig({
  websocket: {
    path: '/socket',
    origin: ['https://app.example.com', 'https://admin.example.com'],
    pingInterval: '25s',
    pingTimeout: '5s',
  },
})
```

### WebSocket options

| Option                      | Default     | Purpose                                                  |
| --------------------------- | ----------- | -------------------------------------------------------- |
| `path`                      | `/socket`   | WebSocket endpoint                                       |
| `origin`                    | Same origin | Browser origin policy                                    |
| `middleware`                | `[]`        | AdonisJS HTTP middleware run during upgrade              |
| `authenticate`              | None        | Resolves the user attached to the socket                 |
| `pingInterval`              | Disabled    | Interval between WebSocket pings                         |
| `pingTimeout`               | Disabled    | Time allowed for a pong response                         |
| `maxPayload`                | 1 MiB       | Maximum inbound WebSocket message size                   |
| `maxQueuedMessages`         | `100`       | Pending non-ping messages allowed per socket             |
| `maxMessagesPerInterval`    | `1000`      | Messages accepted per rate limit window                  |
| `messageRateInterval`       | `1s`        | Per-socket rate limit window                             |
| `maxBufferedAmount`         | 16 MiB      | Outbound buffer limit before disconnecting a slow socket |
| `maxOutboundPayload`        | 1 MiB       | Maximum serialized outbound message size                 |
| `maxSubscriptionsPerSocket` | `100`       | Active subscriptions allowed per socket                  |
| `maxChannelNameLength`      | `255`       | Maximum channel name length                              |
| `shutdownTimeout`           | `5s`        | Time allowed for handlers and hooks during shutdown      |

Heartbeat is disabled when neither `pingInterval` nor `pingTimeout` is set. If you set only one,
the other defaults to `25s` or `5s` respectively.

The message rate and queue limits protect individual connections. They do not replace connection
limits or abuse protection at the proxy.

### Origin checks

Browser upgrades are restricted to the request's origin by default. The `origin` option accepts the
same values as the AdonisJS CORS `origin` option:

- a boolean
- a string, including `'*'` or a comma-separated list
- an array of strings
- `(origin, httpContext) => value`

Matching is case-sensitive. Comma-separated entries are not trimmed. A configured policy replaces
the same-origin default. Clients without an `Origin` header remain supported.

AdonisJS CORS middleware does not authorize WebSocket upgrades. The package checks the origin before
upgrade middleware and authentication. This protects cookie-authenticated sockets from cross-site
WebSocket hijacking.

Your reverse proxy must preserve the public `Host` and replace `X-Forwarded-Proto` with the trusted
public protocol. If the proxy rewrites the host, configure the public origin explicitly.

### Authentication

Use `websocket.middleware` to prepare request services such as sessions and auth. Then resolve the
user with `websocket.authenticate`.

```ts
// config/socket.ts
import { authenticateWithAdonisAuth, defineConfig } from '@rlanz/socket'

type User = { id: string; name: string }

export default defineConfig({
  websocket: {
    middleware: [
      () => import('@adonisjs/session/session_middleware'),
      () => import('@adonisjs/auth/initialize_auth_middleware'),
    ],
    authenticate: authenticateWithAdonisAuth<User>(),
  },
})
```

Return `false`, `null`, or throw to reject the upgrade with `401 Unauthorized`. A successful result
becomes `socket.user`.

You can provide a custom handler instead.

```ts
export default defineConfig({
  websocket: {
    async authenticate({ httpContext }) {
      await httpContext.auth.authenticateUsing()
      return httpContext.auth.getUserOrFail()
    },
  },
})
```

Upgrade middleware and channel middleware have different jobs. Upgrade middleware prepares the HTTP
handshake. Channel middleware authorizes one subscription.

Every socket retains the initial HTTP context at `socket.raw.httpContext`. This context describes the
handshake. It is not an active request after the `101` response and is not installed in AdonisJS
async-local storage. Put mutable socket-lifetime state in `socket.raw.data`. Copy durable identity
into `socket.user` instead of retaining response-bound resources.

## Define channels

The default Assembler hook discovers `app/channels/**/*_channel.{ts,js}`. Each file must export a
default class that extends `BaseChannel`.

```ts
import { BaseChannel } from '@rlanz/socket'

export default class ChatChannel extends BaseChannel {
  static pattern = 'chat/:roomId'
}
```

Patterns support:

- literal segments such as `announcements`
- required parameters such as `chat/:roomId`
- one final optional parameter such as `threads/:threadId?`
- one final wildcard such as `files/*`

Generated clients use the declared pattern and a separate parameter object.

```ts
socket.channel('announcements')
socket.channel('chat/:roomId', { roomId: 'general' })
socket.channel('threads/:threadId?', {})
socket.channel('files/*', { wildcard: 'docs/guides/start' })
```

An untyped `new Socket()` accepts concrete channel names instead.

### Generated client contracts

The Assembler hook writes:

- `.adonisjs/client/socket.ts` for browser types
- `.adonisjs/server/socket_channels.ts` for runtime channel discovery

Configure the frontend project to resolve an alias such as `#generated/socket` to the client file.
This alias is separate from the AdonisJS server mapping for `#generated/*`.

Keep the standard AdonisJS `#generated/*` mapping pointed at `./.adonisjs/server/*.js`. The provider
uses it to load the generated channel manifest.

The generator recognizes:

- default-exported classes that directly extend `BaseChannel`
- direct string values assigned to `static pattern`
- named public methods decorated with `@onMessage('event')`
- required, optional, or absent handler payloads

Runtime inheritance remains valid, but channels that use intermediate inheritance or patterns that
cannot be read statically are omitted from client types. The generator reports why it omitted them.
Abstract channels, private or static handlers, rest parameters, and required parameters after the
payload fail generation.

Generated types validate TypeScript calls. They do not validate runtime payloads.

To change channel discovery or the generated client path, replace the package hook with a local one.

```ts
// hooks/socket.ts
import { generateSocketRegistry } from '@rlanz/socket/assembler_hook'

export default generateSocketRegistry({
  source: './app/realtime',
  glob: ['**/*.ts'],
  output: '.adonisjs/client/socket.d.ts',
})
```

```ts
// adonisrc.ts
export default defineConfig({
  providers: [
    {
      file: () => import('@rlanz/socket/provider'),
      environment: ['web'],
    },
  ],
  hooks: {
    init: [() => import('./hooks/socket.js')],
  },
})
```

Any source directory inside `app` is supported. Generated imports use the matching AdonisJS `#app`
alias without TypeScript extensions.

### Dependency injection

The AdonisJS container creates a channel instance for every subscription. Constructor injection
works like it does in controllers and other container-managed classes.

```ts
import { inject } from '@adonisjs/core'
import { BaseChannel } from '@rlanz/socket'
import MessageService from '#services/message_service'

@inject()
export default class ChatChannel extends BaseChannel {
  static pattern = 'chat/:roomId'

  constructor(private messages: MessageService) {
    super()
  }
}
```

### Middleware

Channel middleware runs before subscription. A middleware can be a function, an object with a
`handle` method, or a class.

```ts
import { BaseChannel } from '@rlanz/socket'
import type { MiddlewareContext } from '@rlanz/socket/types'

type User = { id: string; name: string }

async function auth(ctx: MiddlewareContext<User>, next: () => Promise<void>) {
  ctx.socket.getUserOrFail()
  await next()
}

export default class ChatChannel extends BaseChannel<User> {
  static pattern = 'chat/:roomId'
  static middlewares = [auth]
}
```

The container resolves middleware classes for every subscription, so they support constructor
injection too.

```ts
import { inject } from '@adonisjs/core'
import type { MiddlewareContext } from '@rlanz/socket/types'
import RoomService from '#services/room_service'

type User = { id: string; name: string }

@inject()
class EnsureRoomAccess {
  constructor(private rooms: RoomService) {}

  async handle(ctx: MiddlewareContext<User>, next: () => Promise<void>) {
    await this.rooms.authorize(ctx.socket.getUserOrFail(), ctx.params.roomId)
    await next()
  }
}
```

Only `SocketResponseError` messages are sent to clients. The server logs unexpected middleware,
join, and message-handler errors and returns a generic protocol error. Use `SocketResponseError`
only for messages that are safe to expose.

## Exchange messages

Decorate a channel method with `@onMessage` to handle a client event.

```ts
import { BaseChannel } from '@rlanz/socket'
import { onMessage } from '@rlanz/socket/decorators'
import type { AuthenticatedSocket } from '@rlanz/socket/types'

type User = { id: string; name: string }

type ServerEvents = {
  'chat:message': { user: User | undefined; body: string }
}

export default class ChatChannel extends BaseChannel<User, ServerEvents> {
  static pattern = 'chat/:roomId'

  @onMessage('chat:send')
  async sendMessage(socket: AuthenticatedSocket<User>, payload: { body: string }) {
    this.broadcast('chat:message', {
      user: socket.user,
      body: payload.body,
    })

    return { delivered: true }
  }
}
```

The handler's second parameter defines the client payload. Its awaited return value defines the
acknowledgement returned by `sendWithAck()`.

```ts
const result = await channel.sendWithAck('chat:send', { body: 'Hello' })
console.log(result.delivered)
```

Unknown events receive a negative acknowledgement. Handler results must be JSON-serializable. The
server rejects a non-serializable result instead of sending a broken acknowledgement.

The second `BaseChannel` generic declares server events. `broadcast()` and `broadcastExcept()` use
this map to check event names and payloads. A channel without this generic cannot broadcast.

```ts
type ServerEvents = {
  'chat:message': { id: string; body: string }
}

export default class ChatChannel extends BaseChannel<User, ServerEvents> {
  publish(message: ServerEvents['chat:message'], socket: AuthenticatedSocket<User>) {
    this.broadcast('chat:message', message)
    this.broadcastExcept(socket.id, 'chat:message', message)
  }
}
```

Use the socket service to broadcast outside a channel instance.

```ts
import socket from '@rlanz/socket/services/main'

socket.broadcast('maintenance', { active: true })
socket.to('chat/general').emit('chat:message', message)
socket.to('chat/general').except(socketId).emit('chat:message', message)
```

You can extract payload types from a generated registry without creating a channel.

```ts
import type { ChannelBroadcastPayload, ChannelClientEventPayload } from '@rlanz/socket/client'
import type { AppSocket } from '#generated/socket'

type IncomingMessage = ChannelBroadcastPayload<AppSocket, 'chat/:roomId', 'chat:message'>
type OutgoingMessage = ChannelClientEventPayload<AppSocket, 'chat/:roomId', 'chat:send'>
```

### Client-to-client events

Use `whisper()` for temporary events such as typing indicators. The server accepts whispers only
from subscribed sockets and forwards them to the other channel members.

```ts
channel.listenForWhisper<{ typing: boolean }>('typing', ({ typing }) => {
  console.log('typing:', typing)
})

channel.whisper('typing', { typing: true })
```

Whispers use an internal `client:` prefix and cannot impersonate server events. Their types remain
caller-defined because they do not come from a server handler.

## Presence

Enable presence on a channel and return the public data for each member.

```ts
import { BaseChannel } from '@rlanz/socket'
import type { AuthenticatedSocket, PresenceInfo, PresenceMember } from '@rlanz/socket/types'

type User = { id: string; name: string }

type ServerEvents = {
  'room:member_joined': PresenceMember
  'room:member_left': PresenceMember
}

export default class RoomChannel extends BaseChannel<User, ServerEvents> {
  static pattern = 'rooms/:roomId'
  static options = { presence: true }

  getPresenceInfo(socket: AuthenticatedSocket<User>): PresenceInfo {
    const user = socket.getUserOrFail()

    return {
      id: user.id,
      data: { name: user.name },
    }
  }

  async onMemberJoin(socket: AuthenticatedSocket<User>, member: PresenceMember) {
    this.broadcastExcept(socket.id, 'room:member_joined', member)
  }

  async onMemberLeave(_socket: AuthenticatedSocket<User>, member: PresenceMember) {
    this.broadcast('room:member_left', member)
  }
}
```

The member `id` groups multiple sockets or browser tabs for the same person. These sockets remain
separate broadcast targets but produce one entry in the presence snapshot. `onMemberJoin` runs for
the first locally observed socket. `onMemberLeave` runs after the last one leaves. `onJoin` and
`onLeave` still run for every socket.

Snapshots flatten `data` into `{ id, ...data, joinedAt }`. Use JSON-compatible values. The package
checks snapshot serialization before running join hooks.

Distributed presence is a best-effort view, not an authoritative membership store. Timeouts,
partitions, restarts, and concurrent changes can produce incomplete snapshots or repeated or missing
member hooks. Keep presence hooks idempotent.

## Use the browser client

The client uses the current browser origin and `/socket` by default.

```ts
import { Socket } from '@rlanz/socket/client'
import type { AppSocket } from '#generated/socket'

const socket = new Socket<AppSocket>({
  url: 'https://app.example.com',
  path: '/socket',
  autoReconnect: true,
  reconnectDelay: 250,
  reconnectMaxDelay: 5000,
  onResubscribeError({ channel, error }) {
    if (error.message === 'Session expired') return
    console.error(`Could not resubscribe to ${channel}`, error)
  },
})

socket.onStateChange((state) => {
  console.log('socket state:', state)
})

socket.on('connect', () => {
  console.log('connected')
})

await socket.connect()
```

Subscribe before sending channel events.

```ts
const channel = socket.channel('chat/:roomId', { roomId: 'general' })

channel
  .here((users) => console.log('present users', users))
  .joining((user) => console.log('joined', user))
  .leaving((user) => console.log('left', user))
  .listen('chat:message', (message) => console.log(message))
  .listenForWhisper('typing', (payload) => console.log(payload))

await channel.subscribe()

channel.send('chat:send', { body: 'No acknowledgement needed' })
const ack = await channel.sendWithAck('chat:send', { body: 'Wait for the server' })

channel.stopListening('chat:message')
channel.stopListeningForWhisper('typing')
await channel.unsubscribe()
await socket.leave(channel.name)
socket.disconnect()
```

`subscribe()` and `unsubscribe()` accept `{ timeout }` in milliseconds. The default is `5000`.

Automatic reconnection starts at `250ms` and doubles up to `5s`. After reconnecting, the client
subscribes again to desired channels. Pending acknowledgements fail and authentication and channel
middleware run again. Events sent while disconnected are not replayed. A rejected automatic
subscription calls `onResubscribeError` with the channel name and error. The client does not report
this protocol outcome as a global JavaScript error.

Set `autoReconnect: false` to disable retries. A deliberate server disconnect is terminal by
default. Use `shouldReconnect(closeEvent)` to override the decision for any close code or reason.

### React

Create one typed set of hooks for the application.

```tsx
// src/socket.ts
import { Socket } from '@rlanz/socket/client'
import { createSocketHooks } from '@rlanz/socket/client/react'
import type { AppSocket } from '#generated/socket'

export const socket = new Socket<AppSocket>()
export const { SocketProvider, useChannel, useChannelEvent, useSocketState } =
  createSocketHooks<AppSocket>()
```

```tsx
function App() {
  return (
    <SocketProvider socket={socket} owned>
      <Chat />
    </SocketProvider>
  )
}

function Chat() {
  const channel = useChannel('chat/:roomId', { roomId: 'general' })

  useChannelEvent('chat/:roomId', { roomId: 'general' }, 'chat:message', (message) =>
    console.log(message.id)
  )

  return <button onClick={() => channel?.send('chat:send', { text: 'Hello' })}>Send</button>
}
```

### Vue

Create one typed set of composables for the application.

```ts
// src/socket.ts
import { Socket } from '@rlanz/socket/client'
import { createSocketComposables } from '@rlanz/socket/client/vue'
import type { AppSocket } from '#generated/socket'

export const socket = new Socket<AppSocket>()
export const { provideSocket, useChannel, useChannelEvent, useSocketState } =
  createSocketComposables<AppSocket>()
```

```ts
// Root component
provideSocket(socket, { owned: true })

// Descendant component
const channel = useChannel('chat/:roomId', { roomId: 'general' })

useChannelEvent('chat/:roomId', { roomId: 'general' }, 'chat:message', (message) => {
  console.log(message.id)
})
```

The adapters share one subscription between consumers of the same channel and release it after the
last consumer unmounts. React returns `null` until its effect acquires the channel. Vue returns a
nullable shallow ref.

Adapters borrow the socket by default and do not connect or disconnect it. Pass `owned` to
`SocketProvider` or `{ owned: true }` to `provideSocket` only when that framework root owns the
socket lifecycle.

## Test broadcasts

`socket.fake()` captures outgoing events without writing to WebSocket clients or the distributed
transport.

```ts
import { test } from '@japa/runner'
import socket from '@rlanz/socket/services/main'

test.group('Notifications', (group) => {
  group.each.teardown(() => socket.restore())

  test('broadcasts a notification', async ({ client }) => {
    const fake = socket.fake()

    await client.post('/notifications').json({ message: 'Hello' })

    fake.assertBroadcasted('notification:created', {
      data: { message: 'Hello' },
    })
  })
})
```

Use `using fake = socket.fake()` to restore the real service when the scope exits.

```ts
fake.assertBroadcasted('maintenance', { data: { active: true } })
fake.assertNotBroadcasted('deploy:started')

fake.assertEmittedTo('chat/general', 'chat:message', {
  data: (data) => data.text === 'Hello',
})
fake.assertNotEmittedTo('chat/general', 'chat:typing')

fake.assertCount(2, { target: 'channel', channel: 'chat/general' })
fake.assertCount(0)
```

## Run multiple instances

Without a transport, subscriptions, broadcasts, and presence stay inside one application instance.
Configure an `@boringnode/bus` transport when multiple instances must exchange events.

Install the dependency required by your transport. For Redis:

```sh
yarn add ioredis
```

```ts
// config/socket.ts
import { redis } from '@boringnode/bus/transports/redis'
import { defineConfig } from '@rlanz/socket'

export default defineConfig({
  transport: {
    driver: redis({
      host: '127.0.0.1',
      port: 6379,
    }),
    channel: 'socket::broadcast',
    presenceTimeout: '100ms',
    retryQueue: {
      maxSize: 1000,
    },
  },
})
```

All instances in one deployment must use a compatible transport and the same bus channel. The
default channel is `socket::broadcast`.

Broadcasts are delivered locally and published to the transport. Publication does not wait for
remote delivery. Failed publications enter a bounded in-memory retry queue. The queue holds `1000`
entries by default, removes duplicate messages, and drops the oldest entry when full. Configure
`transport.retryQueue` to change these rules or disable retries. Setting `maxSize: null` creates an
unbounded queue. A transport reconnect can deliver queued events after newer events.

Presence uses the transport to collect snapshots. `presenceTimeout`, which defaults to `100ms`, sets
how long an instance waits for replies.

The transport does not make events durable. The package does not persist messages, acknowledge
recipients, or recover events after a process restart. Persist application state separately when a
client must recover missed events.

## Production

The package attaches to the AdonisJS HTTP server. HTTP and WebSocket traffic share the same process,
listener, event loop, memory, and failure domain. A process failure disconnects every socket on that
instance.

### Reverse proxy

Configure the proxy or load balancer to:

- route `websocket.path` to the AdonisJS HTTP listener
- forward HTTP/1.1 upgrades with `Upgrade` and `Connection: upgrade`
- use `wss://` publicly and terminate TLS safely
- preserve a validated public `Host`
- replace `X-Forwarded-Proto` with the trusted public protocol
- keep upgraded connections attached to their original instance
- set the idle timeout above the heartbeat cadence
- prevent direct access to the upstream

Sticky sessions are not required and do not replace a shared transport.

### Delivery guarantees

Socket delivery is temporary and best-effort:

- reconnecting creates a new socket session
- pending acknowledgements fail on disconnect
- events sent while disconnected are not replayed
- `sendWithAck()` confirms that the server handler returned, not that a broadcast reached a client
- transport publication does not confirm remote delivery
- distributed presence can be incomplete during failures or concurrent changes

Applications that need durable notifications or at-least-once processing must store events or state
outside this package. Define application-level event IDs, cursors, deduplication, acknowledgements,
and replay where needed.

### Shutdown and resource limits

During shutdown, the service becomes unready, rejects new upgrades, closes existing sockets, runs
subscription and disconnect hooks, and then disconnects the bus. It does not drain established
sockets or wait for pending client acknowledgements.

After `shutdownTimeout`, the service releases its internal socket and subscription state. JavaScript
handlers that ignore cancellation can continue running.

Remove an instance from new traffic before a rolling restart when your platform allows it. Give the
process enough termination time to run hooks and close the bus.

There is no built-in total connection limit. Each connection consumes a file descriptor, memory,
heartbeat state, message queues, and channel subscriptions. Set memory and file descriptor limits,
cap connections at the edge, and load test realistic connection counts, payloads, message rates, and
broadcast fan-out.

Slow consumers are disconnected when `maxBufferedAmount` is exceeded. Oversized outbound messages
are rejected rather than buffered or retried.

### Production checklist

- [ ] Forward WebSocket upgrades on the configured path.
- [ ] Preserve `Host` and replace `X-Forwarded-Proto` at the proxy.
- [ ] Configure exact browser origins and upgrade authentication.
- [ ] Set and test the heartbeat and proxy idle timeout.
- [ ] Configure one shared transport for every related instance.
- [ ] Make clients tolerate reconnects, repeated subscriptions, duplicates, and missed events.
- [ ] Store and replay application-critical events outside the socket transport.
- [ ] Tune payload, rate, queue, subscription, connection, memory, and file descriptor limits.
- [ ] Add readiness, liveness, logs, metrics, and transport monitoring.
- [ ] Allow enough shutdown time for hooks and bus disconnection.

## Health checks

Add `SocketHealthCheck` to the application readiness checks.

```ts
// start/health.ts
import { DiskSpaceCheck, HealthChecks, MemoryHeapCheck } from '@adonisjs/core/health'
import { SocketHealthCheck } from '@rlanz/socket/health_check'
import socket from '@rlanz/socket/services/main'

export const healthChecks = new HealthChecks().register([
  new DiskSpaceCheck(),
  new MemoryHeapCheck(),
  new SocketHealthCheck(socket),
])
```

The check reports `ok` after the WebSocket service boots. It reports `error` while the service is
starting, stopping, stopped, or failed. Metadata includes local connection and channel counts.

This check does not probe the transport, remote instances, or end-to-end delivery. Monitor those
separately and keep a process liveness check so a readiness failure does not cause a restart loop.

## OpenTelemetry

The package emits `diagnostics_channel` events and provides optional OpenTelemetry instrumentation.

```ts
import { registerInstrumentations } from '@opentelemetry/instrumentation'
import { SocketInstrumentation } from '@rlanz/socket/otel'

const unregister = registerInstrumentations({
  instrumentations: [new SocketInstrumentation()],
})
```

You can pass the same instrumentation to an OpenTelemetry `NodeSDK` or an integration such as
`@adonisjs/otel`. Registration enables it. Call `unregister()` only when removing a standalone
registration.

The instrumentation creates spans for connections, disconnections, subscriptions,
unsubscriptions, incoming channel messages, and broadcast delivery. It records counters for active
connections, active subscriptions, received messages, broadcasts, and local deliveries.

Also monitor event-loop delay, memory, CPU, open file descriptors, proxy upgrade failures, reconnect
rates, and transport health. A broadcast delivery counter records a write to a local `ws` object. It
does not confirm that the application received the event.

## Package exports

```ts
import { BaseChannel, SocketResponseError, defineConfig } from '@rlanz/socket'
import { generateSocketRegistry } from '@rlanz/socket/assembler_hook'
import { Socket } from '@rlanz/socket/client'
import { createSocketHooks } from '@rlanz/socket/client/react'
import { createSocketComposables } from '@rlanz/socket/client/vue'
import { onMessage } from '@rlanz/socket/decorators'
import { SocketHealthCheck } from '@rlanz/socket/health_check'
import { SocketInstrumentation } from '@rlanz/socket/otel'
import socket from '@rlanz/socket/services/main'
import { SocketFake } from '@rlanz/socket/testing'
import type { AuthenticatedSocket, SocketConfig } from '@rlanz/socket/types'
import type { PresenceData, SocketOptions } from '@rlanz/socket/client/types'
```

Available entry points:

- `@rlanz/socket`
- `@rlanz/socket/provider`
- `@rlanz/socket/assembler_hook`
- `@rlanz/socket/services/main`
- `@rlanz/socket/decorators`
- `@rlanz/socket/health_check`
- `@rlanz/socket/otel`
- `@rlanz/socket/testing`
- `@rlanz/socket/types`
- `@rlanz/socket/types/tracing_channels`
- `@rlanz/socket/client`
- `@rlanz/socket/client/types`
- `@rlanz/socket/client/react`
- `@rlanz/socket/client/vue`
