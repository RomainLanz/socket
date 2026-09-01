import diagnostics_channel from 'node:diagnostics_channel'
import type { TracingChannel } from 'node:diagnostics_channel'
import type {
  SocketBroadcastMessage,
  SocketChannelMessage,
  SocketConnectMessage,
  SocketDisconnectMessage,
  SocketSubscribeMessage,
  SocketUnsubscribeMessage,
} from './types/tracing_channels.js'

type OperationRunner = <Result>(operation: () => Result) => Result

const operationRunners = new WeakMap<object, OperationRunner>()

export function setTracingOperationRunner(message: object, runner: OperationRunner) {
  operationRunners.set(message, runner)
}

function createTracingChannel<Context extends object>(
  name: string
): TracingChannel<Context, Context> {
  const channel = diagnostics_channel.tracingChannel<Context, Context>(name)

  return {
    start: channel.start,
    end: channel.end,
    asyncStart: channel.asyncStart,
    asyncEnd: channel.asyncEnd,
    error: channel.error,
    get hasSubscribers() {
      return channel.hasSubscribers
    },
    subscribe: channel.subscribe.bind(channel),
    unsubscribe: channel.unsubscribe.bind(channel),
    traceSync(fn, message = {} as Context, thisArg, ...args) {
      try {
        return channel.traceSync(() => {
          const runner = operationRunners.get(message)
          return runner
            ? runner(() => Reflect.apply(fn, thisArg, args))
            : Reflect.apply(fn, thisArg, args)
        }, message)
      } finally {
        operationRunners.delete(message)
      }
    },
    async tracePromise(fn, message = {} as Context, thisArg, ...args) {
      try {
        return await channel.tracePromise(() => {
          const runner = operationRunners.get(message)
          return runner
            ? runner(() => Reflect.apply(fn, thisArg, args))
            : Reflect.apply(fn, thisArg, args)
        }, message)
      } finally {
        operationRunners.delete(message)
      }
    },
    traceCallback: channel.traceCallback.bind(channel),
  } as TracingChannel<Context, Context>
}

export const connectChannel = createTracingChannel<SocketConnectMessage>('rlanz.socket.connect')

export const disconnectChannel =
  createTracingChannel<SocketDisconnectMessage>('rlanz.socket.disconnect')

export const subscribeChannel =
  createTracingChannel<SocketSubscribeMessage>('rlanz.socket.subscribe')

export const unsubscribeChannel = createTracingChannel<SocketUnsubscribeMessage>(
  'rlanz.socket.unsubscribe'
)

export const channelMessageChannel =
  createTracingChannel<SocketChannelMessage>('rlanz.socket.message')

export const broadcastChannel =
  createTracingChannel<SocketBroadcastMessage>('rlanz.socket.broadcast')
