export { BaseChannel, SocketResponseError } from './src/base_channel.js'
export { defineConfig } from './src/define_config.js'
export { authenticateWithAdonisAuth } from './src/adonis_auth.js'
export { configure } from './configure.js'
export type {
  AuthenticatedSocket,
  ChannelAck,
  ChannelMessage,
  ChannelOptions,
  ChannelServerEventsOf,
  Duration,
  Middleware,
  MiddlewareClass,
  MiddlewareConstructor,
  MiddlewareContext,
  MiddlewareFn,
  PresenceInfo,
  PresenceMember,
  RawSocket,
  SocketAuthenticationHandler,
  SocketAuthenticationResult,
  SocketConfig,
  SocketEvents,
  SocketHealthSnapshot,
  SocketHttpMiddleware,
  SocketOriginResolver,
  SocketOriginValues,
  SocketRetryQueueConfig,
  SocketServiceStatus,
  SocketTransportConfig,
  SocketUpgradeContext,
} from './src/types.js'
