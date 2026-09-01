import type { SocketAuthenticationHandler, SocketConfig } from './types.js'

type WebSocketConfig<User> = NonNullable<SocketConfig<User>['websocket']>

type AuthenticatedUserOf<Authenticate extends SocketAuthenticationHandler> = Exclude<
  Awaited<ReturnType<Authenticate>>,
  false | null
>

type AuthenticatedSocketConfig<Authenticate extends SocketAuthenticationHandler> = Omit<
  SocketConfig<AuthenticatedUserOf<Authenticate>>,
  'websocket'
> & {
  websocket: Omit<WebSocketConfig<AuthenticatedUserOf<Authenticate>>, 'authenticate'> & {
    authenticate: Authenticate
  }
}

type UnauthenticatedSocketConfig = Omit<SocketConfig<unknown>, 'websocket'> & {
  websocket?: Omit<WebSocketConfig<unknown>, 'authenticate'> & {
    authenticate?: never
  }
}

export function defineConfig<Authenticate extends SocketAuthenticationHandler>(
  config: AuthenticatedSocketConfig<Authenticate>
): SocketConfig<AuthenticatedUserOf<Authenticate>>
export function defineConfig(config: UnauthenticatedSocketConfig): SocketConfig<unknown>
export function defineConfig(config: unknown): unknown {
  return config
}
