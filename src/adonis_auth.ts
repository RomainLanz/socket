import type { SocketAuthenticationHandler } from './types.js'

interface AdonisAuthenticator {
  authenticateUsing: () => unknown
  getUserOrFail: () => unknown
}

export function authenticateWithAdonisAuth<User = unknown>(): SocketAuthenticationHandler<User> {
  return async (ctx) => {
    const auth: unknown = Reflect.get(ctx.httpContext, 'auth')
    if (
      typeof auth !== 'object' ||
      auth === null ||
      typeof Reflect.get(auth, 'authenticateUsing') !== 'function' ||
      typeof Reflect.get(auth, 'getUserOrFail') !== 'function'
    ) {
      throw new Error(
        'AdonisJS Auth must be initialized by websocket middleware before authenticating sockets'
      )
    }

    const authenticator: AdonisAuthenticator = {
      authenticateUsing: Reflect.get(auth, 'authenticateUsing').bind(auth),
      getUserOrFail: Reflect.get(auth, 'getUserOrFail').bind(auth),
    }
    await authenticator.authenticateUsing()

    // The configured Adonis authenticator owns the application's User type.
    return authenticator.getUserOrFail() as User
  }
}
