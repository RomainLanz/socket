import { SocketService as InternalSocketService } from '../../src/socket_service.js'
import { makeHttpContext } from './http_context.js'

/** Internal Node-level tests do not boot Adonis, so use one deliberately minimal context fixture. */
export class SocketService<User = unknown> extends InternalSocketService<User> {
  override boot(
    ...args: Parameters<InternalSocketService<User>['boot']> extends [...infer Head, unknown]
      ? Head
      : never
  ): Promise<void> {
    return super.boot(...args, async (_request, handler, shortCircuitBeforeMiddleware) => {
      const httpContext = makeHttpContext()
      const earlyResult = await shortCircuitBeforeMiddleware?.(httpContext)
      return earlyResult === undefined ? handler(httpContext) : earlyResult
    })
  }
}
