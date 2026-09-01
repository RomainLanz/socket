import { BaseCheck, Result } from '@adonisjs/core/health'
import type { SocketService } from './socket_service.js'

export class SocketHealthCheck extends BaseCheck {
  name = 'Socket health check'

  constructor(private socket: SocketService) {
    super()
  }

  async run() {
    const snapshot = this.socket.health()
    const result = snapshot.ready
      ? Result.ok('Socket service is ready')
      : Result.failed(`Socket service is ${snapshot.status}`)

    return result.setMetaData(snapshot)
  }
}
