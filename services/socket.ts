import app from '@adonisjs/core/services/app'
import type { SocketService } from '../src/socket_service.js'

let socket: SocketService

await app.booted(async () => {
  socket = await app.container.make('socket')
})

export { socket as default }
