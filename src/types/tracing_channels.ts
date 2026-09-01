export interface SocketConnectMessage {
  socketId: string
}

export interface SocketDisconnectMessage {
  socketId: string
  reason: string
  subscriptions?: number
}

export interface SocketSubscribeMessage {
  socketId: string
  channel: string
  created?: boolean
  ok?: boolean
  error?: string
}

export interface SocketUnsubscribeMessage {
  socketId: string
  channel: string
  removed?: boolean
  ok?: boolean
  error?: string
}

export interface SocketChannelMessage {
  socketId: string
  channel: string
  event: string
  ok?: boolean
  error?: string
}

export interface SocketBroadcastMessage {
  target: 'channel' | 'global'
  event: string
  channel?: string
  except?: string[]
  via: 'local' | 'bus'
  delivered?: number
}
