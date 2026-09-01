/**
 * Types shared between the server and the client.
 */

/** WebSocket close code used when the server deliberately ends a client session. */
export const SERVER_DISCONNECT_CODE = 4000

/**
 * Message sent by the client to a channel.
 */
export interface ChannelMessage {
  channel: string
  event: string
  data?: unknown
}

/**
 * Channel handler acknowledgement response.
 */
export type ChannelAck<T = unknown> =
  | { ok: true; data?: T; error?: never }
  | { ok: false; error: string; data?: never }

/** Extracts the server-to-client event map declared by a channel instance. */
export type ChannelServerEventsOf<T> = T extends { readonly $serverEvents: infer Events }
  ? Events
  : unknown

/**
 * Client-to-server JSON protocol messages.
 */
export type ClientProtocolMessage =
  | { id?: string; type: 'subscribe'; channel: string }
  | { id?: string; type: 'unsubscribe'; channel: string }
  | { id?: string; type: 'message'; channel: string; event: string; data?: unknown }
  | { id?: string; type: 'whisper'; channel: string; event: string; data?: unknown }
  | { id?: string; type: 'ping' }

/**
 * Server-to-client JSON protocol messages.
 */
export type ServerProtocolMessage =
  | ({ id?: string; type: 'ack' } & ChannelAck)
  | { type: 'event'; channel?: string; event: string; data?: unknown }
  | { type: 'error'; error: string }
  | { id?: string; type: 'pong' }
