export type SocketEmission =
  | {
      target: 'global'
      event: string
      data: unknown
    }
  | {
      target: 'channel'
      channel: string
      event: string
      data: unknown
      except?: string[]
    }

export type SocketEmissionTarget = SocketEmission['target']

export interface SocketEmissionSink {
  dispatch(emission: SocketEmission): void
}
