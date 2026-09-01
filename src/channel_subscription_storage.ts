import type { BaseChannel } from './base_channel.js'

export interface StoredChannelSubscription {
  channelName: string
  instance: BaseChannel<any, any>
  paramValues: string[]
}

/**
 * Stores durable Channel subscriptions and keeps Socket/Channel indexes in sync.
 */
export class ChannelSubscriptionStorage {
  #subscriptions = new Map<string, Map<string, StoredChannelSubscription>>()
  #subscribers = new Map<string, Set<string>>()

  has(socketId: string, channelName: string): boolean {
    return this.#subscriptions.get(socketId)?.has(channelName) ?? false
  }

  get(socketId: string, channelName: string): StoredChannelSubscription | undefined {
    return this.#subscriptions.get(socketId)?.get(channelName)
  }

  set(socketId: string, channelName: string, subscription: StoredChannelSubscription): void {
    if (!this.#subscriptions.has(socketId)) {
      this.#subscriptions.set(socketId, new Map())
    }

    this.#subscriptions.get(socketId)!.set(channelName, subscription)

    if (!this.#subscribers.has(channelName)) {
      this.#subscribers.set(channelName, new Set())
    }

    this.#subscribers.get(channelName)!.add(socketId)
  }

  delete(socketId: string, channelName: string): void {
    const subscriptions = this.#subscriptions.get(socketId)

    if (!subscriptions) {
      return
    }

    subscriptions.delete(channelName)
    if (subscriptions.size === 0) {
      this.#subscriptions.delete(socketId)
    }

    const subscribers = this.#subscribers.get(channelName)
    subscribers?.delete(socketId)
    if (subscribers?.size === 0) {
      this.#subscribers.delete(channelName)
    }
  }

  deleteSocket(socketId: string): void {
    for (const channelName of this.channelNamesFor(socketId)) {
      this.delete(socketId, channelName)
    }
  }

  channelNamesFor(socketId: string): string[] {
    return [...(this.#subscriptions.get(socketId)?.keys() ?? [])]
  }

  countForSocket(socketId: string): number {
    return this.#subscriptions.get(socketId)?.size ?? 0
  }

  socketIdsFor(channelName: string): string[] {
    return [...(this.#subscribers.get(channelName) ?? [])]
  }

  get channelsCount(): number {
    return this.#subscribers.size
  }

  clear(): void {
    this.#subscriptions.clear()
    this.#subscribers.clear()
  }
}
