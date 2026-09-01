import { test } from '@japa/runner'
import { BaseChannel } from '../src/base_channel.js'
import {
  ChannelSubscriptionStorage,
  type StoredChannelSubscription,
} from '../src/channel_subscription_storage.js'

function makeSubscription(channelName: string): StoredChannelSubscription {
  return {
    channelName,
    instance: new BaseChannel(),
    paramValues: [],
  }
}

test.group('channel subscription storage', () => {
  test('indexes stored subscriptions by socket and channel', ({ assert }) => {
    const storage = new ChannelSubscriptionStorage()
    const subscription = makeSubscription('chat/general')

    storage.set('socket-1', 'chat/general', subscription)

    assert.isTrue(storage.has('socket-1', 'chat/general'))
    assert.strictEqual(storage.get('socket-1', 'chat/general'), subscription)
    assert.deepEqual(storage.channelNamesFor('socket-1'), ['chat/general'])
    assert.equal(storage.countForSocket('socket-1'), 1)
    assert.deepEqual(storage.socketIdsFor('chat/general'), ['socket-1'])
  })

  test('keeps both indexes in sync when deleting a single subscription', ({ assert }) => {
    const storage = new ChannelSubscriptionStorage()

    storage.set('socket-1', 'chat/general', makeSubscription('chat/general'))
    storage.set('socket-1', 'chat/random', makeSubscription('chat/random'))
    storage.set('socket-2', 'chat/general', makeSubscription('chat/general'))

    storage.delete('socket-1', 'chat/general')

    assert.isFalse(storage.has('socket-1', 'chat/general'))
    assert.deepEqual(storage.channelNamesFor('socket-1'), ['chat/random'])
    assert.equal(storage.countForSocket('socket-1'), 1)
    assert.deepEqual(storage.socketIdsFor('chat/general'), ['socket-2'])
    assert.deepEqual(storage.socketIdsFor('chat/random'), ['socket-1'])
  })

  test('removes a socket from every channel without touching other sockets', ({ assert }) => {
    const storage = new ChannelSubscriptionStorage()

    storage.set('socket-1', 'chat/general', makeSubscription('chat/general'))
    storage.set('socket-1', 'chat/random', makeSubscription('chat/random'))
    storage.set('socket-2', 'chat/general', makeSubscription('chat/general'))

    storage.deleteSocket('socket-1')

    assert.deepEqual(storage.channelNamesFor('socket-1'), [])
    assert.equal(storage.countForSocket('socket-1'), 0)
    assert.deepEqual(storage.socketIdsFor('chat/random'), [])
    assert.deepEqual(storage.socketIdsFor('chat/general'), ['socket-2'])
  })

  test('clears all subscription indexes', ({ assert }) => {
    const storage = new ChannelSubscriptionStorage()

    storage.set('socket-1', 'chat/general', makeSubscription('chat/general'))
    storage.set('socket-2', 'chat/random', makeSubscription('chat/random'))

    storage.clear()

    assert.deepEqual(storage.channelNamesFor('socket-1'), [])
    assert.deepEqual(storage.channelNamesFor('socket-2'), [])
    assert.deepEqual(storage.socketIdsFor('chat/general'), [])
    assert.deepEqual(storage.socketIdsFor('chat/random'), [])
  })
})
