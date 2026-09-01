import { test } from '@japa/runner'
import { BaseChannel } from '../src/base_channel.js'
import { ChannelRouter } from '../src/channel_router.js'
import { ChannelSubscriptions } from '../src/channel_subscriptions.js'
import { onMessage } from '../src/decorators.js'
import { PRESENCE_DATA_KEY, PresenceManager, type PresenceData } from '../src/presence_manager.js'

function makeSocket(id = 'socket-1') {
  const emitted: { event: string; data: unknown }[] = []

  return {
    id,
    user: { id: 'user-1', fullName: 'Ada' },
    emitted,
    emit(event: string, data: unknown) {
      emitted.push({ event, data })
    },
    disconnect() {},
    raw: { id, data: {} },
  } as any
}

function makeSocketService(getSocketIds: (channelName: string) => string[] = () => []) {
  const broadcasts: Record<string, unknown>[] = []

  return {
    broadcasts,
    to(channelName: string) {
      return {
        emit(event: string, data: unknown) {
          broadcasts.push({ channelName, event, data, recipients: getSocketIds(channelName) })
        },
        except(socketId: string) {
          return {
            emit(event: string, data: unknown) {
              broadcasts.push({
                channelName,
                except: socketId,
                event,
                data,
                recipients: getSocketIds(channelName).filter((id) => id !== socketId),
              })
            },
          }
        },
      }
    },
  } as any
}

function makeLogger() {
  return {
    warnings: [] as Record<string, unknown>[],
    warn(message: string, error: unknown) {
      this.warnings.push({ message, error })
    },
  } as any
}

test.group('channel subscriptions', () => {
  test('create a durable subscription used for message dispatch and leave', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
      static leftParams: string[] = []

      @onMessage('message')
      message(_socket: any, data: unknown) {
        return { roomId: this.params.roomId, data }
      }

      async onLeave(_socket: any, ...params: string[]) {
        ChatChannel.leftParams = params
      }
    }

    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    const socket = makeSocket()
    let subscriptions: ChannelSubscriptions
    const socketService = makeSocketService((channelName) => {
      return subscriptions.getSocketIds(channelName)
    })
    subscriptions = new ChannelSubscriptions(socketService, router, makeLogger())

    assert.deepEqual(await subscriptions.subscribe(socket, 'chat/general'), {
      created: true,
      ack: {
        ok: true,
        presenceData: null,
      },
    })
    assert.equal(subscriptions.subscriptionCountFor(socket.id), 1)
    assert.deepEqual(subscriptions.getSocketIds('chat/general'), [socket.id])

    socketService.to('chat/general').emit('notice', { text: 'hello' })
    assert.deepEqual(socketService.broadcasts.at(-1), {
      channelName: 'chat/general',
      event: 'notice',
      data: { text: 'hello' },
      recipients: [socket.id],
    })

    assert.deepEqual(
      await subscriptions.handleMessage(socket, {
        channel: 'chat/general',
        event: 'message',
        data: 'hello',
      }),
      {
        ok: true,
        data: { roomId: 'general', data: 'hello' },
      }
    )

    assert.isTrue(await subscriptions.leave(socket, 'chat/general'))

    assert.deepEqual(ChatChannel.leftParams, ['general'])
    assert.equal(subscriptions.subscriptionCountFor(socket.id), 0)
    assert.deepEqual(subscriptions.getSocketIds('chat/general'), [])

    socketService.to('chat/general').emit('notice', { text: 'gone' })
    assert.deepEqual(socketService.broadcasts.at(-1), {
      channelName: 'chat/general',
      event: 'notice',
      data: { text: 'gone' },
      recipients: [],
    })
    assert.deepEqual(
      await subscriptions.handleMessage(socket, {
        channel: 'chat/general',
        event: 'message',
      }),
      { ok: false, error: 'Not subscribed' }
    )
    assert.isFalse(await subscriptions.leave(socket, 'chat/general'))

    await subscriptions.subscribe(socket, 'chat/general')
    assert.deepEqual(subscriptions.getSocketIds('chat/general'), [socket.id])

    subscriptions.deleteSocket(socket.id)
    assert.deepEqual(subscriptions.getSocketIds('chat/general'), [])
  })

  test('removes the durable subscription before best-effort leave cleanup', async ({ assert }) => {
    const leaveStarted = Promise.withResolvers<void>()
    const finishLeave = Promise.withResolvers<void>()

    class SlowLeaveChannel extends BaseChannel {
      static pattern = 'slow-leave/:roomId'

      @onMessage('message')
      message() {
        return 'handled'
      }

      async onLeave() {
        leaveStarted.resolve()
        await finishLeave.promise
        throw new Error('cleanup failed')
      }
    }

    const router = new ChannelRouter()
    router.register(SlowLeaveChannel as any)
    const logger = makeLogger()
    const socket = makeSocket()
    const subscriptions = new ChannelSubscriptions(makeSocketService(), router, logger)

    await subscriptions.subscribe(socket, 'slow-leave/general')
    const leaving = subscriptions.leave(socket, 'slow-leave/general')
    await leaveStarted.promise

    assert.equal(subscriptions.subscriptionCountFor(socket.id), 0)
    assert.deepEqual(subscriptions.getSocketIds('slow-leave/general'), [])
    assert.deepEqual(
      await subscriptions.handleMessage(socket, {
        channel: 'slow-leave/general',
        event: 'message',
      }),
      { ok: false, error: 'Not subscribed' }
    )
    assert.deepEqual(
      subscriptions.relayWhisper(socket, {
        channel: 'slow-leave/general',
        event: 'typing',
      }),
      { ok: false, error: 'Not subscribed' }
    )

    finishLeave.resolve()

    assert.isTrue(await leaving)
    assert.lengthOf(logger.warnings, 1)
    assert.isFalse(await subscriptions.leave(socket, 'slow-leave/general'))
  })

  test('removes presence membership before waiting for another channel operation', async ({
    assert,
  }) => {
    const secondJoinStarted = Promise.withResolvers<void>()
    const finishSecondJoin = Promise.withResolvers<void>()

    class PresenceChannel extends BaseChannel {
      static pattern = 'serialized-presence/:roomId'
      static options = { presence: true }

      getPresenceInfo(socket: any) {
        return { id: socket.id, data: { name: socket.id } }
      }

      async onJoin(socket: any) {
        if (socket.id === 'socket-2') {
          secondJoinStarted.resolve()
          await finishSecondJoin.promise
        }
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)
    const presence = new PresenceManager()
    const firstSocket = makeSocket('socket-1')
    const secondSocket = makeSocket('socket-2')
    const subscriptions = new ChannelSubscriptions(
      makeSocketService(),
      router,
      makeLogger(),
      presence
    )

    await subscriptions.subscribe(firstSocket, 'serialized-presence/general')
    const secondSubscribe = subscriptions.subscribe(secondSocket, 'serialized-presence/general')
    await secondJoinStarted.promise

    const firstLeave = subscriptions.leave(firstSocket, 'serialized-presence/general')

    assert.equal(subscriptions.subscriptionCountFor(firstSocket.id), 0)
    assert.deepEqual(subscriptions.getSocketIds('serialized-presence/general'), [])
    assert.deepEqual(
      await subscriptions.handleMessage(firstSocket, {
        channel: 'serialized-presence/general',
        event: 'message',
      }),
      { ok: false, error: 'Not subscribed' }
    )

    finishSecondJoin.resolve()

    assert.isTrue((await secondSubscribe).created)
    assert.isTrue(await firstLeave)
    assert.deepEqual(subscriptions.getSocketIds('serialized-presence/general'), [secondSocket.id])
  })

  test('does not serialize non-presence joins for the same channel', async ({ assert }) => {
    const firstJoinStarted = Promise.withResolvers<void>()
    const releaseFirstJoin = Promise.withResolvers<void>()

    class ChatChannel extends BaseChannel {
      static pattern = 'concurrent/:roomId'

      async onJoin(socket: any) {
        if (socket.id === 'socket-1') {
          firstJoinStarted.resolve()
          await releaseFirstJoin.promise
        }
      }
    }

    const router = new ChannelRouter()
    router.register(ChatChannel as any)
    const subscriptions = new ChannelSubscriptions(makeSocketService(), router, makeLogger())
    const firstSubscription = subscriptions.subscribe(makeSocket('socket-1'), 'concurrent/general')
    await firstJoinStarted.promise

    const secondSubscription = subscriptions.subscribe(makeSocket('socket-2'), 'concurrent/general')
    const secondCompletedFirst = await Promise.race([
      secondSubscription.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 20)),
    ])
    releaseFirstJoin.resolve()
    await firstSubscription

    assert.isTrue(secondCompletedFirst)
  })

  test('limits retained subscriptions and channel name length', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const router = new ChannelRouter()
    router.register(ChatChannel as any)
    const socket = makeSocket()
    const subscriptions = new ChannelSubscriptions(
      makeSocketService(),
      router,
      makeLogger(),
      undefined,
      { maxSubscriptionsPerSocket: 1, maxChannelNameLength: 12 }
    )

    assert.isTrue((await subscriptions.subscribe(socket, 'chat/first')).created)
    assert.deepEqual(await subscriptions.subscribe(socket, 'chat/second'), {
      ack: { ok: false, error: 'Socket subscription limit exceeded' },
      created: false,
    })
    assert.deepEqual(await subscriptions.subscribe(socket, 'chat/name-too-long'), {
      ack: { ok: false, error: 'Channel name is too long' },
      created: false,
    })

    const duplicate = await subscriptions.subscribe(socket, 'chat/first')
    assert.isFalse(duplicate.created)
    assert.isTrue(duplicate.ack.ok)
  })

  test('inherits decorated handlers, lets children override events, and binds this', async ({
    assert,
  }) => {
    class ParentChannel extends BaseChannel {
      static pattern = 'decorated/:roomId'

      inherited() {
        return `parent:${this.params.roomId}`
      }

      overridden() {
        return 'parent'
      }
    }

    class ChildChannel extends ParentChannel {
      prefix = 'child'

      overridden() {
        return `${this.prefix}:${this.params.roomId}`
      }
    }

    onMessage('inherited')(
      ParentChannel.prototype,
      'inherited',
      Object.getOwnPropertyDescriptor(ParentChannel.prototype, 'inherited')!
    )
    onMessage('overridden')(
      ParentChannel.prototype,
      'overridden',
      Object.getOwnPropertyDescriptor(ParentChannel.prototype, 'overridden')!
    )
    onMessage('overridden')(
      ChildChannel.prototype,
      'overridden',
      Object.getOwnPropertyDescriptor(ChildChannel.prototype, 'overridden')!
    )

    const router = new ChannelRouter()
    router.register(ChildChannel as any)
    const socket = makeSocket()
    const subscriptions = new ChannelSubscriptions(makeSocketService(), router, makeLogger())
    await subscriptions.subscribe(socket, 'decorated/general')

    assert.deepEqual(
      await subscriptions.handleMessage(socket, {
        channel: 'decorated/general',
        event: 'inherited',
      }),
      { ok: true, data: 'parent:general' }
    )
    assert.deepEqual(
      await subscriptions.handleMessage(socket, {
        channel: 'decorated/general',
        event: 'overridden',
      }),
      { ok: true, data: 'child:general' }
    )
  })

  test('leaveAll removes every durable subscription for the socket', async ({ assert }) => {
    class ChatChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
      static leftChannels: string[] = []

      async onLeave(_socket: any) {
        ChatChannel.leftChannels.push(this.params.roomId)
      }
    }

    const router = new ChannelRouter()
    router.register(ChatChannel as any)

    const socket = makeSocket()
    const subscriptions = new ChannelSubscriptions(makeSocketService(), router, makeLogger())

    await subscriptions.subscribe(socket, 'chat/general')
    await subscriptions.subscribe(socket, 'chat/random')

    await subscriptions.leaveAll(socket)

    assert.sameMembers(ChatChannel.leftChannels, ['general', 'random'])
    assert.equal(subscriptions.subscriptionCountFor(socket.id), 0)
    assert.deepEqual(subscriptions.getSocketIds('chat/general'), [])
    assert.deepEqual(subscriptions.getSocketIds('chat/random'), [])
  })

  test('rollback durable membership and presence when join fails', async ({ assert }) => {
    class FailingPresenceChannel extends BaseChannel {
      static pattern = 'presence/:roomId'
      static options = { presence: true }

      getPresenceInfo() {
        return { id: 'user-1', data: { name: 'Ada' } }
      }

      async onJoin() {
        throw new Error('boom')
      }
    }

    const router = new ChannelRouter()
    router.register(FailingPresenceChannel as any)

    const socket = makeSocket()
    const presence = new PresenceManager()
    let subscriptions: ChannelSubscriptions
    const socketService = makeSocketService((channelName) => {
      return subscriptions.getSocketIds(channelName)
    })
    subscriptions = new ChannelSubscriptions(socketService, router, makeLogger(), presence)

    assert.deepEqual(await subscriptions.subscribe(socket, 'presence/general'), {
      created: false,
      ack: {
        ok: false,
        error: 'Join failed',
      },
    })
    assert.equal(subscriptions.subscriptionCountFor(socket.id), 0)
    assert.deepEqual(subscriptions.getSocketIds('presence/general'), [])
    assert.equal(await presence.count('presence/general'), 0)
  })

  test('rollback presence when distributed snapshot fails during join', async ({ assert }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'presence/:roomId'
      static options = { presence: true }

      getPresenceInfo() {
        return { id: 'user-1', data: { name: 'Ada' } }
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)

    const socket = makeSocket()
    const presence = new PresenceManager()
    presence.setSocketFetcher(async () => {
      throw new Error('distributed snapshot timeout')
    })

    const socketService = makeSocketService()
    const subscriptions = new ChannelSubscriptions(socketService, router, makeLogger(), presence)

    assert.deepEqual(await subscriptions.subscribe(socket, 'presence/general'), {
      created: false,
      ack: {
        ok: false,
        error: 'Join failed',
      },
    })
    assert.equal(subscriptions.subscriptionCountFor(socket.id), 0)
    assert.deepEqual(subscriptions.getSocketIds('presence/general'), [])
    assert.isFalse(presence.hasLocal('presence/general', socket.id))
    assert.deepEqual(socket.raw.data, {})
  })

  test('failed joins are not durable and may be retried', async ({ assert }) => {
    class FlakyChannel extends BaseChannel {
      static pattern = 'flaky/:roomId'
      static shouldFail = true
      static joinCount = 0

      async onJoin() {
        FlakyChannel.joinCount += 1
        if (FlakyChannel.shouldFail) {
          throw new Error('boom')
        }
      }
    }

    const router = new ChannelRouter()
    router.register(FlakyChannel as any)

    const socket = makeSocket()
    const subscriptions = new ChannelSubscriptions(makeSocketService(), router, makeLogger())

    const failed = await subscriptions.subscribe(socket, 'flaky/general')

    FlakyChannel.shouldFail = false
    const retried = await subscriptions.subscribe(socket, 'flaky/general')

    assert.deepEqual(failed, {
      created: false,
      ack: {
        ok: false,
        error: 'Join failed',
      },
    })
    assert.equal(subscriptions.subscriptionCountFor(socket.id), 1)
    assert.isTrue(retried.created)
    assert.isTrue(retried.ack.ok)
    assert.equal(FlakyChannel.joinCount, 2)
    assert.deepEqual(subscriptions.getSocketIds('flaky/general'), [socket.id])
  })

  test('join failure after presence add leaves no broadcast target for the failed socket', async ({
    assert,
  }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'presence-targets/:roomId'
      static options = { presence: true }

      getPresenceInfo(socket: any) {
        return { id: socket.user.id, data: { name: socket.user.fullName } }
      }

      async onMemberJoin(socket: any) {
        if (socket.id === 'socket-2') {
          throw new Error('boom')
        }
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)

    const firstSocket = makeSocket('socket-1')
    const secondSocket = makeSocket('socket-2')
    secondSocket.user = { id: 'user-2', fullName: 'Grace' }
    const presence = new PresenceManager()
    let subscriptions: ChannelSubscriptions
    const socketService = makeSocketService((channelName) => {
      return subscriptions.getSocketIds(channelName)
    })
    subscriptions = new ChannelSubscriptions(socketService, router, makeLogger(), presence)

    await subscriptions.subscribe(firstSocket, 'presence-targets/general')
    const failed = await subscriptions.subscribe(secondSocket, 'presence-targets/general')

    assert.deepEqual(failed, {
      created: false,
      ack: {
        ok: false,
        error: 'Join failed',
      },
    })
    assert.deepEqual(subscriptions.getSocketIds('presence-targets/general'), [firstSocket.id])
    assert.equal(subscriptions.subscriptionCountFor(secondSocket.id), 0)
    assert.isFalse(presence.hasLocal('presence-targets/general', secondSocket.id))
    assert.equal(await presence.count('presence-targets/general'), 1)
    assert.notInclude(
      socketService.broadcasts.flatMap((broadcast: Record<string, unknown>) => {
        return broadcast.recipients as string[]
      }),
      secondSocket.id
    )
  })

  test('presence channels require getPresenceInfo', async ({ assert }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'presence/:roomId'
      static options = { presence: true }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)

    const socket = makeSocket()
    const presence = new PresenceManager()
    const socketService = makeSocketService()
    const subscriptions = new ChannelSubscriptions(socketService, router, makeLogger(), presence)

    assert.deepEqual(await subscriptions.subscribe(socket, 'presence/general'), {
      created: false,
      ack: {
        ok: false,
        error: 'Presence channels must implement getPresenceInfo(socket)',
      },
    })
    assert.equal(subscriptions.subscriptionCountFor(socket.id), 0)
    assert.isFalse(presence.hasLocal('presence/general', socket.id))
    assert.deepEqual(socket.raw.data, {})
  })

  test('duplicate subscribe returns current presence snapshot without joining again', async ({
    assert,
  }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'online/:roomId'
      static options = { presence: true }
      static joinCount = 0

      getPresenceInfo() {
        return { id: 'user-1' }
      }

      async onJoin() {
        PresenceChannel.joinCount += 1
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)

    const socket = makeSocket()
    const presence = new PresenceManager()
    const subscriptions = new ChannelSubscriptions(
      makeSocketService(),
      router,
      makeLogger(),
      presence
    )

    const first = await subscriptions.subscribe(socket, 'online/general')
    const second = await subscriptions.subscribe(socket, 'online/general')

    assert.isTrue(first.created)
    assert.isTrue(first.ack.ok)
    assert.isFalse(second.created)
    assert.isTrue(second.ack.ok)
    assert.equal(second.ack.presenceData?.count, 1)
    assert.equal(PresenceChannel.joinCount, 1)
  })

  test('keeps an existing subscription when its refreshed presence ack is not serializable', async ({
    assert,
  }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'online/:roomId'
      static options = { presence: true }

      getPresenceInfo() {
        return { id: 'user-1', data: { name: 'Ada' } }
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)
    const socket = makeSocket()
    const presence = new PresenceManager()
    const logger = makeLogger()
    const subscriptions = new ChannelSubscriptions(makeSocketService(), router, logger, presence)

    assert.isTrue((await subscriptions.subscribe(socket, 'online/general')).created)
    const presenceByChannel = socket.raw.data[PRESENCE_DATA_KEY] as Record<
      string,
      Record<string, unknown>
    >
    presenceByChannel['online/general'].counter = 1n

    assert.deepEqual(await subscriptions.subscribe(socket, 'online/general'), {
      ack: { ok: false, error: 'Subscription response is not serializable' },
      created: false,
    })
    assert.deepEqual(subscriptions.getSocketIds('online/general'), [socket.id])
    assert.equal(subscriptions.subscriptionCountFor(socket.id), 1)
    assert.lengthOf(logger.warnings, 1)
  })

  test('presence snapshots preserve custom member fields', async ({ assert }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'team/:teamId'
      static options = { presence: true }

      getPresenceInfo() {
        return {
          id: 'user-1',
          data: {
            name: 'Ada',
            avatarUrl: 'https://example.com/ada.png',
            role: 'admin',
            activity: {
              lastSeen: new Date('2026-07-31T12:00:00.000Z'),
            },
          },
        }
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)

    const socket = makeSocket()
    const presence = new PresenceManager()
    const subscriptions = new ChannelSubscriptions(
      makeSocketService(),
      router,
      makeLogger(),
      presence
    )

    const result = await subscriptions.subscribe(socket, 'team/core')
    const presenceData = result.ack.presenceData as PresenceData | undefined
    const member = presenceData?.users[0]

    assert.isTrue(result.ack.ok)
    assert.equal(member?.id, 'user-1')
    assert.equal(member?.name, 'Ada')
    assert.equal(member?.avatarUrl, 'https://example.com/ada.png')
    assert.equal(member?.role, 'admin')
    assert.deepEqual(member?.activity, { lastSeen: new Date('2026-07-31T12:00:00.000Z') })
    assert.instanceOf(member?.joinedAt, Date)
  })

  test('presence snapshots include members returned by the distributed socket fetcher', async ({
    assert,
  }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'distributed/:roomId'
      static options = { presence: true }

      getPresenceInfo(socket: any) {
        return {
          id: socket.user.id,
          data: { name: socket.user.fullName },
        }
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)

    const socket = makeSocket('socket-1')
    const remoteRawSocket = { id: 'socket-2', data: {} } as any
    const presence = new PresenceManager()

    presence.join('distributed/general', remoteRawSocket, {
      id: 'user-2',
      data: { name: 'Grace' },
    })
    presence.setSocketFetcher(async () => {
      return [socket.raw, remoteRawSocket]
    })

    const subscriptions = new ChannelSubscriptions(
      makeSocketService(),
      router,
      makeLogger(),
      presence
    )

    const result = await subscriptions.subscribe(socket, 'distributed/general')
    const presenceData = result.ack.presenceData as PresenceData | undefined

    assert.isTrue(result.ack.ok)
    assert.equal(presenceData?.count, 2)
    assert.sameDeepMembers(
      presenceData?.users.map((user) => ({ id: user.id, name: user.name })) ?? [],
      [
        { id: 'user-1', name: 'Ada' },
        { id: 'user-2', name: 'Grace' },
      ]
    )
  })

  test('deduplicates presence and member hooks across sockets for the same user', async ({
    assert,
  }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'multi-tab/:roomId'
      static options = { presence: true }
      static memberJoins = 0
      static memberLeaves = 0
      static joins = 0
      static leaves = 0

      getPresenceInfo(socket: any) {
        return { id: socket.user.id, data: { name: socket.user.fullName } }
      }

      async onMemberJoin() {
        PresenceChannel.memberJoins += 1
      }

      async onMemberLeave() {
        PresenceChannel.memberLeaves += 1
      }

      async onJoin() {
        PresenceChannel.joins += 1
      }

      async onLeave() {
        PresenceChannel.leaves += 1
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)
    const firstSocket = makeSocket('socket-1')
    const secondSocket = makeSocket('socket-2')
    secondSocket.user = { id: 'user-1', fullName: 'Ada from another tab' }
    const presence = new PresenceManager()
    const subscriptions = new ChannelSubscriptions(
      makeSocketService(),
      router,
      makeLogger(),
      presence
    )

    const [, second] = await Promise.all([
      subscriptions.subscribe(firstSocket, 'multi-tab/general'),
      subscriptions.subscribe(secondSocket, 'multi-tab/general'),
    ])
    const secondPresence = second.ack.presenceData as PresenceData

    assert.equal(secondPresence.count, 1)
    assert.lengthOf(secondPresence.users, 1)
    assert.equal(secondPresence.users[0].name, 'Ada')
    assert.equal(PresenceChannel.memberJoins, 1)
    assert.equal(PresenceChannel.joins, 2)

    await subscriptions.leave(firstSocket, 'multi-tab/general')
    assert.equal(await presence.count('multi-tab/general'), 1)
    assert.equal(PresenceChannel.memberLeaves, 0)

    await subscriptions.leave(secondSocket, 'multi-tab/general')
    assert.equal(await presence.count('multi-tab/general'), 0)
    assert.equal(PresenceChannel.memberLeaves, 1)
    assert.equal(PresenceChannel.leaves, 2)
  })

  test('removes retained presence without recalculating member data on leave', async ({
    assert,
  }) => {
    class PresenceChannel extends BaseChannel {
      static pattern = 'stable-member/:roomId'
      static options = { presence: true }
      static presenceReads = 0
      static memberLeaves: unknown[] = []
      static leaves = 0

      getPresenceInfo() {
        PresenceChannel.presenceReads += 1
        if (PresenceChannel.presenceReads > 1) {
          throw new Error('member source is no longer available')
        }
        return { id: 'user-1', data: { name: 'Ada' } }
      }

      async onMemberLeave(_socket: any, member: unknown) {
        PresenceChannel.memberLeaves.push(member)
      }

      async onLeave() {
        PresenceChannel.leaves += 1
      }
    }

    const router = new ChannelRouter()
    router.register(PresenceChannel as any)
    const socket = makeSocket()
    const presence = new PresenceManager()
    const subscriptions = new ChannelSubscriptions(
      makeSocketService(),
      router,
      makeLogger(),
      presence
    )

    await subscriptions.subscribe(socket, 'stable-member/general')
    await subscriptions.leave(socket, 'stable-member/general')

    assert.equal(PresenceChannel.presenceReads, 1)
    assert.deepEqual(PresenceChannel.memberLeaves, [{ id: 'user-1', name: 'Ada' }])
    assert.equal(PresenceChannel.leaves, 1)
    assert.equal(await presence.count('stable-member/general'), 0)
    assert.deepEqual(socket.raw.data, {})
  })

  test('deduplicates distributed presence using the earliest connection metadata', async ({
    assert,
  }) => {
    const presence = new PresenceManager()
    const first = { id: 'socket-z', data: {} } as any
    const second = { id: 'socket-a', data: {} } as any
    presence.join('distributed/general', first, {
      id: 'user-1',
      data: { name: 'First' },
    })
    presence.join('distributed/general', second, {
      id: 'user-1',
      data: { name: 'Second' },
    })

    const presenceByChannel = first.data[PRESENCE_DATA_KEY] as Record<
      string,
      Record<string, unknown>
    >
    presenceByChannel['distributed/general'].joinedAt = new Date('2026-08-03T08:00:00.000Z')
    const secondPresenceByChannel = second.data[PRESENCE_DATA_KEY] as Record<
      string,
      Record<string, unknown>
    >
    secondPresenceByChannel['distributed/general'].joinedAt = new Date('2026-08-03T09:00:00.000Z')
    presence.setSocketFetcher(async () => [second, first])

    const snapshot = await presence.snapshot('distributed/general')

    assert.equal(snapshot.count, 1)
    assert.lengthOf(snapshot.users, 1)
    assert.equal(snapshot.users[0].name, 'First')
  })
})
