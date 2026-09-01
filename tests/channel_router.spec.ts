import { test } from '@japa/runner'
import { BaseChannel } from '../src/base_channel.js'
import { ChannelPattern } from '../src/channel_pattern.js'
import { ChannelRouter } from '../src/channel_router.js'

test.group('channel router', () => {
  test('channel patterns match channel names and extract params', ({ assert }) => {
    const pattern = ChannelPattern.from('chat/:roomId/messages/:messageId')

    assert.deepEqual(pattern.match('chat/general/messages/42'), {
      roomId: 'general',
      messageId: '42',
    })
  })

  test('channel patterns reject names with different static segments', ({ assert }) => {
    const pattern = ChannelPattern.from('chat/:roomId')

    assert.isNull(pattern.match('presence/general'))
  })

  test('channel patterns reject names with different segment counts', ({ assert }) => {
    const pattern = ChannelPattern.from('chat/:roomId')

    assert.isNull(pattern.match('chat/general/thread'))
  })

  test('runtime routing keeps patterns outside the generated subset', ({ assert }) => {
    const pattern = ChannelPattern.from('users/:id.json')

    assert.deepEqual(pattern.match('users/42.json'), { id: '42' })
  })

  test('preserves optional, wildcard, root, and decoding semantics', ({ assert }) => {
    assert.deepEqual(ChannelPattern.from('threads/:threadId?').match('threads'), {})
    assert.deepEqual(ChannelPattern.from('files/*').match('files/images/ada%20lovelace.png'), {
      '*': 'images/ada lovelace.png',
    })
    assert.deepEqual(ChannelPattern.from('*').match('/'), {})
  })

  test('router returns the most specific matching channel', ({ assert }) => {
    class GeneralChannel extends BaseChannel {
      static pattern = 'chat/general'
    }

    class RoomChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    const router = new ChannelRouter()
    router.register(RoomChannel as any)
    router.register(GeneralChannel as any)

    const matched = router.match('chat/general')

    assert.equal(matched?.channel, GeneralChannel)
    assert.equal(matched?.pattern, 'chat/general')
    assert.deepEqual(matched?.params, {})
  })

  test('router preserves registration order for equally specific channels', ({ assert }) => {
    class FirstChannel extends BaseChannel {
      static pattern = 'chat/:roomId'
    }

    class SecondChannel extends BaseChannel {
      static pattern = 'chat/:id'
    }

    const router = new ChannelRouter()
    router.register(FirstChannel as any)
    router.register(SecondChannel as any)

    const matched = router.match('chat/general')

    assert.equal(matched?.channel, FirstChannel)
    assert.equal(matched?.pattern, 'chat/:roomId')
    assert.deepEqual(matched?.params, { roomId: 'general' })
  })

  test('resolves channel instances through the configured factory', async ({ assert }) => {
    const dependency = { name: 'injected' }

    class InjectedChannel extends BaseChannel {
      static pattern = 'injected'

      constructor(readonly injectedDependency: typeof dependency) {
        super()
      }
    }

    const router = new ChannelRouter((Channel) => new Channel(dependency))
    router.register(InjectedChannel as any)

    const result = await router.authorize({} as any, 'injected')

    assert.isTrue(result.success)
    if (result.success) {
      assert.strictEqual((result.instance as InjectedChannel).injectedDependency, dependency)
    }
  })

  test('allows each middleware to advance the chain only once', async ({ assert }) => {
    const calls: string[] = []

    class GuardedChannel extends BaseChannel {
      static pattern = 'guarded'
      static middlewares = [
        async (_ctx: unknown, next: () => Promise<void>) => {
          calls.push('first')
          await next()
          await next()
        },
        async () => {
          calls.push('second')
        },
        async () => {
          calls.push('third')
        },
      ]
    }

    const router = new ChannelRouter()
    router.register(GuardedChannel)

    const result = await router.authorize({} as any, 'guarded')

    assert.isTrue(result.success)
    assert.deepEqual(calls, ['first', 'second'])
  })
})
