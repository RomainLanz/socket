import { AssertionError } from 'node:assert'
import { test } from '@japa/runner'
import { SocketService } from '../src/socket_service.js'

test.group('socket fake', () => {
  test('records broadcasted events in memory', async ({ assert }) => {
    const socket = new SocketService()
    const fake = socket.fake()

    socket.broadcast('maintenance', { active: true })

    fake.assertBroadcasted('maintenance')
    fake.assertBroadcasted('maintenance', { data: { active: true } })
    fake.assertCount(1)

    assert.deepEqual(fake.emissions, [
      {
        target: 'global',
        event: 'maintenance',
        data: { active: true },
      },
    ])
  })

  test('records channel events and exclusions in memory', async () => {
    const socket = new SocketService()
    const fake = socket.fake()

    socket.to('chat/general').emit('chat:message', { text: 'hello' })
    socket.to('chat/general').except('socket-1').emit('chat:typing', { typing: true })

    fake.assertEmittedTo('chat/general', 'chat:message', {
      data: { text: 'hello' },
    })
    fake.assertEmittedTo('chat/general', 'chat:typing', {
      data: (data: unknown) => {
        return (data as { typing: boolean }).typing === true
      },
      except: ['socket-1'],
    })
    fake.assertNotEmittedTo('chat/general', 'chat:missing')
    fake.assertCount(2, { target: 'channel', channel: 'chat/general' })
  })

  test('throws assertion errors when events do not match', async ({ assert }) => {
    const socket = new SocketService()
    const fake = socket.fake()

    socket.broadcast('maintenance', { active: true })

    assert.instanceOf(
      capture(() => fake.assertBroadcasted('deploy:started')),
      AssertionError
    )
    assert.instanceOf(
      capture(() => fake.assertNotBroadcasted('maintenance', { data: { active: true } })),
      AssertionError
    )
    assert.instanceOf(
      capture(() => fake.assertCount(0)),
      AssertionError
    )
  })

  test('restores the real socket service', async ({ assert }) => {
    const socket = new SocketService()
    const fake = socket.fake()

    socket.broadcast('maintenance', { active: true })
    socket.restore()
    socket.broadcast('maintenance', { active: false })

    assert.deepEqual(fake.emissions, [
      {
        target: 'global',
        event: 'maintenance',
        data: { active: true },
      },
    ])
  })

  test('supports explicit resource management restore', async ({ assert }) => {
    const socket = new SocketService()
    const firstFake = socket.fake()
    const secondFake = socket.fake()

    firstFake[Symbol.dispose]()
    socket.broadcast('maintenance', { active: true })

    assert.deepEqual(firstFake.emissions, [])
    secondFake.assertBroadcasted('maintenance', { data: { active: true } })

    secondFake[Symbol.dispose]()
    socket.broadcast('maintenance', { active: false })

    assert.deepEqual(secondFake.emissions, [
      {
        target: 'global',
        event: 'maintenance',
        data: { active: true },
      },
    ])
  })
})

function capture(callback: () => void): unknown {
  try {
    callback()
    return null
  } catch (error) {
    return error
  }
}
