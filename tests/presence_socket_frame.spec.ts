import { test } from '@japa/runner'
import { PRESENCE_DATA_KEY, type PresenceSocket } from '../src/presence_manager.js'
import { PresenceSocketFrame } from '../src/presence_socket_frame.js'

test.group('PresenceSocketFrame', () => {
  test('serializes and restores presence dates and custom fields without requiring name', ({
    assert,
  }) => {
    const socket: PresenceSocket = {
      id: 'socket-1',
      data: {
        [PRESENCE_DATA_KEY]: {
          chat: {
            id: 'user-1',
            joinedAt: new Date('2026-07-31T12:00:00.000Z'),
            role: 'admin',
          },
        },
      },
    }

    const transport = PresenceSocketFrame.fromSocket('chat', socket).toTransport()
    assert.deepEqual(transport, {
      id: 'socket-1',
      data: {
        [PRESENCE_DATA_KEY]: {
          chat: {
            id: 'user-1',
            joinedAt: '2026-07-31T12:00:00.000Z',
            role: 'admin',
          },
        },
      },
    })

    const restored = PresenceSocketFrame.fromTransport(transport)?.toSocket()
    const user = restored?.data[PRESENCE_DATA_KEY]?.chat
    assert.instanceOf(user?.joinedAt, Date)
    assert.equal(user?.joinedAt.toISOString(), '2026-07-31T12:00:00.000Z')
    assert.equal(user?.role, 'admin')
    assert.notProperty(user!, 'name')
  })

  test('rejects malformed presence transport', ({ assert }) => {
    assert.isNull(
      PresenceSocketFrame.fromTransport({
        id: 'socket-1',
        data: {
          [PRESENCE_DATA_KEY]: {
            chat: { id: 'user-1', joinedAt: 'not-a-date' },
          },
        },
      })
    )
    assert.isNull(PresenceSocketFrame.fromTransport({ id: 'socket-1', data: [] }))
  })
})
