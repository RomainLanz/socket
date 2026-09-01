import { test } from '@japa/runner'
import { ClientPresenceSnapshot } from '../src/client/presence_snapshot.js'

const ada = { id: '1', name: 'Ada', joinedAt: '2026-06-03T08:00:00.000Z' }
const bert = { id: '2', name: 'Bert', joinedAt: '2026-06-03T08:01:00.000Z' }

test.group('ClientPresenceSnapshot', () => {
  test('validates its channel and derives an immutable count from validated users', ({
    assert,
  }) => {
    const snapshot = ClientPresenceSnapshot.fromTransport(
      { channel: 'presence/general', users: [ada], count: 1 },
      'presence/general'
    )!

    assert.equal(snapshot.count, 1)
    assert.deepEqual(snapshot.users, [ada])
    assert.isTrue(Object.isFrozen(snapshot.users))
    assert.isTrue(Object.isFrozen(snapshot.users[0]))
    assert.isNull(
      ClientPresenceSnapshot.fromTransport(
        { channel: 'presence/other', users: [ada], count: 1 },
        'presence/general'
      )
    )
    assert.isNull(
      ClientPresenceSnapshot.fromTransport(
        { channel: 'presence/general', users: [ada], count: 2 },
        'presence/general'
      )
    )
  })

  test('rejects duplicate user IDs', ({ assert }) => {
    assert.isNull(
      ClientPresenceSnapshot.fromTransport(
        { channel: 'presence/general', users: [ada, { ...ada }], count: 2 },
        'presence/general'
      )
    )
  })

  test('computes joining and leaving users in snapshot order', ({ assert }) => {
    const previous = ClientPresenceSnapshot.fromTransport(
      { channel: 'presence/general', users: [ada], count: 1 },
      'presence/general'
    )!
    const current = ClientPresenceSnapshot.fromTransport(
      { channel: 'presence/general', users: [bert], count: 1 },
      'presence/general'
    )!

    assert.deepEqual(current.diff(previous), { joining: [bert], leaving: [ada] })
  })
})
