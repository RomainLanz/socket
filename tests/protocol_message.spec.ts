import { test } from '@japa/runner'
import { Message } from '../src/protocol/message.js'

test.group('protocol message', () => {
  test('decodes valid channel messages', ({ assert }) => {
    const message = Message.fromTransport(
      JSON.stringify({
        id: 'message-1',
        type: 'message',
        channel: 'chat/general',
        event: 'typing',
        data: { active: true },
      })
    )

    assert.equal(message.valid, true)
    assert.equal(message.type, 'message')

    if (message.type !== 'message') {
      assert.fail('Expected a channel protocol message')
      return
    }

    assert.equal(message.id, 'message-1')
    assert.deepEqual(message.toChannelMessage(), {
      channel: 'chat/general',
      event: 'typing',
      data: { active: true },
    })
  })

  test('decodes valid channel whispers', ({ assert }) => {
    const message = Message.fromTransport(
      JSON.stringify({
        id: 'whisper-1',
        type: 'whisper',
        channel: 'chat/general',
        event: 'typing',
        data: { active: true },
      })
    )

    assert.equal(message.valid, true)
    assert.equal(message.type, 'whisper')

    if (message.type !== 'whisper') {
      assert.fail('Expected a channel whisper protocol message')
      return
    }

    assert.equal(message.id, 'whisper-1')
    assert.deepEqual(message.toChannelMessage(), {
      channel: 'chat/general',
      event: 'typing',
      data: { active: true },
    })
  })

  test('keeps the transport id for invalid acknowledgements', ({ assert }) => {
    const message = Message.fromTransport(
      JSON.stringify({
        id: 'subscribe-1',
        type: 'subscribe',
        channel: null,
      })
    )

    assert.equal(message.valid, false)
    assert.equal(message.type, 'invalid')

    if (message.valid) {
      assert.fail('Expected an invalid protocol message')
      return
    }

    assert.deepEqual(message.toRejectionFrame(), {
      id: 'subscribe-1',
      type: 'ack',
      ok: false,
      error: 'Invalid socket message',
    })
  })

  test('uses an error frame when invalid messages have no transport id', ({ assert }) => {
    const message = Message.fromTransport(
      JSON.stringify({
        type: 'message',
        channel: 'chat/general',
        event: 42,
      })
    )

    assert.equal(message.valid, false)

    if (message.valid) {
      assert.fail('Expected an invalid protocol message')
      return
    }

    assert.deepEqual(message.toRejectionFrame(), {
      type: 'error',
      error: 'Invalid socket message',
    })
  })
})
