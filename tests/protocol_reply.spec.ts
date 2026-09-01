import { test } from '@japa/runner'
import { Reply } from '../src/protocol/reply.js'
import type { ChannelAck, ServerProtocolMessage } from '../src/shared_types.js'

const successfulAck: ChannelAck<number> = { ok: true, data: 1 }
const failedAck: ChannelAck<number> = { ok: false, error: 'failed' }
const successfulFrame: ServerProtocolMessage = { type: 'ack', ok: true, data: 1 }
const failedFrame: ServerProtocolMessage = { type: 'ack', ok: false, error: 'failed' }
void [successfulAck, failedAck, successfulFrame, failedFrame]

// @ts-expect-error successful acknowledgements cannot contain an error
const contradictorySuccess: ChannelAck = { ok: true, error: 'failed' }
// @ts-expect-error failed acknowledgements require an error
const missingError: ChannelAck = { ok: false }
const contradictoryFailure: ServerProtocolMessage = {
  type: 'ack',
  ok: false,
  error: 'failed',
  // @ts-expect-error failed acknowledgement frames cannot contain data
  data: null,
}
void [contradictorySuccess, missingError, contradictoryFailure]

test.group('protocol reply', () => {
  test('builds successful acknowledgement frames', ({ assert }) => {
    assert.deepEqual(Reply.ok('message-1', { delivered: true }).toFrame(), {
      id: 'message-1',
      type: 'ack',
      ok: true,
      data: { delivered: true },
    })
  })

  test('builds failed acknowledgement frames', ({ assert }) => {
    assert.deepEqual(Reply.error('message-1', 'Handler error').toFrame(), {
      id: 'message-1',
      type: 'ack',
      ok: false,
      error: 'Handler error',
    })
  })

  test('builds invalid message rejections with or without an id', ({ assert }) => {
    assert.deepEqual(Reply.invalidMessage('message-1'), {
      id: 'message-1',
      type: 'ack',
      ok: false,
      error: 'Invalid socket message',
    })

    assert.deepEqual(Reply.invalidMessage(undefined), {
      type: 'error',
      error: 'Invalid socket message',
    })
  })

  test('wraps subscribe presence data under the transport data envelope', ({ assert }) => {
    const presenceData = {
      channel: 'presence/general',
      users: [],
      count: 0,
    }

    assert.deepEqual(
      Reply.fromSubscribeResult('subscribe-1', {
        created: true,
        ack: {
          ok: true,
          presenceData,
        },
      }).toFrame(),
      {
        id: 'subscribe-1',
        type: 'ack',
        ok: true,
        data: { presenceData },
      }
    )
  })
})
