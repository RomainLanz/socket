import { test } from '@japa/runner'
import { BindableChannelPattern } from '../src/bindable_channel_pattern.js'
import { buildChannelName } from '../src/client/socket.js'

test.group('BindableChannelPattern', () => {
  test('parses the generated grammar and exposes its canonical pattern', ({ assert }) => {
    const pattern = BindableChannelPattern.parse('/chat/:roomId/messages/:messageId?/')!

    assert.equal(pattern.canonicalPattern, 'chat/:roomId/messages/:messageId?')
    assert.deepEqual(pattern.parameters, [
      { name: 'roomId', optional: false },
      { name: 'messageId', optional: true },
    ])
    assert.equal(pattern.bind({ roomId: 'general' }), 'chat/general/messages')
    assert.equal(pattern.bind({ roomId: 'general', messageId: 42 }), 'chat/general/messages/42')
  })

  test('keeps slash-separated wildcard values', ({ assert }) => {
    const pattern = BindableChannelPattern.parse('files/*')!

    assert.deepEqual(pattern.parameters, [{ name: 'wildcard', optional: false }])
    assert.equal(
      pattern.bind({ wildcard: 'images/avatars/ada.png' }),
      'files/images/avatars/ada.png'
    )
  })

  test('rejects values that cannot remain one ordinary segment', ({ assert }) => {
    const pattern = BindableChannelPattern.parse('chat/:roomId')!

    assert.throws(() => pattern.bind({ roomId: 'team/general' }), 'Invalid "roomId" parameter')
    assert.throws(() => pattern.bind({ roomId: '' }), 'Invalid "roomId" parameter')
  })

  test('rejects ambiguous parameter names and unsupported Matchit grammar', ({ assert }) => {
    assert.isNull(BindableChannelPattern.parse('chat/:id/messages/:id'))
    assert.isNull(BindableChannelPattern.parse('chat/:wildcard'))
    assert.isNull(BindableChannelPattern.parse('chat/:wildcard/*'))
    assert.isNull(BindableChannelPattern.parse('teams/:team?/rooms'))
    assert.isNull(BindableChannelPattern.parse('users/:id.json'))
  })

  test('client binding crosses the same validation seam', ({ assert }) => {
    assert.equal(buildChannelName('/chat/:roomId/', { roomId: 'general' }), 'chat/general')
    assert.throws(
      () => buildChannelName('chat/:roomId', { roomId: 'team/general' }),
      'Invalid "roomId" parameter'
    )
    assert.throws(
      () => buildChannelName('teams/:team?/rooms', { team: 'core' }),
      'Unsupported generated channel pattern'
    )
  })
})
