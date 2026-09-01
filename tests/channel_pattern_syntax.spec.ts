import { test } from '@japa/runner'
import { ChannelPatternSyntax } from '../src/channel_pattern_syntax.js'

test.group('ChannelPatternSyntax', () => {
  test('is an immutable parsed value with matching behavior', ({ assert }) => {
    const syntax = ChannelPatternSyntax.from('/users/:id.json/')

    assert.equal(syntax.canonicalPattern, 'users/:id.json')
    assert.isTrue(Object.isFrozen(syntax))
    assert.deepEqual(syntax.match('users/42.json'), { id: '42' })
  })

  test('compares specificity without exposing parsed segments', ({ assert }) => {
    const staticPattern = ChannelPatternSyntax.from('chat/general')
    const parameterPattern = ChannelPatternSyntax.from('chat/:roomId')
    const optionalPattern = ChannelPatternSyntax.from('chat/:roomId?')
    const wildcardPattern = ChannelPatternSyntax.from('chat/*')

    assert.isBelow(staticPattern.compareSpecificity(parameterPattern), 0)
    assert.isBelow(parameterPattern.compareSpecificity(optionalPattern), 0)
    assert.isBelow(optionalPattern.compareSpecificity(wildcardPattern), 0)
  })

  test('selects the first matching parsed value', ({ assert }) => {
    const matched = ChannelPatternSyntax.firstMatch(
      [ChannelPatternSyntax.from('chat/general'), ChannelPatternSyntax.from('chat/:roomId')],
      'chat/general'
    )

    assert.deepEqual(matched, { index: 0, params: {} })
  })
})
