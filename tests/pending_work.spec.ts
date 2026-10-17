import { test } from '@japa/runner'
import { PendingWork } from '../src/pending_work.js'

test.group('PendingWork', () => {
  test('waits for work tracked after an earlier batch settled', async ({ assert }) => {
    const work = new PendingWork()
    work.track(Promise.resolve())
    await work.settled()
    assert.isNull(work.settled())

    const second = Promise.withResolvers<void>()
    work.track(second.promise)
    let settled = false
    const waiting = work.settled()!.then(() => {
      settled = true
    })
    await new Promise((resolve) => setTimeout(resolve, 10))
    assert.isFalse(settled)

    second.resolve()
    await waiting
    assert.isNull(work.settled())
  })
})
