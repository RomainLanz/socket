import { execFile } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'
import { test } from '@japa/runner'

const execFileAsync = promisify(execFile)

test.group('channel declaration emit', () => {
  test('emits externally addressable channel methods and an explicit AppSocket registry', async ({
    assert,
    cleanup,
  }) => {
    const output = await mkdtemp(join(tmpdir(), 'socket-channel-declaration-'))
    cleanup(() => rm(output, { recursive: true, force: true }))

    await execFileAsync(
      process.execPath,
      [
        'node_modules/typescript/bin/tsc',
        '--declaration',
        '--emitDeclarationOnly',
        '--module',
        'NodeNext',
        '--moduleResolution',
        'NodeNext',
        '--target',
        'ESNext',
        '--strict',
        '--experimentalDecorators',
        '--skipLibCheck',
        '--rootDir',
        '.',
        '--outDir',
        output,
        'tests/fixtures/channel_declaration.ts',
      ],
      { cwd: process.cwd() }
    )

    const declaration = await readFile(
      join(output, 'tests/fixtures/channel_declaration.d.ts'),
      'utf8'
    )
    assert.include(declaration, 'export default class DeclarationChannel extends BaseChannel<')
    assert.include(declaration, 'sendMessage(')
    assert.include(declaration, 'export interface AppSocket')
    assert.include(declaration, 'readonly params: {')
    assert.include(declaration, 'readonly roomId: string | number;')
    assert.include(declaration, "readonly 'chat:send': 'sendMessage';")
    assert.notInclude(declaration, '$clientEvents')
  }).timeout(30_000)
})
