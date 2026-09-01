import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import ts from 'typescript'
import { test } from '@japa/runner'
import { generateSocketRegistry } from '../src/assembler_hook.js'

class Buffer {
  #lines: string[] = []
  #indent = 0

  writeLine(value: string) {
    this.#lines.push(`${'  '.repeat(this.#indent)}${value}`)
    return this
  }

  indent() {
    this.#indent++
    return this
  }

  dedent() {
    this.#indent--
    return this
  }

  toString() {
    return this.#lines.join('\n')
  }
}

function generate(files: { name: string; source: string }[]) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'socket-assembler-'))
  const paths = files.map((file) => {
    const filePath = path.join(root, file.name)
    fs.mkdirSync(path.dirname(filePath), { recursive: true })
    const source = file.source.replace(
      /export default class ([A-Za-z_$][\w$]*)(\s*)\{/,
      `export default class $1 extends BaseChannel$2{`
    )
    fs.writeFileSync(filePath, `import { BaseChannel } from '@rlanz/socket'\n${source}`)
    return filePath
  })
  const configs = new Map<string, any>()
  const hook = generateSocketRegistry() as { run: (...args: any[]) => void }
  hook.run(
    null,
    { add() {} },
    {
      add(name: string, value: unknown) {
        configs.set(name, value)
      },
      async addFile() {},
    }
  )
  const config = configs.get('socketChannels')
  const buffer = new Buffer()
  config.as(
    { asList: () => Object.fromEntries(paths.map((filePath, index) => [String(index), filePath])) },
    buffer,
    config,
    { toImportPath: (filePath: string) => `#app/channels/${path.basename(filePath, '.ts')}` }
  )
  fs.rmSync(root, { recursive: true })
  return buffer.toString()
}

test.group('assembler hook', () => {
  test('exports a lazy-loadable Assembler init hook', async ({ assert }) => {
    const { default: assemblerHook } = await import('../src/assembler_hook.js')
    const configs = new Map<string, unknown>()
    const registeredHooks: string[] = []

    assert.isFunction(generateSocketRegistry().run)
    assemblerHook(
      null as never,
      {
        add(name: string) {
          registeredHooks.push(name)
        },
      } as never,
      {
        add(name: string, config: unknown) {
          configs.set(name, config)
        },
        async addFile() {},
      } as never
    )

    assert.deepEqual([...configs.keys()], ['socketChannels', 'socketServerChannels'])
    assert.deepEqual(registeredHooks, ['fileChanged'])
  })

  test('generates a channel map from decorated handlers', ({ assert }) => {
    const output = generate([
      {
        name: 'chat_channel.ts',
        source: `
          import { onMessage as socketMessage } from '@rlanz/socket/decorators'
          export default class ChatChannel {
            static pattern = 'chat/:roomId'
            @socketMessage('chat:send')
            sendMessage(socket: unknown, payload: { body: string }) {}
          }
        `,
      },
      {
        name: 'alerts_channel.ts',
        source: `
          import { onMessage as socketMessage } from '@rlanz/socket/decorators'
          export default class AlertsChannel {
            static pattern = 'alerts'
            @socketMessage('alerts:read')
            read(socket: unknown, payload: { id: number }) {}
          }
        `,
      },
      {
        name: 'threads_channel.ts',
        source: `export default class ThreadsChannel { static pattern = 'threads/:threadId?' }`,
      },
      {
        name: 'files_channel.ts',
        source: `export default class FilesChannel { static pattern = 'files/*' }`,
      },
    ])

    assert.include(output, `readonly 'chat/:roomId': {`)
    assert.include(output, `readonly 'roomId': string | number`)
    assert.include(output, `readonly params: undefined`)
    assert.include(output, `readonly 'threadId'?: string | number`)
    assert.include(output, `readonly 'wildcard': string | number`)
    assert.include(output, `readonly 'chat:send': 'sendMessage'`)
    assert.include(output, `readonly 'alerts:read': 'read'`)
    assert.isBelow(
      output.indexOf('#app/channels/chat_channel'),
      output.indexOf('#app/channels/alerts_channel')
    )
    assert.include(output, `readonly channel: typeof import('#app/channels/chat_channel').default`)
    const generatedSource = ts.createSourceFile(
      'socket.ts',
      output,
      ts.ScriptTarget.Latest,
      true
    ) as ts.SourceFile & { parseDiagnostics: readonly ts.Diagnostic[] }
    assert.lengthOf(generatedSource.parseDiagnostics, 0)
  })

  test('registers customizable application discovery and output conventions', ({ assert }) => {
    const configs = new Map<string, any>()
    const hook = generateSocketRegistry({
      source: './app/realtime',
      glob: ['**/*.socket.ts'],
      output: '.adonisjs/socket.ts',
    }) as { run: (...args: any[]) => void }

    hook.run(
      null,
      { add() {} },
      {
        add(name: string, value: unknown) {
          configs.set(name, value)
        },
        async addFile() {},
      }
    )

    const clientConfig = configs.get('socketChannels')
    const serverConfig = configs.get('socketServerChannels')
    assert.equal(clientConfig.source, './app/realtime')
    assert.deepEqual(clientConfig.glob, ['**/*.socket.ts'])
    assert.equal(clientConfig.output, '.adonisjs/socket.ts')
    assert.equal(clientConfig.importAlias, '#app/realtime')
    assert.equal(serverConfig.source, './app/realtime')
    assert.deepEqual(serverConfig.glob, ['**/*.socket.ts'])
    assert.equal(serverConfig.output, './.adonisjs/server/socket_channels.ts')
    assert.equal(serverConfig.importAlias, '#app/realtime')
  })

  test('rejects channel discovery outside the application directory', ({ assert }) => {
    assert.throws(
      () =>
        generateSocketRegistry({ source: './realtime' }).run(
          null as never,
          { add() {} } as never,
          { add() {}, async addFile() {} } as never
        ),
      '[socket] Channel source must be inside the app directory'
    )
  })

  test('generates the runtime manifest from the same channel files', ({ assert }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'socket-assembler-server-'))
    const filePath = path.join(root, 'chat_channel.ts')
    fs.writeFileSync(filePath, 'export default class ChatChannel {}')
    const configs = new Map<string, any>()
    const hook = generateSocketRegistry() as { run: (...args: any[]) => void }
    hook.run(
      null,
      { add() {} },
      {
        add(name: string, value: unknown) {
          configs.set(name, value)
        },
        async addFile() {},
      }
    )

    const buffer = new Buffer()
    configs
      .get('socketServerChannels')
      .as({ asList: () => ({ chat: filePath }) }, buffer, configs.get('socketServerChannels'), {
        toImportPath: () => '#app/channels/chat_channel.ts',
      })
    fs.rmSync(root, { recursive: true })

    assert.include(buffer.toString(), `import Channel0 from '#app/channels/chat_channel'`)
    assert.include(buffer.toString(), 'export const socketChannels = [Channel0] as const')
  })

  test('regenerates indexes when an existing channel file changes', async ({ assert }) => {
    let fileChanged: ((_relativePath: string, absolutePath: string) => Promise<void>) | undefined
    const regenerated: string[] = []
    const hook = generateSocketRegistry() as { run: (...args: any[]) => void }

    hook.run(
      null,
      {
        add(name: string, callback: typeof fileChanged) {
          if (name === 'fileChanged') fileChanged = callback
        },
      },
      {
        add() {},
        async addFile(filePath: string) {
          regenerated.push(filePath)
        },
      }
    )

    await fileChanged?.('app/channels/chat_channel.ts', '/app/channels/chat_channel.ts')
    assert.deepEqual(regenerated, ['/app/channels/chat_channel.ts'])
  })

  test('generates a certain registry for the literal root pattern', ({ assert }) => {
    const output = generate([
      {
        name: 'root_channel.ts',
        source: `export default class RootChannel { static pattern = '/' }`,
      },
    ])

    assert.include(output, `readonly '/': {`)
  })

  test('semantically compiles handler references for short and optional handlers', ({ assert }) => {
    const output = generate([
      {
        name: 'payloads_channel.ts',
        source: `
          export default class PayloadsChannel {
            static pattern = 'payloads'
            @onMessage('zero')
            zero() {}
            @onMessage('one')
            one(socket: unknown) {}
            @onMessage('two')
            two(socket: unknown, payload: { body: string }) {}
            @onMessage('optional')
            optional(socket: unknown, payload?: { body: string }) {}
          }
        `.replace(
          'export default class PayloadsChannel',
          `import { onMessage } from '@rlanz/socket/decorators'\nexport default class PayloadsChannel`
        ),
      },
    ]).replaceAll(`'#app/channels/payloads_channel'`, `'./channel.js'`)

    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'socket-generated-types-'))
    try {
      fs.writeFileSync(path.join(root, 'socket.ts'), output)
      fs.writeFileSync(
        path.join(root, 'channel.d.ts'),
        `declare class PayloadsChannel {
          zero(): void
          one(socket: unknown): void
          two(socket: unknown, payload: { body: string }): void
          optional(socket: unknown, payload?: { body: string }): void
        }
        export default PayloadsChannel`
      )

      const program = ts.createProgram([path.join(root, 'socket.ts')], {
        module: ts.ModuleKind.NodeNext,
        moduleResolution: ts.ModuleResolutionKind.NodeNext,
        noEmit: true,
        strict: true,
        skipLibCheck: true,
        target: ts.ScriptTarget.ESNext,
      })
      const diagnostics = ts.getPreEmitDiagnostics(program)
      assert.deepEqual(
        diagnostics.map((diagnostic) =>
          ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n')
        ),
        []
      )
    } finally {
      fs.rmSync(root, { recursive: true })
    }
  })

  for (const pattern of ['users/:id.json', 'users/:id?.json', 'users/:id.json?']) {
    test(`omits unsupported Matchit parameter suffix ${pattern}`, ({ assert }) => {
      const output = generate([
        {
          name: 'suffix_channel.ts',
          source: `export default class SuffixChannel { static pattern = '${pattern}' }`,
        },
      ])

      assert.include(output, 'is not supported by generated typing')
      assert.notInclude(output, `readonly pattern: '${pattern}'`)
    })
  }

  test('omits channels with an intermediate base class', ({ assert }) => {
    const output = generate([
      {
        name: 'inherited_channel.ts',
        source: `
          class SharedChannel extends BaseChannel {}
          export default class InheritedChannel extends SharedChannel {
            static pattern = 'inherited'
          }
        `,
      },
    ])

    assert.include(output, 'generated contracts do not support channel inheritance')
    assert.notInclude(output, `readonly pattern: 'inherited'`)
  })

  test('omits a dynamic pattern with a generated diagnostic', ({ assert }) => {
    const output = generate([
      {
        name: 'dynamic_channel.ts',
        source: `export default class DynamicChannel { static pattern = getPattern() }`,
      },
    ])

    assert.include(output, '// [socket] Omitted #app/channels/dynamic_channel')
    assert.notInclude(output, 'readonly channel:')
  })

  test('omits patterns unsupported by generated matching without restricting runtime routing', ({
    assert,
  }) => {
    const output = generate([
      {
        name: 'dynamic_channel.ts',
        source: `export default class DynamicChannel { static pattern = 'teams/:team?/rooms' }`,
      },
    ])

    assert.include(output, 'is not supported by generated typing')
    assert.notInclude(output, 'readonly channel:')
  })

  test('rejects a dynamic decorator event', ({ assert }) => {
    assert.throws(
      () =>
        generate([
          {
            name: 'invalid_channel.ts',
            source: `import { onMessage } from '@rlanz/socket/decorators'
            export default class InvalidChannel {
              static pattern = 'invalid'
              @onMessage(event) send() {}
            }`,
          },
        ]),
      'needs a literal event'
    )
  })

  test('rejects abstract channels', ({ assert }) => {
    assert.throws(
      () =>
        generate([
          {
            name: 'abstract_channel.ts',
            source: `export default abstract class AbstractChannel extends BaseChannel {
              static pattern = 'abstract'
              abstract send(socket: unknown, payload: unknown): void
            }`,
          },
        ]),
      'must not be abstract'
    )
  })

  for (const [name, source, error] of [
    [
      'decorated handler with a third required parameter',
      `@onMessage('send') send(socket: unknown, payload: unknown, required: unknown) {}`,
      'must not require parameters after the payload',
    ],
    [
      'decorated handler with tuple rest parameters',
      `@onMessage('send') send(...args: [unknown, unknown, unknown]) {}`,
      'must not use rest parameters',
    ],
  ]) {
    test(`rejects ${name}`, ({ assert }) => {
      assert.throws(
        () =>
          generate([
            {
              name: 'invalid_signature_channel.ts',
              source: `import { onMessage } from '@rlanz/socket/decorators'
                export default class InvalidSignatureChannel {
                  static pattern = 'invalid'
                  ${source}
                }`,
            },
          ]),
        error
      )
    })
  }

  test('ignores unrelated decorators named onMessage', ({ assert }) => {
    const output = generate([
      {
        name: 'unrelated_channel.ts',
        source: `
          import { onMessage } from 'another-package'
          export default class UnrelatedChannel {
            static pattern = 'unrelated'
            @onMessage('not-a-socket-event') send() {}
          }
        `,
      },
    ])

    assert.notInclude(output, 'not-a-socket-event')
  })

  test('rejects static socket decorators', ({ assert }) => {
    assert.throws(
      () =>
        generate([
          {
            name: 'static_channel.ts',
            source: `
              import * as socket from '@rlanz/socket/decorators'
              export default class StaticChannel {
                static pattern = 'static'
                @socket.onMessage('send') static send() {}
              }
            `,
          },
        ]),
      'static method'
    )
  })

  test('rejects duplicate channel patterns', ({ assert }) => {
    assert.throws(
      () =>
        generate([
          {
            name: 'first_channel.ts',
            source: `export default class FirstChannel { static pattern = 'duplicate' }`,
          },
          {
            name: 'second_channel.ts',
            source: `export default class SecondChannel { static pattern = 'duplicate' }`,
          },
        ]),
      "pattern 'duplicate' is declared more than once"
    )
  })

  test('rejects duplicate normalized channel patterns', ({ assert }) => {
    assert.throws(
      () =>
        generate([
          {
            name: 'first_channel.ts',
            source: `export default class FirstChannel { static pattern = '/chat/:id/' }`,
          },
          {
            name: 'second_channel.ts',
            source: `export default class SecondChannel { static pattern = 'chat/:id' }`,
          },
        ]),
      "pattern 'chat/:id' is declared more than once"
    )
  })
})
