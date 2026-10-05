import { test } from '@japa/runner'
import { configure } from '../index.js'

test.group('configure', () => {
  test('registers the provider, the assembler hook, and the channels directory', async ({
    assert,
  }) => {
    const calls: unknown[][] = []

    await configure({
      async createCodemods() {
        return {
          async updateRcFile(callback: (rcFile: any) => void) {
            callback({
              addProvider(path: string, environments?: string[]) {
                calls.push(['addProvider', path, environments])
              },
              addAssemblerHook(name: string, path: string) {
                calls.push(['addAssemblerHook', name, path])
              },
            })
          },
          async addImportAlias(alias: string, target: string) {
            calls.push(['addImportAlias', alias, target])
          },
          async createDirectory(directory: string) {
            calls.push(['createDirectory', directory])
          },
        }
      },
    } as unknown as Parameters<typeof configure>[0])

    assert.deepEqual(calls, [
      ['addProvider', '@rlanz/socket/provider', ['web']],
      ['addAssemblerHook', 'init', '@rlanz/socket/assembler_hook'],
      ['addImportAlias', '#channels/*', './app/channels/*.js'],
      ['createDirectory', 'app/channels'],
    ])
  })
})
