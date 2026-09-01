import { test } from '@japa/runner'
import { configure } from '../index.js'

test.group('configure', () => {
  test('registers the provider for web only and installs the assembler hook', async ({
    assert,
  }) => {
    const providers: Array<{ path: string; environments?: string[] }> = []
    const hooks: Array<{ name: string; path: string }> = []

    await configure({
      async createCodemods() {
        return {
          async updateRcFile(callback: (rcFile: any) => void) {
            callback({
              addProvider(path: string, environments?: string[]) {
                providers.push({ path, environments })
              },
              addAssemblerHook(name: string, path: string) {
                hooks.push({ name, path })
              },
            })
          },
        }
      },
    } as unknown as Parameters<typeof configure>[0])

    assert.deepEqual(providers, [{ path: '@rlanz/socket/provider', environments: ['web'] }])
    assert.deepEqual(hooks, [{ name: 'init', path: '@rlanz/socket/assembler_hook' }])
  })
})
