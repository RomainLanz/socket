import { test } from '@japa/runner'
import { AppFactory } from '@adonisjs/core/factories/app'
import { setApp } from '@adonisjs/core/services/app'
import type { ContainerBindings } from '@adonisjs/core/types'
import type { AppEnvironments } from '@adonisjs/core/types/app'
import SocketProvider from '../providers/socket_provider.js'
import { configure } from '../index.js'

async function configuredProviderEnvironments(): Promise<AppEnvironments[]> {
  let environments: AppEnvironments[] = []

  await configure({
    async createCodemods() {
      return {
        async updateRcFile(callback: (rcFile: any) => void) {
          callback({
            addProvider(_path: string, providerEnvironments: AppEnvironments[]) {
              environments = providerEnvironments
            },
            addAssemblerHook() {},
          })
        },
        async addImportAlias() {},
        async createDirectory() {},
      }
    },
  } as unknown as Parameters<typeof configure>[0])

  return environments
}

test.group('services/main', () => {
  test('resolves the socket service when the app runs tests', async ({ assert, cleanup }) => {
    const warnings: string[] = []
    const logger = {
      child() {
        return this
      },
      info() {},
      warn(message: string) {
        warnings.push(message)
      },
    }

    const app = new AppFactory<ContainerBindings>()
      .merge({ environment: 'test', importer: async () => ({ socketChannels: [] }) })
      .create(new URL('../', import.meta.url))
    app.rcContents({
      providers: [
        {
          file: async () => ({ default: SocketProvider }),
          environment: await configuredProviderEnvironments(),
        },
      ],
    })

    await app.init()
    app.useConfig({ socket: {} })
    app.container.bindValue('logger', logger as any)
    app.container.bindValue('server', { getNodeServer: () => undefined } as any)
    await app.boot()
    await app.start(async () => {})
    cleanup(() => app.terminate())
    setApp(app)

    const { default: socket } = await import('../services/socket.js')

    socket.broadcast('maintenance', { active: true })
    const fake = socket.fake()
    socket.to('chat/general').emit('chat:message', { text: 'Hello' })
    socket.restore()

    fake.assertEmittedTo('chat/general', 'chat:message', { data: { text: 'Hello' } })
    fake.assertNotBroadcasted('maintenance')
    assert.deepEqual(warnings, ['HTTP server not available; socket server was not started'])
  })
})
