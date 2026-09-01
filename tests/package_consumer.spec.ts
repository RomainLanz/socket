import { execFile } from 'node:child_process'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'
import { test } from '@japa/runner'
import sourcePackageJson from '../package.json' with { type: 'json' }

const execFileAsync = promisify(execFile)

const publicExports = Object.keys(sourcePackageJson.exports).map((subpath) =>
  subpath === '.' ? sourcePackageJson.name : `${sourcePackageJson.name}/${subpath.slice(2)}`
)

async function install(directory: string, packages: string[]) {
  await execFileAsync(
    'npm',
    [
      'install',
      '--ignore-scripts',
      '--omit=optional',
      '--no-package-lock',
      '--no-audit',
      '--no-fund',
      ...packages,
    ],
    { cwd: directory }
  )
}

async function writeConsumer(directory: string, source: string, skipLibCheck = false) {
  await writeFile(
    join(directory, 'package.json'),
    JSON.stringify({ private: true, type: 'module' })
  )
  await writeFile(join(directory, 'consumer.ts'), source)
  await writeFile(
    join(directory, 'tsconfig.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ESNext',
        module: 'NodeNext',
        moduleResolution: 'NodeNext',
        strict: true,
        noEmit: true,
        skipLibCheck,
      },
      include: ['consumer.ts'],
    })
  )
}

test.group('packaged consumers', () => {
  test('resolves JavaScript and declarations through every public package export', async ({
    assert,
    cleanup,
  }) => {
    const staging = await mkdtemp(join(tmpdir(), 'socket-package-'))
    cleanup(() => rm(staging, { recursive: true, force: true }))

    await execFileAsync('corepack', ['yarn@4.18.0', 'build'], { cwd: process.cwd() })
    const { stdout } = await execFileAsync(
      'npm',
      ['pack', '--json', '--ignore-scripts', '--pack-destination', staging],
      { cwd: process.cwd() }
    )
    const [{ filename }] = JSON.parse(stdout) as Array<{ filename: string }>
    const tarball = join(staging, filename)

    const plainClient = join(staging, 'plain-client')
    await mkdir(plainClient)
    await writeConsumer(
      plainClient,
      `import { Socket } from '@rlanz/socket/client'
import type { ChannelAck, SocketOptions, SubscribeResult } from '@rlanz/socket/client/types'

const options: SocketOptions = { autoReconnect: false }
const success: ChannelAck<number> = { ok: true, data: 1 }
const failure: SubscribeResult = { ok: false, error: 'failed' }
// @ts-expect-error success cannot contain an error
const contradictorySuccess: ChannelAck = { ok: true, error: 'failed' }
// @ts-expect-error failure requires an error
const missingError: SubscribeResult = { ok: false }

void [new Socket(options), success, failure, contradictorySuccess, missingError]
`
    )
    await install(plainClient, [tarball, 'typescript@^5.9.0'])
    await execFileAsync('npx', ['tsc', '--project', 'tsconfig.json'], { cwd: plainClient })

    for (const consumer of [
      {
        name: 'react-client',
        source:
          "import { createSocketHooks } from '@rlanz/socket/client/react'\nvoid createSocketHooks()\n",
        packages: ['typescript@^5.9.0', 'react@^19.0.0', '@types/react@^19.0.0'],
      },
      {
        name: 'vue-client',
        source:
          "import { createSocketComposables } from '@rlanz/socket/client/vue'\nvoid createSocketComposables()\n",
        packages: ['typescript@^5.9.0', 'vue@^3.3.0'],
      },
    ]) {
      const directory = join(staging, consumer.name)
      await mkdir(directory)
      await writeConsumer(directory, consumer.source)
      await install(directory, [tarball, ...consumer.packages])
      await execFileAsync('npx', ['tsc', '--project', 'tsconfig.json'], { cwd: directory })
    }

    const completeConsumer = join(staging, 'complete-consumer')
    await mkdir(completeConsumer)
    await writeConsumer(
      completeConsumer,
      publicExports
        .map(
          (specifier) =>
            `type Export${publicExports.indexOf(specifier)} = typeof import('${specifier}')`
        )
        .join('\n'),
      true
    )
    await install(completeConsumer, [
      tarball,
      'typescript@^5.9.0',
      '@adonisjs/assembler@^8.0.0',
      '@adonisjs/core@^7.0.1',
      '@opentelemetry/api@^1.9.0',
      '@opentelemetry/instrumentation@^0.200.0',
      'react@^19.0.0',
      '@types/react@^19.0.0',
      '@types/node@^24.0.0',
      '@types/ws@^8.18.0',
      'vue@^3.3.0',
    ])
    await execFileAsync('npx', ['tsc', '--project', 'tsconfig.json'], { cwd: completeConsumer })
    await writeFile(
      join(completeConsumer, 'consumer.js'),
      `${publicExports.map((specifier) => `import.meta.resolve('${specifier}')`).join('\n')}\n${publicExports
        .filter((specifier) => specifier !== '@rlanz/socket/services/main')
        .map((specifier) => `await import('${specifier}')`)
        .join('\n')}\n`
    )
    await execFileAsync(process.execPath, ['consumer.js'], { cwd: completeConsumer })

    const packageJson = JSON.parse(
      await readFile(
        join(completeConsumer, 'node_modules', '@rlanz', 'socket', 'package.json'),
        'utf8'
      )
    )
    assert.equal(packageJson.packageManager, 'yarn@4.18.0')
    assert.notProperty(packageJson.peerDependencies, '@opentelemetry/core')
    assert.notProperty(packageJson.peerDependenciesMeta, '@opentelemetry/core')
  }).timeout(120_000)
})
