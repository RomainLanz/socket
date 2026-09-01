import fs from 'node:fs'
import { createRequire } from 'node:module'
import type TypeScript from 'typescript'
import type { CommonHooks } from '@adonisjs/assembler/types'
import type { IndexGeneratorSourceConfig } from '@adonisjs/assembler/types'
import { BindableChannelPattern } from './bindable_channel_pattern.js'

const require = createRequire(import.meta.url)
let ts: typeof TypeScript

export interface SocketAssemblerHookOptions {
  source?: string
  glob?: string[]
  output?: string
}

type Handler = { event: string; method: string }
type Channel = {
  pattern: string
  bindablePattern: BindableChannelPattern
  importPath: string
  handlers: Handler[]
}
type ChannelImports = {
  decoratorImports: Set<string>
  decoratorNamespaces: Set<string>
  baseChannelImports: Set<string>
  socketNamespaces: Set<string>
}
type SocketAssemblerHook = Extract<CommonHooks['init'][number], { run: (...args: any[]) => any }>

function quote(value: string) {
  return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`
}

function generatedImportPath(importPath: string): string {
  return importPath.replace(/\.ts$/, '')
}

function appImportAlias(source: string): string {
  const normalized = source.replace(/^\.\//, '')
  if (normalized === 'app') return '#app'
  if (normalized.startsWith('app/')) return `#app/${normalized.slice('app/'.length)}`
  throw new Error('[socket] Channel source must be inside the app directory')
}

function fail(filePath: string, message: string): never {
  throw new Error(`[socket] Cannot generate client types for ${filePath}: ${message}`)
}

function propertyName(node: TypeScript.PropertyName | undefined): string | undefined {
  if (node && (ts.isIdentifier(node) || ts.isStringLiteral(node))) return node.text
}

function isPublic(method: TypeScript.MethodDeclaration) {
  return !method.modifiers?.some(
    (modifier) =>
      modifier.kind === ts.SyntaxKind.PrivateKeyword ||
      modifier.kind === ts.SyntaxKind.ProtectedKeyword
  )
}

function isStatic(member: TypeScript.ClassElement): boolean {
  return (
    ts.canHaveModifiers(member) &&
    !!ts.getModifiers(member)?.some((modifier) => modifier.kind === ts.SyntaxKind.StaticKeyword)
  )
}

function validateHandlerMethod(
  filePath: string,
  method: TypeScript.MethodDeclaration,
  name: string
): void {
  const parameters = method.parameters.filter(
    (parameter) => !(ts.isIdentifier(parameter.name) && parameter.name.text === 'this')
  )
  if (parameters.some((parameter) => parameter.dotDotDotToken)) {
    fail(filePath, `handler method ${name} must not use rest parameters`)
  }
  if (parameters.slice(2).some((parameter) => !parameter.questionToken && !parameter.initializer)) {
    fail(filePath, `handler method ${name} must not require parameters after the payload`)
  }
}

function collectChannelImports(source: TypeScript.SourceFile): ChannelImports {
  const decoratorImports = new Set<string>()
  const decoratorNamespaces = new Set<string>()
  const baseChannelImports = new Set<string>()
  const socketNamespaces = new Set<string>()

  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier))
      continue

    const module = statement.moduleSpecifier.text
    if (module !== '@rlanz/socket/decorators' && module !== '@rlanz/socket') continue

    const bindings = statement.importClause?.namedBindings
    if (bindings && ts.isNamedImports(bindings)) {
      for (const binding of bindings.elements) {
        const importedName = (binding.propertyName ?? binding.name).text
        if (module === '@rlanz/socket/decorators' && importedName === 'onMessage') {
          decoratorImports.add(binding.name.text)
        }
        if (module === '@rlanz/socket' && importedName === 'BaseChannel') {
          baseChannelImports.add(binding.name.text)
        }
      }
    } else if (bindings && ts.isNamespaceImport(bindings)) {
      if (module === '@rlanz/socket/decorators') {
        decoratorNamespaces.add(bindings.name.text)
      } else {
        socketNamespaces.add(bindings.name.text)
      }
    }
  }

  return { decoratorImports, decoratorNamespaces, baseChannelImports, socketNamespaces }
}

function findChannelClass(
  source: TypeScript.SourceFile,
  filePath: string
): TypeScript.ClassDeclaration {
  const classes = source.statements.filter(ts.isClassDeclaration)
  let channelClass = classes.find((node) =>
    node.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.DefaultKeyword)
  )

  if (!channelClass) {
    const defaultExport = source.statements.find(
      (node): node is TypeScript.ExportAssignment =>
        ts.isExportAssignment(node) && !node.isExportEquals
    )
    if (defaultExport && ts.isIdentifier(defaultExport.expression)) {
      const defaultClassName = defaultExport.expression.text
      channelClass = classes.find((node) => node.name?.text === defaultClassName)
    }
  }
  if (!channelClass) fail(filePath, 'a default-exported channel class was not found')
  if (channelClass.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.AbstractKeyword)) {
    fail(filePath, 'the default-exported channel class must not be abstract')
  }
  return channelClass
}

function directlyExtendsBaseChannel(
  channelClass: TypeScript.ClassDeclaration,
  imports: ChannelImports
): boolean {
  const extendsType = channelClass.heritageClauses
    ?.find((clause) => clause.token === ts.SyntaxKind.ExtendsKeyword)
    ?.types.at(0)
  const extendsExpression = extendsType?.expression
  return !!(
    (extendsExpression &&
      ts.isIdentifier(extendsExpression) &&
      imports.baseChannelImports.has(extendsExpression.text)) ||
    (extendsExpression &&
      ts.isPropertyAccessExpression(extendsExpression) &&
      ts.isIdentifier(extendsExpression.expression) &&
      imports.socketNamespaces.has(extendsExpression.expression.text) &&
      extendsExpression.name.text === 'BaseChannel')
  )
}

function findPatternMember(
  channelClass: TypeScript.ClassDeclaration
): TypeScript.PropertyDeclaration | undefined {
  return channelClass.members.find(
    (member): member is TypeScript.PropertyDeclaration =>
      ts.isPropertyDeclaration(member) &&
      propertyName(member.name) === 'pattern' &&
      !!member.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.StaticKeyword)
  )
}

function onMessageCall(
  decorator: TypeScript.Decorator,
  imports: ChannelImports
): TypeScript.CallExpression | undefined {
  const expression = decorator.expression
  if (!ts.isCallExpression(expression)) return

  const callee = expression.expression
  if (ts.isIdentifier(callee) && imports.decoratorImports.has(callee.text)) return expression
  if (
    ts.isPropertyAccessExpression(callee) &&
    ts.isIdentifier(callee.expression) &&
    imports.decoratorNamespaces.has(callee.expression.text) &&
    callee.name.text === 'onMessage'
  ) {
    return expression
  }
}

function inspectHandlers(
  channelClass: TypeScript.ClassDeclaration,
  filePath: string,
  imports: ChannelImports
): Handler[] {
  const handlers: Handler[] = []
  for (const method of channelClass.members.filter(ts.isMethodDeclaration)) {
    for (const decorator of ts.getDecorators(method) ?? []) {
      const expression = onMessageCall(decorator, imports)
      if (!expression) continue

      if (isStatic(method)) {
        fail(filePath, '@onMessage cannot generate a contract for a static method')
      }

      if (expression.arguments.length !== 1 || !ts.isStringLiteral(expression.arguments[0])) {
        fail(
          filePath,
          `@onMessage on ${propertyName(method.name) ?? '<computed>'} needs a literal event`
        )
      }
      const event = expression.arguments[0].text
      const name = propertyName(method.name)
      if (!name || !ts.isIdentifier(method.name)) fail(filePath, '@onMessage methods must be named')
      if (!isPublic(method)) fail(filePath, `decorated handler method ${name} must be public`)
      validateHandlerMethod(filePath, method, name)
      if (handlers.some((handler) => handler.event === event)) {
        fail(filePath, `event "${event}" is mapped more than once`)
      }
      handlers.push({ event, method: name })
    }
  }
  return handlers
}

function inspectChannel(filePath: string, importPath: string): Channel | string {
  const source = ts.createSourceFile(
    filePath,
    fs.readFileSync(filePath, 'utf8'),
    ts.ScriptTarget.Latest,
    true,
    filePath.endsWith('.js') ? ts.ScriptKind.JS : ts.ScriptKind.TS
  )
  const imports = collectChannelImports(source)
  const channelClass = findChannelClass(source, filePath)

  if (!directlyExtendsBaseChannel(channelClass, imports)) {
    return (
      `Omitted ${importPath}: generated contracts do not support channel inheritance; ` +
      'the default export must directly extend BaseChannel imported from @rlanz/socket.'
    )
  }

  const patternMember = findPatternMember(channelClass)
  if (!patternMember?.initializer || !ts.isStringLiteral(patternMember.initializer)) {
    return `Omitted ${importPath}: static pattern must be a direct string literal.`
  }

  const bindablePattern = BindableChannelPattern.parse(patternMember.initializer.text)
  if (!bindablePattern) {
    return (
      `Omitted ${importPath}: pattern ${quote(patternMember.initializer.text)} is not supported by ` +
      'generated typing; use literals, required params, a final optional param, or a final wildcard.'
    )
  }

  const handlers = inspectHandlers(channelClass, filePath, imports)

  return { pattern: patternMember.initializer.text, bindablePattern, importPath, handlers }
}

/** Registers generation of the application socket registry with AdonisJS Assembler. */
export function generateSocketRegistry(
  options: SocketAssemblerHookOptions = {}
): SocketAssemblerHook {
  return {
    run(_parent, hooks, indexGenerator) {
      ts ??= require('typescript') as typeof TypeScript

      const source = options.source ?? './app/channels'
      const glob = options.glob ?? ['**/*_channel.{ts,js}']
      const importAlias = appImportAlias(source)

      indexGenerator.add('socketChannels', {
        source,
        glob,
        importAlias,
        output: options.output ?? './.adonisjs/client/socket.ts',
        as(vfs, buffer, _config, helpers) {
          const channels: Channel[] = []
          const diagnostics: string[] = []
          for (const filePath of Object.values(vfs.asList())) {
            const importPath = generatedImportPath(helpers.toImportPath(filePath))
            const inspected = inspectChannel(filePath, importPath)
            if (typeof inspected === 'string') diagnostics.push(inspected)
            else channels.push(inspected)
          }

          const patterns = new Set<string>()
          for (const channel of channels) {
            if (patterns.has(channel.bindablePattern.canonicalPattern)) {
              fail(
                channel.importPath,
                `pattern ${quote(channel.pattern)} is declared more than once`
              )
            }
            patterns.add(channel.bindablePattern.canonicalPattern)
          }

          for (const diagnostic of diagnostics) buffer.writeLine(`// [socket] ${diagnostic}`)
          if (diagnostics.length) buffer.writeLine('')
          buffer.writeLine('export interface AppSocket {').indent()
          buffer.writeLine('readonly channels: {').indent()
          for (const channel of channels) {
            buffer.writeLine(`readonly ${quote(channel.pattern)}: {`).indent()
            if (channel.bindablePattern.parameters.length === 0) {
              buffer.writeLine('readonly params: undefined')
            } else {
              buffer.writeLine('readonly params: {').indent()
              for (const parameter of channel.bindablePattern.parameters) {
                buffer.writeLine(
                  `readonly ${quote(parameter.name)}${parameter.optional ? '?' : ''}: string | number`
                )
              }
              buffer.dedent().writeLine('}')
            }
            buffer.writeLine(
              `readonly channel: typeof import(${quote(channel.importPath)}).default`
            )
            buffer.writeLine('readonly handlers: {').indent()
            for (const handler of channel.handlers) {
              buffer.writeLine(`readonly ${quote(handler.event)}: ${quote(handler.method)}`)
            }
            buffer.dedent().writeLine('}')
            buffer.dedent().writeLine('}')
          }
          buffer.dedent().writeLine('}')
          buffer.dedent().writeLine('}')
        },
      } satisfies IndexGeneratorSourceConfig)

      indexGenerator.add('socketServerChannels', {
        source,
        glob,
        importAlias,
        output: './.adonisjs/server/socket_channels.ts',
        as(vfs, buffer, _config, helpers) {
          const filePaths = Object.values(vfs.asList())
          filePaths.forEach((filePath, index) => {
            const importPath = generatedImportPath(helpers.toImportPath(filePath))
            buffer.writeLine(`import Channel${index} from ${quote(importPath)}`)
          })
          if (filePaths.length) buffer.writeLine('')
          buffer.writeLine(
            `export const socketChannels = [${filePaths.map((_filePath, index) => `Channel${index}`).join(', ')}] as const`
          )
        },
      } satisfies IndexGeneratorSourceConfig)

      hooks.add('fileChanged', (_relativePath, absolutePath) => {
        return indexGenerator.addFile(absolutePath)
      })
    },
  }
}

const socketRegistryHook = generateSocketRegistry()
const lazySocketRegistryHook: SocketAssemblerHook['run'] = socketRegistryHook.run

export default lazySocketRegistryHook
