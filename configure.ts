import type ConfigureCommand from '@adonisjs/core/commands/configure'

/** Configures the Socket provider and Assembler hook in an AdonisJS application. */
export async function configure(command: Pick<ConfigureCommand, 'createCodemods'>): Promise<void> {
  const codemods = await command.createCodemods()

  await codemods.updateRcFile((rcFile) => {
    rcFile.addProvider('@rlanz/socket/provider', ['web'])
    rcFile.addAssemblerHook('init', '@rlanz/socket/assembler_hook')
  })
}
