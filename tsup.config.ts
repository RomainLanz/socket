import { defineConfig } from 'tsup'

export default defineConfig({
  entry: [
    './index.ts',
    './providers/socket_provider.ts',
    './src/assembler_hook.ts',
    './services/socket.ts',
    './src/decorators.ts',
    './src/health_check.ts',
    './src/otel.ts',
    './src/testing.ts',
    './src/types.ts',
    './src/types/tracing_channels.ts',
    './src/client/index.ts',
    './src/client/types.ts',
    './src/client/react.ts',
    './src/client/vue.ts',
  ],
  outDir: './build',
  clean: true,
  format: 'esm',
  dts: true,
  sourcemap: true,
  target: 'esnext',
  outExtension() {
    return { js: '.js' }
  },
})
