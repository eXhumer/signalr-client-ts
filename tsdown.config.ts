import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: {
    index: 'src/index.ts',
  },
  format: ['esm', 'cjs'],
  platform: 'node',
  target: 'node18',
  sourcemap: true,
  dts: {
    sourcemap: true,
    // tough-cookie v6 ships separate ESM (.d.ts) and CJS (.d.cts)
    // declarations with incompatible private fields. Pinning both builds to
    // the CJS declaration avoids that dual-package type conflict.
    compilerOptions: {
      paths: {
        'tough-cookie': ['./node_modules/tough-cookie/dist/index.d.cts'],
      },
    },
  },
})
