import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: {
    // tough-cookie v6 ships separate ESM (.d.ts) and CJS (.d.cts) declarations
    // that have incompatible private fields (dual-package hazard).  Pinning to
    // the .d.cts file via tsconfig.typecheck.json's `paths` override ensures
    // both contexts resolve the same declaration and eliminates the conflict.
    // All runtime deps are marked external so the path redirect is invisible
    // to esbuild's JS bundler.
    compilerOptions: {
      paths: {
        'tough-cookie': ['./node_modules/tough-cookie/dist/index.d.cts'],
      },
    },
  },
  sourcemap: true,
  platform: 'node',
  target: 'node18',
  clean: true,
});
