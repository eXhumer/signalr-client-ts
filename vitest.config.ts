import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    /**
     * Run in Node.js - no browser / jsdom needed.
     */
    environment: 'node',

    /**
     * Glob that picks up every test file.
     */
    include: ['tests/**/*.test.ts'],

    /**
     * Individual test timeout (ms).  Transport tests spin up real TCP servers
     * and do real HTTP/WebSocket handshakes, so 30 s is comfortable.
     */
    testTimeout: 30_000,

    /**
     * beforeAll / afterAll hook timeout (ms).  Server startup can take a
     * moment, so give it the same headroom as individual tests.
     */
    hookTimeout: 30_000,
  },

  benchmark: {
    /**
     * Glob that picks up every benchmark file.
     * Run with: npx vitest bench
     */
    include: ['tests/bench/**/*.bench.ts'],

    /**
     * Timeout per benchmark group (ms).  Network-round-trip benches
     * iterate hundreds of times against a local server, so give them room.
     */
    hookTimeout: 60_000,
  },
});
