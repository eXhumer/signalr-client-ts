/**
 * pipeline-else-branch.test.ts
 *
 * Covers the FALSE branch of `if (!resolved)` inside PipelineHttpClient.stream()'s
 * undiciPipeline handler (source line 586).
 *
 * In normal usage, undici calls the handler exactly once, so `resolved` is always
 * false when the handler fires.  The else branch (handler called while resolved is
 * already true) is a defensive guard that can only be reached by invoking the
 * captured handler function a second time — which we do here by replacing undici's
 * `pipeline` export with a vi.fn() that lets us hold on to the handler reference.
 *
 * vi.mock() is hoisted before all static imports so it intercepts the pipeline
 * import inside http-client.ts before any module code runs.
 */

import { describe, it, expect, vi, afterEach } from 'vitest';
import { PassThrough }                          from 'node:stream';

// ── Intercept undici.pipeline before anything imports it ─────────────────────
vi.mock('undici', async (importOriginal) => {
  const actual = await importOriginal<typeof import('undici')>();
  return { ...actual, pipeline: vi.fn() };
});

// ── Import AFTER the mock is in place ─────────────────────────────────────────
import { PipelineHttpClient } from '../../src/http-client.js';
import { pipeline as pipelineMock } from 'undici';

// ── Handler type expected by undiciPipeline ───────────────────────────────────
type PipelineHandler = (resp: {
  statusCode: number;
  headers:    Record<string, string | string[]>;
  body:       PassThrough;
}) => PassThrough;

const mockPipeline = vi.mocked(pipelineMock);

// ─────────────────────────────────────────────────────────────────────────────

describe('PipelineHttpClient.stream - else branch of !resolved (line 586)', () => {
  afterEach(() => {
    vi.resetAllMocks();
  });

  it('second handler invocation is silently ignored (covers else branch)', async () => {
    // Set up a fake Duplex (PassThrough satisfies all Duplex methods used by
    // the production code: .on(), .write(), .end(), .destroy())
    const fakeDuplex = new PassThrough();
    let capturedHandler: PipelineHandler | null = null;

    // The mock captures the handler without calling it, and returns fakeDuplex
    // so that the production code can attach listeners and call end().
    mockPipeline.mockImplementation((_url, _opts, handler) => {
      capturedHandler = handler as unknown as PipelineHandler;
      return fakeDuplex as unknown as ReturnType<typeof pipelineMock>;
    });

    const client = new PipelineHttpClient({ timeout: 5_000 });

    // client.stream() executes the Promise executor synchronously:
    //   1. calls mockPipeline → captures handler, returns fakeDuplex
    //   2. attaches duplex.on('error', ...) listener
    //   3. calls duplex.end()  (no body on a GET)
    //   4. suspends at `await` — Promise is still pending
    const streamPromise = client.stream('GET', 'http://example.com/test');

    // The Promise executor ran synchronously, so capturedHandler is set now.
    expect(capturedHandler).not.toBeNull();

    const fakeBody = new PassThrough();

    // ── First invocation: resolved = false → TRUE branch ──────────────────
    // Sets resolved = true and resolves the Promise with a StreamResult.
    capturedHandler!({ statusCode: 200, headers: {}, body: fakeBody });

    const result = await streamPromise;
    expect(result.statusCode).toBe(200);

    // ── Second invocation: resolved = true → FALSE branch (else) ──────────
    // The production code's `if (!resolved)` is false, so the block is skipped.
    // This is the branch Istanbul marks as uncovered without this test.
    // It must complete without throwing and without resolving/rejecting the
    // (already settled) Promise.
    expect(() => {
      capturedHandler!({ statusCode: 200, headers: {}, body: fakeBody });
    }).not.toThrow();

    // Clean up
    fakeDuplex.destroy();
    fakeBody.destroy();
  });
});
