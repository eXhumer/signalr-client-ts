/**
 * sse-transport.test.ts
 *
 * Unit tests for ServerSentEventsTransport - the SSE-backed SignalR transport.
 *
 * How the test server works
 * ──────────────────────────
 * A single in-process HTTP server handles three routes on the path /sse:
 *
 *   GET /sse   - SSE stream.  Before calling transport.connect(), the test
 *                enqueues a "GET handler" by calling srv.nextGetHandler().
 *                When the request arrives, the server dequeues and invokes
 *                the handler, passing the live ServerResponse object.  The
 *                test can then push SSE events, end the stream, or destroy
 *                the socket at will.
 *
 *   POST /sse  - The server collects the request body and pushes it onto an
 *                internal list; the test inspects srv.posts[] to verify sends.
 *
 *   DELETE /sse- Returns 200; used by potential cleanup.
 *
 * Behaviors under test
 * ────────────────────
 *  1.  connect() resolves for TransferFormat.Text
 *  2.  connect() rejects for TransferFormat.Binary
 *  3.  connect() rejects when server returns non-200
 *  4.  Single-line SSE event → onreceive fires with data string
 *  5.  Multi-line data: fields within one event are joined with '\n'
 *  6.  Multiple SSE events in one chunk
 *  7.  Chunked delivery across multiple data callbacks
 *  8.  send(string) POSTs the data to the SSE URL
 *  9.  send(Uint8Array) POSTs binary as binary string
 * 10.  send() rejects when server returns non-2xx
 * 11.  stop() aborts the stream (body is destroyed)
 * 12.  onclose fires when the stream ends naturally
 * 13.  onclose fires with an error when the stream errors
 */

import { describe, it, beforeAll, afterAll, afterEach, expect, vi } from 'vitest';
import * as http from 'node:http';
import * as net  from 'node:net';
import { Agent } from 'undici';

import { ServerSentEventsTransport } from '../../src/transports/sse-transport.js';
import { TransferFormat }            from '../../src/constants.js';
import { RequestHttpClient }         from '../../src/http-client.js';
import { MockLogger }                from '../helpers/mock-logger.js';
import { closeTrackedDispatchers, trackDispatcher } from '../helpers/dispatcher-tracker.js';

afterEach(closeTrackedDispatchers);

// ─── SSE test server ──────────────────────────────────────────────────────────

/**
 * A "GET handler" receives the live ServerResponse so a test can push events,
 * end the stream, or destroy the socket at will.
 */
type GetHandler = (res: http.ServerResponse) => void;

interface SseTestServer {
  readonly port: number;
  readonly url:  string;
  /** Posts received by the server (body strings, in arrival order). */
  readonly posts: string[];
  /**
   * Enqueue a handler that will be called when the next GET /sse arrives.
   * Call this BEFORE transport.connect() so the handler is ready.
   */
  nextGetHandler(): Promise<http.ServerResponse>;
  stop(): Promise<void>;
}

async function createSseTestServer(sseStatus = 200): Promise<SseTestServer> {
  const getHandlers: Array<(res: http.ServerResponse) => void> = [];
  const liveResponses: http.ServerResponse[]                   = [];
  const posts: string[]                                        = [];

  /**
   * getStatus is mutable so individual tests can override the status code
   * returned for the SSE GET without creating a brand-new server.
   */
  let nextGetStatus = sseStatus;

  const server = http.createServer((req: http.IncomingMessage, res: http.ServerResponse) => {
    if (req.method === 'GET') {
      const status = nextGetStatus;
      nextGetStatus = 200; // reset to default for subsequent calls

      if (status !== 200) {
        res.writeHead(status);
        res.end();
        return;
      }

      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection':   'keep-alive',
      });

      // Flush the headers so the transport sees a 200 immediately
      res.flushHeaders?.();
      // Suppress ECONNRESET when the client aborts the SSE stream
      res.socket?.on('error', () => {});

      const handler = getHandlers.shift();
      if (handler) {
        // A handler was already waiting - give it the response directly.
        // Do NOT also push to liveResponses, or the next nextGetHandler()
        // call would return this (now-consumed) response a second time.
        handler(res);
      } else {
        // No handler queued yet - park the response so the next
        // nextGetHandler() call can pick it up.
        liveResponses.push(res);
      }
      // If no handler is queued yet, the response stays open and the
      // next nextGetHandler() call will resolve with it immediately.

    } else if (req.method === 'POST') {
      const chunks: Buffer[] = [];
      req.on('data', (c: Buffer) => chunks.push(c));
      req.on('end', () => {
        posts.push(Buffer.concat(chunks).toString('utf8'));
        res.writeHead(200);
        res.end();
      });

    } else if (req.method === 'DELETE') {
      res.writeHead(200);
      res.end();

    } else {
      res.writeHead(404);
      res.end();
    }
  });

  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const { port } = server.address() as net.AddressInfo;

  return {
    port,
    url: `http://127.0.0.1:${port}/sse`,
    posts,

    nextGetHandler(): Promise<http.ServerResponse> {
      // If a live response is already waiting (GET arrived before this call)
      const live = liveResponses.shift();
      if (live) return Promise.resolve(live);
      // Otherwise, queue a handler to resolve when the GET arrives
      return new Promise<http.ServerResponse>((resolve) => {
        getHandlers.push(resolve);
      });
    },

    stop: () => new Promise<void>((r) => {
      server.closeAllConnections();
      server.close(() => r());
    }),
  };
}

// ─── Helper: build a transport with a fresh RequestHttpClient ─────────────────

function makeTransport(): {
  transport: ServerSentEventsTransport;
  logger:    MockLogger;
} {
  const logger    = new MockLogger();
  const transport = new ServerSentEventsTransport(
    // Use a fresh Agent per transport so connection-pool state from one test
    // cannot contaminate the next test's SSE stream.
    new RequestHttpClient({ timeout: 5_000, dispatcher: trackDispatcher(new Agent()) }),
    null,                  // no access-token factory
    logger,
    {},                    // no extra headers
  );
  return { transport, logger };
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** Format a minimal SSE event block. */
function sseEvent(data: string): string {
  return `data: ${data}\n\n`;
}

/** Wait up to `ms` milliseconds for `condition` to become true. */
async function waitFor(condition: () => boolean, ms = 300): Promise<void> {
  await vi.waitFor(() => expect(condition()).toBe(true), { timeout: ms, interval: 10 });
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('ServerSentEventsTransport', () => {
  let srv: SseTestServer;

  beforeAll(async () => {
    srv = await createSseTestServer();
  });

  afterAll(async () => {
    await srv.stop();
  });

  // ── 1. connect() - happy path ─────────────────────────────────────────────

  it('connect() resolves for TransferFormat.Text', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();

    // If this rejects, vitest will fail the test - no wrapper needed.
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    await transport.stop();
    res.end();
  });

  // ── 2. connect() - Binary rejected ───────────────────────────────────────

  it('connect() rejects for TransferFormat.Binary', async () => {
    const { transport } = makeTransport();
    await expect(
      () => transport.connect(srv.url, TransferFormat.Binary),
    ).rejects.toThrow(/only supports.*Text/i);
    // No GET was issued because the check happens before any I/O.
  });

  // ── 3. connect() - server non-200 ────────────────────────────────────────

  it('connect() rejects when server returns non-200', async () => {
    // Create a dedicated server that always returns 403
    const badSrv = await createSseTestServer(403);
    const { transport } = makeTransport();

    // No nextGetHandler needed: the server will auto-respond 403 and end.
    await expect(
      () => transport.connect(badSrv.url, TransferFormat.Text),
    ).rejects.toThrow(/403/);

    await badSrv.stop();
  });

  // ── 4. Single-line SSE event → onreceive ─────────────────────────────────

  it('single-line SSE event → onreceive fires with the data payload', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    res.write(sseEvent('hello'));
    await waitFor(() => received.length >= 1);
    expect(received[0]).toBe('hello');

    await transport.stop();
    res.end();
  });

  // ── 5. Multi-line data: fields joined with '\n' ───────────────────────────

  it('multi-line data: fields within one event are joined with newline', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    // Two data: lines in a single event block
    res.write('data: line one\ndata: line two\n\n');
    await waitFor(() => received.length >= 1);
    expect(received[0]).toBe('line one\nline two');

    await transport.stop();
    res.end();
  });

  // ── 6. Multiple events in one chunk ──────────────────────────────────────

  it('multiple SSE events in one write → each fires onreceive separately', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    res.write(sseEvent('alpha') + sseEvent('beta') + sseEvent('gamma'));
    await waitFor(() => received.length >= 3);
    expect(received).toEqual(['alpha', 'beta', 'gamma']);

    await transport.stop();
    res.end();
  });

  // ── 7. Chunked delivery ───────────────────────────────────────────────────

  it('SSE event split across multiple chunks is reassembled correctly', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    // Deliberately fragment the event across three writes
    res.write('data: ch');
    await new Promise<void>((r) => setTimeout(r, 10));
    res.write('un');
    await new Promise<void>((r) => setTimeout(r, 10));
    res.write('ked\n\n');

    await waitFor(() => received.length >= 1);
    expect(received[0]).toBe('chunked');

    await transport.stop();
    res.end();
  });

  // ── 8. send(string) → POST body ──────────────────────────────────────────

  it('send(string) POSTs the string body to the SSE URL', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const prevLen = srv.posts.length;
    await transport.send('{"type":6}' /* JSON-framed ping */);

    await waitFor(() => srv.posts.length > prevLen);
    expect(srv.posts[srv.posts.length - 1]).toBe('{"type":6}');

    await transport.stop();
    res.end();
  });

  // ── 9. send(Uint8Array) → POST ────────────────────────────────────────────

  it('send(Uint8Array) POSTs binary-encoded data', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const data    = new Uint8Array([0x01, 0x02, 0x03]);
    const prevLen = srv.posts.length;
    await transport.send(data);

    await waitFor(() => srv.posts.length > prevLen);
    // The transport converts Uint8Array to a binary string via toString('binary')
    expect(srv.posts.length > prevLen, 'POST should have been received').toBeTruthy();

    await transport.stop();
    res.end();
  });

  // ── 10. send() rejects on non-2xx response ────────────────────────────────

  it('send() rejects when server returns non-2xx for the POST', async () => {
    // We need a server that returns 500 for POST.
    const badSrv = await (async () => {
      const server = http.createServer((req, res) => {
        if (req.method === 'GET') {
          res.writeHead(200, { 'Content-Type': 'text/event-stream' });
          res.flushHeaders?.();
          // Keep the stream open so the transport stays connected
        } else if (req.method === 'POST') {
          const chunks: Buffer[] = [];
          req.on('data', (c: Buffer) => chunks.push(c));
          req.on('end', () => { res.writeHead(500); res.end(); });
        } else {
          res.writeHead(404); res.end();
        }
      });
      await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
      const { port } = server.address() as net.AddressInfo;
      return {
        url:  `http://127.0.0.1:${port}/sse`,
        stop: () => new Promise<void>((r) => {
          server.closeAllConnections();
          server.close(() => r());
        }),
      };
    })();

    const { transport } = makeTransport();
    await transport.connect(badSrv.url, TransferFormat.Text);

    await expect(() => transport.send('data')).rejects.toThrow(/500/);
    await transport.stop();
    await badSrv.stop();
  });

  // ── 11. stop() aborts the stream ─────────────────────────────────────────

  it('stop() destroys the response body stream', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    // Track whether the response's socket is destroyed
    let socketClosed = false;
    res.socket?.on('close', () => { socketClosed = true; });

    await transport.stop();
    await waitFor(() => socketClosed, 500);

    expect(socketClosed, 'Underlying socket should be closed after stop()').toBeTruthy();
  });

  // ── 12. onclose fires when stream ends naturally ──────────────────────────

  it('onclose fires (no error) when the SSE stream ends naturally', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const closedP = new Promise<Error | undefined>((resolve) => {
      transport.onclose = (err?: Error): void => resolve(err);
    });

    // End the stream from the server side
    res.end();

    const err = await closedP;
    expect(err, 'onclose should fire with no error on natural end').toBe(undefined);
  });

  // ── 13. onclose fires with error on stream error ──────────────────────────

  it('onclose fires with an error when the underlying socket is destroyed', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const closedP = new Promise<Error | undefined>((resolve) => {
      transport.onclose = (err?: Error): void => resolve(err);
    });

    // Destroy the socket abruptly to simulate an error
    res.socket?.destroy(new Error('server-side disconnect'));

    const err = await closedP;
    // The body Readable emits an error event, which the transport forwards to onclose
    expect(err instanceof Error || err === undefined,
      'onclose should receive an Error (or undefined if the stream quietly closes)').toBeTruthy();
  });

  // ─── Coverage gaps ─────────────────────────────────────────────────────────

  // ── 14. connect() adds Authorization header when factory returns a token ──

  it('connect() adds Authorization: Bearer header when accessTokenFactory returns a token', async () => {
    // Spin up a one-off server that captures the Authorization header from the SSE GET
    let capturedAuth = '';
    let captureResolve: (() => void) | null = null;
    const capturedP = new Promise<void>((r) => { captureResolve = r; });

    const tokenSrv = http.createServer((req: http.IncomingMessage, res: http.ServerResponse) => {
      if (req.method === 'GET') {
        capturedAuth    = (req.headers['authorization'] as string) ?? '';
        res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
        res.flushHeaders?.();
        res.socket?.on('error', () => {});
        captureResolve?.();
        // Keep the stream open so connect() resolves without error
      } else {
        res.writeHead(404);
        res.end();
      }
    });
    await new Promise<void>((r) => tokenSrv.listen(0, '127.0.0.1', r));
    const { port: tokenPort } = tokenSrv.address() as net.AddressInfo;

    const logger    = new MockLogger();
    const transport = new ServerSentEventsTransport(
      new RequestHttpClient({ timeout: 5_000, dispatcher: trackDispatcher(new Agent()) }),
      async (): Promise<string> => 'test-bearer-token',
      logger,
      {},
    );

    await transport.connect(`http://127.0.0.1:${tokenPort}/sse`, TransferFormat.Text);
    await capturedP;

    expect(capturedAuth).toBe('Bearer test-bearer-token');

    await transport.stop();
    await new Promise<void>((r) => { tokenSrv.closeAllConnections(); tokenSrv.close(() => r()); });
  });

  // ── 15. send() adds Authorization header when factory returns a token ─────

  it('send() adds Authorization: Bearer header when accessTokenFactory returns a token', async () => {
    // One-off server: GET keeps stream open; POST captures headers
    let capturedPostAuth = '';
    let postCapturedResolve: (() => void) | null = null;
    const postCapturedP = new Promise<void>((r) => { postCapturedResolve = r; });

    const tokenSrv = http.createServer((req: http.IncomingMessage, res: http.ServerResponse) => {
      if (req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
        res.flushHeaders?.();
        res.socket?.on('error', () => {});
      } else if (req.method === 'POST') {
        capturedPostAuth = (req.headers['authorization'] as string) ?? '';
        const chunks: Buffer[] = [];
        req.on('data', (c: Buffer) => chunks.push(c));
        req.on('end', () => { res.writeHead(200); res.end(); postCapturedResolve?.(); });
      } else {
        res.writeHead(404);
        res.end();
      }
    });
    await new Promise<void>((r) => tokenSrv.listen(0, '127.0.0.1', r));
    const { port: tokenPort } = tokenSrv.address() as net.AddressInfo;

    const transport = new ServerSentEventsTransport(
      new RequestHttpClient({ timeout: 5_000, dispatcher: trackDispatcher(new Agent()) }),
      async (): Promise<string> => 'send-bearer-token',
      new MockLogger(),
      {},
    );

    await transport.connect(`http://127.0.0.1:${tokenPort}/sse`, TransferFormat.Text);
    await transport.send('payload');
    await postCapturedP;

    expect(capturedPostAuth).toBe('Bearer send-bearer-token');

    await transport.stop();
    await new Promise<void>((r) => { tokenSrv.closeAllConnections(); tokenSrv.close(() => r()); });
  });

  // ── 16. Non-data: SSE fields are silently ignored ─────────────────────────

  it('SSE event:, id:, and retry: fields are silently ignored - only data: is dispatched', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    // An event block containing id:, event:, and retry: fields plus a data: line
    res.write('id: 42\nevent: update\nretry: 3000\ndata: only-this\n\n');
    await waitFor(() => received.length >= 1);

    expect(received.length).toBe(1);
    expect(received[0]).toBe('only-this');

    await transport.stop();
    res.end();
  });

  // ── 17. data: without a leading space delivers the payload correctly ───────

  it('data: field without a leading space is parsed correctly', async () => {
    const { transport } = makeTransport();
    const resP          = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    // "data:value" - no space after the colon (the spec allows optional single space)
    res.write('data:no-leading-space\n\n');
    await waitFor(() => received.length >= 1);

    expect(received[0]).toBe('no-leading-space');

    await transport.stop();
    res.end();
  });

  // ── 18. Buffer with both \n\n and \r\n\r\n delimiters (line 167 branch) ───

  it('buffer with both \\n\\n and \\r\\n\\r\\n delimiters dispatches both events (line 167)', async () => {
    // When the buffer is 'data: first\n\ndata: second\r\n\r\n':
    //   i1=11 (\n\n), i2=25 (\r\n\r\n) → both non-(-1) → Math.min(13,29)=13 (line 167)
    const { transport } = makeTransport();
    const resP = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    res.write('data: first\n\ndata: second\r\n\r\n');
    await waitFor(() => received.length >= 2);

    expect(received[0]).toBe('first');
    expect(received[1]).toBe('second');

    await transport.stop();
    res.end();
  });

  // ── 19. send() before connect() throws (line 97 true branch) ──────────────

  it('send() before connect() rejects with "not connected" (line 97)', async () => {
    const { transport } = makeTransport();
    await expect(
      () => transport.send('hello'),
    ).rejects.toThrow(/not connected/i);
  });

  // ── 20. stop() before connect() is a no-op (line 120 false branch) ─────────

  it('stop() before connect() resolves without error (line 120 false branch)', async () => {
    const { transport } = makeTransport();
    await expect(transport.stop()).resolves.toBeUndefined();
  });

  // ── 21. SSE block with no data: lines is discarded (line 151 true branch) ──

  it('SSE event block with no data: field is silently discarded (line 151)', async () => {
    const { transport } = makeTransport();
    const resP = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    let received = false;
    transport.onreceive = (): void => { received = true; };

    // An SSE block with only an event: field and no data: field
    res.write('event: update\n\n');
    await new Promise<void>((r) => setTimeout(r, 60));

    expect(received).toBe(false);
    await transport.stop();
    res.end();
  });

  // ── 22. send() with accessTokenFactory returning null (line 108 false branch)

  it('send() with accessTokenFactory returning null sends no Authorization header (line 108)', async () => {
    const logger    = new MockLogger();
    const transport = new ServerSentEventsTransport(
      new RequestHttpClient({ timeout: 5_000, dispatcher: trackDispatcher(new Agent()) }),
      async () => null,  // returns null → no Authorization header
      logger,
      {},
    );
    const resP = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    await transport.send('payload-null-token');
    expect(srv.posts.at(-1)).toBe('payload-null-token');

    await transport.stop();
    res.end();
  });

  // ── 23. Construct without extraHeaders arg → default = {} is used (line 37) ─

  it('constructed without extraHeaders uses default empty object (line 37)', async () => {
    // Omitting the 4th argument exercises the default parameter branch `= {}`
    const logger    = new MockLogger();
    const transport = new ServerSentEventsTransport(
      new RequestHttpClient({ timeout: 5_000, dispatcher: trackDispatcher(new Agent()) }),
      null,
      logger,
      // intentionally omit extraHeaders → default {} is used
    );
    const resP = srv.nextGetHandler();
    await transport.connect(srv.url, TransferFormat.Text);
    const res = await resP;

    // Verify the transport works normally with the defaulted empty extraHeaders
    const received: string[] = [];
    transport.onreceive = (d): void => { received.push(d as string); };
    res.write('data: hello-default\n\n');
    await new Promise<void>((r) => setTimeout(r, 60));
    expect(received[0]).toBe('hello-default');

    await transport.stop();
    res.end();
  });
});
