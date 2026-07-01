/**
 * long-polling-transport.test.ts
 *
 * Unit tests for LongPollingTransport - the HTTP long-poll-backed SignalR
 * transport.
 *
 * How the test server works
 * ──────────────────────────
 * A single in-process HTTP server handles three routes on the path /poll:
 *
 *   GET /poll    - Each call dequeues one response from `srv.queueGet()`.
 *                  If the queue is empty, the request hangs until a response
 *                  is enqueued (simulating a real long-poll).  Tests always
 *                  pre-stage enough responses to avoid infinite hangs.
 *
 *   POST /poll   - Collects the request body and enqueues it onto
 *                  `srv.receivedPosts`.  Responds 200 by default, or with a
 *                  configurable status via `srv.setNextPostStatus()`.
 *
 *   DELETE /poll - Records the DELETE and responds 200; inspectable via
 *                  `srv.deleteCount`.
 *
 * LongPolling internals
 * ─────────────────────
 * connect(url) issues an initial GET ("connection validation" with isConnect=true):
 *   - 204  → sets running=false, calls onclose(), connect() resolves normally
 *   - 2xx  → calls onreceive() if body.length > 0; connect() resolves normally
 *   - non-2xx → throws from connect()
 *
 * After connect(), #schedulePoll() starts a background setImmediate loop.
 * Each loop iteration calls #poll(url, false):
 *   - 204  → sets running=false, calls onclose()
 *   - 2xx  → calls onreceive() if body present; re-schedules next poll
 *   - non-2xx → sets running=false, calls onclose(error)
 *
 * stop():
 *   1. Sets running=false immediately
 *   2. Calls clearImmediate on the pending setImmediate (so no more polls fire)
 *   3. Issues HTTP DELETE to the poll URL
 *
 * Behaviors under test
 * ────────────────────
 *  1.  connect() makes the initial GET (connection validation)
 *  2.  connect() rejects when initial GET returns non-2xx
 *  3.  onreceive fires when the initial GET returns a non-empty body
 *  4.  onreceive fires during the background poll loop
 *  5.  send(string) POSTs the data to the poll URL
 *  6.  send(Uint8Array) POSTs binary data
 *  7.  send() rejects when not connected
 *  8.  send() rejects when server returns non-2xx
 *  9.  stop() sends DELETE and halts the poll loop
 * 10.  Server returning 204 triggers onclose without error
 * 11.  Server non-2xx in the poll loop triggers onclose with error
 */

import { describe, it, beforeAll, afterAll, expect, vi } from 'vitest';
import * as http from 'node:http';
import * as net  from 'node:net';
import { Readable } from 'node:stream';

import { LongPollingTransport } from '../../src/transports/long-polling-transport.js';
import { TransferFormat }       from '../../src/constants.js';
import { RequestHttpClient }    from '../../src/http-client.js';
import { MockLogger }           from '../helpers/mock-logger.js';
import type { IHttpClient, HttpResponse } from '../../src/interfaces.js';

// ─── LongPoll test server ─────────────────────────────────────────────────────

interface StagedGetResponse {
  /** HTTP status code to return. */
  status: number;
  /** Response body (empty string = no body). */
  body:   string | Buffer;
}

interface LpTestServer {
  readonly port:          number;
  readonly url:           string;
  /** Bodies received via POST, in arrival order. */
  readonly receivedPosts: string[];
  /** Number of GET polls received. */
  readonly getCount:      number;
  /** Number of DELETE requests received. */
  deleteCount:            number;

  /**
   * Stage a response for the next GET /poll.  If a GET is already waiting
   * (the server is holding a request that arrived before this call), it is
   * answered immediately.
   */
  queueGet(response: StagedGetResponse): void;

  /**
   * Control the status code returned for the next POST /poll.
   * Defaults to 200 if not set.
   */
  setNextPostStatus(status: number): void;

  stop(): Promise<void>;
}

async function createLpTestServer(): Promise<LpTestServer> {
  /** Queue of pre-staged GET responses. */
  const getQueue: StagedGetResponse[] = [];
  /** Resolvers for GET requests that arrived before a response was staged. */
  const getWaiters: Array<(r: StagedGetResponse) => void> = [];

  const receivedPosts: string[] = [];
  let deleteCount               = 0;
  let getCount                  = 0;
  let nextPostStatus            = 200;

  const server = http.createServer(
    (req: http.IncomingMessage, res: http.ServerResponse) => {
      if (req.method === 'GET') {
        getCount++;
        // Dequeue a staged response or wait until one is available
        const staged = getQueue.shift();
        if (staged) {
          sendGetResponse(res, staged);
        } else {
          getWaiters.push((r) => sendGetResponse(res, r));
        }

      } else if (req.method === 'POST') {
        const status = nextPostStatus;
        nextPostStatus = 200; // reset for next call
        const chunks: Buffer[] = [];
        req.on('data', (c: Buffer) => chunks.push(c));
        req.on('end', () => {
          receivedPosts.push(Buffer.concat(chunks).toString('utf8'));
          res.writeHead(status);
          res.end();
        });

      } else if (req.method === 'DELETE') {
        deleteCount++;
        res.writeHead(200);
        res.end();

      } else {
        res.writeHead(404);
        res.end();
      }
    },
  );

  function sendGetResponse(res: http.ServerResponse, staged: StagedGetResponse): void {
    res.writeHead(staged.status);
    res.end(staged.body);
  }

  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  const { port } = server.address() as net.AddressInfo;

  const obj: LpTestServer = {
    port,
    url: `http://127.0.0.1:${port}/poll`,
    receivedPosts,
    get getCount() { return getCount; },
    deleteCount,

    queueGet(response: StagedGetResponse): void {
      const waiter = getWaiters.shift();
      if (waiter) {
        waiter(response);
      } else {
        getQueue.push(response);
      }
    },

    setNextPostStatus(status: number): void {
      nextPostStatus = status;
    },

    stop: () => new Promise<void>((r) => {
      server.closeAllConnections();
      server.close(() => r());
    }),
  };

  // `deleteCount` is a primitive - mirror it through the object reference
  Object.defineProperty(obj, 'deleteCount', {
    get:  ()    => deleteCount,
    set:  (v)   => { deleteCount = v; },
    enumerable: true,
  });

  return obj;
}

// ─── Helper: build a transport ────────────────────────────────────────────────

function makeTransport(): {
  transport: LongPollingTransport;
  logger:    MockLogger;
} {
  const logger    = new MockLogger();
  const transport = new LongPollingTransport(
    new RequestHttpClient({ timeout: 10_000 }),
    null,
    logger,
    {},
  );
  return { transport, logger };
}

/** Wait up to `ms` ms for `condition` to become true. */
async function waitFor(condition: () => boolean, ms = 500): Promise<void> {
  await vi.waitFor(() => expect(condition()).toBe(true), { timeout: ms, interval: 10 });
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('LongPollingTransport', () => {
  let srv: LpTestServer;

  beforeAll(async () => {
    srv = await createLpTestServer();
  });

  afterAll(async () => {
    await srv.stop();
  });

  // ── 1. connect() makes the initial GET ───────────────────────────────────

  it('connect() issues an initial GET (connection validation)', async () => {
    const { transport } = makeTransport();

    // Stage one 200 for the initial poll, then 204 to stop the loop.
    srv.queueGet({ status: 200, body: '' });
    srv.queueGet({ status: 204, body: '' });

    const closed = new Promise<void>((r) => { transport.onclose = (): void => r(); });
    await transport.connect(srv.url, TransferFormat.Text);

    // Wait for 204 to stop the background loop
    await closed;

    expect(srv.getCount).toBeGreaterThanOrEqual(2);
  });

  // ── 2. connect() rejects on non-2xx initial GET ───────────────────────────

  it('connect() rejects when initial GET returns non-2xx', async () => {
    const { transport } = makeTransport();

    srv.queueGet({ status: 503, body: 'Service Unavailable' });

    await expect(
      () => transport.connect(srv.url, TransferFormat.Text),
    ).rejects.toThrow(/503/);
  });

  // ── 3. onreceive fires when initial GET has a body ────────────────────────

  it('onreceive fires when the initial GET returns a non-empty body', async () => {
    const { transport } = makeTransport();
    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    // Initial poll returns a body; 204 terminates the loop.
    srv.queueGet({ status: 200, body: 'initial-message\x1e' });
    srv.queueGet({ status: 204, body: '' });

    const closed = new Promise<void>((r) => { transport.onclose = (): void => r(); });
    await transport.connect(srv.url, TransferFormat.Text);
    await closed;

    expect(received.some((m) => m.includes('initial-message')),
      `Expected onreceive with 'initial-message', got: ${JSON.stringify(received)}`).toBeTruthy();
  });

  // ── 4. onreceive fires in the background poll loop ────────────────────────

  it('onreceive fires for messages received in the background poll loop', async () => {
    const { transport } = makeTransport();
    const received: string[] = [];
    transport.onreceive = (data: string | Uint8Array): void => {
      received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
    };

    // initial GET: empty 200 → connect resolves; then loop poll: 200 + body;
    // then 204 to stop.
    srv.queueGet({ status: 200, body: '' });
    srv.queueGet({ status: 200, body: 'from-loop\x1e' });
    srv.queueGet({ status: 204, body: '' });

    const closed = new Promise<void>((r) => { transport.onclose = (): void => r(); });
    await transport.connect(srv.url, TransferFormat.Text);
    await closed;

    expect(received.some((m) => m.includes('from-loop')),
      `Expected 'from-loop' in received, got: ${JSON.stringify(received)}`).toBeTruthy();
  });

  // ── 5. send(string) → POST ────────────────────────────────────────────────

  it('send(string) POSTs the string payload to the poll URL', async () => {
    const { transport } = makeTransport();

    srv.queueGet({ status: 200, body: '' }); // initial poll

    await transport.connect(srv.url, TransferFormat.Text);

    const prevLen = srv.receivedPosts.length;
    await transport.send('{"type":6}\x1e');

    await waitFor(() => srv.receivedPosts.length > prevLen);
    expect(
      srv.receivedPosts[srv.receivedPosts.length - 1],
    ).toBe('{"type":6}\x1e');

    // Clean up: stop() sends DELETE and terminates the poll loop
    srv.queueGet({ status: 204, body: '' });
    await transport.stop();
  });

  // ── 6. send(Uint8Array) → POST ────────────────────────────────────────────

  it('send(Uint8Array) POSTs binary data', async () => {
    const { transport } = makeTransport();

    srv.queueGet({ status: 200, body: '' });

    await transport.connect(srv.url, TransferFormat.Text);

    const data    = new Uint8Array([0x01, 0x02, 0x03]);
    const prevLen = srv.receivedPosts.length;
    await transport.send(data);

    await waitFor(() => srv.receivedPosts.length > prevLen);
    // LongPollingTransport converts Uint8Array via toString('binary')
    expect(srv.receivedPosts.length > prevLen, 'POST should have been received').toBeTruthy();

    srv.queueGet({ status: 204, body: '' });
    await transport.stop();
  });

  // ── 7. send() rejects when not connected ─────────────────────────────────

  it('send() rejects when the transport has not connected', async () => {
    const { transport } = makeTransport();
    await expect(
      () => transport.send('not connected'),
    ).rejects.toThrow(/not connected/i);
  });

  // ── 8. send() rejects when server returns non-2xx on POST ─────────────────

  it('send() rejects when server returns non-2xx for POST', async () => {
    const { transport } = makeTransport();

    srv.queueGet({ status: 200, body: '' });
    await transport.connect(srv.url, TransferFormat.Text);

    // Make the next POST return 500
    srv.setNextPostStatus(500);
    await expect(() => transport.send('data')).rejects.toThrow(/500/);

    srv.queueGet({ status: 204, body: '' });
    await transport.stop();
  });

  // ── 9. stop() sends DELETE and halts the poll loop ────────────────────────

  it('stop() sends an HTTP DELETE and prevents further poll GETs', async () => {
    const { transport } = makeTransport();

    // stop() sets running=false and calls clearImmediate() synchronously
    // before any await, so the background poll setImmediate is always
    // cancelled before it fires.  No second response is needed.
    srv.queueGet({ status: 200, body: '' }); // initial validation only

    await transport.connect(srv.url, TransferFormat.Text);

    const beforeDelete = (srv as unknown as { deleteCount: number }).deleteCount;
    await transport.stop();

    await waitFor(() =>
      (srv as unknown as { deleteCount: number }).deleteCount > beforeDelete
    );

    const afterDelete = (srv as unknown as { deleteCount: number }).deleteCount;
    expect(afterDelete > beforeDelete, 'stop() should have sent a DELETE request').toBeTruthy();
  });

  // ── 10. 204 from background poll → onclose with no error ─────────────────

  it('background poll returning 204 triggers onclose without error', async () => {
    const { transport } = makeTransport();

    let closedErr: Error | undefined = new Error('sentinel');
    const closedP = new Promise<void>((resolve) => {
      transport.onclose = (err?: Error): void => {
        closedErr = err;
        resolve();
      };
    });

    // Initial poll: empty body; background poll: 204 → triggers onclose
    srv.queueGet({ status: 200, body: '' });
    srv.queueGet({ status: 204, body: '' });

    await transport.connect(srv.url, TransferFormat.Text);
    await closedP;

    expect(closedErr, 'onclose should fire with no error when server returns 204').toBe(undefined);
  });

  // ── 11. Non-2xx in background poll → onclose with error ──────────────────

  it('non-2xx in the background poll loop triggers onclose with an error', async () => {
    const { transport } = makeTransport();

    let closedErr: Error | undefined;
    const closedP = new Promise<void>((resolve) => {
      transport.onclose = (err?: Error): void => {
        closedErr = err;
        resolve();
      };
    });

    srv.queueGet({ status: 200, body: '' });  // initial poll: ok
    srv.queueGet({ status: 500, body: '' });  // background poll: error → onclose

    await transport.connect(srv.url, TransferFormat.Text);
    await closedP;

    expect(closedErr instanceof Error,
      'onclose should receive an Error when background poll returns non-2xx').toBeTruthy();
    expect((closedErr as unknown as Error).message).toMatch(/500/);
  });

  // ─── Coverage gaps ─────────────────────────────────────────────────────────

  // ── 12. stop() DELETE failure is logged as warning and not propagated ─────

  it('stop() DELETE failure is logged as a warning and stop() still resolves', async () => {
    // Create a dedicated server where DELETE destroys the socket (simulating a
    // network error) so the DELETE request throws inside stop().
    const deleteErrSrv = await (async () => {
      const s = http.createServer((req: http.IncomingMessage, res: http.ServerResponse) => {
        if (req.method === 'GET') {
          // Return a valid initial poll response so connect() succeeds
          res.writeHead(200);
          res.end('');
        } else if (req.method === 'DELETE') {
          // Destroy the socket abruptly - this causes the DELETE to throw on the client
          req.socket?.destroy();
        } else {
          res.writeHead(404);
          res.end();
        }
      });
      await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
      const { port } = s.address() as net.AddressInfo;
      return {
        url:  `http://127.0.0.1:${port}/poll`,
        stop: (): Promise<void> => new Promise<void>((r) => {
          s.closeAllConnections();
          s.close(() => r());
        }),
      };
    })();

    const { transport, logger } = makeTransport();
    await transport.connect(deleteErrSrv.url, TransferFormat.Text);

    // stop() must resolve even though DELETE threw
    await expect(transport.stop()).resolves.toBeUndefined();

    // The DELETE error should have been logged as a warning
    expect(
      logger.hasMessage('DELETE'),
      'Expected a warning mentioning DELETE',
    ).toBeTruthy();

    await deleteErrSrv.stop();
  });

  // ── 13. Empty body in poll loop does not call onreceive ───────────────────

  it('empty response body in the background poll loop does not call onreceive', async () => {
    const { transport } = makeTransport();
    let receiveCalled = false;
    transport.onreceive = (): void => { receiveCalled = true; };

    // Sequence: empty 200 (initial) → empty 200 (loop - onreceive must NOT fire) → 204 (close)
    srv.queueGet({ status: 200, body: '' }); // initial: connect() validates
    srv.queueGet({ status: 200, body: '' }); // background poll with empty body
    srv.queueGet({ status: 204, body: '' }); // graceful shutdown

    const closedP = new Promise<void>((r) => { transport.onclose = (): void => r(); });
    await transport.connect(srv.url, TransferFormat.Text);
    await closedP;

    expect(receiveCalled,
      'onreceive must not be called when the poll body is empty').toBeFalsy();
  });

  // ── 14. connect() accepts TransferFormat.Binary (parameter is unused) ─────

  it('delivers binary poll responses without UTF-8 conversion', async () => {
    const { transport } = makeTransport();

    const receivedP = new Promise<string | Uint8Array>((resolve) => {
      transport.onreceive = resolve;
    });
    srv.queueGet({ status: 200, body: Buffer.from([0x00, 0x80, 0xff]) });
    srv.queueGet({ status: 204, body: '' }); // graceful shutdown

    const closedP = new Promise<void>((r) => { transport.onclose = (): void => r(); });

    await expect(
      transport.connect(srv.url, TransferFormat.Binary),
    ).resolves.toBeUndefined();

    expect(Array.from(await receivedP as Uint8Array)).toEqual([0x00, 0x80, 0xff]);
    await closedP;
  });
});

// ─── Mock IHttpClient tests (catch-block + buildHeaders coverage) ─────────────

function makeMockClient(overrides: {
  get?:    (url: string, opts?: unknown) => Promise<HttpResponse>;
  post?:   (url: string, opts?: unknown) => Promise<HttpResponse>;
  delete?: (url: string, opts?: unknown) => Promise<HttpResponse>;
}): IHttpClient {
  const ok: HttpResponse = { status: 200, headers: {}, body: '' };
  const get = overrides.get ?? (() => Promise.resolve(ok));
  return {
    get,
    post:    overrides.post   ?? (() => Promise.resolve(ok)),
    delete:  overrides.delete ?? (() => Promise.resolve(ok)),
    request: () => Promise.resolve(ok),
    stream:  async (_method: string, url: string, opts?: unknown) => {
      const response = await get(url, opts);
      const body = Readable.from([Buffer.from(response.body)]);
      return {
        statusCode: response.status,
        headers: response.headers,
        body,
        abort: () => body.destroy(),
      };
    },
  } as unknown as IHttpClient;
}

describe('LongPollingTransport - request errors and authorization headers', () => {

  it('accessTokenFactory returning a token sends Authorization header (lines 164-165)', async () => {
    let capturedAuth = '';
    const mockClient = makeMockClient({
      get: async (_url: string, opts?: unknown) => {
        capturedAuth = (opts as { headers?: Record<string, string> })?.headers?.['Authorization'] ?? '';
        return { status: 204, headers: {}, body: '' };
      },
    });
    const logger = new MockLogger();
    const transport = new LongPollingTransport(
      mockClient, async () => 'bearer-abc', logger, {},
    );
    const closedP = new Promise<void>((r) => { transport.onclose = (): void => r(); });
    await transport.connect('http://fake.invalid/poll', TransferFormat.Text);
    await closedP;
    expect(capturedAuth).toBe('Bearer bearer-abc');
  });

  it('accessTokenFactory returning null sends no Authorization header (lines 164-165)', async () => {
    let capturedHeaders: Record<string, string> = {};
    const mockClient = makeMockClient({
      get: async (_url: string, opts?: unknown) => {
        capturedHeaders = (opts as { headers?: Record<string, string> })?.headers ?? {};
        return { status: 204, headers: {}, body: '' };
      },
    });
    const logger = new MockLogger();
    const transport = new LongPollingTransport(
      mockClient, async () => null, logger, {},
    );
    const closedP = new Promise<void>((r) => { transport.onclose = (): void => r(); });
    await transport.connect('http://fake.invalid/poll', TransferFormat.Text);
    await closedP;
    expect('Authorization' in capturedHeaders).toBeFalsy();
  });

  it('poll GET throws while running → onclose fires with the error (line 133)', async () => {
    let callCount = 0;
    const mockClient = makeMockClient({
      get: async () => {
        callCount++;
        if (callCount === 1) return { status: 200, headers: {}, body: '' };
        throw new Error('simulated network failure');
      },
    });
    const logger = new MockLogger();
    const transport = new LongPollingTransport(mockClient, null, logger, {});
    let closedErr: Error | undefined;
    const closedP = new Promise<void>((r) => {
      transport.onclose = (err?: Error): void => { closedErr = err; r(); };
    });
    await transport.connect('http://fake.invalid/poll', TransferFormat.Text);
    await closedP;
    expect(closedErr instanceof Error).toBeTruthy();
    expect(closedErr!.message).toMatch(/simulated network failure/);
  });

  it('poll GET throws after stop() → error swallowed, onclose not called (line 132)', async () => {
    let resolveGet!: (v: HttpResponse) => void;
    let rejectGet!:  (e: Error) => void;
    let callCount = 0;
    const mockClient = makeMockClient({
      get: async () => {
        callCount++;
        if (callCount === 1) return { status: 200, headers: {}, body: '' };
        return new Promise<HttpResponse>((res, rej) => {
          resolveGet = res; rejectGet = rej;
        });
      },
    });
    const logger = new MockLogger();
    const transport = new LongPollingTransport(mockClient, null, logger, {});
    let closeCalled = false;
    transport.onclose = (): void => { closeCalled = true; };
    await transport.connect('http://fake.invalid/poll', TransferFormat.Text);
    // Wait for background poll to start its second GET
    await new Promise<void>((r) => {
      const tick = (): void => { if (callCount >= 2) r(); else setImmediate(tick); };
      setImmediate(tick);
    });
    await transport.stop();
    rejectGet(new Error('connection reset after stop'));
    await new Promise<void>((r) => setTimeout(r, 50));
    expect(closeCalled).toBe(false);
  });

  it('stop() before connect() is a no-op: hits false branches at lines 82 and 87', async () => {
    // #pollImmediate is null (never scheduled) → line 82 false branch
    // #url is null (never connected) → line 87 false branch
    const mockClient = makeMockClient({});
    const logger = new MockLogger();
    const transport = new LongPollingTransport(mockClient, null, logger);
    await expect(transport.stop()).resolves.toBeUndefined();
  });

  // ── L118 optional-chain null branch: onclose not set when poll errors ────
  // When a background poll throws and this.onclose is null, the optional-call
  // this.onclose?.(...) short-circuits (branch 1 of the ?. operator).
  // The error must still be logged, and stop() should observe the transport
  // is no longer running.

  it('poll error with no onclose handler: error is logged, onclose?.() is a no-op (L125 ?. branch)', async () => {
    let callCount = 0;
    const mockClient = makeMockClient({
      get: async () => {
        callCount++;
        if (callCount === 1) return { status: 200, headers: {}, body: '' };
        throw new Error('background-poll-failure');
      },
    });
    const logger = new MockLogger();
    const transport = new LongPollingTransport(mockClient, null, logger, {});
    // Intentionally leave transport.onclose as null - covers the ?. null branch.

    await transport.connect('http://fake.invalid/poll', TransferFormat.Text);

    // Wait for the background poll to fail and log the error.
    await waitFor(() => logger.hasMessage('Poll error'), 2_000);

    expect(logger.hasMessage('Poll error')).toBeTruthy();
    expect(logger.hasMessage('background-poll-failure')).toBeTruthy();
  });

  // ── L125 ternary false branch: poll throws a non-Error value ─────────────
  // JavaScript allows throwing any value.  When err is not an Error instance,
  // the ternary `err instanceof Error ? err : new Error(String(err))` takes
  // the false branch (branch 1) and wraps the value in a new Error.

  it('poll throws a non-Error value: new Error(String(err)) wraps it (L125 ternary branch)', async () => {
    let callCount = 0;
    const mockClient = makeMockClient({
      get: async () => {
        callCount++;
        if (callCount === 1) return { status: 200, headers: {}, body: '' };
        // eslint-disable-next-line @typescript-eslint/only-throw-error
        throw 'string-thrown-by-poll'; // non-Error value → ternary false branch
      },
    });
    const logger = new MockLogger();
    const transport = new LongPollingTransport(mockClient, null, logger, {});
    let closedErr: Error | undefined;
    const closedP = new Promise<void>((r) => {
      transport.onclose = (err?: Error): void => { closedErr = err; r(); };
    });

    await transport.connect('http://fake.invalid/poll', TransferFormat.Text);
    await closedP;

    // The string was wrapped in new Error(String('string-thrown-by-poll'))
    expect(closedErr instanceof Error).toBeTruthy();
    expect(closedErr!.message).toBe('string-thrown-by-poll');
  });
});
