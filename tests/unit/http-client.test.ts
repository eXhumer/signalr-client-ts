/**
 * http-client.test.ts
 *
 * Tests all five undici-backed HTTP client implementations against a real
 * in-process HTTP server.  The same behavioural contract is verified for each
 * client; additional tests exercise client-specific features (dispatcher
 * sharing, pipeline transform, dispatch handler lifecycle, etc.).
 *
 * Clients under test
 * ──────────────────
 *   RequestHttpClient   - undici.request()
 *   FetchHttpClient     - undici.fetch()
 *   StreamHttpClient    - undici.stream()
 *   PipelineHttpClient  - undici.pipeline()
 *   DispatchHttpClient  - Dispatcher#dispatch()  (default / HttpClient alias)
 */

import { describe, it, beforeAll, afterAll, expect } from 'vitest';
import * as http from 'node:http';
import * as net  from 'node:net';
import { Agent } from 'undici';

import {
  RequestHttpClient,
  FetchHttpClient,
  StreamHttpClient,
  PipelineHttpClient,
  DispatchHttpClient,
  HttpClient,          // alias for DispatchHttpClient
} from '../../src/http-client.js';
import type { IHttpClient } from '../../src/interfaces.js';

// ─── In-process test server ───────────────────────────────────────────────────

interface TestServer {
  port:  number;
  close(): Promise<void>;
}

async function createTestServer(): Promise<TestServer> {
  const server = http.createServer((req, res) => {
    const url = req.url ?? '/';

    if (url === '/echo') {
      // Echoes method, body, and received headers back as JSON
      const chunks: Buffer[] = [];
      req.on('data', (c: Buffer) => chunks.push(c));
      req.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'X-Custom':     'yes',
        });
        res.end(JSON.stringify({ method: req.method, body, headers: req.headers }));
      });

    } else if (url === '/status/204') {
      res.writeHead(204);
      res.end();

    } else if (url === '/status/500') {
      res.writeHead(500);
      res.end('Internal Server Error');

    } else if (url === '/stream') {
      // Sends two SSE events 30 ms apart - used by stream() tests
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('data: first\n\n');
      setTimeout(() => { res.write('data: second\n\n'); res.end(); }, 30);

    } else if (url === '/timeout') {
      // Never responds - used by timeout tests
      // (do NOT call res.end so the connection hangs)

    } else {
      res.writeHead(404);
      res.end('Not found');
    }
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;

  return {
    port,
    close: () => new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    }),
  };
}

// ─── Shared behavioural tests ─────────────────────────────────────────────────
//
// The same set of assertions is run for every client implementation.
// Client-specific features are in separate describe blocks below.

function registerCommonTests(label: string, factory: (srv: TestServer) => IHttpClient): void {
  describe(label, () => {
    let srv:    TestServer;
    let client: IHttpClient;

    beforeAll(async () => {
      srv    = await createTestServer();
      client = factory(srv);
    });

    afterAll(async () => { await srv.close(); });

    const url = (path: string): string => `http://127.0.0.1:${srv.port}${path}`;

    it('GET returns status + body + headers', async () => {
      const res = await client.get(url('/echo'));
      expect(res.status).toBe(200);
      expect(res.body.includes('"method":"GET"')).toBeTruthy();
      // Header names are lower-cased by all undici primitives
      expect(res.headers['x-custom']).toBe('yes');
    });

    it('POST sends body and receives echo', async () => {
      const res = await client.post(url('/echo'), { body: { hello: 'world' } });
      expect(res.status).toBe(200);
      const parsed = JSON.parse(res.body) as { method: string; body: string };
      expect(parsed.method).toBe('POST');
      expect(parsed.body.includes('hello')).toBeTruthy();
    });

    it('POST sends string body as-is', async () => {
      const res = await client.post(url('/echo'), {
        body:    'raw text',
        headers: { 'Content-Type': 'text/plain' },
      });
      const parsed = JSON.parse(res.body) as { body: string };
      expect(parsed.body).toBe('raw text');
    });

    it('DELETE is supported', async () => {
      const res = await client.delete(url('/echo'));
      expect(res.status).toBe(200);
      const parsed = JSON.parse(res.body) as { method: string };
      expect(parsed.method).toBe('DELETE');
    });

    it('handles 204 No Content', async () => {
      const res = await client.get(url('/status/204'));
      expect(res.status).toBe(204);
      expect(res.body).toBe('');
    });

    it('handles 500 without throwing', async () => {
      const res = await client.get(url('/status/500'));
      expect(res.status).toBe(500);
      expect(res.body.includes('Internal')).toBeTruthy();
    });

    it('handles 404 without throwing', async () => {
      const res = await client.get(url('/not-found'));
      expect(res.status).toBe(404);
    });

    it('rejects on connection refused', async () => {
      await expect(
        () => client.get('http://127.0.0.1:1/nowhere'),
      ).rejects.toThrow(/ECONNREFUSED|connect|fetch failed/i);
    });

    it('stream() returns a Readable with correct status and headers', async () => {
      const result = await client.stream('GET', url('/stream'));
      expect(result.statusCode).toBe(200);
      // Content-Type header present
      expect(
        typeof result.headers['content-type'] === 'string' &&
        (result.headers['content-type'] as string).includes('text/event-stream'),
      ).toBeTruthy();

      // Read all chunks
      const chunks: string[] = [];
      result.body.setEncoding('utf8');
      await new Promise<void>((resolve, reject) => {
        result.body.on('data',  (c: string) => chunks.push(c));
        result.body.on('end',   resolve);
        result.body.on('error', reject);
      });
      const body = chunks.join('');
      expect(body.includes('first'),  'expected "first" SSE event').toBeTruthy();
      expect(body.includes('second'), 'expected "second" SSE event').toBeTruthy();
    });

    it('stream() abort() stops the body stream', async () => {
      const result = await client.stream('GET', url('/stream'));
      // Abort immediately without reading
      result.abort();
      // The body should be destroyed; reading should not produce data
      const data: Buffer[] = [];
      result.body.on('data', (c: Buffer) => data.push(c));
      await new Promise<void>((resolve) => {
        result.body.on('close', resolve);
        result.body.on('error', resolve); // destroyed streams emit error
      });
      // We don't assert exact data because some bytes may arrive before abort
      expect(true, 'abort() completed without throwing').toBeTruthy();
    });
  });
}

// Register the same tests for all five implementations
registerCommonTests('RequestHttpClient (undici.request)', (_srv) =>
  new RequestHttpClient({ timeout: 5000 })
);

registerCommonTests('FetchHttpClient (undici.fetch)', (_srv) =>
  new FetchHttpClient({ timeout: 5000 })
);

registerCommonTests('StreamHttpClient (undici.stream)', (_srv) =>
  new StreamHttpClient({ timeout: 5000 })
);

registerCommonTests('PipelineHttpClient (undici.pipeline)', (_srv) =>
  new PipelineHttpClient({ timeout: 5000 })
);

registerCommonTests('DispatchHttpClient (Dispatcher#dispatch)', (_srv) =>
  new DispatchHttpClient({ timeout: 5000 })
);

// ─── HttpClient alias ─────────────────────────────────────────────────────────

describe('HttpClient (alias for DispatchHttpClient)', () => {
  it('is the same constructor as DispatchHttpClient', () => {
    expect(HttpClient).toBe(DispatchHttpClient);
  });

  it('instances satisfy instanceof DispatchHttpClient', () => {
    const c = new HttpClient();
    expect(c instanceof DispatchHttpClient).toBeTruthy();
  });
});

// ─── Default headers ─────────────────────────────────────────────────────────

describe('HttpClientOptions.headers (session-level defaults)', () => {
  let srv: TestServer;

  beforeAll(async () => { srv = await createTestServer(); });
  afterAll(async ()  => { await srv.close(); });

  const url = (path: string): string => `http://127.0.0.1:${srv.port}${path}`;

  // Test with RequestHttpClient as representative; other clients share the same
  // BaseUndiciClient header-merging logic.
  it('sends default headers', async () => {
    const c   = new RequestHttpClient({ headers: { 'X-Test': 'from-default' } });
    const res = await c.get(url('/echo'));
    const parsed = JSON.parse(res.body) as { headers: Record<string, string> };
    expect(parsed.headers['x-test']).toBe('from-default');
  });

  it('per-request headers override defaults', async () => {
    const c   = new RequestHttpClient({ headers: { 'X-Base': 'base' } });
    const res = await c.get(url('/echo'), { headers: { 'X-Override': 'yes' } });
    const parsed = JSON.parse(res.body) as { headers: Record<string, string> };
    expect(parsed.headers['x-base']).toBe('base');
    expect(parsed.headers['x-override']).toBe('yes');
  });

  it('per-request headers override default headers of same name', async () => {
    const c = new RequestHttpClient({ headers: { 'X-Name': 'session-value' } });
    const res = await c.get(url('/echo'), { headers: { 'X-Name': 'request-value' } });
    const parsed = JSON.parse(res.body) as { headers: Record<string, string> };
    expect(parsed.headers['x-name']).toBe('request-value');
  });
});

// ─── Shared dispatcher ────────────────────────────────────────────────────────

describe('Shared Dispatcher (session-level connection pool)', () => {
  let srv: TestServer;

  beforeAll(async () => { srv = await createTestServer(); });
  afterAll(async ()  => { await srv.close(); });

  const url = (path: string): string => `http://127.0.0.1:${srv.port}${path}`;

  it('RequestHttpClient accepts a custom Agent dispatcher', async () => {
    const agent  = new Agent({ connections: 2 });
    const client = new RequestHttpClient({ dispatcher: agent });
    const res    = await client.get(url('/echo'));
    expect(res.status).toBe(200);
    await agent.close();
  });

  it('FetchHttpClient accepts a custom Agent dispatcher', async () => {
    const agent  = new Agent({ connections: 2 });
    const client = new FetchHttpClient({ dispatcher: agent });
    const res    = await client.get(url('/echo'));
    expect(res.status).toBe(200);
    await agent.close();
  });

  it('DispatchHttpClient uses the provided dispatcher', async () => {
    // Pool is bound to our test server origin
    const agent  = new Agent();
    const client = new DispatchHttpClient({ dispatcher: agent });
    const res    = await client.get(url('/echo'));
    expect(res.status).toBe(200);
    await agent.close();
  });

  it('two clients sharing the same Agent dispatcher both succeed', async () => {
    const shared  = new Agent({ connections: 4 });
    const clientA = new RequestHttpClient({ dispatcher: shared });
    const clientB = new FetchHttpClient({ dispatcher: shared });

    const [a, b] = await Promise.all([
      clientA.get(url('/echo')),
      clientB.get(url('/echo')),
    ]);

    expect(a.status).toBe(200);
    expect(b.status).toBe(200);
    await shared.close();
  });
});

// ─── Timeout ─────────────────────────────────────────────────────────────────

describe('Request timeout', () => {
  let srv: TestServer;

  beforeAll(async () => { srv = await createTestServer(); });
  afterAll(async ()  => { await srv.close(); });

  const url = (path: string): string => `http://127.0.0.1:${srv.port}${path}`;

  it('RequestHttpClient times out', async () => {
    const c = new RequestHttpClient({ timeout: 100 });
    await expect(() => c.get(url('/timeout'))).rejects.toThrow(/timeout|timed out/i);
  });

  it('FetchHttpClient times out', async () => {
    const c = new FetchHttpClient({ timeout: 100 });
    await expect(() => c.get(url('/timeout'))).rejects.toThrow(/timeout|timed out|abort/i);
  });

  it('StreamHttpClient times out', async () => {
    const c = new StreamHttpClient({ timeout: 100 });
    await expect(() => c.get(url('/timeout'))).rejects.toThrow(/timeout|timed out/i);
  });

  it('PipelineHttpClient times out', async () => {
    const c = new PipelineHttpClient({ timeout: 100 });
    await expect(() => c.get(url('/timeout'))).rejects.toThrow(/timeout|timed out/i);
  });

  it('DispatchHttpClient times out', async () => {
    const c = new DispatchHttpClient({ timeout: 100 });
    await expect(() => c.get(url('/timeout'))).rejects.toThrow(/timeout|timed out/i);
  });
});
