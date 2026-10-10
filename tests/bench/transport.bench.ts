/**
 * tests/bench/transport.bench.ts
 *
 * Transport and HTTP-client benchmarks.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │ SECTION 1 - HTTP clients × negotiate                                    │
 * │   All five undici-backed clients doing POST /negotiate.                 │
 * │   This isolates the overhead of each HTTP primitive without any         │
 * │   SignalR protocol overhead.                                            │
 * ├─────────────────────────────────────────────────────────────────────────┤
 * │ SECTION 2 - WebSocket startup                                            │
 * │   Each iteration negotiates with the selected HTTP client, upgrades to  │
 * │   WebSocket, completes the SignalR handshake, and closes cleanly.        │
 * ├─────────────────────────────────────────────────────────────────────────┤
 * │ SECTION 3 - SSE transport: first-message latency                        │
 * │   Time to connect via SSE and receive the first message push.           │
 * ├─────────────────────────────────────────────────────────────────────────┤
 * │ SECTION 4 - Long-polling transport: round-trip                          │
 * │   Time to POST a message and receive it back via the next poll.         │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Run with:
 *   npx vitest bench tests/bench/transport.bench.ts
 */

import { test, describe, beforeAll, afterAll } from 'vitest';

import {
  RequestHttpClient,
  FetchHttpClient,
  StreamHttpClient,
  PipelineHttpClient,
  DispatchHttpClient,
} from '../../src/http-client.js';
import { startBenchServer } from '../helpers/bench-server.js';
import { MsgpackHubProtocol } from '../../src/protocols/msgpack-hub-protocol.js';
import { MessageType, RECORD_SEPARATOR, TransferFormat } from '../../src/constants.js';
import { NullLogger } from '../../src/logger.js';
import { WebSocketTransport } from '../../src/transports/websocket-transport.js';
import type { BenchServer }   from '../helpers/bench-server.js';
import type { IHttpClient }   from '../../src/interfaces.js';

// ─── Shared state ─────────────────────────────────────────────────────────────

let srv: BenchServer;

const log  = NullLogger.instance;
// HTTP client instances (one per variant, reused across benchmarks)
let reqClient:      IHttpClient;
let fetchClient:    IHttpClient;
let streamClient:   IHttpClient;
let pipelineClient: IHttpClient;
let dispatchClient: IHttpClient;

beforeAll(async () => {
  srv = await startBenchServer();

  reqClient      = new RequestHttpClient();
  fetchClient    = new FetchHttpClient();
  streamClient   = new StreamHttpClient();
  pipelineClient = new PipelineHttpClient();
  dispatchClient = new DispatchHttpClient();
}, 30_000);

afterAll(async () => {
  await srv.close();
}, 15_000);

// ─── SECTION 1: HTTP clients × negotiate ─────────────────────────────────────

describe('POST /negotiate - HTTP client comparison', () => {

  function negotiate(httpClient: IHttpClient) {
    return httpClient.post(`${srv.hubUrl}/negotiate`, {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body:    '',
    });
  }

  test('compare', async ({ bench }) => {
    await bench.compare(
      bench('RequestHttpClient  (undici.request)', () => negotiate(reqClient)),
      bench('FetchHttpClient    (undici.fetch)', () => negotiate(fetchClient)),
      bench('StreamHttpClient   (undici.stream)', () => negotiate(streamClient)),
      bench('PipelineHttpClient (undici.pipeline)', () => negotiate(pipelineClient)),
      bench('DispatchHttpClient (Dispatcher#dispatch)', () => negotiate(dispatchClient)),
    );
  });
});

// ─── SECTION 2: WebSocket connection startup ─────────────────────────────────
//
// The HTTP client is only involved in WebSocket negotiation. Once upgraded,
// every variant uses the same WebSocketTransport and therefore has identical
// steady-state behavior. Each iteration measures the meaningful distinction:
// negotiate, upgrade, SignalR handshake, and clean shutdown.

describe('WebSocket startup - per HTTP client for negotiate', () => {

  async function wsStartup(httpClient: IHttpClient): Promise<void> {
    const negotiation = await httpClient.post(`${srv.hubUrl}/negotiate`, {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body: '',
    });
    const response = JSON.parse(negotiation.body) as {
      connectionToken: string;
    };
    const transport = new WebSocketTransport(null, log, {}, undefined);
    const wsUrl = `${srv.hubUrl.replace('http://', 'ws://')}?id=${encodeURIComponent(response.connectionToken)}`;

    try {
      await transport.connect(wsUrl, TransferFormat.Text);
      await new Promise<void>((resolve, reject) => {
        transport.onreceive = (data: string | Uint8Array) => {
          const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
          if (text.includes(RECORD_SEPARATOR)) resolve();
        };
        transport.send(
          JSON.stringify({ protocol: 'json', version: 1 }) + RECORD_SEPARATOR,
        ).catch(reject);
      });
    } finally {
      transport.onreceive = null;
      await transport.stop();
    }
  }

  test('compare', async ({ bench }) => {
    await bench.compare(
      bench('RequestHttpClient  - WS startup', () => wsStartup(reqClient)),
      bench('FetchHttpClient    - WS startup', () => wsStartup(fetchClient)),
      bench('StreamHttpClient   - WS startup', () => wsStartup(streamClient)),
      bench('PipelineHttpClient - WS startup', () => wsStartup(pipelineClient)),
      bench('DispatchHttpClient - WS startup', () => wsStartup(dispatchClient)),
    );
  });
});

// ─── SECTION 3: SSE - first-message receive latency ──────────────────────────
//
// Each iteration: open an SSE connection, wait for the handshake frame,
// then abort.  Measures the full connect-to-first-byte latency for SSE.

describe('SSE connect-to-first-message latency - per HTTP client', () => {

  async function sseLatency(httpClient: IHttpClient): Promise<void> {
    const id  = Math.random().toString(36).slice(2);
    const url = `${srv.hubUrl}?id=${id}`;

    const result = await httpClient.stream('GET', url, {
      headers: { Accept: 'text/event-stream' },
    });

    await new Promise<void>((resolve, reject) => {
      result.body.once('error', reject);
      result.body.once('data', () => {
        // The bench-server closes the SSE stream after the handshake frame, so
        // the body will reach 'end' on its own.  Remove the error listener,
        // install a no-op one for any late errors, then drain to let the stream
        // finish cleanly without blocking the benchmark loop.
        result.body.removeListener('error', reject);
        result.body.on('error', () => {});
        result.body.resume();
        resolve();
      });
    });
  }

  test('compare', async ({ bench }) => {
    await bench.compare(
      bench('RequestHttpClient  - SSE first-message', () => sseLatency(reqClient)),
      bench('FetchHttpClient    - SSE first-message', () => sseLatency(fetchClient)),
      bench('StreamHttpClient   - SSE first-message', () => sseLatency(streamClient)),
      bench('PipelineHttpClient - SSE first-message', () => sseLatency(pipelineClient)),
      bench('DispatchHttpClient - SSE first-message', () => sseLatency(dispatchClient)),
    );
  });
});

// ─── SECTION 4: Long-polling round-trip ───────────────────────────────────────
//
// Each iteration:
//   1. POST a Ping message (send path)
//   2. GET /hub/poll (receive path - server returns the echoed message)

describe('Long-polling round-trip - per HTTP client', () => {

  const PING_WIRE = JSON.stringify({ type: MessageType.Ping }) + RECORD_SEPARATOR;

  async function lpRoundtrip(httpClient: IHttpClient): Promise<void> {
    const id      = Math.random().toString(36).slice(2);
    const baseUrl = `${srv.hubUrl}?id=${id}`;
    const pollUrl = `${srv.origin}/hub/poll?id=${id}`;

    // First GET establishes the connection and returns the handshake
    await httpClient.get(pollUrl);

    // POST the message (server buffers it)
    await httpClient.post(baseUrl, {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body:    PING_WIRE,
    });

    // Poll to receive it
    await httpClient.get(pollUrl);

    // Disconnect
    await httpClient.delete(baseUrl);
  }

  test('compare', async ({ bench }) => {
    await bench.compare(
      bench('RequestHttpClient  - LP round-trip', () => lpRoundtrip(reqClient)),
      bench('FetchHttpClient    - LP round-trip', () => lpRoundtrip(fetchClient)),
      bench('StreamHttpClient   - LP round-trip', () => lpRoundtrip(streamClient)),
      bench('PipelineHttpClient - LP round-trip', () => lpRoundtrip(pipelineClient)),
      bench('DispatchHttpClient - LP round-trip', () => lpRoundtrip(dispatchClient)),
    );
  });
});

// ─── SECTION 5: Protocol overhead on WebSocket (JSON vs MessagePack) ──────────
//
// Uses a single pre-established WebSocket connection; each iteration sends a
// pre-encoded wire frame and awaits the echo.  Separates transport overhead
// from protocol overhead by keeping the transport constant.

// Pre-encode the MessagePack Ping wire frame at module load time
const _mpPingBuf   = new MsgpackHubProtocol().writeMessage({ type: MessageType.Ping });
const _mpPingBytes = new Uint8Array(_mpPingBuf);

describe('WS echo - JSON vs MessagePack wire payload', () => {

  let wsConn: {
    transport: WebSocketTransport;
    send: (data: string | Uint8Array) => Promise<void>;
  };

  const jsonPingWire = JSON.stringify({ type: MessageType.Ping }) + RECORD_SEPARATOR;
  const mpPingBytes  = _mpPingBytes;

  beforeAll(async () => {
    const transport = new WebSocketTransport(null, log, {}, undefined);
    const wsUrl = srv.hubUrl.replace('http://', 'ws://');
    await transport.connect(wsUrl, TransferFormat.Text);

    // Complete handshake
    await new Promise<void>((resolve, reject) => {
      transport.onreceive = (data: string | Uint8Array) => {
        const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
        if (text.includes(RECORD_SEPARATOR)) {
          transport.onreceive = null;
          resolve();
        }
      };
      transport.send(
        JSON.stringify({ protocol: 'json', version: 1 }) + RECORD_SEPARATOR,
      ).catch(reject);
    });

    wsConn = { transport, send: (d) => transport.send(d) };
  }, 15_000);

  afterAll(async () => {
    await wsConn.transport.stop().catch(() => {});
  });

  function echoRoundtrip(data: string | Uint8Array): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      wsConn.transport.onreceive = () => {
        wsConn.transport.onreceive = null;
        resolve();
      };
      wsConn.send(data).catch(reject);
    });
  }

  test('compare', async ({ bench }) => {
    await bench.compare(
      bench('JSON   Ping wire (WS echo)', () => echoRoundtrip(jsonPingWire)),
      bench('MP     Ping wire (WS echo)', () => echoRoundtrip(mpPingBytes)),
    );
  });
});
