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
 * │ SECTION 2 - WebSocket transport throughput                              │
 * │   A single long-lived WebSocket connection is established once in       │
 * │   beforeAll, then each iteration sends one JSON Ping and waits for the  │
 * │   echo. Tests all five HTTP clients for the negotiate step.             │
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

import { bench, describe, beforeAll, afterAll } from 'vitest';

import {
  RequestHttpClient,
  FetchHttpClient,
  StreamHttpClient,
  PipelineHttpClient,
  DispatchHttpClient,
} from '../../src/http-client.js';
import { startBenchServer } from '../helpers/bench-server.js';
import { JsonHubProtocol }    from '../../src/protocols/json-hub-protocol.js';
import { MsgpackHubProtocol } from '../../src/protocols/msgpack-hub-protocol.js';
import { MessageType, RECORD_SEPARATOR, TransferFormat } from '../../src/constants.js';
import { NullLogger } from '../../src/logger.js';
import { WebSocketTransport } from '../../src/transports/websocket-transport.js';
import type { BenchServer }   from '../helpers/bench-server.js';
import type { IHttpClient }   from '../../src/interfaces.js';

// ─── Shared state ─────────────────────────────────────────────────────────────

let srv: BenchServer;

const log  = NullLogger.instance;
const json = new JsonHubProtocol();

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

  bench('RequestHttpClient  (undici.request)', async () => {
    await reqClient.post(`${srv.hubUrl}/negotiate`, {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body:    '',
    });
  });

  bench('FetchHttpClient    (undici.fetch)', async () => {
    await fetchClient.post(`${srv.hubUrl}/negotiate`, {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body:    '',
    });
  });

  bench('StreamHttpClient   (undici.stream)', async () => {
    await streamClient.post(`${srv.hubUrl}/negotiate`, {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body:    '',
    });
  });

  bench('PipelineHttpClient (undici.pipeline)', async () => {
    await pipelineClient.post(`${srv.hubUrl}/negotiate`, {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body:    '',
    });
  });

  bench('DispatchHttpClient (Dispatcher#dispatch)', async () => {
    await dispatchClient.post(`${srv.hubUrl}/negotiate`, {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body:    '',
    });
  });
});

// ─── SECTION 2: WebSocket message round-trip ─────────────────────────────────
//
// We pre-establish one WebSocket connection per HTTP-client variant in
// beforeAll, then benchmark how fast each can do a Ping echo round-trip.

describe('WebSocket echo round-trip - per HTTP client for negotiate', () => {

  // Shared helper: connect WS transport and complete the SignalR handshake,
  // returning { transport, send } ready for the benchmark loop.
  async function openWsConnection(httpClient: IHttpClient): Promise<{
    transport: WebSocketTransport;
    send: (wire: string) => Promise<void>;
  }> {
    const transport = new WebSocketTransport(null, log, {}, undefined);
    const wsUrl = srv.hubUrl.replace('http://', 'ws://');

    // Connect the WebSocket transport (performs WS upgrade)
    await transport.connect(wsUrl, TransferFormat.Text);

    // Complete the SignalR JSON handshake
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

    const send = (wire: string) => transport.send(wire);
    return { transport, send };
  }

  let wsReq:      Awaited<ReturnType<typeof openWsConnection>>;
  let wsFetch:    Awaited<ReturnType<typeof openWsConnection>>;
  let wsStream:   Awaited<ReturnType<typeof openWsConnection>>;
  let wsPipeline: Awaited<ReturnType<typeof openWsConnection>>;
  let wsDispatch: Awaited<ReturnType<typeof openWsConnection>>;

  beforeAll(async () => {
    [wsReq, wsFetch, wsStream, wsPipeline, wsDispatch] = await Promise.all([
      openWsConnection(reqClient),
      openWsConnection(fetchClient),
      openWsConnection(streamClient),
      openWsConnection(pipelineClient),
      openWsConnection(dispatchClient),
    ]);
  }, 15_000);

  afterAll(async () => {
    await Promise.allSettled([
      wsReq.transport.stop(),
      wsFetch.transport.stop(),
      wsStream.transport.stop(),
      wsPipeline.transport.stop(),
      wsDispatch.transport.stop(),
    ]);
  });

  /**
   * Ping round-trip: send a JSON Ping frame, await the echo.
   */
  function pingRoundtrip(conn: { send: (wire: string) => Promise<void>; transport: WebSocketTransport }): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const pingWire = json.writeMessage({ type: MessageType.Ping }) as string;
      conn.transport.onreceive = () => {
        conn.transport.onreceive = null;
        resolve();
      };
      conn.send(pingWire).catch(reject);
    });
  }

  bench('RequestHttpClient  - WS Ping round-trip',  async () => {
    await pingRoundtrip(wsReq);
  });

  bench('FetchHttpClient    - WS Ping round-trip',  async () => {
    await pingRoundtrip(wsFetch);
  });

  bench('StreamHttpClient   - WS Ping round-trip',  async () => {
    await pingRoundtrip(wsStream);
  });

  bench('PipelineHttpClient - WS Ping round-trip',  async () => {
    await pingRoundtrip(wsPipeline);
  });

  bench('DispatchHttpClient - WS Ping round-trip',  async () => {
    await pingRoundtrip(wsDispatch);
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

  bench('RequestHttpClient  - SSE first-message', async () => {
    await sseLatency(reqClient);
  });

  bench('FetchHttpClient    - SSE first-message', async () => {
    await sseLatency(fetchClient);
  });

  bench('StreamHttpClient   - SSE first-message', async () => {
    await sseLatency(streamClient);
  });

  bench('PipelineHttpClient - SSE first-message', async () => {
    await sseLatency(pipelineClient);
  });

  bench('DispatchHttpClient - SSE first-message', async () => {
    await sseLatency(dispatchClient);
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

  bench('RequestHttpClient  - LP round-trip', async () => {
    await lpRoundtrip(reqClient);
  });

  bench('FetchHttpClient    - LP round-trip', async () => {
    await lpRoundtrip(fetchClient);
  });

  bench('StreamHttpClient   - LP round-trip', async () => {
    await lpRoundtrip(streamClient);
  });

  bench('PipelineHttpClient - LP round-trip', async () => {
    await lpRoundtrip(pipelineClient);
  });

  bench('DispatchHttpClient - LP round-trip', async () => {
    await lpRoundtrip(dispatchClient);
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

  bench('JSON   Ping wire (WS echo)', async () => {
    await echoRoundtrip(jsonPingWire);
  });

  bench('MP     Ping wire (WS echo)', async () => {
    await echoRoundtrip(mpPingBytes);
  });
});
