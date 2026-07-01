/**
 * websocket-transport.test.ts
 *
 * Unit tests for WebSocketTransport - the undici-WebSocket-backed SignalR
 * transport introduced in the undici rewrite.
 *
 * Test server strategy
 * ────────────────────
 * We spin up a real in-process HTTP server and handle WebSocket upgrades via
 * Node's `server.on('upgrade', ...)` event.  After the handshake, the raw
 * socket carries RFC 6455 WebSocket frames, which we encode/decode manually
 * so the test has full visibility into what the client sent and can send
 * precisely crafted frames back.
 *
 * Why not the raw-TCP approach from ws-client.test.ts?
 * ─────────────────────────────────────────────────────
 * The old WebSocketClient connected at the TCP level and sent a hand-crafted
 * HTTP upgrade request.  undici's built-in WebSocket uses the higher-level
 * `http.ClientRequest` path internally, so it works correctly against a
 * proper `http.Server` that fires the `upgrade` event.  A raw `net.Server`
 * also works (undici sends a standard HTTP/1.1 upgrade), but using
 * `http.Server` lets us inspect request headers (Authorization, etc.) more
 * easily.
 *
 * Behaviors under test
 * ────────────────────
 *  1.  connect() resolves when the WebSocket handshake succeeds
 *  2.  connect() converts http:// URL to ws://
 *  3.  connect() rejects when server is unreachable (ECONNREFUSED)
 *  4.  Text frame from server → onreceive fires with a string
 *  5.  Binary frame from server → onreceive fires with a Buffer
 *      (undici delivers ArrayBuffer; transport converts to Buffer)
 *  6.  send(string) → server receives a masked text frame
 *  7.  send(Uint8Array) → server receives a masked binary frame
 *  8.  stop() sends a close frame; onclose does NOT fire (intentional stop)
 *  9.  Server sends close frame → onclose fires without an error
 * 10.  accessTokenFactory → Authorization header present in upgrade request
 * 11.  Custom Agent dispatcher accepted and connection succeeds
 */

import { describe, it, beforeAll, afterAll, afterEach, expect } from 'vitest';
import * as http   from 'node:http';
import * as net    from 'node:net';
import * as crypto from 'node:crypto';
import { Agent, type Dispatcher } from 'undici';

import { WebSocketTransport } from '../../src/transports/websocket-transport.js';
import { TransferFormat }     from '../../src/constants.js';
import { MockLogger }         from '../helpers/mock-logger.js';
import { closeTrackedDispatchers, trackDispatcher } from '../helpers/dispatcher-tracker.js';

afterEach(closeTrackedDispatchers);

// ─── RFC 6455 constants ───────────────────────────────────────────────────────

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function computeAccept(key: string): string {
  return crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
}

// ─── Frame helpers (server-side, no masking: server → client) ────────────────

/**
 * Encode a WebSocket frame that the server sends to the client.
 * Server-to-client frames are NOT masked per RFC 6455.
 */
function encodeFrame(opcode: number, payload: Buffer): Buffer {
  const fin = 0x80; // always FIN for these tests (single-fragment messages)
  const b0  = fin | opcode;
  const len = payload.length;

  if (len < 126) {
    return Buffer.concat([Buffer.from([b0, len]), payload]);
  }
  if (len < 65_536) {
    const h = Buffer.allocUnsafe(4);
    h[0] = b0; h[1] = 126; h.writeUInt16BE(len, 2);
    return Buffer.concat([h, payload]);
  }
  // >= 65536
  const h = Buffer.allocUnsafe(10);
  h[0] = b0; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(len, 6);
  return Buffer.concat([h, payload]);
}

const textFrame   = (s: string): Buffer  => encodeFrame(0x1, Buffer.from(s, 'utf8'));
const binaryFrame = (b: Buffer): Buffer  => encodeFrame(0x2, b);
const closeFrame  = (code = 1000): Buffer => {
  const b = Buffer.allocUnsafe(2);
  b.writeUInt16BE(code, 0);
  return encodeFrame(0x8, b);
};

// ─── Frame helpers (client → server, masked) ─────────────────────────────────

/**
 * Parse one WebSocket frame from a raw buffer.
 * Client-to-server frames are MASKED per RFC 6455.
 * Returns null if the buffer doesn't yet contain a complete frame.
 */
function parseFrame(
  buf: Buffer,
): { opcode: number; payload: Buffer; consumed: number } | null {
  if (buf.length < 2) return null;
  const opcode = (buf[0] ?? 0) & 0x0f;
  const masked  = ((buf[1] ?? 0) & 0x80) !== 0;
  let   payLen  = (buf[1] ?? 0) & 0x7f;
  let   offset  = 2;

  if (payLen === 126) {
    if (buf.length < 4) return null;
    payLen = buf.readUInt16BE(2);
    offset = 4;
  } else if (payLen === 127) {
    if (buf.length < 10) return null;
    // Only use low 32 bits (tests don't exceed 4 GB)
    payLen = buf.readUInt32BE(6);
    offset = 10;
  }

  const maskLen = masked ? 4 : 0;
  if (buf.length < offset + maskLen + payLen) return null;

  let payload: Buffer;
  if (masked) {
    const key = buf.subarray(offset, offset + 4);
    const raw = buf.subarray(offset + 4, offset + 4 + payLen);
    payload   = Buffer.from(raw);
    for (let i = 0; i < payload.length; i++) {
      payload[i] = ((payload[i] ?? 0) ^ (key[i % 4] ?? 0));
    }
    offset += 4;
  } else {
    payload = buf.subarray(offset, offset + payLen);
  }

  return { opcode, payload, consumed: offset + payLen };
}

// ─── WsTestClient - server-side handle to one connected client ────────────────

interface WsTestClient {
  /** Headers from the upgrade request (Authorization, etc.). */
  readonly upgradeHeaders: http.IncomingHttpHeaders;
  sendText(s: string): void;
  sendBinary(b: Buffer): void;
  sendClose(code?: number): void;
  destroy(): void;
  /** Resolves with the next complete frame the client sent. */
  nextFrame(): Promise<{ opcode: number; payload: Buffer }>;
}

class WsTestClientImpl implements WsTestClient {
  readonly upgradeHeaders: http.IncomingHttpHeaders;

  readonly #socket:       net.Socket;
  #buf:                   Buffer = Buffer.alloc(0);
  readonly #incoming:     Array<{ opcode: number; payload: Buffer }> = [];
  readonly #waiters:      Array<(f: { opcode: number; payload: Buffer }) => void> = [];

  constructor(socket: net.Socket, headers: http.IncomingHttpHeaders) {
    this.#socket        = socket;
    this.upgradeHeaders = headers;

    // Suppress ECONNRESET when the transport closes the connection
    socket.on('error', () => {});

    socket.on('data', (chunk: Buffer) => {
      this.#buf = Buffer.concat([this.#buf, chunk]);
      let frame: ReturnType<typeof parseFrame>;
      while ((frame = parseFrame(this.#buf)) !== null) {
        this.#buf = this.#buf.subarray(frame.consumed);
        const { opcode, payload } = frame;
        const waiter = this.#waiters.shift();
        if (waiter) {
          waiter({ opcode, payload });
        } else {
          this.#incoming.push({ opcode, payload });
        }
      }
    });
  }

  sendText(s: string):          void { this.#socket.write(textFrame(s)); }
  sendBinary(b: Buffer):        void { this.#socket.write(binaryFrame(b)); }
  sendClose(code?: number):     void { this.#socket.write(closeFrame(code)); }
  destroy():                    void { this.#socket.destroy(); }

  nextFrame(): Promise<{ opcode: number; payload: Buffer }> {
    const queued = this.#incoming.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise((resolve) => this.#waiters.push(resolve));
  }
}

// ─── WsTestServer ─────────────────────────────────────────────────────────────

interface WsTestServer {
  readonly port: number;
  stop(): Promise<void>;
  /** Resolves with the handle for the next client that completes the handshake. */
  nextClient(): Promise<WsTestClient>;
}

async function createWsTestServer(): Promise<WsTestServer> {
  const pending: Array<(c: WsTestClient) => void> = [];
  const server = http.createServer();

  server.on('upgrade', (req: http.IncomingMessage, socket: net.Socket) => {
    const key = req.headers['sec-websocket-key'];
    if (typeof key !== 'string') { socket.destroy(); return; }

    // Complete the RFC 6455 handshake
    socket.write([
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${computeAccept(key)}`,
      '',
      '',
    ].join('\r\n'));

    const client = new WsTestClientImpl(socket, req.headers);
    const resolve = pending.shift();
    resolve?.(client);
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;

  return {
    port,
    nextClient: () => new Promise<WsTestClient>((r) => pending.push(r)),
    stop: () => new Promise<void>((r) => {
      server.closeAllConnections();
      server.close(() => r());
    }),
  };
}

// ─── Helper: build a transport ────────────────────────────────────────────────

function makeTransport(opts: {
  factory?:    (() => Promise<string | null>) | null;
  headers?:    Record<string, string>;
  dispatcher?: Dispatcher;
} = {}): { transport: WebSocketTransport; logger: MockLogger } {
  const logger    = new MockLogger();
  const transport = new WebSocketTransport(
    opts.factory  ?? null,
    logger,
    opts.headers  ?? {},
    // Use a fresh Agent per transport so connection-pool state from one test
    // cannot contaminate the next test's WebSocket handshake.
    opts.dispatcher ?? trackDispatcher(new Agent()),
  );
  return { transport, logger };
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('WebSocketTransport', () => {
  let srv: WsTestServer;

  beforeAll(async () => {
    srv = await createWsTestServer();
  });

  afterAll(async () => {
    await srv.stop();
  });

  // ── 1. connect() resolves on successful handshake ─────────────────────────

  it('connect() resolves when the WebSocket handshake succeeds', async () => {
    const { transport } = makeTransport();
    const clientP = srv.nextClient();

    // The transport converts http:// → ws:// internally.
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);

    const client = await clientP;
    expect(client.upgradeHeaders['upgrade']).toBe('websocket');

    await transport.stop();
    client.destroy();
  });

  // ── 2. connect() converts http:// URL to ws:// ────────────────────────────

  it('connect() converts http:// URL to ws:// (WebSocket handshake succeeds)', async () => {
    // The fact that connect() works at all when given an http:// URL proves the
    // internal http→ws rewrite is in place.
    const { transport } = makeTransport();
    const clientP = srv.nextClient();

    // If this rejects, the test will fail - no need for doesNotReject wrapper.
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);

    const client = await clientP;
    await transport.stop();
    client.destroy();
  });

  // ── 3. connect() rejects when server is unreachable ──────────────────────

  it('connect() rejects with an error when server is unreachable', async () => {
    const { transport } = makeTransport();
    // Port 1 is never open in test environments.
    await expect(
      () => transport.connect('http://127.0.0.1:1', TransferFormat.Text),
    ).rejects.toThrow(/ECONNREFUSED|connect|network/i);
  });

  // ── 4. Text frames: server → client → onreceive ──────────────────────────

  it('server text frame → onreceive fires with the string payload', async () => {
    const { transport } = makeTransport();
    const clientP       = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;

    const receivedP = new Promise<string | Uint8Array>((resolve) => {
      transport.onreceive = resolve;
    });

    client.sendText('hello from server');
    const received = await receivedP;
    expect(typeof received).toBe('string');
    expect(received).toBe('hello from server');

    await transport.stop();
    client.destroy();
  });

  // ── 5. Binary frames: server → client → onreceive (ArrayBuffer → Buffer) ──

  it('server binary frame → onreceive fires with a Buffer', async () => {
    // binaryType = 'arraybuffer' so undici gives us ArrayBuffer;
    // the transport converts to Buffer via Buffer.from(data).
    const { transport } = makeTransport();
    const clientP       = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Binary);
    const client = await clientP;

    const receivedP = new Promise<string | Uint8Array>((resolve) => {
      transport.onreceive = resolve;
    });

    const rawData = Buffer.from([0x01, 0x02, 0x03, 0xde, 0xad, 0xbe, 0xef]);
    client.sendBinary(rawData);
    const received = await receivedP;

    // The transport wraps it in a Buffer (which IS a Uint8Array)
    expect(Buffer.isBuffer(received), 'Expected a Buffer').toBeTruthy();
    expect(Buffer.from(received as Buffer)).toEqual(rawData);

    await transport.stop();
    client.destroy();
  });

  // ── 6. send(string) → server receives a text frame ───────────────────────

  it('send(string) delivers a masked text frame to the server', async () => {
    const { transport } = makeTransport();
    const clientP       = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;

    await transport.send('hello from client');

    const frame = await client.nextFrame();
    expect(frame.opcode, 'Expected text opcode (0x1)').toBe(0x1);
    expect(frame.payload.toString('utf8')).toBe('hello from client');

    await transport.stop();
    client.destroy();
  });

  // ── 7. send(Uint8Array) → server receives a binary frame ─────────────────

  it('send(Uint8Array) delivers a masked binary frame to the server', async () => {
    const { transport } = makeTransport();
    const clientP       = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Binary);
    const client = await clientP;

    const data = new Uint8Array([0xca, 0xfe, 0xba, 0xbe]);
    await transport.send(data);

    const frame = await client.nextFrame();
    expect(frame.opcode, 'Expected binary opcode (0x2)').toBe(0x2);
    expect(frame.payload).toEqual(Buffer.from(data));

    await transport.stop();
    client.destroy();
  });

  // ── 8. stop() → close frame sent; onclose does NOT fire ──────────────────

  it('stop() sends a close frame and does not trigger onclose', async () => {
    const { transport } = makeTransport();
    const clientP       = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;

    let closeCalled = false;
    transport.onclose = (): void => { closeCalled = true; };

    await transport.stop();

    // The transport clears #ws before calling ws.close(), so the close event
    // handler sees #ws === null and skips the onclose call.
    const frame = await client.nextFrame();
    expect(frame.opcode, 'Expected close opcode (0x8)').toBe(0x8);

    // Give a short window for any spurious callbacks
    await new Promise<void>((r) => setTimeout(r, 30));
    expect(closeCalled, 'onclose should NOT fire on intentional stop()').toBe(false);

    client.destroy();
  });

  // ── 9. Server sends close frame → onclose fires ───────────────────────────

  it('server close frame → onclose fires without an error argument', async () => {
    const { transport } = makeTransport();
    const clientP       = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;

    const closedP = new Promise<Error | undefined>((resolve) => {
      transport.onclose = (err?: Error): void => resolve(err);
    });

    client.sendClose(1000);
    // Per RFC 6455, after the close-frame exchange the initiator (server here)
    // must close the TCP connection.  undici (the receiver) echoes the close
    // frame and then waits for a TCP FIN/RST before firing its 'close' event.
    // Consume undici's close-frame echo, then destroy the server socket so
    // undici sees the TCP teardown and fires 'close' cleanly (no error).
    await client.nextFrame();  // undici's close-frame echo
    client.destroy();          // server-side TCP close → undici fires 'close'
    const err = await closedP;
    expect(err, 'onclose should receive no error for a clean server close').toBe(undefined);
  });

  // ── 10. accessTokenFactory → Authorization header on upgrade request ──────

  it('accessTokenFactory result appears as Authorization header on the upgrade request', async () => {
    const factory = async (): Promise<string | null> => 'test-bearer-token';
    const { transport } = makeTransport({ factory });
    const clientP       = srv.nextClient();

    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;

    expect(
      client.upgradeHeaders['authorization'],
      'Authorization header should carry the factory token',
    ).toBe('Bearer test-bearer-token');

    await transport.stop();
    client.destroy();
  });

  // ── 11. Custom Agent dispatcher is accepted ───────────────────────────────

  it('withDispatcher(Agent) - connection succeeds using the custom dispatcher', async () => {
    const agent = trackDispatcher(new Agent({ connections: 2 }));
    const { transport } = makeTransport({ dispatcher: agent });
    const clientP       = srv.nextClient();

    // If this rejects, the test will fail - no wrapper needed.
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);

    const client = await clientP;
    await transport.stop();
    client.destroy();
  });

  // ── 12. Binary frame with onreceive null is silently ignored (L166 ?. else) ──

  it('binary frame received when onreceive is null does not throw (L166 ?. else branch)', async () => {
    const { transport } = makeTransport();
    const clientP = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Binary);
    const client = await clientP;

    // onreceive is null (not set) — the optional chain ?. must silently skip the call
    expect(transport.onreceive).toBeNull();

    client.sendBinary(Buffer.from([0xca, 0xfe, 0xba, 0xbe]));
    // Allow time for the message event to be processed
    await new Promise<void>((r) => setTimeout(r, 60));

    // Test passes if no unhandled exception was thrown
    await transport.stop();
    client.destroy();
  });

  // ── 14. Constructor: omitting extraHeaders uses default {} (line 76) ─────────

  it('constructed without extraHeaders arg still connects (default {} branch, line 76)', async () => {
    // Omitting the 3rd argument exercises the `extraHeaders = {}` default parameter.
    const logger    = new MockLogger();
    const transport = new WebSocketTransport(
      null,
      logger,
      // intentionally omit extraHeaders → default {} is used
    );
    const clientP = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;
    expect(client.upgradeHeaders['authorization']).toBeUndefined();
    await transport.stop();
    client.destroy();
  });

  // ── 15. accessTokenFactory returns null → no Authorization header (if(token) FALSE) ──

  it('accessTokenFactory returning null sends no Authorization header (if(token) FALSE branch, line 93)', async () => {
    // Factory is non-null (so the outer `if (this.#accessTokenFactory)` is TRUE),
    // but it returns null, exercising the `if (token)` FALSE branch.
    const factory = async (): Promise<string | null> => null;
    const { transport } = makeTransport({ factory });
    const clientP = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;
    expect(client.upgradeHeaders['authorization']).toBeUndefined();
    await transport.stop();
    client.destroy();
  });

  // ── 14. send() rejects when the socket is not open ───────────────────────

  it('send() rejects with an error when the WebSocket is not open', async () => {
    const { transport } = makeTransport();
    // Not connected yet - send must reject
    await expect(
      () => transport.send('too early'),
    ).rejects.toThrow(/not open/i);
  });

  // ── 15. send() after stop() rejects ──────────────────────────────────────

  it('send() rejects after stop()', async () => {
    const { transport } = makeTransport();
    const clientP       = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;

    await transport.stop();
    client.destroy();

    await expect(
      () => transport.send('after stop'),
    ).rejects.toThrow(/not open/i);
  });

  // ── 16. Multiple messages in sequence ────────────────────────────────────

  it('multiple server text frames are received in order', async () => {
    const { transport } = makeTransport();
    const clientP       = srv.nextClient();
    await transport.connect(`http://127.0.0.1:${srv.port}`, TransferFormat.Text);
    const client = await clientP;

    const received: string[] = [];
    const allDone = new Promise<void>((resolve) => {
      transport.onreceive = (data: string | Uint8Array): void => {
        received.push(typeof data === 'string' ? data : Buffer.from(data).toString());
        if (received.length === 3) resolve();
      };
    });

    client.sendText('msg-1');
    client.sendText('msg-2');
    client.sendText('msg-3');

    await allDone;
    expect(received).toEqual(['msg-1', 'msg-2', 'msg-3']);

    await transport.stop();
    client.destroy();
  });
});
