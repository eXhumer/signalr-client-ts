/**
 * helpers/signalr-server.ts
 *
 * Minimal in-process SignalR-compatible server for integration-style tests.
 * Handles WebSocket upgrade + SignalR JSON protocol handshake, then relays
 * hub messages back to the test via callbacks.
 *
 * Usage:
 *   const srv = await startSignalRServer();
 *   // ...
 *   await srv.close();
 */

import * as http   from 'node:http';
import * as net    from 'node:net';
import * as crypto from 'node:crypto';
import { EventEmitter } from 'node:events';
import { RECORD_SEPARATOR } from '../../src/constants.js';

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ─── Types ────────────────────────────────────────────────────────────────────

export interface ServerClient extends EventEmitter {
  /** Send a raw SignalR message (record separator appended automatically). */
  sendMessage(payload: Record<string, unknown>): void;
  /**
   * Close the WebSocket connection abruptly (TCP destroy).
   * The client sees an error because there is no proper WS close frame.
   */
  close(): void;
  /**
   * Close the WebSocket connection gracefully by sending a WS Close frame
   * (opcode 0x08, no payload) before tearing down the TCP socket.
   * The client receives a clean close event with no error, which means
   * `#onTransportClosed` is invoked with `err = undefined`.
   */
  closeGracefully(): void;
  on(event: 'message', listener: (msg: Record<string, unknown>) => void): this;
  on(event: 'close',   listener: () => void): this;
}

/**
 * A single captured HTTP / WebSocket request, including its headers.
 * Used by tests that need to assert on query-string params or cookie forwarding.
 */
export interface CapturedRequest {
  /** Request-line URL (path + query string, no host). */
  readonly url:     string;
  /** All request headers, lower-cased. */
  readonly headers: Readonly<Record<string, string | string[] | undefined>>;
}

export interface SignalRTestServer {
  readonly port: number;
  readonly url:  string;
  /**
   * All HTTP request URLs seen since the server started (or since the last
   * time the array was manually cleared).  Includes negotiate POSTs and
   * WebSocket upgrade request lines.  Useful for asserting query-string
   * propagation.
   */
  readonly requestUrls: string[];
  /**
   * All captured requests (URL + headers) in arrival order.
   * Clear between tests with `.requests.length = 0`.
   */
  readonly requests: CapturedRequest[];
  /**
   * Extra response headers to include in the next negotiate response.
   * Set these before the client calls `start()` and clear them afterwards.
   * Primarily used to inject `Set-Cookie` headers for cookie tests.
   */
  negotiateResponseHeaders: Record<string, string>;
  /**
   * When set to a non-empty string, the server sends this JSON (plus a record
   * separator) as the handshake response instead of the normal `{}\x1e`.
   * Reset to `''` after each use so it doesn't affect subsequent connections.
   * Used to test error-handshake paths (L544-549 in hub-connection.ts).
   */
  handshakeResponse: string;
  /**
   * When `true`, the server accepts the WebSocket upgrade and waits for the
   * client's SignalR handshake request, but deliberately never sends a
   * handshake response.  Used to trigger the handshake-timeout code-path
   * (L521 in hub-connection.ts).  Reset to `false` after each use.
   */
  hangHandshake: boolean;
  /** Resolves when the next client connects; returns control handle. */
  nextClient(): Promise<ServerClient>;
  close(): Promise<void>;
}

// ─── Implementation ───────────────────────────────────────────────────────────

export async function startSignalRServer(): Promise<SignalRTestServer> {
  const pending:     Array<(c: ServerClient) => void> = [];
  let   handshakeResponse = '';
  let   hangHandshake     = false;
  const activeSockets = new Set<net.Socket>();
  const requestUrls:  string[] = [];
  const requests:     CapturedRequest[] = [];
  let   negotiateResponseHeaders: Record<string, string> = {};
  const server = http.createServer();

  // Track every TCP socket (both plain-HTTP and WebSocket-upgraded) so that
  // close() can forcibly destroy them even if they are still half-open.
  // WebSocketTransport.stop() calls ws.close() and returns immediately without
  // waiting for the TCP teardown; as a result the underlying socket can still
  // be "live" from the server's perspective when afterAll calls srv.close().
  // Manually destroying all sockets ensures server.close() drains quickly.
  server.on('connection', (sock: net.Socket) => {
    activeSockets.add(sock);
    sock.on('close', () => activeSockets.delete(sock));
    // Suppress ECONNRESET / other errors that fire when we destroy them.
    sock.on('error', () => {});
  });

  server.on('request', (req, res) => {
    if (req.url) requestUrls.push(req.url);
    requests.push({ url: req.url ?? '', headers: req.headers as Record<string, string | string[] | undefined> });
    if (req.url?.includes('/negotiate')) {
      // Return a minimal negotiate response, forwarding any extra headers
      // configured by the test (e.g. Set-Cookie).
      const connectionId    = crypto.randomUUID();
      const connectionToken = crypto.randomUUID();
      res.writeHead(200, {
        'Content-Type': 'application/json',
        ...negotiateResponseHeaders,
      });
      res.end(JSON.stringify({
        negotiateVersion:    1,
        connectionId,
        connectionToken,
        availableTransports: [{ transport: 'WebSockets', transferFormats: ['Text'] }],
      }));
    } else {
      res.writeHead(404);
      res.end();
    }
  });

  server.on('upgrade', (req, socket: net.Socket, head: Buffer) => {
    // Note: error suppression is already set up in the 'connection' handler
    // above for the same socket object.
    if (req.url) requestUrls.push(req.url);
    requests.push({ url: req.url ?? '', headers: req.headers as Record<string, string | string[] | undefined> });
    void handleUpgrade(req, socket, head, pending, handshakeResponse, hangHandshake);
    handshakeResponse = ''; // reset after each use
    hangHandshake     = false; // reset after each use
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;

  return {
    port,
    url: `http://127.0.0.1:${port}/hub`,
    requestUrls,
    requests,
    get negotiateResponseHeaders() { return negotiateResponseHeaders; },
    set negotiateResponseHeaders(v: Record<string, string>) { negotiateResponseHeaders = v; },
    get handshakeResponse() { return handshakeResponse; },
    set handshakeResponse(v: string) { handshakeResponse = v; },
    get hangHandshake() { return hangHandshake; },
    set hangHandshake(v: boolean) { hangHandshake = v; },

    nextClient(): Promise<ServerClient> {
      return new Promise<ServerClient>((resolve) => {
        pending.push(resolve);
      });
    },

    close(): Promise<void> {
      return new Promise<void>((resolve, reject) => {
        // Forcibly destroy every tracked socket (HTTP keep-alive + WebSocket)
        // so that server.close() drains in milliseconds rather than waiting
        // the full hookTimeout for connections to idle out.
        for (const sock of activeSockets) sock.destroy();
        server.close((err) => (err ? reject(err) : resolve()));
      });
    },
  };
}

// ─── WebSocket upgrade handler ────────────────────────────────────────────────

async function handleUpgrade(
  req:              http.IncomingMessage,
  socket:           net.Socket,
  _head:            Buffer,
  pending:          Array<(c: ServerClient) => void>,
  hsResponseOverride: string = '',
  hangHandshake:      boolean = false,
): Promise<void> {
  const key    = req.headers['sec-websocket-key'];
  if (typeof key !== 'string') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');

  socket.write([
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${accept}`,
    '',
    '',
  ].join('\r\n'));

  const client = new ServerClientImpl(socket);

  // Wait for the SignalR handshake request, then respond
  const firstMsg = await new Promise<string>((resolve) => {
    client.once('rawMessage', resolve as (s: string) => void);
  });

  // When hangHandshake is true we intentionally never send a response so that
  // the client's handshake timer (L521 in hub-connection.ts) fires.
  if (hangHandshake) return;

  // Echo back an empty handshake response (or an override for error-path tests)
  if (firstMsg.includes('"protocol"')) {
    socket.write(encodeTextFrame(hsResponseOverride || '{}\x1e'));
  }

  const resolver = pending.shift();
  resolver?.(client);
}

// ─── ServerClientImpl ─────────────────────────────────────────────────────────

class ServerClientImpl extends EventEmitter implements ServerClient {
  readonly #socket: net.Socket;
  #buf: Buffer = Buffer.alloc(0);

  constructor(socket: net.Socket) {
    super();
    this.#socket = socket;
    socket.on('data',  (c: Buffer) => this.#onData(c));
    socket.on('close', ()          => this.emit('close'));
  }

  sendMessage(payload: Record<string, unknown>): void {
    const wire = JSON.stringify(payload) + RECORD_SEPARATOR;
    this.#socket.write(encodeTextFrame(wire));
  }

  close(): void { this.#socket.destroy(); }

  closeGracefully(): void {
    // RFC 6455 close frame: FIN=1, opcode=8 (CLOSE), no mask, no payload.
    // Sending this causes undici's WebSocket to fire the 'close' event
    // without an error, so #onTransportClosed receives err=undefined.
    this.#socket.write(Buffer.from([0x88, 0x00]));
    // Allow the write to flush and the client to echo before tearing down.
    this.#socket.end();
  }

  #onData(chunk: Buffer): void {
    this.#buf = Buffer.concat([this.#buf, chunk]);
    let result;
    while ((result = decodeFrame(this.#buf)) !== null) {
      const { text, consumed } = result;
      this.#buf = this.#buf.subarray(consumed);
      this.emit('rawMessage', text);
      for (const part of text.split(RECORD_SEPARATOR)) {
        if (part.trim()) {
          try { this.emit('message', JSON.parse(part) as Record<string, unknown>); }
          catch { /* ignore */ }
        }
      }
    }
  }
}

// ─── WebSocket frame helpers (server-side, no masking) ───────────────────────

function encodeTextFrame(text: string): Buffer {
  const payload = Buffer.from(text, 'utf8');
  const len     = payload.length;
  let header: Buffer;
  if (len < 126) {
    header    = Buffer.allocUnsafe(2);
    header[0] = 0x81; // FIN + TEXT
    header[1] = len;
  } else if (len < 65_536) {
    header    = Buffer.allocUnsafe(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header    = Buffer.allocUnsafe(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeUInt32BE(0,   2);
    header.writeUInt32BE(len, 6);
  }
  return Buffer.concat([header, payload]);
}

function decodeFrame(buf: Buffer): { text: string; consumed: number } | null {
  if (buf.length < 2) return null;
  const b1     = buf[1] as number;
  const masked = (b1 & 0x80) !== 0;
  let payLen   = b1 & 0x7f;
  let offset   = 2;

  if (payLen === 126) {
    if (buf.length < 4) return null;
    payLen  = buf.readUInt16BE(2);
    offset  = 4;
  } else if (payLen === 127) {
    if (buf.length < 10) return null;
    payLen  = buf.readUInt32BE(6);
    offset  = 10;
  }

  let maskKey: Buffer | null = null;
  if (masked) {
    if (buf.length < offset + 4) return null;
    maskKey  = buf.subarray(offset, offset + 4);
    offset  += 4;
  }

  if (buf.length < offset + payLen) return null;

  let payload = buf.subarray(offset, offset + payLen);
  if (masked && maskKey) {
    payload = Buffer.from(payload);
    for (let i = 0; i < payload.length; i++) {
      payload[i] = (payload[i] as number) ^ (maskKey[i % 4] as number);
    }
  }

  return { text: payload.toString('utf8'), consumed: offset + payLen };
}
