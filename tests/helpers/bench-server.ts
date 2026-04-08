/**
 * helpers/bench-server.ts
 *
 * In-process HTTP server that speaks enough SignalR to drive transport
 * benchmarks.  Supports all three transports on the same port:
 *
 *   POST /hub/negotiate    → JSON negotiate response (transport-selectable)
 *   WS   /hub              → WebSocket echo server (JSON SignalR handshake)
 *   GET  /hub?id=…         → SSE stream (JSON SignalR handshake via first data frame)
 *   POST /hub?id=…         → Inbound message for SSE or LP client
 *   GET  /hub/poll?id=…    → Long-poll endpoint (returns buffered messages)
 *   DELETE /hub?id=…       → Disconnect
 *
 * Every endpoint performs the SignalR JSON protocol handshake, then echoes
 * subsequent messages back to the sender.
 *
 * Usage:
 *   const srv = await startBenchServer();
 *   // run benchmarks...
 *   await srv.close();
 */

import * as http   from 'node:http';
import * as net    from 'node:net';
import * as crypto from 'node:crypto';
import { EventEmitter } from 'node:events';
import { RECORD_SEPARATOR } from '../../src/constants.js';

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ─── Public API ───────────────────────────────────────────────────────────────

export interface BenchServer {
  readonly port:   number;
  readonly origin: string; // http://127.0.0.1:{port}
  readonly hubUrl: string; // http://127.0.0.1:{port}/hub
  close(): Promise<void>;
}

// ─── Per-connection state ─────────────────────────────────────────────────────

interface SseClient {
  kind:  'sse';
  res:   http.ServerResponse;
}

interface LpClient {
  kind:       'lp';
  queue:      string[];
  waitingRes: http.ServerResponse | null;
  waitTimer:  ReturnType<typeof setTimeout> | null;
}

type ConnState = SseClient | LpClient;

// ─── Entry point ──────────────────────────────────────────────────────────────

export async function startBenchServer(): Promise<BenchServer> {
  const activeSockets = new Set<net.Socket>();
  const clients       = new Map<string, ConnState>();
  const server        = http.createServer();

  server.on('connection', (sock: net.Socket) => {
    activeSockets.add(sock);
    sock.on('close', () => activeSockets.delete(sock));
    sock.on('error', () => {});
  });

  server.on('request', (req, res) => {
    const url = new URL(req.url ?? '/', `http://127.0.0.1`);
    const id  = url.searchParams.get('id') ?? '';

    // ── POST /hub/negotiate ──────────────────────────────────────────────────
    if (req.method === 'POST' && url.pathname === '/hub/negotiate') {
      const connId    = crypto.randomUUID();
      const connToken = crypto.randomUUID();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        negotiateVersion: 1,
        connectionId:     connId,
        connectionToken:  connToken,
        availableTransports: [
          { transport: 'WebSockets',       transferFormats: ['Text', 'Binary'] },
          { transport: 'ServerSentEvents', transferFormats: ['Text'] },
          { transport: 'LongPolling',      transferFormats: ['Text', 'Binary'] },
        ],
      }));
      return;
    }

    // ── GET /hub - SSE stream ────────────────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/hub' &&
        req.headers['accept'] === 'text/event-stream') {
      // Suppress any socket errors that fire if the client disconnects early
      res.on('error', () => {});
      res.writeHead(200, {
        'Content-Type':  'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection':    'keep-alive',
      });
      // Send SignalR handshake response as first SSE data frame, then close.
      // Benchmarks only need first-message latency; closing from the server side
      // avoids the need for clients to call abort(), which corrupts undici pools.
      try { res.write(`data: ${JSON.stringify({})}\x1e\n\n`); } catch (_) {}
      res.end();
      return;
    }

    // ── POST /hub - inbound message (SSE send or LP send) ───────────────────
    if (req.method === 'POST' && url.pathname === '/hub') {
      let body = '';
      req.on('data', (chunk: Buffer) => { body += chunk.toString(); });
      req.on('end', () => {
        res.writeHead(200);
        res.end();

        const state = clients.get(id);
        if (!state) return;

        if (state.kind === 'sse') {
          // Echo back via SSE (client may have already disconnected)
          for (const part of body.split(RECORD_SEPARATOR)) {
            if (part.trim()) {
              try { state.res.write(`data: ${part}${RECORD_SEPARATOR}\n\n`); } catch (_) {}
            }
          }
        } else if (state.kind === 'lp') {
          // Queue message for next long-poll GET
          state.queue.push(body);
          flushLpQueue(state);
        }
      });
      return;
    }

    // ── GET /hub/poll - long-poll receive ────────────────────────────────────
    if (req.method === 'GET' && url.pathname === '/hub/poll') {
      let state = clients.get(id) as LpClient | undefined;
      if (!state) {
        // First poll - initialise client, respond with handshake
        state = { kind: 'lp', queue: [], waitingRes: null, waitTimer: null };
        clients.set(id, state);
        res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
        res.end(JSON.stringify({}) + RECORD_SEPARATOR);
        return;
      }
      if (state.queue.length > 0) {
        // Drain buffered messages immediately
        const body = state.queue.splice(0).join('');
        res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
        res.end(body);
      } else {
        // Hold the request until a message arrives or 5 s timeout
        state.waitingRes = res;
        state.waitTimer  = setTimeout(() => {
          if (state && state.waitingRes === res) {
            state.waitingRes = null;
            state.waitTimer  = null;
            res.writeHead(204);
            res.end();
          }
        }, 5_000);
      }
      return;
    }

    // ── DELETE /hub - disconnect ─────────────────────────────────────────────
    if (req.method === 'DELETE' && url.pathname === '/hub') {
      const state = clients.get(id);
      if (state?.kind === 'sse')      state.res.end();
      if (state?.kind === 'lp') {
        if (state.waitTimer)  clearTimeout(state.waitTimer);
        if (state.waitingRes) { state.waitingRes.writeHead(204); state.waitingRes.end(); }
      }
      clients.delete(id);
      res.writeHead(202);
      res.end();
      return;
    }

    res.writeHead(404);
    res.end();
  });

  // ── WebSocket upgrade - echo server ───────────────────────────────────────
  server.on('upgrade', (req, socket: net.Socket, _head: Buffer) => {
    socket.on('error', () => {});
    const key = req.headers['sec-websocket-key'];
    if (typeof key !== 'string') { socket.destroy(); return; }
    const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
    socket.write([
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${accept}`,
      '', '',
    ].join('\r\n'));

    const ws = new WsFramer(socket);
    // Send handshake response once client sends its handshake
    ws.once('message', (_data) => {
      ws.sendText(JSON.stringify({}) + RECORD_SEPARATOR);
    });
    // Echo everything after the first message
    ws.on('message', (data: Buffer) => {
      if (ws.isBinary) ws.sendBinary(data);
      else             ws.sendText(data.toString('utf8'));
    });
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;
  const origin = `http://127.0.0.1:${port}`;

  return {
    port,
    origin,
    hubUrl: `${origin}/hub`,
    close(): Promise<void> {
      return new Promise<void>((resolve, reject) => {
        for (const sock of activeSockets) sock.destroy();
        server.close((err) => (err ? reject(err) : resolve()));
      });
    },
  };
}

// ─── LP helper ────────────────────────────────────────────────────────────────

function flushLpQueue(state: LpClient): void {
  if (state.waitingRes && state.queue.length > 0) {
    const body = state.queue.splice(0).join('');
    if (state.waitTimer) clearTimeout(state.waitTimer);
    state.waitTimer  = null;
    const res = state.waitingRes;
    state.waitingRes = null;
    res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
    res.end(body);
  }
}

// ─── Minimal WebSocket framer (server-side, RFC 6455) ────────────────────────

class WsFramer extends EventEmitter {
  readonly #socket: net.Socket;
  #buf:    Buffer = Buffer.alloc(0);
  isBinary = false;

  constructor(socket: net.Socket) {
    super();
    this.#socket = socket;
    socket.on('data', (c: Buffer) => this.#onData(c));
    socket.on('close', () => this.emit('close'));
  }

  sendText(text: string): void {
    this.#send(0x81, Buffer.from(text, 'utf8'));
  }

  sendBinary(data: Buffer): void {
    this.#send(0x82, data);
  }

  #send(firstByte: number, payload: Buffer): void {
    const len = payload.length;
    let header: Buffer;
    if (len < 126) {
      header = Buffer.from([firstByte, len]);
    } else if (len < 65_536) {
      header = Buffer.allocUnsafe(4);
      header[0] = firstByte; header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.allocUnsafe(10);
      header[0] = firstByte; header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    this.#socket.write(Buffer.concat([header, payload]));
  }

  #onData(chunk: Buffer): void {
    this.#buf = Buffer.concat([this.#buf, chunk]);
    let result;
    while ((result = decodeWsFrame(this.#buf)) !== null) {
      const { payload, opcode, consumed } = result;
      this.#buf = this.#buf.subarray(consumed);
      if (opcode === 8) { this.#socket.destroy(); return; } // close
      if (opcode === 1 || opcode === 2) {
        this.isBinary = opcode === 2;
        this.emit('message', payload);
      }
    }
  }
}

function decodeWsFrame(buf: Buffer): {
  payload: Buffer; opcode: number; consumed: number
} | null {
  if (buf.length < 2) return null;
  const b0 = buf[0] as number;
  const b1 = buf[1] as number;
  const opcode = b0 & 0x0f;
  const masked = (b1 & 0x80) !== 0;
  let payLen = b1 & 0x7f;
  let offset = 2;

  if (payLen === 126) {
    if (buf.length < 4) return null;
    payLen = buf.readUInt16BE(2); offset = 4;
  } else if (payLen === 127) {
    if (buf.length < 10) return null;
    payLen = Number(buf.readBigUInt64BE(2)); offset = 10;
  }

  let maskKey: Buffer | null = null;
  if (masked) {
    if (buf.length < offset + 4) return null;
    maskKey = buf.subarray(offset, offset + 4);
    offset += 4;
  }
  if (buf.length < offset + payLen) return null;

  let payload = buf.subarray(offset, offset + payLen);
  if (masked && maskKey) {
    payload = Buffer.from(payload);
    for (let i = 0; i < payload.length; i++) {
      payload[i] = (payload[i] as number) ^ (maskKey[i % 4] as number);
    }
  }
  return { payload: Buffer.from(payload), opcode, consumed: offset + payLen };
}
