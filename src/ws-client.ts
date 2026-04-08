/**
 * ws-client.ts
 *
 * From-scratch WebSocket client (RFC 6455) built on Node's built-in `net`
 * and `tls` modules.  Zero external runtime dependencies.
 *
 * Typed EventEmitter via interface declaration merging.
 *
 * Emitted events
 *   'open'             – handshake complete, ready to send/receive
 *   'message' (string) – UTF-8 text frame received
 *   'binary'  (Buffer) – binary frame received
 *   'ping'    (Buffer) – ping frame received (pong sent automatically)
 *   'pong'    (Buffer) – pong frame received
 *   'close'            – socket fully closed
 *   'error'   (Error)  – socket-level error
 */

import * as net    from 'node:net';
import * as tls    from 'node:tls';
import * as crypto from 'node:crypto';
import { EventEmitter } from 'node:events';

// ─── RFC 6455 opcodes ─────────────────────────────────────────────────────────
const enum Opcode {
  Continuation = 0x0,
  Text         = 0x1,
  Binary       = 0x2,
  Close        = 0x8,
  Ping         = 0x9,
  Pong         = 0xa,
}

// ─── Connection states ────────────────────────────────────────────────────────
export const enum WebSocketReadyState {
  Connecting = 0,
  Open       = 1,
  Closing    = 2,
  Closed     = 3,
}

const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ─── Typed-EventEmitter overloads (declaration merging) ───────────────────────

export interface WebSocketClient {
  on(event: 'open',    listener: () => void):                this;
  on(event: 'message', listener: (data: string) => void):    this;
  on(event: 'binary',  listener: (data: Buffer) => void):    this;
  on(event: 'ping',    listener: (data: Buffer) => void):    this;
  on(event: 'pong',    listener: (data: Buffer) => void):    this;
  on(event: 'close',   listener: () => void):                this;
  on(event: 'error',   listener: (err: Error) => void):      this;

  once(event: 'open',    listener: () => void):              this;
  once(event: 'message', listener: (data: string) => void):  this;
  once(event: 'close',   listener: () => void):              this;
  once(event: 'error',   listener: (err: Error) => void):    this;

  emit(event: 'open'):                     boolean;
  emit(event: 'message', data: string):    boolean;
  emit(event: 'binary',  data: Buffer):    boolean;
  emit(event: 'ping',    data: Buffer):    boolean;
  emit(event: 'pong',    data: Buffer):    boolean;
  emit(event: 'close'):                    boolean;
  emit(event: 'error',   err: Error):      boolean;

  removeAllListeners(event?: string): this;
}

// ─── Parsed frame ─────────────────────────────────────────────────────────────
interface ParsedFrame {
  fin:     boolean;
  opcode:  number;
  payload: Buffer;
}

interface ParseResult {
  frame:    ParsedFrame;
  consumed: number;
}

// ─── Implementation ───────────────────────────────────────────────────────────

export class WebSocketClient extends EventEmitter {
  #socket:          net.Socket | null  = null;
  #state:           WebSocketReadyState = WebSocketReadyState.Closed;
  #frameBuf:        Buffer            = Buffer.alloc(0);
  #fragments:       Buffer[]          = [];
  #fragmentOpcode:  number | null     = null;
  #handshakeBuf:    Buffer            = Buffer.alloc(0);
  #handshakeResolve: (() => void)     | null = null;
  #handshakeReject:  ((e: Error) => void) | null = null;

  get readyState(): WebSocketReadyState { return this.#state; }

  // ─── Public API ─────────────────────────────────────────────────────────

  /**
   * Open a WebSocket connection.
   * @param url      ws:// or wss:// URL
   * @param headers  Extra HTTP headers for the upgrade request
   */
  connect(url: string, headers: Record<string, string> = {}): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (this.#state !== WebSocketReadyState.Closed) {
        reject(new Error('WebSocket is already open or connecting.'));
        return;
      }
      this.#state            = WebSocketReadyState.Connecting;
      this.#handshakeResolve = resolve;
      this.#handshakeReject  = reject;

      const parsed   = new URL(url);
      const isSecure = parsed.protocol === 'wss:';
      const host     = parsed.hostname;
      const port     = Number(parsed.port) || (isSecure ? 443 : 80);
      const resource = (parsed.pathname || '/') + (parsed.search || '');
      const portSuffix = parsed.port ? `:${parsed.port}` : '';

      const nonce    = crypto.randomBytes(16).toString('base64');
      const expected = computeAccept(nonce);

      const upgradeLines: string[] = [
        `GET ${resource} HTTP/1.1`,
        `Host: ${host}${portSuffix}`,
        'Upgrade: websocket',
        'Connection: Upgrade',
        `Sec-WebSocket-Key: ${nonce}`,
        'Sec-WebSocket-Version: 13',
        ...Object.entries(headers).map(([k, v]) => `${k}: ${v}`),
        '',
        '',
      ];
      const request = upgradeLines.join('\r\n');

      const onSocketData = (chunk: Buffer): void => {
        this.#onHandshakeData(chunk, expected);
      };

      const socketOpts = { host, port };
      if (isSecure) {
        this.#socket = tls.connect({ ...socketOpts, servername: host }, () => {
          this.#socket!.write(request);
        });
      } else {
        this.#socket = net.connect(socketOpts, () => {
          this.#socket!.write(request);
        });
      }

      this.#socket.on('data',  onSocketData);
      this.#socket.on('error', (err: Error) => {
        if (this.#state === WebSocketReadyState.Connecting) {
          this.#state = WebSocketReadyState.Closed;
          this.#handshakeReject!(err);
        } else if (this.#state !== WebSocketReadyState.Closing) {
          // Errors during the Closing state (e.g. ECONNRESET after ws.close())
          // are expected and should not surface to callers.
          this.emit('error', err);
        }
      });
      this.#socket.on('close', () => this.#onSocketClose());
      this.#socket.on('end',   () => this.#socket?.destroy());
    });
  }

  /**
   * Send a text or binary message.
   * @throws if the socket is not open.
   */
  send(data: string | Buffer | Uint8Array): void {
    if (this.#state !== WebSocketReadyState.Open) {
      throw new Error('WebSocket is not open.');
    }
    if (typeof data === 'string') {
      this.#writeFrame(Opcode.Text, Buffer.from(data, 'utf8'));
    } else {
      this.#writeFrame(Opcode.Binary, Buffer.isBuffer(data) ? data : Buffer.from(data));
    }
  }

  /**
   * Initiate a clean close handshake.
   * @param code    WebSocket close code (default 1000 = Normal Closure)
   * @param reason  UTF-8 reason string (max 123 bytes)
   */
  close(code = 1000, reason = ''): void {
    if (this.#state !== WebSocketReadyState.Open) return;
    this.#state = WebSocketReadyState.Closing;
    const reasonBuf = Buffer.from(reason, 'utf8');
    const payload   = Buffer.allocUnsafe(2 + reasonBuf.length);
    payload.writeUInt16BE(code, 0);
    reasonBuf.copy(payload, 2);
    this.#writeFrame(Opcode.Close, payload);
  }

  // ─── Handshake ───────────────────────────────────────────────────────────

  #onHandshakeData(chunk: Buffer, expectedAccept: string): void {
    this.#handshakeBuf = Buffer.concat([this.#handshakeBuf, chunk]);

    const idx = indexOfCRLFCRLF(this.#handshakeBuf);
    if (idx === -1) return; // Wait for the full header block

    const headerStr  = this.#handshakeBuf.subarray(0, idx).toString('ascii');
    const remainder  = this.#handshakeBuf.subarray(idx + 4);

    const lines      = headerStr.split('\r\n');
    const statusLine = lines[0] ?? '';

    if (!statusLine.includes('101')) {
      this.#failHandshake(
        new Error(`WebSocket upgrade failed - server replied: ${statusLine}`)
      );
      return;
    }

    let accept = '';
    for (let i = 1; i < lines.length; i++) {
      const line  = lines[i]!;
      const colon = line.indexOf(':');
      if (colon === -1) continue;
      if (line.slice(0, colon).trim().toLowerCase() === 'sec-websocket-accept') {
        accept = line.slice(colon + 1).trim();
        break;
      }
    }

    if (accept !== expectedAccept) {
      this.#failHandshake(
        new Error(`Invalid Sec-WebSocket-Accept (got "${accept}", expected "${expectedAccept}")`)
      );
      return;
    }

    // Success - switch to frame-processing mode
    this.#state        = WebSocketReadyState.Open;
    this.#handshakeBuf = Buffer.alloc(0);
    this.#socket!.removeAllListeners('data');
    this.#socket!.on('data', (c: Buffer) => this.#onFrameData(c));

    const r = this.#handshakeResolve!;
    this.#handshakeResolve = null;
    this.#handshakeReject  = null;
    r();
    this.emit('open');

    if (remainder.length > 0) this.#onFrameData(remainder);
  }

  #failHandshake(err: Error): void {
    this.#state = WebSocketReadyState.Closed;
    this.#socket?.destroy();
    const r = this.#handshakeReject!;
    this.#handshakeResolve = null;
    this.#handshakeReject  = null;
    r(err);
  }

  // ─── Frame parsing ───────────────────────────────────────────────────────

  #onFrameData(chunk: Buffer): void {
    this.#frameBuf = Buffer.concat([this.#frameBuf, chunk]);
    this.#drainFrames();
  }

  #drainFrames(): void {
    for (;;) {
      const result = tryParseFrame(this.#frameBuf);
      if (!result) break;
      this.#frameBuf = this.#frameBuf.subarray(result.consumed);
      this.#dispatchFrame(result.frame);
    }
  }

  #dispatchFrame({ fin, opcode, payload }: ParsedFrame): void {
    switch (opcode) {
      case Opcode.Text:
      case Opcode.Binary:
        if (!fin) {
          this.#fragmentOpcode = opcode;
          this.#fragments      = [payload];
        } else {
          this.#emitData(opcode, payload);
        }
        break;

      case Opcode.Continuation:
        this.#fragments.push(payload);
        if (fin) {
          const full  = Buffer.concat(this.#fragments);
          const op    = this.#fragmentOpcode!;
          this.#fragments      = [];
          this.#fragmentOpcode = null;
          this.#emitData(op, full);
        }
        break;

      case Opcode.Ping:
        this.#writeFrame(Opcode.Pong, payload);
        this.emit('ping', payload);
        break;

      case Opcode.Pong:
        this.emit('pong', payload);
        break;

      case Opcode.Close:
        if (this.#state === WebSocketReadyState.Open) {
          this.#writeFrame(Opcode.Close, payload);
        }
        this.#state = WebSocketReadyState.Closed;
        this.#socket?.destroy();
        this.emit('close');
        break;

      default:
        // Unknown opcode - ignore per RFC 6455 §5.2
        break;
    }
  }

  #emitData(opcode: number, payload: Buffer): void {
    if (opcode === Opcode.Text) {
      this.emit('message', payload.toString('utf8'));
    } else {
      this.emit('binary', payload);
    }
  }

  // ─── Frame writing ───────────────────────────────────────────────────────

  /**
   * Encode and send one WebSocket frame.
   * All client→server frames MUST be masked (RFC 6455 §5.3).
   */
  #writeFrame(opcode: Opcode, payload: Buffer = Buffer.alloc(0), fin = true): void {
    if (!this.#socket || this.#socket.destroyed) return;

    const len = payload.length;

    // ── Header ──────────────────────────────────────────────────────────
    let header: Buffer;
    if (len < 126) {
      header = Buffer.allocUnsafe(2);
      header[1] = 0x80 | len;   // MASK bit + length
    } else if (len < 65_536) {
      header = Buffer.allocUnsafe(4);
      header[1] = 0x80 | 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.allocUnsafe(10);
      header[1] = 0x80 | 127;
      header.writeUInt32BE(0,   2);
      header.writeUInt32BE(len, 6);
    }
    header[0] = (fin ? 0x80 : 0x00) | opcode;

    // ── Masking ─────────────────────────────────────────────────────────
    const maskKey       = crypto.randomBytes(4);
    const maskedPayload = Buffer.allocUnsafe(len);
    for (let i = 0; i < len; i++) {
      maskedPayload[i] = payload[i]! ^ maskKey[i % 4]!;
    }

    this.#socket.write(Buffer.concat([header, maskKey, maskedPayload]));
  }

  // ─── Socket events ───────────────────────────────────────────────────────

  #onSocketClose(): void {
    if (this.#state !== WebSocketReadyState.Closed) {
      this.#state = WebSocketReadyState.Closed;
      this.emit('close');
    }
  }
}

// ─── Module-level helpers ─────────────────────────────────────────────────────

function computeAccept(key: string): string {
  return crypto.createHash('sha1').update(key + WS_MAGIC).digest('base64');
}

function indexOfCRLFCRLF(buf: Buffer): number {
  for (let i = 0; i <= buf.length - 4; i++) {
    if (buf[i] === 0x0d && buf[i + 1] === 0x0a &&
        buf[i + 2] === 0x0d && buf[i + 3] === 0x0a) {
      return i;
    }
  }
  return -1;
}

function tryParseFrame(buf: Buffer): ParseResult | null {
  if (buf.length < 2) return null;

  const b0     = buf[0]!;
  const b1     = buf[1]!;
  const fin    = (b0 & 0x80) !== 0;
  const opcode = b0 & 0x0f;
  const masked = (b1 & 0x80) !== 0;
  let payLen   = b1 & 0x7f;
  let offset   = 2;

  if (payLen === 126) {
    if (buf.length < offset + 2) return null;
    payLen  = buf.readUInt16BE(offset);
    offset += 2;
  } else if (payLen === 127) {
    if (buf.length < offset + 8) return null;
    const hi = buf.readUInt32BE(offset);
    if (hi !== 0) throw new Error('Received a frame with payload > 4 GB - not supported.');
    payLen  = buf.readUInt32BE(offset + 4);
    offset += 8;
  }

  let maskBytes: Buffer | null = null;
  if (masked) {
    if (buf.length < offset + 4) return null;
    maskBytes  = buf.subarray(offset, offset + 4);
    offset    += 4;
  }

  if (buf.length < offset + payLen) return null;

  let payload = buf.subarray(offset, offset + payLen);
  if (masked && maskBytes !== null) {
    payload = Buffer.from(payload);
    for (let i = 0; i < payload.length; i++) {
      // noUncheckedIndexedAccess: both sides are within-bounds by construction
      payload[i] = (payload[i] as number) ^ (maskBytes[i % 4] as number);
    }
  }

  return { frame: { fin, opcode, payload }, consumed: offset + payLen };
}
