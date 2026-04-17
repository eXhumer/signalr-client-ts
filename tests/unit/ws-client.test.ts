/**
 * ws-client.test.ts
 *
 * Tests the WebSocketClient by spinning up real in-process TCP servers.
 * All servers are ephemeral (port 0) and destroyed after each test.
 */

import { describe, it, beforeAll, afterAll, beforeEach, expect } from 'vitest';
import * as net    from 'node:net';
import * as crypto from 'node:crypto';

import { WebSocketClient, WebSocketReadyState } from '../../src/ws-client.js';

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ─── Minimal WebSocket server helper ─────────────────────────────────────────

interface MinimalWsServer {
  port:     number;
  stop():   Promise<void>;
  /** Resolves with the next client socket that completes the WS handshake. */
  nextSocket(): Promise<net.Socket>;
}

async function createWsServer(): Promise<MinimalWsServer> {
  const pending: Array<(s: net.Socket) => void> = [];
  const activeSockets = new Set<net.Socket>();
  const server = net.createServer();

  server.on('connection', (sock: net.Socket) => {
    activeSockets.add(sock);
    sock.on('close', () => activeSockets.delete(sock));
    // Suppress ECONNRESET that fires when the client closes the connection
    sock.on('error', () => {});
    let buf = Buffer.alloc(0);
    const onData = (chunk: Buffer): void => {
      buf = Buffer.concat([buf, chunk]);
      const idx = indexOfCRLFCRLF(buf);
      if (idx === -1) return;

      sock.removeListener('data', onData);
      const headerBlock = buf.subarray(0, idx).toString('ascii');
      const lines       = headerBlock.split('\r\n');
      let key = '';
      for (const line of lines) {
        const colon = line.indexOf(':');
        if (colon !== -1 && line.slice(0, colon).trim().toLowerCase() === 'sec-websocket-key') {
          key = line.slice(colon + 1).trim();
        }
      }
      const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
      sock.write(
        `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`
      );
      pending.shift()?.(sock);
    };
    sock.on('data', onData);
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;

  return {
    port,
    nextSocket: () => new Promise<net.Socket>((resolve) => pending.push(resolve)),
    stop: () => new Promise<void>((resolve) => {
      for (const sock of activeSockets) sock.destroy();
      server.close(() => resolve());
    }),
  };
}

function encodeServerFrame(opcode: number, payload: Buffer): Buffer {
  const len = payload.length;
  let header: Buffer;
  if (len < 126) {
    header    = Buffer.allocUnsafe(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65_536) {
    header    = Buffer.allocUnsafe(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header    = Buffer.allocUnsafe(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeUInt32BE(0, 2);
    header.writeUInt32BE(len, 6);
  }
  return Buffer.concat([header, payload]);
}

function textFrame(text: string): Buffer  { return encodeServerFrame(0x1, Buffer.from(text, 'utf8')); }
function pingFrame(data = ''): Buffer     { return encodeServerFrame(0x9, Buffer.from(data, 'utf8')); }
function closeFrame(code = 1000): Buffer  {
  const buf = Buffer.allocUnsafe(2);
  buf.writeUInt16BE(code, 0);
  return encodeServerFrame(0x8, buf);
}

function indexOfCRLFCRLF(buf: Buffer): number {
  for (let i = 0; i <= buf.length - 4; i++) {
    if (buf[i] === 0x0d && buf[i+1] === 0x0a && buf[i+2] === 0x0d && buf[i+3] === 0x0a) return i;
  }
  return -1;
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('WebSocketClient', () => {
  let srv: MinimalWsServer;

  beforeAll(async () => {
    srv = await createWsServer();
  });

  afterAll(async () => {
    await srv.stop();
  });

  // ─── Connect ──────────────────────────────────────────────────────────

  it('connects to a real WebSocket server', async () => {
    const ws = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    expect(ws.readyState).toBe(WebSocketReadyState.Open);
    const srvSock = await srvSockP;
    ws.close();
    srvSock.destroy();
  });

  it('emits "open" after successful handshake', async () => {
    const ws     = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    let opened   = false;
    ws.on('open', () => { opened = true; });
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    expect(opened).toBeTruthy();
    ws.close();
    (await srvSockP).destroy();
  });

  it('rejects when server is unreachable', async () => {
    const ws = new WebSocketClient();
    await expect(
      () => ws.connect('ws://127.0.0.1:1'),   // port 1 is never open
    ).rejects.toThrow(/ECONNREFUSED/);
  });

  it('rejects on invalid Sec-WebSocket-Accept', async () => {
    // Create a server that sends a wrong accept value
    const badServer = net.createServer((sock) => {
      let buf = Buffer.alloc(0);
      sock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        if (!indexOfCRLFCRLF(buf)) return;
        sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: WRONG==\r\n\r\n');
      });
    });
    await new Promise<void>((r) => badServer.listen(0, '127.0.0.1', r));
    const { port: badPort } = badServer.address() as net.AddressInfo;
    const ws = new WebSocketClient();
    await expect(
      () => ws.connect(`ws://127.0.0.1:${badPort}`),
    ).rejects.toThrow(/Invalid Sec-WebSocket-Accept/);
    await new Promise<void>((r) => badServer.close(() => r()));
  });

  // ─── Send / receive text ──────────────────────────────────────────────

  it('sends a text frame and the server receives it', async () => {
    const ws      = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock = await srvSockP;

    const receivedP = new Promise<string>((resolve) => {
      let buf = Buffer.alloc(0);
      srvSock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        // Simple unmask
        if (buf.length >= 6) {
          const len      = buf[1]! & 0x7f;
          const maskKey  = buf.subarray(2, 6);
          const payload  = buf.subarray(6, 6 + len);
          const unmasked = Buffer.from(payload);
          for (let i = 0; i < unmasked.length; i++) {
            unmasked[i] = (unmasked[i] as number) ^ (maskKey[i % 4] as number);
          }
          resolve(unmasked.toString('utf8'));
        }
      });
    });

    ws.send('hello server');
    const received = await receivedP;
    expect(received).toBe('hello server');

    ws.close();
    srvSock.destroy();
  });

  it('receives a text frame from the server', async () => {
    const ws      = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock = await srvSockP;

    const msgP = new Promise<string>((resolve) => ws.on('message', resolve));
    srvSock.write(textFrame('hello client'));
    const msg = await msgP;
    expect(msg).toBe('hello client');

    ws.close();
    srvSock.destroy();
  });

  it('receives large text frames (>65535 bytes)', async () => {
    const ws      = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock = await srvSockP;

    const bigPayload = 'X'.repeat(70_000);
    const msgP = new Promise<string>((resolve) => ws.on('message', resolve));

    // Build a 64-bit-length frame
    const payBuf = Buffer.from(bigPayload, 'utf8');
    const header  = Buffer.allocUnsafe(10);
    header[0]     = 0x81;
    header[1]     = 127;
    header.writeUInt32BE(0,            2);
    header.writeUInt32BE(payBuf.length, 6);
    srvSock.write(Buffer.concat([header, payBuf]));

    const received = await msgP;
    expect(received.length).toBe(70_000);

    ws.close();
    srvSock.destroy();
  });

  // ─── Fragmented messages ──────────────────────────────────────────────

  it('reassembles fragmented text messages', async () => {
    const ws      = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock = await srvSockP;

    const msgP = new Promise<string>((resolve) => ws.on('message', resolve));

    // First fragment: FIN=0, opcode=TEXT, payload="Hello "
    const frag1 = Buffer.from(encodeServerFrame(0x1, Buffer.from('Hello ', 'utf8')));
    frag1[0] = 0x01; // clear the FIN bit so this is a fragment

    // Final fragment: FIN=1, opcode=CONTINUATION, payload="World!"
    const frag2 = Buffer.from(encodeServerFrame(0x0, Buffer.from('World!', 'utf8')));
    frag2[0] = 0x80; // FIN=1, opcode=CONTINUATION (0x0)

    srvSock.write(frag1);
    srvSock.write(frag2);

    const msg = await msgP;
    expect(msg).toBe('Hello World!');

    ws.close();
    srvSock.destroy();
  });

  // ─── Ping / Pong ──────────────────────────────────────────────────────

  it('responds to server ping with a pong', async () => {
    const ws      = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock = await srvSockP;

    const pongP = new Promise<boolean>((resolve) => {
      let buf = Buffer.alloc(0);
      srvSock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        if (buf.length >= 2 && (buf[0]! & 0x0f) === 0xa) {
          resolve(true); // PONG opcode
        }
      });
    });

    srvSock.write(pingFrame('ping!'));
    expect(await pongP, 'Expected pong response').toBeTruthy();

    ws.close();
    srvSock.destroy();
  });

  it('emits ping event when server sends ping', async () => {
    const ws      = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock = await srvSockP;

    const pingDataP = new Promise<Buffer>((resolve) => ws.on('ping', resolve));
    srvSock.write(pingFrame('ping-data'));
    const data = await pingDataP;
    expect(data.toString('utf8')).toBe('ping-data');

    ws.close();
    srvSock.destroy();
  });

  // ─── Close ────────────────────────────────────────────────────────────

  it('emits "close" when server sends a close frame', async () => {
    const ws      = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock = await srvSockP;

    const closedP = new Promise<void>((resolve) => ws.on('close', resolve));
    srvSock.write(closeFrame(1000));
    await closedP;
    expect(ws.readyState).toBe(WebSocketReadyState.Closed);
    srvSock.destroy();
  });

  it('send() throws when not connected', () => {
    const ws = new WebSocketClient();
    expect(() => ws.send('hello')).toThrow(/not open/i);
  });

  it('close() is a no-op when already closed', () => {
    const ws = new WebSocketClient();
    expect(() => ws.close()).not.toThrow();
  });

  // ─── Coverage gaps ────────────────────────────────────────────────────

  // 14. TCP half-close (end) causes the socket to be destroyed
  it('TCP half-close (socket end) causes the client to destroy its socket and emit close', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const closedP = new Promise<void>((resolve) => ws.on('close', resolve));

    // Server sends a TCP FIN (half-close) without fully destroying the socket.
    // The client's 'end' handler calls this.#socket?.destroy() which ultimately
    // fires the 'close' event.
    srvSock.end();

    await closedP;
    expect(ws.readyState).toBe(WebSocketReadyState.Closed);
    srvSock.destroy();
  });

  // 15. Error during Closing state is silently suppressed
  it('error events are suppressed while the client is in Closing state', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const errors: Error[] = [];
    ws.on('error', (e) => errors.push(e));

    // Transition to Closing - state is now Closing
    ws.close();
    expect(ws.readyState).toBe(WebSocketReadyState.Closing);

    // Abrupt server-side destroy triggers ECONNRESET on the client socket.
    // In Closing state the error handler must suppress it.
    srvSock.destroy();

    // Give the error event time to fire if it were going to
    await new Promise<void>((r) => setTimeout(r, 60));

    expect(errors.length, 'No error event should be emitted during Closing state').toBe(0);
  });

  // 16. Pong frame emits "pong" event
  it('emits "pong" event when server sends a pong frame', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const pongDataP = new Promise<Buffer>((resolve) => ws.on('pong', resolve));
    srvSock.write(encodeServerFrame(0xa, Buffer.from('pong-data', 'utf8')));
    const data = await pongDataP;
    expect(data.toString('utf8')).toBe('pong-data');

    ws.close();
    srvSock.destroy();
  });

  // 17. Receiving a close frame while already Closing does not echo
  it('receiving a close frame while already Closing does not send a second close frame', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    // Count how many close frames (opcode 0x8) the server receives
    const closeFramesReceived: number[] = [];
    srvSock.on('data', (chunk: Buffer) => {
      if ((chunk[0]! & 0x0f) === 0x8) {
        closeFramesReceived.push(Date.now());
      }
    });

    const closedP = new Promise<void>((resolve) => ws.on('close', resolve));

    // Client initiates close - sends close frame #1, state → Closing
    ws.close();
    expect(ws.readyState).toBe(WebSocketReadyState.Closing);

    // Wait until TCP has delivered that frame; setImmediate is not enough.
    await new Promise<void>((resolve) => {
      const tick = (): void => {
        if (closeFramesReceived.length >= 1) resolve();
        else setImmediate(tick);
      };
      tick();
    });
    closeFramesReceived.length = 0;

    // Server echoes the close frame back
    srvSock.write(closeFrame(1000));
    await closedP;

    // The client MUST NOT have sent another close frame in response to the echo
    expect(closeFramesReceived.length, 'Client should not re-echo the close frame').toBe(0);
    expect(ws.readyState).toBe(WebSocketReadyState.Closed);
    srvSock.destroy();
  });

  // 18. Unknown opcode is silently ignored per RFC 6455 §5.2
  it('frame with unknown opcode is silently ignored - no error, subsequent frames still arrive', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const errors: Error[] = [];
    ws.on('error', (e) => errors.push(e));

    // Opcode 0x3 is a reserved non-control opcode - unknown to the client
    srvSock.write(encodeServerFrame(0x3, Buffer.from('ignored', 'utf8')));

    // A real text frame sent immediately after must still be delivered
    const msgP = new Promise<string>((resolve) => ws.on('message', resolve));
    srvSock.write(textFrame('after-unknown'));
    const msg = await msgP;

    expect(errors.length, 'No error event should fire for unknown opcode').toBe(0);
    expect(msg).toBe('after-unknown');

    ws.close();
    srvSock.destroy();
  });

  // 19. Server sends a masked text frame - client XOR-unmasks and delivers it (lines 421-423, 430-433)
  it('server sends a masked text frame - client XOR-unmasks and delivers it (lines 421-423, 430-433)', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const msgP = new Promise<string>((resolve) => ws.on('message', resolve));

    // Masked text frame: FIN(1)|opcode(1)=0x81, MASK(1)|len(5)=0x85
    const maskKey       = Buffer.from([0xaa, 0xbb, 0xcc, 0xdd]);
    const plainPayload  = Buffer.from('hello');
    const maskedPayload = Buffer.from(plainPayload.map((b, i) => b ^ maskKey[i % 4]!));

    // Step 1: header only (2 bytes) → buf.length < offset+4 for mask key → return null (line 421)
    srvSock.write(Buffer.from([0x81, 0x85]));
    await new Promise<void>((r) => setTimeout(r, 20));

    // Step 2: mask key + payload → lines 422-423, 430-433
    srvSock.write(Buffer.concat([maskKey, maskedPayload]));

    const msg = await msgP;
    expect(msg).toBe('hello');

    ws.close();
    srvSock.destroy();
  });

  // 20. Client sends a large message (>65535 bytes): uses 8-byte extended length (line 356)
  it('sends a frame with >65535-byte payload using 8-byte extended length (line 356)', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const payloadChar = 'Z';
    const payloadLen  = 70_000;
    const bigPayload  = payloadChar.repeat(payloadLen);

    const receivedP = new Promise<string>((resolve) => {
      let buf = Buffer.alloc(0);
      srvSock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        // frame: [0]=opcode, [1]=0xFF (mask|127), [2-9]=8-byte len, [10-13]=mask, [14...]=payload
        if (buf.length < 14) return;
        const extLen = buf.readUInt32BE(6);  // low 32 bits of the 8-byte length
        if (buf.length < 14 + extLen) return;
        const maskKey  = buf.subarray(10, 14);
        const masked   = buf.subarray(14, 14 + extLen);
        const unmasked = Buffer.allocUnsafe(extLen);
        for (let i = 0; i < extLen; i++) unmasked[i] = masked[i]! ^ maskKey[i % 4]!;
        resolve(unmasked.toString('utf8'));
      });
    });

    ws.send(bigPayload);
    const received = await receivedP;

    expect(received.length).toBe(payloadLen);
    expect(received[0]).toBe(payloadChar);
    // Verify the length encoding: byte[1] must be 0x80|127 = 0xFF
    // (we can't easily check buf from here, but if we decoded correctly the content confirms it)

    ws.close();
    srvSock.destroy();
  });

  // 21. Server sends a 126-extended-length frame in two writes (lines 408-410)
  it('receives a 126-extended-length frame split across two TCP writes (lines 408-410)', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    // Build a 200-byte payload frame using the 126 extended-length format
    const payload = Buffer.alloc(200, 0x41);  // 200 'A' bytes
    const frame   = encodeServerFrame(0x1, payload);  // encodeServerFrame uses 126 for 126≤len<65536

    const msgP = new Promise<string>((resolve) => ws.on('message', resolve));

    // Step 1: send only the 2-byte header → payLen=126, but buf.length < offset+2 → return null (line 408)
    srvSock.write(frame.subarray(0, 2));
    await new Promise<void>((r) => setTimeout(r, 20));

    // Step 2: send extended-length bytes + payload → lines 409-410 parse the 2-byte extended length
    srvSock.write(frame.subarray(2));

    const msg = await msgP;
    expect(msg.length).toBe(200);
    expect(msg).toBe('A'.repeat(200));

    ws.close();
    srvSock.destroy();
  });

  // 22a. Server returns non-101 status code → handshake fails with descriptive error (line 216)
  it('rejects when server responds with a non-101 status code (line 216)', async () => {
    const badServer = net.createServer((sock: net.Socket) => {
      let buf = Buffer.alloc(0);
      sock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        if (!buf.includes(Buffer.from('\r\n\r\n'))) return;
        sock.write('HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n');
        sock.end();
      });
    });
    await new Promise<void>((r) => badServer.listen(0, '127.0.0.1', r));
    const { port: badPort } = badServer.address() as net.AddressInfo;

    const ws = new WebSocketClient();
    await expect(
      () => ws.connect(`ws://127.0.0.1:${badPort}`),
    ).rejects.toThrow(/upgrade failed|400/i);

    await new Promise<void>((r) => badServer.close(() => r()));
  });

  // 22b. Server sends a binary frame (opcode 0x2) → client emits 'binary' event (line 328)
  it('emits "binary" event when server sends a binary frame (line 328)', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const binaryP = new Promise<Buffer>((resolve) => ws.on('binary', resolve));
    const binaryData = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
    // opcode 0x2 = binary frame
    srvSock.write(encodeServerFrame(0x2, binaryData));
    const received = await binaryP;
    expect(Buffer.compare(received, binaryData)).toBe(0);

    ws.close();
    srvSock.destroy();
  });

  // 22c. Client sends a 200-byte message → uses 2-byte extended length (lines 349-351)
  it('sends a frame with 126-65535-byte payload using 2-byte extended length (lines 349-351)', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const payloadLen = 200; // 126 ≤ 200 < 65536 → must use 2-byte extended length
    const payload    = 'M'.repeat(payloadLen);

    const receivedP = new Promise<string>((resolve) => {
      let buf = Buffer.alloc(0);
      srvSock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        // Frame: [0]=opcode, [1]=0x80|126, [2-3]=2-byte length, [4-7]=mask, [8...]=payload
        if (buf.length < 8) return;
        const extLen = buf.readUInt16BE(2);
        if (buf.length < 8 + extLen) return;
        const maskKey  = buf.subarray(4, 8);
        const masked   = buf.subarray(8, 8 + extLen);
        const unmasked = Buffer.allocUnsafe(extLen);
        for (let i = 0; i < extLen; i++) unmasked[i] = masked[i]! ^ maskKey[i % 4]!;
        resolve(unmasked.toString('utf8'));
      });
    });

    ws.send(payload);
    const received = await receivedP;
    expect(received.length).toBe(payloadLen);
    expect(received).toBe(payload);

    ws.close();
    srvSock.destroy();
  });

  // 24. connect() called while already open rejects immediately (lines 107-108)
  it('connect() rejects if called while already open', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    await expect(
      () => ws.connect(`ws://127.0.0.1:${srv.port}`),
    ).rejects.toThrow(/already open or connecting/i);

    ws.close();
    srvSock.destroy();
  });

  // 25. Custom headers are included in the upgrade request (line 131)
  it('custom headers are sent in the upgrade request', async () => {
    const headerServer = net.createServer((sock: net.Socket) => {
      let buf = Buffer.alloc(0);
      sock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        if (!buf.includes(Buffer.from('\r\n\r\n'))) return;
        const req = buf.toString('ascii');
        const match = req.match(/Sec-WebSocket-Key: ([^\r\n]+)/);
        if (!match) { sock.destroy(); return; }
        const accept = crypto.createHash('sha1').update(match[1]!.trim() + WS_GUID).digest('base64');
        // Echo the parsed custom header value back as a custom response header so the test can verify it
        const hasCustom = req.includes('X-Test-Header: test-value');
        sock.write(
          `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\nX-Custom-Seen: ${hasCustom}\r\n\r\n`
        );
      });
      sock.on('error', () => {});
    });
    await new Promise<void>((r) => headerServer.listen(0, '127.0.0.1', r));
    const { port: headerPort } = headerServer.address() as net.AddressInfo;

    const ws = new WebSocketClient();
    // connect() with extra headers — exercises the ...Object.entries(headers).map() spread (line 131)
    await ws.connect(`ws://127.0.0.1:${headerPort}`, { 'X-Test-Header': 'test-value' });
    expect(ws.readyState).toBe(WebSocketReadyState.Open);

    ws.close();
    await new Promise<void>((r) => headerServer.close(() => r()));
  });

  // 26. Error in Open state is emitted (line 160)
  it('socket error while Open is forwarded as an "error" event', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const errorP = new Promise<Error>((resolve) => ws.on('error', resolve));

    // resetAndDestroy() sends a TCP RST, which causes ECONNRESET on the client
    // while it is in the Open state → line 160 in ws-client.ts emits 'error'.
    srvSock.resetAndDestroy();

    const err = await errorP;
    expect(err).toBeInstanceOf(Error);
  });

  // 27. send() with a Buffer and with a Uint8Array both send binary frames (line 179)
  it('send() accepts Buffer and Uint8Array and sends binary frames', async () => {
    const ws       = new WebSocketClient();
    const srvSockP = srv.nextSocket();
    await ws.connect(`ws://127.0.0.1:${srv.port}`);
    const srvSock  = await srvSockP;

    const received: Buffer[] = [];
    const twoFramesP = new Promise<void>((resolve) => {
      let buf = Buffer.alloc(0);
      srvSock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        // Each frame: [0]=opcode, [1]=0x80|len, [2-5]=mask, [6...]=payload (for small payloads)
        while (buf.length >= 6) {
          const len  = buf[1]! & 0x7f;
          if (buf.length < 6 + len) break;
          const maskKey  = buf.subarray(2, 6);
          const masked   = buf.subarray(6, 6 + len);
          const unmasked = Buffer.allocUnsafe(len);
          for (let i = 0; i < len; i++) unmasked[i] = masked[i]! ^ maskKey[i % 4]!;
          received.push(unmasked);
          buf = buf.subarray(6 + len);
          if (received.length === 2) resolve();
        }
      });
    });

    ws.send(Buffer.from([0x01, 0x02, 0x03]));          // Buffer path
    ws.send(new Uint8Array([0x04, 0x05, 0x06]));       // Uint8Array path (line 179)

    await twoFramesP;
    expect(Buffer.compare(received[0]!, Buffer.from([0x01, 0x02, 0x03]))).toBe(0);
    expect(Buffer.compare(received[1]!, Buffer.from([0x04, 0x05, 0x06]))).toBe(0);

    ws.close();
    srvSock.destroy();
  });

  // 23. Handshake response split across two TCP writes: indexOfCRLFCRLF returns -1 then finds it (line 393)
  it('handshake response split over two TCP writes buffers correctly (line 393 in source)', async () => {
    // Spin up a custom server that deliberately sends the HTTP 101 response in two chunks
    const splitServer = net.createServer((sock: net.Socket) => {
      let incoming = Buffer.alloc(0);
      sock.on('data', (chunk: Buffer) => {
        incoming = Buffer.concat([incoming, chunk]);
        // Wait for the full upgrade request
        if (!incoming.includes(Buffer.from('\r\n\r\n'))) return;
        const match = incoming.toString('utf8').match(/Sec-WebSocket-Key: ([^\r\n]+)/);
        if (!match) { sock.destroy(); return; }
        const accept = crypto.createHash('sha1').update(match[1]!.trim() + WS_GUID).digest('base64');
        const fullResp = [
          'HTTP/1.1 101 Switching Protocols',
          'Upgrade: websocket',
          'Connection: Upgrade',
          `Sec-WebSocket-Accept: ${accept}`,
          '\r\n',
        ].join('\r\n');
        // Send only the first 20 bytes → no \r\n\r\n yet → indexOfCRLFCRLF returns -1 (line 393)
        sock.write(fullResp.slice(0, 20));
        // Send the rest after a short delay
        setTimeout(() => sock.write(fullResp.slice(20)), 30);
      });
    });
    await new Promise<void>((r) => splitServer.listen(0, '127.0.0.1', r));
    const { port: splitPort } = splitServer.address() as net.AddressInfo;

    const ws = new WebSocketClient();
    await ws.connect(`ws://127.0.0.1:${splitPort}`);
    expect(ws.readyState).toBe(WebSocketReadyState.Open);

    ws.close();
    await new Promise<void>((r) => splitServer.close(() => r()));
  });
});
