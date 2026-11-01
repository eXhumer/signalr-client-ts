/**
 * helpers/scripted-hub-server.ts
 *
 * A SignalR server whose handshake and negotiate behaviour each test scripts,
 * for the timing paths `startSignalRServer` can't reach: a socket dying during
 * the handshake, a slow negotiate, a rejected upgrade.
 */

import * as crypto from 'node:crypto';
import * as http   from 'node:http';
import type * as net from 'node:net';

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function frame(payload: string | Buffer, opcode = 1): Buffer {
  const body = Buffer.from(payload);
  const head = body.length < 126
    ? Buffer.from([0x80 | opcode, body.length])
    : Buffer.from([0x80 | opcode, 126, body.length >> 8, body.length & 0xff]);
  return Buffer.concat([head, body]);
}

export class ScriptedSocket {
  constructor(readonly socket: net.Socket, readonly index: number) {}

  /** Complete the SignalR handshake. */
  handshake(): void { this.raw('{}\x1e'); }
  /** Send a text frame as it is. */
  raw(text: string): void { this.socket.write(frame(text)); }
  /** Send a hub message. */
  send(message: Record<string, unknown>): void { this.raw(`${JSON.stringify(message)}\x1e`); }
  /** Drop the connection with no close frame. */
  destroy(): void { this.socket.destroy(); }
  /** Send a WebSocket close frame, then drop the connection. */
  closeWith(code: number, reason = ''): void {
    const payload = Buffer.concat([Buffer.from([code >> 8, code & 0xff]), Buffer.from(reason)]);
    this.socket.end(frame(payload, 8));
  }
}

export interface HubScript {
  /** Called when the client's handshake request arrives. Default: complete it. */
  onHandshake?(socket: ScriptedSocket): void;
  /** Replaces the negotiate response, or delays it. `undefined` keeps the default. */
  negotiate?(attempt: number): Promise<object | undefined> | object | undefined;
  /** Return true to answer this upgrade (1-based) with a 403. */
  rejectUpgrade?(attempt: number): boolean;
  /** Handles any other HTTP request. Default: 404. */
  http?(req: http.IncomingMessage, res: http.ServerResponse): void;
  /** Transports the negotiate response offers. Default: WebSockets. */
  transports?: readonly string[];
}

export interface ScriptedHub {
  readonly url: string;
  /** Every WebSocket accepted, in order. */
  readonly sockets: ScriptedSocket[];
  /** The Authorization header of each negotiate request. */
  readonly negotiateAuth: (string | undefined)[];
  readonly openSockets: () => number;
  close(): void;
}

export async function startScriptedHub(script: HubScript = {}): Promise<ScriptedHub> {
  const sockets: ScriptedSocket[] = [];
  const negotiateAuth: (string | undefined)[] = [];
  let upgrades = 0;

  const server = http.createServer((req, res) => {
    if (!req.url?.includes('/negotiate')) {
      if (script.http) script.http(req, res);
      else { res.writeHead(404); res.end(); }
      return;
    }
    negotiateAuth.push(req.headers.authorization);
    const attempt = negotiateAuth.length;
    void Promise.resolve(script.negotiate?.(attempt)).then((body) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(body ?? {
        negotiateVersion: 1,
        connectionId:     `id-${attempt}`,
        connectionToken:  `token-${attempt}`,
        availableTransports: (script.transports ?? ['WebSockets'])
          .map((transport) => ({ transport, transferFormats: ['Text'] })),
      }));
    });
  });

  server.on('upgrade', (req, socket: net.Socket) => {
    socket.on('error', () => {});
    upgrades += 1;
    if (script.rejectUpgrade?.(upgrades)) {
      socket.end('HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n');
      return;
    }
    const accept = crypto.createHash('sha1').update(`${req.headers['sec-websocket-key']}${WS_GUID}`).digest('base64');
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
    const scripted = new ScriptedSocket(socket, upgrades);
    sockets.push(scripted);
    // A client close frame needs no reply here: dropping the socket ends it.
    socket.on('data', (data: Buffer) => {
      if ((data[0]! & 0x0f) === 8) socket.destroy();
    });
    socket.once('data', () => {
      if (script.onHandshake) script.onHandshake(scripted);
      else scripted.handshake();
    });
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as net.AddressInfo;

  return {
    url: `http://127.0.0.1:${port}/hub`,
    sockets,
    negotiateAuth,
    openSockets: () => sockets.filter(({ socket }) => !socket.destroyed).length,
    close() {
      server.closeAllConnections();
      for (const { socket } of sockets) socket.destroy();
      server.close();
    },
  };
}
