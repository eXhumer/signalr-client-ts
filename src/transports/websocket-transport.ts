/**
 * websocket-transport.ts
 *
 * SignalR WebSocket transport - rewritten to use undici's built-in
 * WHATWG-compatible `WebSocket` implementation.
 *
 * Why undici WebSocket instead of the hand-rolled RFC 6455 client?
 * ─────────────────────────────────────────────────────────────────
 * • undici is the HTTP engine Node.js core ships with (≥ v18).
 * • Its WebSocket implementation passes the Autobahn test suite.
 * • It accepts the same `Dispatcher` used by the HTTP clients, so the
 *   entire SignalR session (negotiate, handshake, upgrade) shares a single
 *   connection pool / proxy config.
 * • It supports custom headers on the upgrade request (needed for auth
 *   and extra headers), which the browser WebSocket API does not.
 *
 * undici WebSocket quirks in Node.js
 * ────────────────────────────────────
 * • The constructor signature is:
 *     new WebSocket(url, { headers?, dispatcher?, protocols? })
 *   The `headers` and `dispatcher` fields are undici extensions not in the
 *   WHATWG spec.
 * • `MessageEvent.data` type depends on `ws.binaryType`:
 *     'arraybuffer' (what we set) → ArrayBuffer
 *     'blob'                      → Blob
 *   We set `binaryType = 'arraybuffer'` and convert ArrayBuffer → Buffer on
 *   receipt.  ('nodebuffer' is a valid undici runtime value but is absent from
 *   the WHATWG BinaryType union in undici's TypeScript declarations.)
 * • `CloseEvent.code` / `.reason` behave identically to the browser.
 */

import { WebSocket } from 'undici';
import type { Dispatcher } from 'undici';
import { TransferFormat, LogLevel } from '../constants.js';
import type { ITransport, ILogger } from '../interfaces.js';

// ─── ReadyState constants (mirrors window.WebSocket) ─────────────────────────

export const WebSocketReadyState = Object.freeze({
  Connecting: 0,
  Open:       1,
  Closing:    2,
  Closed:     3,
} as const satisfies Record<string, number>);

export type WebSocketReadyState =
  (typeof WebSocketReadyState)[keyof typeof WebSocketReadyState];

// ─── Transport implementation ─────────────────────────────────────────────────

export class WebSocketTransport implements ITransport {
  readonly name = 'WebSockets' as const;

  readonly #accessTokenFactory: (() => Promise<string | null>) | null;
  readonly #logger:              ILogger;
  readonly #extraHeaders:        Record<string, string>;
  readonly #dispatcher:          Dispatcher | undefined;

  #ws: WebSocket | null = null;

  onreceive: ((data: string | Uint8Array) => void) | null = null;
  onclose:   ((error?: Error) => void)             | null = null;

  /**
   * @param accessTokenFactory  Async factory for Bearer tokens, or null.
   * @param logger              Logger instance.
   * @param extraHeaders        Headers forwarded on every upgrade request.
   * @param dispatcher          Optional undici Dispatcher (Agent/Pool/Client/
   *                            ProxyAgent).  When omitted, undici's default
   *                            global agent is used.  Pass the same dispatcher
   *                            used by your HTTP clients to share the pool.
   */
  constructor(
    accessTokenFactory: (() => Promise<string | null>) | null,
    logger:             ILogger,
    extraHeaders:       Record<string, string> = {},
    dispatcher?:        Dispatcher,
  ) {
    this.#accessTokenFactory = accessTokenFactory;
    this.#logger             = logger;
    this.#extraHeaders       = extraHeaders;
    this.#dispatcher         = dispatcher;
  }

  async connect(url: string, _transferFormat: TransferFormat): Promise<void> {
    this.#logger.log(LogLevel.Trace, `(WebSockets transport) Connecting to ${url}.`);

    // ── Build headers ───────────────────────────────────────────────────────
    const headers: Record<string, string> = { ...this.#extraHeaders };

    if (this.#accessTokenFactory) {
      const token = await this.#accessTokenFactory();
      if (token) headers['Authorization'] = `Bearer ${token}`;
    }

    // ── Convert http(s) → ws(s) ─────────────────────────────────────────────
    const wsUrl = url.replace(/^http/, 'ws');

    // ── Open the WebSocket ──────────────────────────────────────────────────
    // undici's WebSocket constructor accepts headers + dispatcher as its
    // second argument.  These are undici-specific extensions to the spec.
    const ws = new WebSocket(wsUrl, {
      headers,
      ...(this.#dispatcher !== undefined && { dispatcher: this.#dispatcher }),
    } as ConstructorParameters<typeof WebSocket>[1]);

    this.#ws = ws;

    // Keep binary frames as ArrayBuffer.  'nodebuffer' works at runtime but
    // is absent from the WHATWG BinaryType union in undici's type declarations,
    // so 'arraybuffer' is used here and converted to Buffer on receipt.
    ws.binaryType = 'arraybuffer';

    // ── Wait for the socket to be open before resolving ─────────────────────
    // We wrap the open/error events in a Promise so `connect()` only resolves
    // once the TCP + TLS + WebSocket handshake is fully complete.
    await new Promise<void>((resolve, reject) => {
      ws.addEventListener('open', () => {
        this.#logger.log(LogLevel.Information, '(WebSockets transport) Connected.');
        resolve();
      });

      ws.addEventListener('error', (event) => {
        // ErrorEvent is a browser DOM global absent from Node.js lib targets.
        // Cast to a structural equivalent to access `.error` / `.message`.
        const e = event as Event & { error?: unknown; message?: string };
        let err: Error;
        if (e.error instanceof Error) {
          // undici sometimes wraps the real network error (e.g. ECONNREFUSED)
          // inside `error.cause` and leaves `error.message` as an empty string.
          // Prefer the cause when the outer message is empty so callers see a
          // meaningful message (ECONNREFUSED, etc.) rather than ''.
          err = e.error.message
            ? e.error
            : (e.error.cause instanceof Error
                ? e.error.cause
                : new Error('WebSocket connection failed'));
        } else {
          err = new Error(String(e.message ?? 'WebSocket error'));
        }
        this.#logger.log(LogLevel.Error, `(WebSockets transport) Error during connect: ${err.message}`);
        reject(err);
      });
    });

    // ── Wire ongoing event handlers (after open) ─────────────────────────────

    // MessageEvent is defined in two conflicting type sources (undici-types
    // bundled inside @types/node and undici's own .d.ts).  Their `.ports`
    // fields are incompatible, so typing the listener parameter as
    // `MessageEvent` triggers an overload-mismatch error.  We let TypeScript
    // infer the parameter type (falling back to the generic `string` overload)
    // and then extract `.data` through a structural intersection cast.
    //
    // Because binaryType = 'arraybuffer', data is always string | ArrayBuffer.
    ws.addEventListener('message', (event) => {
      const data = (event as Event & { data: unknown }).data;
      if (typeof data === 'string') {
        this.#logger.log(LogLevel.Trace,
          `(WebSockets transport) Received text (${data.length} chars).`);
        this.onreceive?.(data);
      /* istanbul ignore else - binaryType='arraybuffer' constrains delivery
         to string | ArrayBuffer; any other type is unreachable in practice. */
      } else if (data instanceof ArrayBuffer) {
        const buf = Buffer.from(data);
        this.#logger.log(LogLevel.Trace,
          `(WebSockets transport) Received binary (${buf.length} bytes).`);
        this.onreceive?.(buf);
      }
    });

    // CloseEvent is a browser DOM global absent from Node.js lib targets.
    // Cast to a structural equivalent to access `.code` / `.reason`.
    ws.addEventListener('close', (event) => {
      const closeEvent = event as Event & { code: number; reason: string };
      this.#logger.log(LogLevel.Information,
        `(WebSockets transport) Socket closed (code=${closeEvent.code}).`);
      // Only call onclose for unexpected closures (not our own stop() call).
      if (this.#ws !== null) {
        this.#ws = null;
        this.onclose?.();
      }
    });

    ws.addEventListener('error', (event) => {
      // Errors after the initial open.
      // Same structural-cast pattern as above: ErrorEvent not in Node lib.
      if (ws.readyState !== WebSocket.CONNECTING) {
        const e   = event as Event & { error?: unknown; message?: string };
        const err = e.error instanceof Error
          ? e.error
          : new Error(String(e.message ?? 'WebSocket error'));
        this.#logger.log(LogLevel.Error, `(WebSockets transport) Error: ${err.message}`);
        // Null #ws BEFORE calling onclose so the 'close' event handler that
        // fires next sees this.#ws === null and does not call onclose a second
        // time (which would start two concurrent reconnect loops).
        if (this.#ws !== null) {
          this.#ws = null;
          this.onclose?.(err);
        }
      }
    });
  }

  send(data: string | Uint8Array): Promise<void> {
    if (!this.#ws || this.#ws.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error('WebSocket is not open.'));
    }
    try {
      // undici's WebSocket.send() accepts string | ArrayBufferLike |
      // ArrayBufferView | Blob.  Uint8Array satisfies ArrayBufferView.
      this.#ws.send(data);
      return Promise.resolve();
    /* istanbul ignore next - ws.send() on an already-open socket does not
       throw synchronously; this catch is a belt-and-suspenders fallback. */
    } catch (err) {
      return Promise.reject(err instanceof Error ? err : new Error(String(err)));
    }
  }

  stop(): Promise<void> {
    if (this.#ws) {
      const ws  = this.#ws;
      this.#ws  = null; // clear BEFORE close so the close handler skips onclose
      ws.close(1000, 'Normal closure');
    }
    return Promise.resolve();
  }
}
