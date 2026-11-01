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

import { getGlobalDispatcher, WebSocket } from 'undici';
import type { Dispatcher } from 'undici';
import { TransferFormat, LogLevel } from '../constants.js';
import { TransportError } from '../errors.js';
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

/** The error behind a WebSocket `error` event. */
function errorFromEvent(event: Event): Error {
  // ErrorEvent is a browser DOM global absent from Node.js lib targets.
  // Cast to a structural equivalent to access `.error` / `.message`.
  const e = event as Event & { error?: unknown; message?: string };
  if (!(e.error instanceof Error)) return new Error(String(e.message ?? 'WebSocket error'));
  // undici sometimes wraps the real network error (e.g. ECONNREFUSED) inside
  // `error.cause` and leaves `error.message` as an empty string.  Prefer the
  // cause so callers see a meaningful message rather than ''.
  if (e.error.message) return e.error;
  return e.error.cause instanceof Error ? e.error.cause : new Error('WebSocket connection failed');
}

/**
 * Reports the status of a rejected upgrade. undici's error event leaves it out,
 * and a 101 goes to `onRequestUpgrade`, so this only sees rejections.
 */
function rejectedStatus(report: (status: number) => void): Dispatcher.DispatcherComposeInterceptor {
  return (dispatch) => (opts, handler) => dispatch(opts, new Proxy(handler, {
    get(target, key) {
      if (key === 'onResponseStart') {
        return (...args: Parameters<NonNullable<Dispatcher.DispatchHandler['onResponseStart']>>) => {
          report(args[1]);
          target.onResponseStart?.(...args);
        };
      }
      const value: unknown = Reflect.get(target, key);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  }));
}

// ─── Transport implementation ─────────────────────────────────────────────────

export class WebSocketTransport implements ITransport {
  readonly name = 'WebSockets' as const;

  readonly #accessTokenFactory: (() => Promise<string | null>) | null;
  readonly #logger:              ILogger;
  readonly #extraHeaders:        Record<string, string>;
  readonly #dispatcher:          Dispatcher | undefined;

  #ws: WebSocket | null = null;
  #onreceive: ((data: string | Uint8Array) => void) | null = null;
  #pendingMessages: Array<string | Uint8Array> = [];

  onclose:   ((error?: Error) => void)             | null = null;

  get onreceive(): ((data: string | Uint8Array) => void) | null {
    return this.#onreceive;
  }

  set onreceive(handler: ((data: string | Uint8Array) => void) | null) {
    this.#onreceive = handler;
    /* istanbul ignore else -- HubConnection installs a receiver after connect */
    if (handler) {
      const pending = this.#pendingMessages;
      this.#pendingMessages = [];
      for (const message of pending) {
        /* istanbul ignore next -- requires a server frame in the connect/onreceive microtask gap */
        handler(message);
      }
    }
  }

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
    let upgradeStatus: number | undefined;
    const ws = new WebSocket(wsUrl, {
      headers,
      dispatcher: (this.#dispatcher ?? getGlobalDispatcher()).compose(
        rejectedStatus((status) => { upgradeStatus = status; }),
      ),
    } as ConstructorParameters<typeof WebSocket>[1]);

    this.#ws = ws;

    // Keep binary frames as ArrayBuffer.  'nodebuffer' works at runtime but
    // is absent from the WHATWG BinaryType union in undici's type declarations,
    // so 'arraybuffer' is used here and converted to Buffer on receipt.
    ws.binaryType = 'arraybuffer';

    // Servers may send the SignalR handshake response immediately after the
    // WebSocket opens. Listen before awaiting `open`, and queue those frames
    // until HubConnection installs its receiver after connect() resolves.
    ws.addEventListener('message', (event) => {
      const data = (event as Event & { data: unknown }).data;
      const message = typeof data === 'string' ? data : Buffer.from(data as ArrayBuffer);
      this.#logger.log(LogLevel.Trace, typeof message === 'string'
        ? `(WebSockets transport) Received text (${message.length} chars).`
        : `(WebSockets transport) Received binary (${message.length} bytes).`);
      if (this.#onreceive) this.#onreceive(message);
      else this.#pendingMessages.push(message);
    });

    // ── Wait for the socket to be open before resolving ─────────────────────
    // We wrap the open/error events in a Promise so `connect()` only resolves
    // once the TCP + TLS + WebSocket handshake is fully complete.
    await new Promise<void>((resolve, reject) => {
      const cleanup = (): void => {
        ws.removeEventListener('open', onOpen);
        ws.removeEventListener('error', onError);
      };
      const onOpen = (): void => {
        cleanup();
        this.#logger.log(LogLevel.Information, '(WebSockets transport) Connected.');
        resolve();
      };

      const onError = (event: Event): void => {
        cleanup();
        const err = upgradeStatus === undefined
          ? errorFromEvent(event)
          : new TransportError(`WebSocket upgrade failed with status code ${upgradeStatus}.`, upgradeStatus);
        this.#logger.log(LogLevel.Error, `(WebSockets transport) Error during connect: ${err.message}`);
        this.#ws = null;
        ws.close();
        reject(err);
      };
      ws.addEventListener('open', onOpen);
      ws.addEventListener('error', onError);
    });

    // ── Wire ongoing event handlers (after open) ─────────────────────────────

    // CloseEvent is a browser DOM global absent from Node.js lib targets.
    // Cast to a structural equivalent to access `.code` / `.reason`.
    ws.addEventListener('close', (event) => {
      const closeEvent = event as Event & { code: number; reason: string };
      this.#logger.log(LogLevel.Information,
        `(WebSockets transport) Socket closed (code=${closeEvent.code}).`);
      // Only call onclose for unexpected closures (not our own stop() call).
      if (this.#ws !== null) {
        this.#ws = null;
        // 1000 is a normal closure, and 1005 a Close frame with no status.
        const clean = closeEvent.code === 1000 || closeEvent.code === 1005;
        this.onclose?.(clean ? undefined : new Error(
          `WebSocket closed with status code ${closeEvent.code} (${closeEvent.reason || 'no reason given'}).`,
        ));
      }
    });

    ws.addEventListener('error', (event) => {
      // Errors after the initial open.
      if (ws.readyState !== WebSocket.CONNECTING) {
        const err = errorFromEvent(event);
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
