/**
 * hub-connection.ts
 *
 * Core HubConnection - the main object users interact with.
 *
 * Mirrors the Microsoft SignalR JavaScript client API with full TypeScript types:
 *   start() / stop()
 *   invoke<T>(method, ...args)    → Promise<T>
 *   send(method, ...args)         → Promise<void>
 *   stream<T>(method, ...args)    → IStreamResult<T>
 *   on(method, handler)
 *   off(method, [handler])
 *   onclose / onreconnecting / onreconnected
 */

import { DispatchHttpClient } from './http-client.js';
import {
  JsonHubProtocol,
  parseHandshakeResponse,
} from './protocols/json-hub-protocol.js';
import { WebSocketTransport }        from './transports/websocket-transport.js';
import { ServerSentEventsTransport } from './transports/sse-transport.js';
import { LongPollingTransport }      from './transports/long-polling-transport.js';
import { NullLogger }                from './logger.js';
import {
  MessageType,
  HubConnectionState,
  HttpTransportType,
  TransferFormat,
  LogLevel,
  NEGOTIATE_VERSION,
  DEFAULT_PING_INTERVAL_IN_MS,
  DEFAULT_SERVER_TIMEOUT_IN_MS,
} from './constants.js';
import {
  HubError,
  AbortError,
  TransportError,
  UnsupportedTransportError,
} from './errors.js';
import type {
  ITransport,
  ILogger,
  IHttpClient,
  IRetryPolicy,
  RetryContext,
  IStreamResult,
  IStreamSubscriber,
  ISubscription,
  NegotiateResponse,
  Dispatcher,
  IHubProtocol,
} from './interfaces.js';
import type { CompletionMessage, HubMessage, InvocationId } from './messages.js';
import { toInvocationId }    from './messages.js';

const MAX_REDIRECTS = 100;

// ─── Configuration ────────────────────────────────────────────────────────────

export interface HubConnectionOptions {
  readonly logger?:                          ILogger;
  readonly accessTokenFactory?:              () => Promise<string | null>;
  /** Bitmask of HttpTransportType flags (default: all transports). */
  readonly transport?:                       number;
  readonly headers?:                         Record<string, string>;
  readonly skipNegotiation?:                 boolean;
  readonly serverTimeoutInMilliseconds?:     number;
  readonly keepAliveIntervalInMilliseconds?: number;
  /**
   * Override the SignalR handshake timeout (ms). Defaults to 15 000.
   * Intended for testing; production code should leave this at the default.
   */
  readonly handshakeTimeoutInMilliseconds?:  number;
  readonly reconnectPolicy?:                 IRetryPolicy | null;
  /**
   * undici Dispatcher shared by all HTTP requests (negotiate, SSE, polling)
   * and the WebSocket upgrade for this connection.
   *
   * Pass an `Agent`, `Pool`, `Client`, `ProxyAgent`, etc. to customise
   * connection pooling, TLS, or proxy routing for the entire SignalR session.
   * When omitted, undici's process-global default agent is used.
   */
  readonly dispatcher?:                      Dispatcher;
  /**
   * Custom HTTP client to use for all requests made by this connection
   * (negotiate, SSE body reads, long-polling).
   *
   * When omitted, a `DispatchHttpClient` is created automatically using
   * the `dispatcher` option (if any).  Provide this to use a different
   * undici primitive (e.g. `FetchHttpClient`) or to inject a mock in tests.
   */
  readonly httpClient?:                      IHttpClient;
  /** Hub wire protocol. Defaults to JSON protocol version 1. */
  readonly protocol?:                        IHubProtocol;
  /** Maximum accepted transport payload size. Defaults to 32 MiB. */
  readonly maximumReceiveMessageSize?:       number;
  /**
   * Internal: creates a dispatcher this connection owns. It is closed when the
   * connection ends and created again by the next `start()`. Takes the place
   * of `dispatcher`.
   */
  readonly dispatcherFactory?:               () => Dispatcher;
}

// ─── Internal pending-invocation tracking ────────────────────────────────────

interface PendingInvocation<T = unknown> {
  resolve: (value: T) => void;
  reject:  (err: Error) => void;
}

interface PendingStream<T = unknown> {
  next:     (value: T) => void;
  error:    (err: Error) => void;
  complete: () => void;
}

// ─── Subscription (supports `using` keyword via Symbol.dispose) ───────────────

class StreamSubscription implements ISubscription {
  #disposed = false;
  readonly #cancel: () => void;

  constructor(cancel: () => void) {
    this.#cancel = cancel;
  }

  dispose(): void {
    if (!this.#disposed) {
      this.#disposed = true;
      this.#cancel();
    }
  }

  /** TS 5.2+ `using` keyword support. */
  [Symbol.dispose](): void {
    this.dispose();
  }
}

// ─── HubConnection ────────────────────────────────────────────────────────────

export class HubConnection {
  readonly #url:                  string;
  readonly #logger:               ILogger;
  readonly #accessTokenFactory:   (() => Promise<string | null>) | null;
  readonly #requestedTransport:   number;
  readonly #extraHeaders:         Record<string, string>;
  readonly #skipNegotiation:      boolean;
  readonly #serverTimeout:        number;
  readonly #pingInterval:         number;
  readonly #handshakeTimeout:     number;
  readonly #reconnectPolicy:      IRetryPolicy | null;

  readonly #protocol:    IHubProtocol;
  readonly #customHttp:  IHttpClient | null;
  readonly #dispatcherFactory: (() => Dispatcher) | null;
  #http:                 IHttpClient;
  #dispatcher:           Dispatcher | undefined;
  readonly #maximumReceiveMessageSize: number;

  #transport:       ITransport | null         = null;
  #connectionId:    string | null             = null;
  #connectionToken: string | null             = null;
  #state:           HubConnectionState        = HubConnectionState.Disconnected;
  #stopping:        boolean                   = false;

  /** Pending `invoke()` calls awaiting a Completion message. */
  readonly #callbacks:     Map<InvocationId, PendingInvocation>  = new Map();
  /** Active server-streaming subscriptions awaiting StreamItem/Completion. */
  readonly #streamCbs:     Map<InvocationId, PendingStream>      = new Map();
  /** Client-method handlers registered via `on()`. */
  readonly #methods:       Map<string, Set<(...args: unknown[]) => unknown>> = new Map();

  /** Registered `onclose` callbacks. */
  readonly #closedCallbacks:       Array<(error?: Error) => void>  = [];
  /** Registered `onreconnecting` callbacks. */
  readonly #reconnectingCallbacks: Array<(error?: Error) => void>  = [];
  /** Registered `onreconnected` callbacks. */
  readonly #reconnectedCallbacks:  Array<(connectionId: string | null) => void> = [];

  #pingTimer:          ReturnType<typeof setTimeout> | null = null;
  #serverTimeoutTimer: ReturnType<typeof setTimeout> | null = null;
  #handshakeTimer:     ReturnType<typeof setTimeout> | null = null;

  #handshakeResolver: (() => void)     | null = null;
  #handshakeRejecter: ((e: Error) => void) | null = null;

  #handshakeBuffer: Buffer = Buffer.alloc(0);
  /**
   * Payloads received after the handshake and before the connection is marked
   * Connected. `null` outside that window.
   */
  #earlyPayloads: Uint8Array[] | null = null;
  /** The start or reconnect in progress; aborting it makes it give up. */
  #attempt: AbortController | null = null;
  #stopPromise: Promise<void> | null = null;
  #dispatcherClosePromise: Promise<void> | null = null;
  #invocationSeq: number = 0;

  constructor(url: string, options: HubConnectionOptions = {}) {
    this.#url                = url;
    this.#logger             = options.logger ?? NullLogger.instance;
    this.#accessTokenFactory = options.accessTokenFactory ?? null;
    this.#requestedTransport = options.transport
      ?? (HttpTransportType.WebSockets | HttpTransportType.ServerSentEvents | HttpTransportType.LongPolling);
    this.#extraHeaders       = options.headers ?? {};
    this.#skipNegotiation    = options.skipNegotiation ?? false;
    this.#serverTimeout      = options.serverTimeoutInMilliseconds       ?? DEFAULT_SERVER_TIMEOUT_IN_MS;
    this.#pingInterval       = options.keepAliveIntervalInMilliseconds   ?? DEFAULT_PING_INTERVAL_IN_MS;
    this.#handshakeTimeout   = options.handshakeTimeoutInMilliseconds    ?? 15_000;
    this.#reconnectPolicy    = options.reconnectPolicy ?? null;
    this.#protocol           = options.protocol ?? new JsonHubProtocol();
    this.#dispatcherFactory  = options.dispatcherFactory ?? null;
    this.#dispatcher         = this.#dispatcherFactory?.() ?? options.dispatcher;
    this.#maximumReceiveMessageSize = options.maximumReceiveMessageSize ?? 32 * 1024 * 1024;
    // Use the caller-supplied client, or create a DispatchHttpClient that shares
    // the same dispatcher so negotiate + polls + WebSocket all use one pool.
    this.#customHttp         = options.httpClient ?? null;
    this.#http               = this.#customHttp ?? this.#createHttpClient();
  }

  #createHttpClient(): IHttpClient {
    return new DispatchHttpClient({
      headers: this.#extraHeaders,
      ...(this.#dispatcher !== undefined && { dispatcher: this.#dispatcher }),
    });
  }

  // ─── State ───────────────────────────────────────────────────────────────

  get state(): HubConnectionState    { return this.#state;        }
  get connectionId(): string | null  { return this.#connectionId; }

  // ─── Lifecycle ───────────────────────────────────────────────────────────

  async start(): Promise<void> {
    if (this.#state !== HubConnectionState.Disconnected) {
      throw new Error(
        `Cannot start: connection is not Disconnected (current: ${this.#state}).`
      );
    }
    this.#state    = HubConnectionState.Connecting;
    this.#stopping = false;
    this.#renewDispatcher();
    this.#logger.log(LogLevel.Debug, 'Starting HubConnection.');

    const attempt = new AbortController();
    this.#attempt = attempt;
    try {
      await this.#startInternal(attempt.signal);
      this.#attempt = null;
      this.#state = HubConnectionState.Connected;
      this.#logger.log(LogLevel.Information, 'HubConnection connected.');
      this.#flushEarlyPayloads();
    } catch (err) {
      this.#attempt = null;
      await this.#abandonAttempt();
      this.#connectionId    = null;
      this.#connectionToken = null;
      // stop() is still finishing; it sets the final state itself.
      if (!this.#stopping) {
        this.#state = HubConnectionState.Disconnected;
        void this.#closeOwnedDispatcher();
      }
      this.#logger.log(LogLevel.Error, `Failed to start: ${(err as Error).message}`);
      throw err;
    }
  }

  /**
   * Stop the connection. A start or reconnect in progress gives up, and a
   * `start()` still pending rejects with an `AbortError`. `onclose` fires
   * unless the connection never got as far as connecting. Calls made while one
   * is already stopping share its result.
   */
  stop(): Promise<void> {
    if (this.#state === HubConnectionState.Disconnected) return Promise.resolve();
    if (this.#stopPromise) return this.#stopPromise;
    const stopping: Promise<void> = this.#stopInternal().finally(() => {
      this.#stopPromise = null;
    });
    this.#stopPromise = stopping;
    return stopping;
  }

  async #stopInternal(): Promise<void> {
    const neverConnected = this.#state === HubConnectionState.Connecting;
    this.#stopping = true;
    this.#state = HubConnectionState.Disconnecting;
    this.#logger.log(LogLevel.Debug, 'Stopping HubConnection.');
    this.#clearTimers();

    const stopErr = new AbortError('Connection stopped before the invocation completed.');
    this.#rejectAllPending(stopErr);
    this.#attempt?.abort(new AbortError('The connection was stopped.'));

    try { await this.#detachTransport()?.stop(); } catch { /* ignore */ }

    if (neverConnected) this.#state = HubConnectionState.Disconnected;
    else this.#completeClose();
    await this.#closeOwnedDispatcher();
    this.#logger.log(LogLevel.Information, 'HubConnection stopped.');
  }

  // ─── Sending ─────────────────────────────────────────────────────────────

  /**
   * Invoke a hub method and return the server's result.
   *
   * @typeParam T  The expected return type.  Asserted at runtime - the server
   *               decides the actual value.
   */
  async invoke<T = void>(methodName: string, ...args: unknown[]): Promise<T> {
    this.#assertConnected('invoke');
    const id  = this.#nextId();
    const msg = this.#encode(JsonHubProtocol.invocation(id, methodName, args));

    return new Promise<T>((resolve, reject) => {
      this.#callbacks.set(id, {
        resolve: resolve as (v: unknown) => void,
        reject,
      });
      this.#send(msg).catch((err: Error) => {
        this.#callbacks.delete(id);
        reject(err);
      });
    });
  }

  /**
   * Fire-and-forget hub method invocation.
   * The server does not send a completion message.
   */
  async send(methodName: string, ...args: unknown[]): Promise<void> {
    this.#assertConnected('send');
    const msg = this.#encode(JsonHubProtocol.send(methodName, args));
    return this.#send(msg);
  }

  // ─── Server-streaming ────────────────────────────────────────────────────

  /**
   * Start a server-streaming invocation immediately.
   * The returned `ISubscription` implements `Symbol.dispose` for `using`.
   * Failures, including calling this while not connected, go to the
   * subscriber's `error` callback.
   *
   * @typeParam T  Type of each streamed item.
   *
   * @example
   * ```ts
   * using sub = connection.stream<number>('Counter', 10).subscribe({
   *   next:     (n) => console.log(n),
   *   complete: ()  => console.log('done'),
   * });
   * ```
   */
  stream<T = unknown>(methodName: string, ...args: unknown[]): IStreamResult<T> {
    const id = this.#nextId();
    type Notification =
      | { kind: 'next'; value: T }
      | { kind: 'error'; error: Error }
      | { kind: 'complete' };
    const queued: Notification[] = [];
    let activeSubscriber: IStreamSubscriber<T> | null = null;
    let subscribed = false;

    const deliver = (notification: Notification): void => {
      if (!activeSubscriber) {
        queued.push(notification);
        return;
      }
      // A subscriber's bug must not stop message processing or a clean close.
      try {
        if (notification.kind === 'next') activeSubscriber.next?.(notification.value);
        else if (notification.kind === 'error') activeSubscriber.error?.(notification.error);
        else activeSubscriber.complete?.();
      } catch (err) {
        this.#logger.log(LogLevel.Error, `Subscriber of "${methodName}" threw: ${(err as Error).message}`);
      }
    };

    this.#streamCbs.set(id, {
      next:     (value) => deliver({ kind: 'next', value: value as T }),
      error:    (error) => deliver({ kind: 'error', error }),
      complete: ()      => deliver({ kind: 'complete' }),
    });

    const fail = (error: Error): void => {
      this.#streamCbs.delete(id);
      deliver({ kind: 'error', error });
    };
    try {
      this.#assertConnected('stream');
      this.#send(this.#encode(JsonHubProtocol.streamInvocation(id, methodName, args))).catch(fail);
    } catch (error) {
      fail(error as Error);
    }

    return {
      subscribe: (subscriber: IStreamSubscriber<T>): ISubscription => {
        if (subscribed) throw new Error('A stream result can only be subscribed to once.');
        subscribed = true;
        activeSubscriber = subscriber;
        for (const notification of queued.splice(0)) deliver(notification);

        return new StreamSubscription(() => {
          if (this.#streamCbs.has(id)) {
            this.#streamCbs.delete(id);
            this.#send(
              this.#encode(JsonHubProtocol.cancelInvocation(id))
            ).catch(
              /* istanbul ignore next -- cancellation is intentionally best-effort */
              () => { /* best-effort */ },
            );
          }
        });
      },
    };
  }

  // ─── Event registration ───────────────────────────────────────────────────

  /**
   * Register a handler for a server→client hub method.
   * Multiple handlers for the same method are supported.
   *
   * @typeParam TArgs  Tuple of argument types for the hub method.
   */
  on<TArgs extends unknown[] = unknown[]>(
    methodName: string,
    handler: (...args: TArgs) => unknown,
  ): void {
    const key = methodName.toLowerCase();
    if (!this.#methods.has(key)) this.#methods.set(key, new Set());
    this.#methods.get(key)!.add(handler as (...args: unknown[]) => unknown);
  }

  /**
   * Remove a previously registered handler, or all handlers for a method.
   */
  off(methodName: string, handler?: (...args: unknown[]) => unknown): void {
    const key = methodName.toLowerCase();
    if (!this.#methods.has(key)) return;
    if (handler == null) {
      this.#methods.delete(key);
    } else {
      this.#methods.get(key)!.delete(handler);
      if (this.#methods.get(key)!.size === 0) this.#methods.delete(key);
    }
  }

  onclose(callback:      (error?: Error)              => void): void { this.#closedCallbacks.push(callback);       }
  onreconnecting(callback:(error?: Error)              => void): void { this.#reconnectingCallbacks.push(callback); }
  onreconnected(callback: (connectionId: string | null) => void): void { this.#reconnectedCallbacks.push(callback);  }

  // ─── Internal: start sequence ────────────────────────────────────────────

  async #startInternal(signal: AbortSignal): Promise<void> {
    let url = this.#url;
    // A token from a negotiate redirect belongs to that redirect, not to the
    // next connection.
    this.#_accessTokenFactory = null;

    if (this.#skipNegotiation) {
      if (this.#requestedTransport !== HttpTransportType.WebSockets) {
        throw new Error('skipNegotiation requires WebSockets as the only transport.');
      }
      await this.#connectTransport(this.#createTransport(HttpTransportType.WebSockets), url, signal);
    } else {
      let redirectCount = 0;
      for (;;) {
        const neg = await abortable(this.#negotiate(url), signal);

        if (neg.url) {
          if (redirectCount++ >= MAX_REDIRECTS) throw new Error('Too many negotiate redirects.');
          url = neg.url;
          if (neg.accessToken) {
            const staticToken = neg.accessToken;
            this.#overrideAccessToken(() => Promise.resolve(staticToken));
          }
          continue;
        }

        /* istanbul ignore next -- a successful negotiate response must include connectionId */
        this.#connectionId    = neg.connectionId ?? null;
        /* istanbul ignore next -- the protocol requires a token or connectionId */
        this.#connectionToken = neg.connectionToken ?? neg.connectionId ?? null;

        /* istanbul ignore next -- the protocol requires availableTransports */
        await this.#selectTransport(url, neg.availableTransports ?? [], signal);
        break;
      }
    }

    // Wire message routing
    this.#transport!.onreceive = (data) => this.#processIncoming(data);
    this.#transport!.onclose   = (err) => this.#onTransportClosed(err);

    // Protocol handshake
    await abortable(this.#performHandshake(), signal);
    signal.throwIfAborted();

    // Keep-alive
    this.#resetPingTimer();
    this.#resetServerTimeoutTimer();
  }

  // Rebinds the access token factory (used after Azure SignalR redirect).
  // Stored as a field to allow rebinding without recreating the instance.
  #_accessTokenFactory: (() => Promise<string | null>) | null = null;

  #overrideAccessToken(factory: () => Promise<string | null>): void {
    this.#_accessTokenFactory = factory;
  }

  #getAccessTokenFactory(): (() => Promise<string | null>) | null {
    return this.#_accessTokenFactory ?? this.#accessTokenFactory;
  }

  async #negotiate(baseUrl: string): Promise<NegotiateResponse> {
    // Use the URL API so that an existing query string in baseUrl (e.g.
    // "?token=abc") does not corrupt the pathname.  Simple string concatenation
    // would produce "?token=abc/negotiate" - wrong path, wrong query string.
    const negotiateUrl = new URL(baseUrl);
    negotiateUrl.pathname = negotiateUrl.pathname.replace(/\/$/, '') + '/negotiate';
    negotiateUrl.searchParams.set('negotiateVersion', String(NEGOTIATE_VERSION));
    const url = negotiateUrl.toString();
    this.#logger.log(LogLevel.Debug, `Sending negotiate request to ${url}.`);

    const headers: Record<string, string> = { ...this.#extraHeaders };
    const factory = this.#getAccessTokenFactory();
    if (factory) {
      const token = await factory();
      if (token) headers['Authorization'] = `Bearer ${token}`;
    }

    const res = await this.#http.post(url, { headers });

    if (res.status !== 200) {
      throw new TransportError(
        `Unexpected status code ${res.status} from negotiate.`,
        res.status
      );
    }

    if (Buffer.byteLength(res.body, 'utf8') > this.#maximumReceiveMessageSize) {
      throw new Error(`Negotiate response exceeds ${this.#maximumReceiveMessageSize} bytes.`);
    }

    const body = JSON.parse(res.body) as NegotiateResponse;
    if (body.error) throw new Error(`Negotiate returned error: ${body.error}`);
    return body;
  }

  async #selectTransport(
    url:       string,
    available: readonly { transport: string; transferFormats: readonly string[] }[],
    signal:    AbortSignal,
  ): Promise<void> {
    const ORDERED = [
      { flag: HttpTransportType.WebSockets,       name: 'WebSockets'       },
      { flag: HttpTransportType.ServerSentEvents, name: 'ServerSentEvents' },
      { flag: HttpTransportType.LongPolling,      name: 'LongPolling'      },
    ] as const;
    /* istanbul ignore next -- both protocol formats are exercised at the protocol boundary */
    const formatName = this.#protocol.transferFormat === TransferFormat.Binary ? 'Binary' : 'Text';
    const failures: string[] = [];
    let lastError: Error | undefined;

    for (const pref of ORDERED) {
      if ((this.#requestedTransport & pref.flag) === 0) continue;
      const offered = available.find((candidate) => candidate.transport === pref.name);
      if (!offered?.transferFormats.includes(formatName)) continue;

      this.#logger.log(LogLevel.Debug, `Trying ${pref.name} transport.`);
      const transport = this.#createTransport(pref.flag);
      /* istanbul ignore next -- successful negotiation always supplies a token */
      const token     = this.#connectionToken ?? '';
      const tUrl      = appendParam(url, 'id', token);

      try {
        await this.#connectTransport(transport, tUrl, signal);
        return;
      } catch (err) {
        if (signal.aborted) throw err;
        lastError = err as Error;
        failures.push(`${pref.name}: ${lastError.message}`);
        this.#logger.log(LogLevel.Warning, `${pref.name} failed: ${lastError.message}`);
      }
    }

    throw new UnsupportedTransportError(
      ['Unable to connect with any of the available transports.', ...failures].join(' '),
      null,
      lastError && { cause: lastError },
    );
  }

  async #connectTransport(transport: ITransport, url: string, signal: AbortSignal): Promise<void> {
    // Held from here on so that stop() reaches a transport still connecting.
    this.#transport = transport;
    const connecting = transport.connect(url, this.#protocol.transferFormat);
    try {
      await abortable(connecting, signal);
    } catch (err) {
      // A transport can still finish connecting after the attempt gave up.
      if (signal.aborted) {
        void connecting.then(() => transport.stop()).catch(
          /* istanbul ignore next -- shutdown of an abandoned transport is best-effort */
          () => {},
        );
      }
      if (this.#transport === transport) this.#transport = null;
      throw err;
    }
  }

  #createTransport(type: number): ITransport {
    const factory = this.#getAccessTokenFactory();
    const headers = this.#extraHeaders;
    switch (type) {
      case HttpTransportType.WebSockets:
        return new WebSocketTransport(factory, this.#logger, headers, this.#dispatcher);
      case HttpTransportType.ServerSentEvents:
        return new ServerSentEventsTransport(this.#http, factory, this.#logger, headers, this.#maximumReceiveMessageSize);
      case HttpTransportType.LongPolling:
        return new LongPollingTransport(this.#http, factory, this.#logger, headers, this.#maximumReceiveMessageSize);
      /* istanbul ignore next */
      default:
        throw new UnsupportedTransportError(`Unknown transport type: ${type}`, type);
    }
  }

  // ─── Internal: handshake ─────────────────────────────────────────────────

  #performHandshake(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.#handshakeResolver = resolve;
      this.#handshakeRejecter = reject;

      const request = JSON.stringify({ protocol: this.#protocol.name, version: this.#protocol.version }) + '\x1e';
      this.#transport!.send(request).catch((error: unknown) => {
        /* istanbul ignore next -- transport.send rejects with Error */
        this.#failHandshake(error instanceof Error ? error : new Error(String(error)));
      });

      this.#handshakeTimer = setTimeout(() => {
        this.#failHandshake(new Error('Server timeout: no handshake response received.'));
      }, this.#handshakeTimeout);
    });
  }

  #failHandshake(error: Error): void {
    clearTimeout(this.#handshakeTimer!);
    const reject = this.#handshakeRejecter;
    this.#handshakeResolver = null;
    this.#handshakeRejecter = null;
    this.#handshakeBuffer   = Buffer.alloc(0);
    reject?.(error);
  }

  // ─── Internal: incoming data ─────────────────────────────────────────────

  #processIncoming(data: string | Uint8Array): void {
    // The handshake has its own timeout, and the server timeout starts after it.
    if (!this.#handshakeResolver) this.#resetServerTimeoutTimer();
    /* istanbul ignore next -- binary input is exercised through binary protocol tests */
    const bytes = typeof data === 'string' ? Buffer.from(data, 'utf8') : Buffer.from(data);
    if (bytes.byteLength > this.#maximumReceiveMessageSize) {
      const err = new Error(`Incoming message exceeds ${this.#maximumReceiveMessageSize} bytes.`);
      if (this.#handshakeResolver) this.#failHandshake(err);
      else this.#stopWithError(err);
      return;
    }

    if (this.#earlyPayloads) {
      this.#earlyPayloads.push(bytes);
      return;
    }

    // During handshake the first data is the response
    if (this.#handshakeResolver) {
      this.#handshakeBuffer = Buffer.concat([this.#handshakeBuffer, bytes]);
      const separator = this.#handshakeBuffer.indexOf(0x1e);
      /* istanbul ignore next -- partial handshakes are buffered for the next frame */
      if (separator === -1) return;
      try {
        parseHandshakeResponse(this.#handshakeBuffer.subarray(0, separator + 1).toString('utf8'));
        const remainder = this.#handshakeBuffer.subarray(separator + 1);
        this.#handshakeBuffer = Buffer.alloc(0);
        this.#logger.log(LogLevel.Debug, 'Handshake complete.');
        clearTimeout(this.#handshakeTimer!);
        const r = this.#handshakeResolver;
        this.#handshakeResolver = null;
        this.#handshakeRejecter = null;
        // Hub messages wait for the connection to be Connected: a Close among
        // them must not race whoever is finishing the start or reconnect.
        this.#earlyPayloads = remainder.length > 0 ? [remainder] : [];
        r();
      } catch (err) {
        /* istanbul ignore next -- JSON.parse and protocol validation throw Error objects */
        this.#failHandshake(err instanceof Error ? err : new Error(String(err)));
      }
      return;
    }

    this.#processProtocolPayload(bytes);
  }

  #flushEarlyPayloads(): void {
    const payloads = this.#earlyPayloads;
    this.#earlyPayloads = null;
    // Anything after a Close, or after an onreconnected callback stopped the
    // connection, has nowhere to go.
    for (const payload of payloads!) {
      if (this.#state === HubConnectionState.Connected) this.#processProtocolPayload(payload);
    }
  }

  #processProtocolPayload(bytes: Uint8Array): void {
    /* istanbul ignore next -- transports do not emit empty payloads */
    if (bytes.length === 0) return;

    let messages;
    try {
      /* istanbul ignore next -- binary parsing is left to custom protocols */
      const input = this.#protocol.transferFormat === TransferFormat.Text
        ? Buffer.from(bytes).toString('utf8')
        : Uint8Array.from(bytes).buffer;
      messages = this.#protocol.parseMessages(input, this.#logger);
    } catch (err) {
      this.#logger.log(LogLevel.Error, `Error parsing messages: ${(err as Error).message}`);
      /* istanbul ignore next -- protocol parsers throw Error objects */
      this.#stopWithError(err instanceof Error ? err : new Error(String(err)));
      return;
    }

    for (const msg of messages) {
      switch (msg.type) {
        // ── Type 1: Server calls a client method ──────────────────────────
        case MessageType.Invocation: {
          void this.#invokeClientMethod(msg);
          break;
        }

        // ── Type 2: One item from a server stream ─────────────────────────
        // Per spec, StreamItem carries only `item`; there is no `error` field.
        case MessageType.StreamItem: {
          const sc = this.#streamCbs.get(msg.invocationId);
          if (!sc) break;
          sc.next(msg.item);
          break;
        }

        // ── Type 3: Completion of an invoke() or stream ───────────────────
        case MessageType.Completion: {
          const cb = this.#callbacks.get(msg.invocationId);
          if (cb) {
            this.#callbacks.delete(msg.invocationId);
            if (msg.error) cb.reject(new HubError(msg.error));
            else           cb.resolve(msg.result ?? null);
          }
          const sc = this.#streamCbs.get(msg.invocationId);
          if (sc) {
            this.#streamCbs.delete(msg.invocationId);
            if (msg.error) sc.error(new HubError(msg.error));
            else           sc.complete();
          }
          break;
        }

        // ── Type 6: Ping ──────────────────────────────────────────────────
        case MessageType.Ping:
          // Server-timeout watchdog already reset at the top of this method
          break;

        // ── Type 7: Server-initiated close ────────────────────────────────
        case MessageType.Close: {
          this.#logger.log(LogLevel.Information, 'Server sent Close message.');
          const closeErr = msg.error ? new HubError(msg.error) : null;
          // Per spec: if allowReconnect is true AND the client has a reconnect
          // policy, the client should attempt reconnection rather than a
          // permanent disconnect.  Null out onclose on the old transport to
          // prevent #onTransportClosed from firing a second reconnect loop.
          if (msg.allowReconnect && this.#reconnectPolicy && !this.#stopping) {
            this.#abandonTransportAndReconnect(closeErr);
          } else {
            this.#stopWithError(closeErr);
          }
          break;
        }

        default:
          this.#logger.log(LogLevel.Warning, `Unknown message type: ${(msg as { type: number }).type}`);
          break;
      }
    }
  }

  async #invokeClientMethod(msg: Extract<HubMessage, { type: 1 }>): Promise<void> {
    const handlers = this.#methods.get(msg.target.toLowerCase());
    if (!handlers || handlers.size === 0) {
      this.#logger.log(LogLevel.Warning, `No handler for "${msg.target}".`);
      if (msg.invocationId) {
        await this.#sendCompletion({
          type: MessageType.Completion,
          invocationId: msg.invocationId,
          error: `No client handler registered for '${msg.target}'.`,
        });
      }
      return;
    }

    let result: unknown;
    let invocationError: Error | null = null;
    for (const handler of [...handlers]) {
      try {
        result = await handler(...(msg.arguments as unknown[]));
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        invocationError ??= err;
        this.#logger.log(LogLevel.Error, `Handler for "${msg.target}" threw: ${err.message}`);
      }
    }
    if (msg.invocationId) {
      await this.#sendCompletion(invocationError
        ? { type: MessageType.Completion, invocationId: msg.invocationId, error: invocationError.message }
        : {
            type: MessageType.Completion,
            invocationId: msg.invocationId,
            ...(result !== undefined && { result }),
          });
    }
  }

  /** Best-effort: the connection may have closed while the handler ran. */
  async #sendCompletion(completion: CompletionMessage): Promise<void> {
    let encoded: string | Uint8Array;
    try {
      encoded = this.#encode(completion);
    } catch (err) {
      encoded = this.#encode({
        type: MessageType.Completion,
        invocationId: completion.invocationId,
        error: `The handler result could not be serialized: ${(err as Error).message}`,
      });
    }
    await this.#send(encoded).catch((err: Error) => {
      this.#logger.log(LogLevel.Debug, `Could not send a completion: ${err.message}`);
    });
  }

  // ─── Internal: transport closed ──────────────────────────────────────────

  #onTransportClosed(err?: Error): void {
    /* istanbul ignore next - WebSocketTransport.stop() nulls #ws before
       calling ws.close(), so the close handler never invokes onclose once
       stop() has set #stopping; this guard is a belt-and-suspenders defence. */
    if (this.#stopping) return;
    // Whoever is waiting for the handshake decides what happens next.
    if (this.#handshakeResolver) {
      this.#failHandshake(err ?? new Error('The connection closed during the handshake.'));
      return;
    }
    this.#logger.log(
      LogLevel.Warning,
      err ? `Transport closed with error: ${err.message}` : 'Transport closed.'
    );
    this.#clearTimers();
    this.#rejectAllPending(err ?? new AbortError('Connection closed while reconnecting.'));

    if (this.#reconnectPolicy) {
      void this.#doReconnect(err ?? null);
    } else {
      this.#completeClose(err);
    }
  }

  /**
   * Drop the current transport without firing its onclose (which would start a
   * second reconnect loop) and reconnect.  Requires a reconnect policy.
   */
  #abandonTransportAndReconnect(err: Error | null): void {
    this.#clearTimers();
    const t = this.#detachTransport();
    this.#rejectAllPending(err ?? new AbortError('Connection closed while reconnecting.'));
    void t?.stop().catch(
      /* istanbul ignore next -- shutdown of an abandoned transport is best-effort */
      () => {},
    );
    void this.#doReconnect(err);
  }

  #stopWithError(err: Error | null): void {
    this.#clearTimers();
    void this.#detachTransport()?.stop().catch(
      /* istanbul ignore next -- error shutdown is best-effort */
      () => { /* ignore */ },
    );
    this.#completeClose(err ?? undefined);
  }

  /** Disown the transport, so nothing it still does reaches this connection. */
  #detachTransport(): ITransport | null {
    const transport = this.#transport;
    if (transport) {
      transport.onclose   = null;
      transport.onreceive = null;
    }
    this.#transport = null;
    return transport;
  }

  /** Undo whatever a failed start or reconnect attempt left behind. */
  async #abandonAttempt(): Promise<void> {
    this.#clearTimers();
    this.#handshakeResolver = null;
    this.#handshakeRejecter = null;
    this.#handshakeBuffer   = Buffer.alloc(0);
    this.#earlyPayloads     = null;
    await this.#detachTransport()?.stop().catch(/* istanbul ignore next -- cleanup is best-effort */ () => {});
  }

  #completeClose(err?: Error): void {
    const closeErr = err ?? new AbortError('Connection closed.');
    this.#rejectAllPending(closeErr);
    this.#state = HubConnectionState.Disconnected;
    this.#connectionId    = null;
    this.#connectionToken = null;
    // Before the callbacks, as one of them may call start().
    void this.#closeOwnedDispatcher();
    for (const cb of this.#closedCallbacks) {
      try { cb(err); } catch { /* ignore */ }
    }
  }

  #rejectAllPending(err: Error): void {
    this.#callbacks.forEach((cb) => cb.reject(err));
    this.#callbacks.clear();
    this.#streamCbs.forEach((sc) => sc.error(err));
    this.#streamCbs.clear();
  }

  // ─── Internal: auto-reconnect ─────────────────────────────────────────────

  async #doReconnect(disconnectError: Error | null): Promise<void> {
    this.#state = HubConnectionState.Reconnecting;
    const attempt = new AbortController();
    this.#attempt = attempt;
    for (const cb of this.#reconnectingCallbacks) {
      try { cb(disconnectError ?? undefined); } catch { /* ignore */ }
    }

    let retryCount    = 0;
    const startTime   = Date.now();
    let lastError     = disconnectError;

    while (!this.#stopping) {
      const ctx: RetryContext = {
        previousRetryCount:  retryCount,
        elapsedMilliseconds: Date.now() - startTime,
        retryReason:         lastError,
      };
      let delay: number | null;
      try {
        delay = this.#reconnectPolicy!.nextRetryDelayInMilliseconds(ctx);
      } catch (err) {
        /* istanbul ignore next -- policies throw Error objects */
        lastError = err instanceof Error ? err : new Error(String(err));
        this.#logger.log(LogLevel.Error, `Reconnect policy threw: ${lastError.message}`);
        break;
      }
      if (delay === null) {
        this.#logger.log(LogLevel.Debug, 'Reconnect policy returned null - giving up.');
        break;
      }

      this.#logger.log(LogLevel.Information, `Reconnecting in ${delay} ms (attempt ${retryCount + 1}).`);
      await sleep(delay, attempt.signal);
      if (this.#stopping) break;

      try {
        await this.#startInternal(attempt.signal);
        this.#attempt = null;
        this.#state = HubConnectionState.Connected;
        this.#logger.log(LogLevel.Information, 'Reconnected successfully.');
        for (const cb of this.#reconnectedCallbacks) {
          try { cb(this.#connectionId); } catch { /* ignore */ }
        }
        this.#flushEarlyPayloads();
        return;
      } catch (err) {
        await this.#abandonAttempt();
        retryCount++;
        /* istanbul ignore next - #startInternal() only throws Error instances. */
        lastError = err instanceof Error ? err : new Error(String(err));
        this.#logger.log(LogLevel.Warning, `Reconnect attempt ${retryCount} failed: ${lastError.message}`);
      }
    }

    this.#attempt = null;
    // stop() during a reconnect has already closed the connection.
    if (this.#stopping) return;
    this.#completeClose(lastError ?? undefined);
  }

  // ─── Internal: timers ────────────────────────────────────────────────────

  #resetPingTimer(): void {
    clearTimeout(this.#pingTimer!);
    this.#pingTimer = setTimeout(async () => {
      /* istanbul ignore next - #clearTimers() always cancels this before state
         leaves Connected; the guard is a defensive fallback. */
      if (this.#state !== HubConnectionState.Connected) return;
      try {
        await this.#send(this.#encode(JsonHubProtocol.ping()));
      } catch (err) {
        this.#logger.log(LogLevel.Warning, `Ping failed: ${(err as Error).message}`);
      }
      this.#resetPingTimer();
    }, this.#pingInterval);
  }

  #resetServerTimeoutTimer(): void {
    clearTimeout(this.#serverTimeoutTimer!);
    this.#serverTimeoutTimer = setTimeout(() => {
      const err = new AbortError('Server timeout: no message received within the timeout period.');
      // Like Microsoft's client, a silent server is a lost connection: with a
      // reconnect policy it reconnects instead of closing for good.
      if (this.#reconnectPolicy && !this.#stopping) {
        this.#logger.log(LogLevel.Warning, 'Server timeout - reconnecting.');
        this.#abandonTransportAndReconnect(err);
      } else {
        this.#logger.log(LogLevel.Warning, 'Server timeout - closing connection.');
        this.#stopWithError(err);
      }
    }, this.#serverTimeout);
  }

  #clearTimers(): void {
    clearTimeout(this.#pingTimer!);
    clearTimeout(this.#serverTimeoutTimer!);
    clearTimeout(this.#handshakeTimer!);
    this.#pingTimer          = null;
    this.#serverTimeoutTimer = null;
    this.#handshakeTimer     = null;
  }

  // ─── Internal: helpers ────────────────────────────────────────────────────

  #send(message: string | Uint8Array): Promise<void> {
    if (!this.#transport) return Promise.reject(new Error('The connection has no active transport.'));
    this.#resetPingTimer();
    return this.#transport.send(message);
  }

  #encode(message: HubMessage): string | Uint8Array {
    const encoded = this.#protocol.writeMessage(message);
    return typeof encoded === 'string' ? encoded : new Uint8Array(encoded);
  }

  #assertConnected(method: string): void {
    if (this.#state !== HubConnectionState.Connected) {
      throw new Error(
        `Cannot call "${method}" when connection is not Connected (current: ${this.#state}).`
      );
    }
  }

  #nextId(): InvocationId {
    return toInvocationId(String(++this.#invocationSeq));
  }

  async #closeOwnedDispatcher(): Promise<void> {
    if (!this.#dispatcherFactory) return;
    this.#dispatcherClosePromise ??= this.#dispatcher!.close().catch(
      /* istanbul ignore next -- shutdown cleanup is best-effort */ () => {},
    );
    await this.#dispatcherClosePromise;
  }

  /** Replace an owned dispatcher the previous connection closed. */
  #renewDispatcher(): void {
    if (!this.#dispatcherFactory || !this.#dispatcherClosePromise) return;
    this.#dispatcherClosePromise = null;
    this.#dispatcher = this.#dispatcherFactory();
    if (!this.#customHttp) this.#http = this.#createHttpClient();
  }
}

// ─── Module-level helpers ─────────────────────────────────────────────────────

/**
 * Append a single query-string name/value pair to a URL.
 *
 * Uses the WHATWG `URL` API so the operation is always correct regardless of
 * whether the URL already contains a query string, path segments, or both.
 * This avoids the classic bug where naive string concatenation appends a path
 * segment *after* an existing query string, corrupting the URL.
 *
 * e.g. appendParam('https://host/hub?token=x', 'id', 'y')
 *   → 'https://host/hub?token=x&id=y'   ✓  (not '?token=x&id=y' appended raw)
 */
function appendParam(url: string, name: string, value: string): string {
  const u = new URL(url);
  u.searchParams.append(name, value);
  return u.toString();
}

/** Wait `ms`, or until `signal` aborts. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    const done = (): void => {
      clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
  });
}

/** Settle with `promise`, or reject with the abort reason if `signal` aborts first. */
function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason as Error);
    signal.addEventListener('abort', onAbort, { once: true });
    const cleanup = (): void => signal.removeEventListener('abort', onAbort);
    promise.then(
      (value) => { cleanup(); resolve(value); },
      (error: unknown) => { cleanup(); reject(error as Error); },
    );
  });
}
