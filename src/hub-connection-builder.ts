/**
 * hub-connection-builder.ts
 *
 * Fluent builder for HubConnection - mirrors the official Microsoft client API.
 * Uses `const` type parameters (TS 5.0+) and `NoInfer<T>` (TS 5.4+) where
 * applicable to preserve literal types and prevent unintended widening.
 *
 * Usage
 * ─────
 *   const connection = new HubConnectionBuilder()
 *     .withUrl('https://example.com/hub', {
 *       accessTokenFactory: () => fetchToken(),
 *       transport: HttpTransportType.WebSockets,
 *     })
 *     .configureLogging(LogLevel.Information)
 *     .withAutomaticReconnect()
 *     .build();
 */

import { HubConnection }  from './hub-connection.js';
import type { HubConnectionOptions } from './hub-connection.js';
import { resolveLogger }  from './logger.js';
import { LogLevel, HttpTransportType } from './constants.js';
import type { ILogger, IHttpClient, IRetryPolicy, RetryContext, Dispatcher } from './interfaces.js';
import { CookieAgent } from '@exhumer/undici-cookie-agent';
import { CookieJar } from 'tough-cookie';

/** Default reconnect delay sequence in ms: 0, 2 s, 10 s, 30 s then give up. */
const DEFAULT_RETRY_DELAYS = [0, 2_000, 10_000, 30_000] as const satisfies readonly number[];

// ─── withUrl option bag ───────────────────────────────────────────────────────

export interface UrlOptions {
  /** Async function returning a Bearer token (or null for unauthenticated). */
  readonly accessTokenFactory?: () => Promise<string | null>;
  /**
   * Bitmask of {@link HttpTransportType} values.
   * Defaults to all three transports if omitted.
   */
  readonly transport?: number;
  /** Extra HTTP headers appended to every request (negotiate, upgrade, poll…). */
  readonly headers?: Record<string, string>;
  /**
   * Skip the /negotiate step and connect directly via WebSocket.
   * Only valid when transport is exclusively `HttpTransportType.WebSockets`.
   */
  readonly skipNegotiation?: boolean;
  /** ms before the connection is considered unresponsive (default: 30 000). */
  readonly serverTimeoutInMilliseconds?: number;
  /** ms between keep-alive pings (default: 15 000). */
  readonly keepAliveIntervalInMilliseconds?: number;
}

// ─── HubConnectionBuilder ────────────────────────────────────────────────────

export class HubConnectionBuilder {
  #url:             string | null       = null;
  #urlOptions:      UrlOptions          = {};
  #logger:          ILogger | null      = null;
  #reconnectPolicy: IRetryPolicy | null = null;
  #dispatcher:      Dispatcher | null   = null;
  #httpClient:      IHttpClient | null  = null;
  #cookieJar:       CookieJar  | null  = null;

  // ─── withUrl ─────────────────────────────────────────────────────────────

  withUrl(url: string, options?: UrlOptions): this {
    if (!url || typeof url !== 'string') {
      throw new TypeError('withUrl requires a non-empty string URL.');
    }
    this.#url        = url;
    this.#urlOptions = options ?? {};
    return this;
  }

  // ─── withCookies ─────────────────────────────────────────────────────────

  /**
   * Enable automatic cookie handling for the entire connection session using
   * a `tough-cookie` `CookieJar` and an `@exhumer/undici-cookie-agent` `CookieAgent`.
   *
   * When enabled, the `CookieAgent` wraps every outgoing request - including
   * the `/negotiate` POST, WebSocket upgrade, SSE long-lived GET, and all
   * long-polling requests - so that:
   *
   *  1. `Set-Cookie` headers in the **negotiate response** (and any subsequent
   *     response) are stored in the jar automatically.
   *  2. All subsequent requests from the same session include the matching
   *     `Cookie` header, exactly as a browser would.
   *
   * @param jar
   *   An existing `CookieJar` to use.  Pass your own jar when you need to
   *   pre-seed cookies (e.g. an auth session cookie obtained before calling
   *   `start()`) or inspect the jar after the session ends.
   *   When omitted a new, empty jar is created automatically.
   *
   * @throws if {@link withDispatcher} has already been called.  To combine a
   *   custom dispatcher (e.g. `ProxyAgent`) with cookie support, compose the
   *   `cookie` interceptor onto your dispatcher and pass it to `withDispatcher()`:
   *
   * ```ts
   * import { ProxyAgent } from 'undici';
   * import { cookie, CookieJar } from '@exhumer/signalr-client';
   *
   * const jar   = new CookieJar();
   * const agent = new ProxyAgent('http://proxy:8080').compose(cookie({ jar }));
   *
   * const conn = new HubConnectionBuilder()
   *   .withUrl('https://example.com/hub')
   *   .withDispatcher(agent)
   *   .build();
   * ```
   *
   * @example
   * ```ts
   * // Automatic jar - no pre-configuration needed
   * const conn = new HubConnectionBuilder()
   *   .withUrl('https://example.com/hub')
   *   .withCookies()
   *   .build();
   *
   * // Pre-seeded jar - inject a session cookie before connecting
   * import { CookieJar } from '@exhumer/signalr-client';
   * const jar = new CookieJar();
   * await jar.setCookie('session=abc123', 'https://example.com');
   *
   * const conn = new HubConnectionBuilder()
   *   .withUrl('https://example.com/hub')
   *   .withCookies(jar)
   *   .build();
   * ```
   */
  withCookies(jar?: CookieJar): this {
    if (this.#dispatcher !== null) {
      throw new Error(
        'withCookies() cannot be used after withDispatcher(). ' +
        'To combine a custom dispatcher with cookie support, compose the ' +
        'cookie interceptor from @exhumer/undici-cookie-agent onto your ' +
        'dispatcher and pass it to withDispatcher() instead.',
      );
    }
    this.#cookieJar = jar ?? new CookieJar();
    return this;
  }

  // ─── withDispatcher ──────────────────────────────────────────────────────

  /**
   * Set the undici `Dispatcher` for the entire connection session.
   *
   * The same dispatcher is used for:
   *   • the `/negotiate` HTTP POST
   *   • the WebSocket upgrade request
   *   • SSE long-lived GET and send POSTs
   *   • long-polling GETs, POSTs, and DELETE
   *
   * Accepts any `Dispatcher` subclass: `Agent`, `Pool`, `Client`,
   * `ProxyAgent`, `MockAgent`, etc.
   *
   * To combine a custom dispatcher with automatic cookie handling, use
   * `createCookieAgentClass()` to build a cookie-aware variant of your
   * dispatcher and pass that here.  Do **not** chain both
   * `withDispatcher()` and `withCookies()` - they are mutually exclusive
   * because `withCookies()` creates its own dispatcher internally.
   *
   * @example
   * ```ts
   * import { ProxyAgent } from 'undici';
   *
   * const conn = new HubConnectionBuilder()
   *   .withUrl('https://example.com/hub')
   *   .withDispatcher(new ProxyAgent('http://proxy:8080'))
   *   .build();
   * ```
   */
  withDispatcher(dispatcher: Dispatcher): this {
    if (this.#cookieJar !== null) {
      throw new Error(
        'withDispatcher() cannot be used after withCookies(). ' +
        'To combine a custom dispatcher with cookie support, compose the ' +
        'cookie interceptor from @exhumer/undici-cookie-agent onto your ' +
        'dispatcher and pass it to withDispatcher() instead.',
      );
    }
    this.#dispatcher = dispatcher;
    return this;
  }

  /**
   * Provide a custom HTTP client implementation.
   *
   * By default `HubConnection` uses `DispatchHttpClient` (undici `dispatch`).
   * Use this to substitute any of the other undici-backed clients, or
   * to inject a mock for testing.
   *
   * @example
   * ```ts
   * import { FetchHttpClient } from '@signalr/client';
   *
   * const conn = new HubConnectionBuilder()
   *   .withUrl('https://example.com/hub')
   *   .withHttpClient(new FetchHttpClient())
   *   .build();
   * ```
   */
  withHttpClient(httpClient: IHttpClient): this {
    this.#httpClient = httpClient;
    return this;
  }

  // ─── configureLogging ────────────────────────────────────────────────────

  /**
   * Set the minimum log level or provide a custom logger.
   *
   * @param logLevelOrLogger
   *   - `LogLevel` number  →  uses the built-in ConsoleLogger
   *   - `ILogger` object   →  used as-is
   */
  configureLogging(logLevelOrLogger: LogLevel | ILogger): this {
    this.#logger = resolveLogger(logLevelOrLogger);
    return this;
  }

  // ─── withAutomaticReconnect ───────────────────────────────────────────────

  /**
   * Enable automatic reconnection on unexpected disconnections.
   *
   * @overload  withAutomaticReconnect()
   *   Uses the built-in delay sequence: 0 ms, 2 s, 10 s, 30 s.
   *
   * @overload  withAutomaticReconnect(retryDelays: readonly number[])
   *   Each element is the wait (ms) before the corresponding retry.
   *   Reconnection stops after all delays are exhausted.
   *   Uses `NoInfer<number>` so literals are not widened unexpectedly.
   *
   * @overload  withAutomaticReconnect(policy: IRetryPolicy)
   *   Custom policy.  Return `null` from
   *   `nextRetryDelayInMilliseconds` to stop retrying.
   */
  withAutomaticReconnect(): this;
  withAutomaticReconnect(retryDelays: readonly NoInfer<number>[]): this;
  withAutomaticReconnect(policy: IRetryPolicy): this;
  withAutomaticReconnect(
    arg?: readonly number[] | IRetryPolicy,
  ): this {
    if (arg === undefined) {
      this.#reconnectPolicy = new DefaultReconnectPolicy([...DEFAULT_RETRY_DELAYS]);
    } else if (Array.isArray(arg)) {
      this.#reconnectPolicy = new DefaultReconnectPolicy(arg);
    } else if (
      typeof arg === 'object' &&
      typeof (arg as IRetryPolicy).nextRetryDelayInMilliseconds === 'function'
    ) {
      this.#reconnectPolicy = arg as IRetryPolicy;
    } else {
      throw new TypeError(
        'withAutomaticReconnect expects no args, a number[], or an IRetryPolicy.'
      );
    }
    return this;
  }

  // ─── build ───────────────────────────────────────────────────────────────

  build(): HubConnection {
    if (!this.#url) {
      throw new Error('withUrl() must be called before build().');
    }

    // If withCookies() was called, create a CookieAgent as the dispatcher so
    // that Set-Cookie headers from /negotiate are stored and Cookie headers
    // are forwarded to every subsequent request (WebSocket upgrade, SSE GET,
    // long-polling GET/POST/DELETE) - all of which share this dispatcher.
    const effectiveDispatcher: Dispatcher | null =
      this.#cookieJar !== null
        ? new CookieAgent({ cookies: { jar: this.#cookieJar } })
        : this.#dispatcher;

    // exactOptionalPropertyTypes: spread only defined values so we never
    // explicitly assign `undefined` to an optional property.
    const options: HubConnectionOptions = {
      ...(this.#logger                              != null && { logger:                          this.#logger }),
      ...(this.#urlOptions.accessTokenFactory       != null && { accessTokenFactory:              this.#urlOptions.accessTokenFactory }),
      ...(this.#urlOptions.transport                != null && { transport:                       this.#urlOptions.transport }),
      ...(this.#urlOptions.headers                  != null && { headers:                         this.#urlOptions.headers }),
      ...(this.#urlOptions.skipNegotiation          != null && { skipNegotiation:                 this.#urlOptions.skipNegotiation }),
      ...(this.#urlOptions.serverTimeoutInMilliseconds     != null && { serverTimeoutInMilliseconds:     this.#urlOptions.serverTimeoutInMilliseconds }),
      ...(this.#urlOptions.keepAliveIntervalInMilliseconds != null && { keepAliveIntervalInMilliseconds: this.#urlOptions.keepAliveIntervalInMilliseconds }),
      ...(this.#reconnectPolicy    != null && { reconnectPolicy: this.#reconnectPolicy }),
      ...(effectiveDispatcher      != null && { dispatcher:      effectiveDispatcher  }),
      ...(this.#httpClient         != null && { httpClient:      this.#httpClient     }),
    };

    return new HubConnection(this.#url, options);
  }
}

// ─── Default reconnect policy ─────────────────────────────────────────────────

export class DefaultReconnectPolicy implements IRetryPolicy {
  readonly #retryDelays: readonly number[];

  constructor(retryDelays: readonly number[] = [...DEFAULT_RETRY_DELAYS]) {
    this.#retryDelays = retryDelays;
  }

  nextRetryDelayInMilliseconds(context: RetryContext): number | null {
    const delay = this.#retryDelays[context.previousRetryCount];
    return delay ?? null;
  }
}
