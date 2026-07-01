/**
 * interfaces.ts
 *
 * Public-contract interfaces used across the library.
 * Keeping them in one file lets consumers import only types they need.
 */

import type { Readable } from 'node:stream';
import type { Dispatcher } from 'undici';
import type { TransferFormat, LogLevel } from './constants.js';
import type { HubMessage } from './messages.js';

// ─── Re-export undici Dispatcher so consumers don't need to import undici ─────

export type { Dispatcher };

// ─── Logger ───────────────────────────────────────────────────────────────────

/** Minimal logger contract.  Implement this to plug in any logging framework. */
export interface ILogger {
  log(logLevel: LogLevel, message: string): void;
}

// ─── Transport ────────────────────────────────────────────────────────────────

/** Low-level transport abstraction (WebSocket / SSE / LongPolling). */
export interface ITransport {
  /** Human-readable name used in log messages and error reporting. */
  readonly name: string;

  /**
   * Open the transport connection.
   * @param url            Fully-qualified URL including any query parameters.
   * @param transferFormat Text or Binary.
   */
  connect(url: string, transferFormat: TransferFormat): Promise<void>;

  /** Send a message over the open transport. */
  send(data: string | Uint8Array): Promise<void>;

  /** Tear down the connection cleanly. */
  stop(): Promise<void>;

  /** Called by HubConnection whenever data arrives. */
  onreceive: ((data: string | Uint8Array) => void) | null;

  /** Called by HubConnection when the transport is closed (optionally with error). */
  onclose: ((error?: Error) => void) | null;
}

// ─── Hub Protocol ─────────────────────────────────────────────────────────────

/** Wire-protocol codec used by a HubConnection. */
export interface IHubProtocol {
  readonly name:           string;
  readonly version:        number;
  readonly transferFormat: TransferFormat;

  /**
   * Decode raw bytes/string into zero or more hub messages.
   * Implementations must handle partial/buffered inputs gracefully.
   */
  parseMessages(input: string | ArrayBuffer, logger: ILogger): HubMessage[];

  /** Encode a single hub message to its wire representation. */
  writeMessage(message: HubMessage): string | ArrayBuffer;
}

// ─── Retry policy ─────────────────────────────────────────────────────────────

/** Context passed to the retry policy on each reconnect attempt. */
export interface RetryContext {
  readonly previousRetryCount:  number;
  readonly elapsedMilliseconds: number;
  readonly retryReason:         Error | null;
}

/**
 * Controls the automatic-reconnect delay strategy.
 * Return `null` to stop retrying.
 */
export interface IRetryPolicy {
  nextRetryDelayInMilliseconds(retryContext: RetryContext): number | null;
}

// ─── Streaming ────────────────────────────────────────────────────────────────

/** Observer passed to `IStreamResult.subscribe`. */
export interface IStreamSubscriber<T> {
  readonly next?:     (value: T) => void;
  readonly error?:    (err: Error) => void;
  readonly complete?: () => void;
}

/**
 * A handle returned by `subscribe` that lets the caller cancel the stream.
 * Implements `Symbol.dispose` for use with the `using` keyword (TS 5.2+).
 */
export interface ISubscription {
  dispose(): void;
  /** Enables `using subscription = connection.stream(...).subscribe(...)` */
  [Symbol.dispose](): void;
}

/**
 * A running server-streaming invocation. It supports one subscriber.
 */
export interface IStreamResult<T> {
  subscribe(subscriber: IStreamSubscriber<T>): ISubscription;
}

// ─── Negotiate response ───────────────────────────────────────────────────────

/** Shape of the /negotiate response from an ASP.NET Core SignalR server. */
export interface NegotiateResponse {
  readonly connectionId?:    string;
  readonly connectionToken?: string;
  readonly negotiateVersion?: number;
  readonly url?:             string;           // Azure SignalR redirect
  readonly accessToken?:     string;           // Azure SignalR redirect token
  readonly error?:           string;
  readonly availableTransports?: readonly AvailableTransport[];
}

export interface AvailableTransport {
  readonly transport:       string;
  readonly transferFormats: readonly string[];
}

// ─── HTTP client ─────────────────────────────────────────────────────────────

/** Buffered HTTP response shared by all client implementations. */
export interface HttpResponse {
  readonly status:  number;
  readonly headers: Readonly<Record<string, string | string[] | undefined>>;
  readonly body:    string;
}

/**
 * Result of a non-buffered (streaming) HTTP request.
 *
 * `body`  - Node.js Readable carrying the response bytes.
 *           The SSE and long-polling transports consume this stream directly.
 *
 * `abort` - Cancels the in-flight request and destroys the body stream.
 *           Replaces the former `req.destroy()` pattern.
 */
export interface StreamResult {
  readonly statusCode: number;
  readonly headers:    Readonly<Record<string, string | string[] | undefined>>;
  readonly body:       Readable;
  abort(): void;
}

/**
 * Common contract satisfied by all five HTTP client variants
 * (Request, Fetch, Stream, Pipeline, Dispatch).
 *
 * HubConnection depends on this interface so any implementation can be
 * injected via `HubConnectionOptions.httpClient`.
 */
export interface IHttpClient {
  get(url: string, options?: import('./http-client.js').RequestOptions): Promise<HttpResponse>;
  post(url: string, options?: import('./http-client.js').RequestOptions): Promise<HttpResponse>;
  delete(url: string, options?: import('./http-client.js').RequestOptions): Promise<HttpResponse>;
  request(
    method:  import('./http-client.js').HttpMethod,
    url:     string,
    options?: import('./http-client.js').RequestOptions,
  ): Promise<HttpResponse>;
  stream(
    method:  import('./http-client.js').HttpMethod,
    url:     string,
    options?: import('./http-client.js').RequestOptions,
  ): Promise<StreamResult>;
}
