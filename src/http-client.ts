/**
 * http-client.ts
 *
 * Five HTTP client implementations backed by undici, each wrapping a different
 * undici primitive.  All five satisfy the `IHttpClient` contract so any one
 * can be used wherever an HTTP client is required (negotiate, SSE, polling…).
 *
 * ┌────────────────────┬───────────────────────────────────────────────────┐
 * │ Class              │ undici primitive                                  │
 * ├────────────────────┼───────────────────────────────────────────────────┤
 * │ RequestHttpClient  │ undici.request()                                  │
 * │ FetchHttpClient    │ undici.fetch()      (WHATWG-compatible)           │
 * │ StreamHttpClient   │ undici.stream()     (factory-based streaming)     │
 * │ PipelineHttpClient │ undici.pipeline()   (Duplex pipe chain)           │
 * │ DispatchHttpClient │ Dispatcher#dispatch (low-level handler interface) ← DEFAULT │
 * └────────────────────┴───────────────────────────────────────────────────┘
 *
 * Dispatcher (session-level):
 *   Every client accepts an optional `dispatcher` option.  Pass an undici
 *   `Agent`, `Pool`, `Client`, `ProxyAgent`, etc. to customise connection
 *   pooling, proxy routing, TLS, or any other transport-level behaviour for
 *   the entire client session.  When omitted, undici's process-global
 *   default agent is used (`getGlobalDispatcher()`).
 *
 * HttpClient:
 *   An alias for `DispatchHttpClient` so existing code that imported
 *   `HttpClient` continues to work without changes.
 */

import { PassThrough, Readable } from 'node:stream';
import {
  request  as undiciRequest,
  stream   as undiciStream,
  pipeline as undiciPipeline,
  fetch    as undiciFetch,
  getGlobalDispatcher,
  type Dispatcher,
} from 'undici';

import type { HttpResponse, StreamResult, IHttpClient } from './interfaces.js';

// ─── Public types ─────────────────────────────────────────────────────────────

export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'HEAD' | 'OPTIONS';

/**
 * Per-request options accepted by every client method.
 * These mirror the previous `node:http`-based API surface exactly so callers
 * do not need to be changed when switching client implementations.
 */
export interface RequestOptions {
  /** Per-request headers merged on top of the instance-level defaults. */
  readonly headers?: Record<string, string>;
  /**
   * Request body.
   * Plain objects are serialised to JSON automatically.
   * Strings are sent as-is (set Content-Type explicitly if needed).
   */
  readonly body?:    string | Uint8Array | Record<string, unknown> | null;
  /** Override the session-level timeout for this single request (ms). */
  readonly timeout?: number;
}

/**
 * Session-level options shared by all client constructors.
 */
export interface HttpClientOptions {
  /** Headers sent with every request (merged before per-request headers). */
  readonly headers?:    Record<string, string>;
  /** Default timeout in milliseconds applied to every request (default: 30 000). */
  readonly timeout?:    number;
  /**
   * undici Dispatcher to use for the entire session.
   *
   * You can pass any `Dispatcher` subclass:
   *   - `new undici.Agent(opts)`              - connection-pool tuning
   *   - `new undici.Pool(origin, opts)`       - per-origin pool
   *   - `new undici.Client(origin, opts)`     - single connection
   *   - `new undici.ProxyAgent(proxyUrl)`     - HTTP proxy
   *   - `new undici.MockAgent()`              - in-process mocking
   *
   * When omitted the process-global dispatcher (`getGlobalDispatcher()`)
   * is used, which is undici's default `Agent`.
   */
  readonly dispatcher?: Dispatcher;
  /** Maximum buffered response body size in bytes (default: 32 MiB). */
  readonly maximumResponseBodySize?: number;
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

/**
 * Serialise the `body` option to a `Buffer` (or `null` for bodyless requests).
 * Returns the buffer AND the inferred Content-Type so callers can set the
 * header automatically.
 */
function prepareBody(
  body: string | Uint8Array | Record<string, unknown> | null | undefined,
): { buf: Buffer | null; inferredContentType: string | null } {
  if (body == null) return { buf: null, inferredContentType: null };
  if (body instanceof Uint8Array) {
    return { buf: Buffer.from(body.buffer, body.byteOffset, body.byteLength), inferredContentType: null };
  }
  if (typeof body === 'string') {
    return { buf: Buffer.from(body, 'utf8'), inferredContentType: null };
  }
  return {
    buf: Buffer.from(JSON.stringify(body), 'utf8'),
    inferredContentType: 'application/json',
  };
}

/**
 * Merge session-level default headers with per-request headers.
 * Per-request headers win on collision.
 * Injects Content-Length and Content-Type when a body is present.
 */
function buildHeaders(
  defaults:            Record<string, string>,
  overrides:           Record<string, string> | undefined,
  body:                Buffer | null,
  inferredContentType: string | null,
): Record<string, string> {
  const merged: Record<string, string> = { ...defaults, ...(overrides ?? {}) };
  if (body !== null) {
    merged['content-length'] = String(body.length);
    if (!merged['content-type'] && inferredContentType !== null) {
      merged['content-type'] = inferredContentType;
    }
  }
  return merged;
}

/**
 * Normalise an undici response headers object
 * (`Record<string, string | string[]>`) to our `HttpResponse.headers` shape
 * (`Record<string, string | string[] | undefined>`).
 */
function normaliseHeaders(
  h: Record<string, string | string[]>,
): Record<string, string | string[] | undefined> {
  // The shapes are structurally compatible; we just widen the type.
  return h as Record<string, string | string[] | undefined>;
}

function assertBodySize(size: number, maximum: number): void {
  if (size > maximum) {
    throw new Error(`HTTP response body exceeds the ${maximum} byte limit.`);
  }
}

// ─── Abstract base ────────────────────────────────────────────────────────────

/**
 * Holds the session-level options and exposes the convenience wrappers
 * (`get`, `post`, `delete`) that delegate to the abstract `request` method.
 *
 * Concrete subclasses only need to implement `request()` and `stream()`.
 */
abstract class BaseUndiciClient implements IHttpClient {
  /** Default headers merged into every outgoing request. */
  protected readonly defaultHeaders: Record<string, string>;
  /** Session-level request timeout in milliseconds. */
  protected readonly defaultTimeout: number;
  /**
   * Shared undici Dispatcher.  May be an Agent, Pool, Client, ProxyAgent, etc.
   * Defaults to undici's process-global agent when not specified.
   */
  protected readonly dispatcher: Dispatcher;
  protected readonly maximumResponseBodySize: number;

  constructor(options: HttpClientOptions = {}) {
    this.defaultHeaders = options.headers    ?? {};
    this.defaultTimeout = options.timeout    ?? 30_000;
    this.dispatcher     = options.dispatcher ?? getGlobalDispatcher();
    this.maximumResponseBodySize = options.maximumResponseBodySize ?? 32 * 1024 * 1024;
  }

  // Convenience wrappers ──────────────────────────────────────────────────────

  get(url: string, options?: RequestOptions): Promise<HttpResponse> {
    return this.request('GET', url, options);
  }

  post(url: string, options?: RequestOptions): Promise<HttpResponse> {
    return this.request('POST', url, options);
  }

  delete(url: string, options?: RequestOptions): Promise<HttpResponse> {
    return this.request('DELETE', url, options);
  }

  // Abstract surface - each subclass provides its own undici primitive ────────

  abstract request(
    method:  HttpMethod,
    url:     string,
    options?: RequestOptions,
  ): Promise<HttpResponse>;

  abstract stream(
    method:  HttpMethod,
    url:     string,
    options?: RequestOptions,
  ): Promise<StreamResult>;
}

// ─── 1. RequestHttpClient ─────────────────────────────────────────────────────

/**
 * HTTP client backed by `undici.request()`.
 *
 * `undici.request` is the recommended high-level API for most use-cases.
 * It returns a Promise that resolves with `{ statusCode, headers, body }`
 * where `body` is an undici `BodyReadable` (a Node `Readable` subclass).
 *
 * For buffered responses (`request()`) we call `body.text()` to collect the
 * full response.  For streaming responses (`stream()`) we return `body`
 * directly as the `StreamResult.body` Readable, giving the caller zero-copy
 * access to the response stream.
 *
 */
export class RequestHttpClient extends BaseUndiciClient {
  /**
   * Buffered request - collects the entire response body as a string.
   *
   * Timeout is implemented via undici's built-in `headersTimeout` /
   * `bodyTimeout` options rather than an external `setTimeout`.
   */
  override async request(
    method:   HttpMethod,
    url:      string,
    options:  RequestOptions = {},
  ): Promise<HttpResponse> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );
    const timeout = options.timeout ?? this.defaultTimeout;

    const { statusCode, headers: respHeaders, body } = await undiciRequest(url, {
      method,
      headers,
      ...(buf !== null && { body: buf }),
      headersTimeout: timeout,
      bodyTimeout:    timeout,
      dispatcher:     this.dispatcher,
    });

    const text = await body.text();
    assertBodySize(Buffer.byteLength(text), this.maximumResponseBodySize);

    return {
      status:  statusCode,
      headers: normaliseHeaders(respHeaders as Record<string, string | string[]>),
      body:    text,
    };
  }

  /**
   * Streaming request - returns the response body `Readable` without
   * buffering.  Used by SSE and long-polling transports.
   *
   * The `abort()` method destroys the underlying socket and the body stream.
   */
  override async stream(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<StreamResult> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );

    const { statusCode, headers: respHeaders, body } = await undiciRequest(url, {
      method,
      headers,
      ...(buf !== null && { body: buf }),
      // No bodyTimeout here - SSE connections are intentionally long-lived.
      headersTimeout: options.timeout ?? this.defaultTimeout,
      dispatcher:     this.dispatcher,
    });

    return {
      statusCode,
      headers: normaliseHeaders(respHeaders as Record<string, string | string[]>),
      body:    body as unknown as Readable,
      abort:   () => body.destroy(),
    };
  }
}

// ─── 2. FetchHttpClient ───────────────────────────────────────────────────────

/**
 * HTTP client backed by `undici.fetch()`.
 *
 * `undici.fetch` is undici's WHATWG-compatible Fetch implementation.
 * It accepts the same options as the browser `fetch()` API, plus an
 * undici-specific `dispatcher` field for custom transport control.
 *
 * Useful when you need browser API compatibility (e.g., tests that run in
 * both Node and a browser-like environment) or when consuming WHATWG `Response`
 * helpers such as `res.json()` / `res.formData()`.
 *
 * For `stream()` we convert the WHATWG `ReadableStream<Uint8Array>` body to a
 * Node.js `Readable` via `Readable.fromWeb()` (Node 17+).
 */
export class FetchHttpClient extends BaseUndiciClient {
  override async request(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<HttpResponse> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );

    const ac      = new AbortController();
    const timeout = options.timeout ?? this.defaultTimeout;
    const timer   = timeout > 0
      ? setTimeout(() => ac.abort(new Error(`HTTP request timed out after ${timeout} ms`)), timeout)
      : null;

    try {
      const res = await undiciFetch(url, {
        method,
        headers,
        ...(buf !== null && { body: buf }),
        signal:     ac.signal,
        // undici-specific: override the dispatcher for this request.
        // undici's fetch type is defined in two conflicting locations
        // (undici-types bundled with @types/node AND undici's own .d.ts),
        // so the intersection cast doesn't satisfy either.  The double-cast
        // via `unknown` sidesteps the conflict without losing safety: at
        // runtime undici.fetch() accepts exactly this shape.
        dispatcher: this.dispatcher,
      } as unknown as Parameters<typeof undiciFetch>[1]);

      const text = await res.text();
      assertBodySize(Buffer.byteLength(text), this.maximumResponseBodySize);
      if (timer !== null) clearTimeout(timer);

      // Convert Headers object to plain Record
      const respHeaders: Record<string, string | string[] | undefined> = {};
      res.headers.forEach((value, key) => { respHeaders[key] = value; });

      return { status: res.status, headers: respHeaders, body: text };
    } catch (err) {
      if (timer !== null) clearTimeout(timer);
      throw err;
    }
  }

  override async stream(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<StreamResult> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );

    const ac = new AbortController();

    const res = await undiciFetch(url, {
      method,
      headers,
      ...(buf !== null && { body: buf }),
      signal:     ac.signal,
      dispatcher: this.dispatcher,
    } as unknown as Parameters<typeof undiciFetch>[1]);

    // Convert WHATWG ReadableStream → Node Readable
    const body = Readable.fromWeb(
      res.body as Parameters<typeof Readable.fromWeb>[0],
    );

    const respHeaders: Record<string, string | string[] | undefined> = {};
    res.headers.forEach((value, key) => { respHeaders[key] = value; });

    return {
      statusCode: res.status,
      headers:    respHeaders,
      body,
      abort:      () => { ac.abort(); body.destroy(); },
    };
  }
}

// ─── 3. StreamHttpClient ──────────────────────────────────────────────────────

/**
 * HTTP client backed by `undici.stream()`.
 *
 * `undici.stream` uses a *factory callback* pattern: you supply a function
 * that receives `{ statusCode, headers, body }` and must return a
 * `Writable` to which the response body is piped.
 *
 * This makes it ideal when you want to pipe response bytes directly to a
 * destination (e.g., a file, a transform, or a compression stream) without
 * ever holding the entire response in memory.
 *
 * For buffered `request()` we use a `PassThrough` as the sink and collect
 * its output.  For streaming `stream()` we expose the PassThrough's
 * readable side as `StreamResult.body`.
 */
export class StreamHttpClient extends BaseUndiciClient {
  override request(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<HttpResponse> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );
    const timeout = options.timeout ?? this.defaultTimeout;

    let statusCode = 0;
    let respHeaders: Record<string, string | string[]> = {};
    const chunks: Buffer[] = [];

    return undiciStream(
      url,
      {
        method,
        headers,
        ...(buf !== null && { body: buf }),
        headersTimeout: timeout,
        bodyTimeout:    timeout,
        dispatcher:     this.dispatcher,
      },
      ({ statusCode: sc, headers: h }) => {
        statusCode  = sc;
        respHeaders = h as Record<string, string | string[]>;

        // Sink that collects the response bytes
        const sink = new PassThrough();
        sink.on('data', (chunk: Buffer) => chunks.push(chunk));
        return sink;
      },
    ).then(() => ({
      status:  statusCode,
      headers: normaliseHeaders(respHeaders),
      body:    (() => {
        const body = Buffer.concat(chunks);
        assertBodySize(body.length, this.maximumResponseBodySize);
        return body.toString('utf8');
      })(),
    }));
  }

  override stream(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<StreamResult> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );

    return new Promise<StreamResult>((resolve, reject) => {
      // The PassThrough is the pipe target inside the factory AND the readable
      // we hand back to the caller - the same object plays both roles.
      const pt = new PassThrough();

      undiciStream(
        url,
        {
          method,
          headers,
          ...(buf !== null && { body: buf }),
          headersTimeout: options.timeout ?? this.defaultTimeout,
          dispatcher:     this.dispatcher,
        },
        ({ statusCode, headers: h }) => {
          resolve({
            statusCode,
            headers: normaliseHeaders(h as Record<string, string | string[]>),
            body:    pt,
            abort:   () => pt.destroy(),
          });
          // Return the same PassThrough as the writable sink.
          // undici pipes the response body into it; callers read from it.
          return pt;
        },
      ).catch((err: unknown) => {
        // Connection failed before headers: Promise has not resolved, so no
        // caller holds a reference to `pt`.  Destroy it without an error arg
        // to avoid emitting an unhandled 'error' event on an orphaned stream.
        pt.destroy();
        reject(err);
      });
    });
  }
}

// ─── 4. PipelineHttpClient ────────────────────────────────────────────────────

/**
 * HTTP client backed by `undici.pipeline()`.
 *
 * `undici.pipeline` returns a `stream.Duplex`:
 *   - **Writable side**: the request body (write / pipe into it).
 *   - **Readable side**: whatever your handler function *returns*.
 *
 * The handler receives `{ statusCode, headers, body }` and must return a
 * `Readable`.  Whatever that readable emits becomes the duplex's output.
 *
 * Typical use: response transformation pipelines
 *   `pipeline(url, opts, ({ body }) => body.pipe(gunzip()))`
 *
 * For `request()` we return the raw body from the handler, collect the duplex
 * output into a buffer, and resolve with the buffered string.
 *
 * For `stream()` we return the raw body from the handler and give the caller
 * the duplex's readable side directly - zero extra buffering.
 */
export class PipelineHttpClient extends BaseUndiciClient {
  override request(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<HttpResponse> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );
    const timeout = options.timeout ?? this.defaultTimeout;

    return new Promise<HttpResponse>((resolve, reject) => {
      let statusCode  = 0;
      let respHeaders: Record<string, string | string[]> = {};
      const chunks: Buffer[] = [];

      const duplex = undiciPipeline(
        url,
        {
          method,
          headers,
          ...(buf !== null && { body: buf }),
          headersTimeout: timeout,
          bodyTimeout:    timeout,
          dispatcher:     this.dispatcher,
        },
        ({ statusCode: sc, headers: h, body }) => {
          statusCode  = sc;
          respHeaders = h as Record<string, string | string[]>;
          // Return the body readable - it becomes the duplex output.
          return body;
        },
      );

      duplex.on('data',  (chunk: Buffer) => chunks.push(chunk));
      duplex.on('end',   () =>
        resolve({
          status:  statusCode,
          headers: normaliseHeaders(respHeaders),
          body:    (() => {
            const body = Buffer.concat(chunks);
            assertBodySize(body.length, this.maximumResponseBodySize);
            return body.toString('utf8');
          })(),
        }),
      );
      duplex.on('error', reject);

      // Signal end-of-request-body (required even for bodyless requests)
      if (buf !== null) {
        duplex.write(buf);
      }
      duplex.end();
    });
  }

  override stream(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<StreamResult> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );

    return new Promise<StreamResult>((resolve, reject) => {
      let resolved = false;

      // `duplex` is assigned by `undiciPipeline` (synchronously before any
      // async I/O), so the closure inside the handler can safely reference it.
      // eslint-disable-next-line prefer-const
      let duplex: ReturnType<typeof undiciPipeline>;

      duplex = undiciPipeline(
        url,
        {
          method,
          headers,
          ...(buf !== null && { body: buf }),
          headersTimeout: options.timeout ?? this.defaultTimeout,
          dispatcher:     this.dispatcher,
        },
        ({ statusCode, headers: h, body }) => {
          if (!resolved) {
            resolved = true;
            resolve({
              statusCode,
              headers: normaliseHeaders(h as Record<string, string | string[]>),
              // The duplex IS the readable - its output is the body.
              body:    duplex as unknown as Readable,
              abort:   () => duplex.destroy(),
            });
          }
          // Return body so pipeline routes it to the duplex's readable side.
          return body;
        },
      );

      duplex.on('error', (err: Error) => {
        if (!resolved) reject(err);
      });

      if (buf !== null) duplex.write(buf);
      duplex.end();
    });
  }
}

// ─── 5. DispatchHttpClient (DEFAULT) ─────────────────────────────────────────

/**
 * HTTP client backed by `Dispatcher#dispatch()`.
 *
 * `dispatch` is the *lowest-level* undici API.  It exposes the full
 * request/response lifecycle as a handler interface (undici 8+):
 *
 *   onRequestStart   → request is about to be sent; receives DispatchController
 *   onResponseStart  → response status + headers received
 *   onResponseData   → response body chunk
 *   onResponseEnd    → response fully consumed
 *   onResponseError  → error at any stage
 *
 * This gives you maximum control: you can abort mid-stream, transform
 * chunks on the fly, or implement custom back-pressure logic.
 *
 * Unlike the other clients, `DispatchHttpClient` *requires* a `Dispatcher`
 * (an `Agent`, `Pool`, or `Client` instance).  If none is provided the
 * process-global agent is used.
 *
 * Note: `dispatcher.dispatch()` needs the URL split into `origin` + `path`
 * (for `Agent` / `Pool`) or just `path` (for `Client` whose origin is
 * already bound).  We always pass `origin` for maximum compatibility.
 */
export class DispatchHttpClient extends BaseUndiciClient {
  override request(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<HttpResponse> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );

    const parsed  = new URL(url);
    const timeout = options.timeout ?? this.defaultTimeout;

    return new Promise<HttpResponse>((resolve, reject) => {
      let statusCode  = 0;
      let respHeaders: Record<string, string | string[] | undefined> = {};
      const chunks: Buffer[] = [];
      let responseSize = 0;
      let controller: Dispatcher.DispatchController | null = null;
      let settled = false;

      let timer: ReturnType<typeof setTimeout> | null = null;
      if (timeout > 0) {
        timer = setTimeout(() => {
          /* istanbul ignore next -- timer is cleared on every settled path */
          if (settled) return;
          settled = true;
          const error = new Error(`HTTP request timed out after ${timeout} ms`);
          controller?.abort(error);
          reject(error);
        }, timeout);
      }

      const clearTimer = (): void => {
        if (timer !== null) { clearTimeout(timer); timer = null; }
      };

      this.dispatcher.dispatch(
        {
          origin:  parsed.origin,
          path:    parsed.pathname + parsed.search,
          method,
          headers,
          ...(buf !== null && { body: buf }),
        },
        {
          onRequestStart: (ctrl: Dispatcher.DispatchController, _context: unknown): void => {
            controller = ctrl;
          },

          onResponseError: (_controller: Dispatcher.DispatchController, err: Error): void => {
            clearTimer();
            /* istanbul ignore next -- undici does not end after reporting an error */
            if (settled) return;
            settled = true;
            reject(err);
          },

          onResponseStart: (
            _controller: Dispatcher.DispatchController,
            sc:          number,
            headers:     Record<string, string | string[] | undefined>,
          ): void => {
            statusCode  = sc;
            respHeaders = headers;
          },

          onResponseData: (_controller: Dispatcher.DispatchController, chunk: Buffer): void => {
            chunks.push(Buffer.from(chunk));
            responseSize += chunk.length;
            if (responseSize > this.maximumResponseBodySize) {
              _controller.abort(new Error(`HTTP response body exceeds the ${this.maximumResponseBodySize} byte limit.`));
            }
          },

          onResponseEnd: (_controller: Dispatcher.DispatchController, _trailers: Record<string, string | string[] | undefined>): void => {
            clearTimer();
            /* istanbul ignore next -- undici does not end after reporting an error */
            if (settled) return;
            settled = true;
            resolve({
              status:  statusCode,
              headers: respHeaders,
              body:    Buffer.concat(chunks).toString('utf8'),
            });
          },
        } satisfies Dispatcher.DispatchHandler,
      );
    });
  }

  override stream(
    method:  HttpMethod,
    url:     string,
    options: RequestOptions = {},
  ): Promise<StreamResult> {
    const { buf, inferredContentType } = prepareBody(options.body);
    const headers = buildHeaders(
      this.defaultHeaders, options.headers, buf, inferredContentType,
    );
    const parsed = new URL(url);

    return new Promise<StreamResult>((resolve, reject) => {
      let resolved    = false;
      // PassThrough exposes a readable side to the caller and accepts
      // body chunks pushed to it from the onResponseData handler.
      const pt = new PassThrough();

      let controller: Dispatcher.DispatchController | null = null;

      this.dispatcher.dispatch(
        {
          origin:  parsed.origin,
          path:    parsed.pathname + parsed.search,
          method,
          headers,
          ...(buf !== null && { body: buf }),
          headersTimeout: options.timeout ?? this.defaultTimeout,
        },
        {
          onRequestStart: (ctrl: Dispatcher.DispatchController, _context: unknown): void => {
            controller = ctrl;
          },

          onResponseError: (_ctrl: Dispatcher.DispatchController, err: Error): void => {
            if (!resolved) {
              // Promise not yet resolved: reject it. The PassThrough stream has
              // not been handed to the caller yet, so destroy it silently to
              // avoid an unhandled 'error' event on a stream nobody is reading.
              resolved = true;
              pt.destroy();
              reject(err);
            } else {
              // Response headers were already delivered; propagate the error
              // through the stream so the caller's reader sees it.
              pt.destroy(err);
            }
          },

          onResponseStart: (
            _ctrl:      Dispatcher.DispatchController,
            statusCode: number,
            headers:    Record<string, string | string[] | undefined>,
          ): void => {
            if (!resolved) {
              resolved = true;
              resolve({
                statusCode,
                headers,
                body:    pt,
                abort:   () => {
                  controller?.abort(new Error('Stream aborted'));
                  pt.destroy();
                },
              });
            }
          },

          onResponseData: (_ctrl: Dispatcher.DispatchController, chunk: Buffer): void => {
            if (!pt.write(chunk)) {
              controller?.pause();
              pt.once('drain', () => controller?.resume());
            }
          },

          onResponseEnd: (_ctrl: Dispatcher.DispatchController, _trailers: Record<string, string | string[] | undefined>): void => {
            pt.end();
          },
        } satisfies Dispatcher.DispatchHandler,
      );
    });
  }
}

// ─── HttpClient - backward-compat alias ──────────────────────────────────────

/**
 * `HttpClient` is an alias for `DispatchHttpClient` (the default).
 *
 * Existing code that `import { HttpClient }` continues to work without
 * modification.  `new HttpClient(opts)` behaves identically to
 * `new DispatchHttpClient(opts)`.
 */
export { DispatchHttpClient as HttpClient };
