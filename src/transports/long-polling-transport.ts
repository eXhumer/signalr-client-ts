/**
 * long-polling-transport.ts
 *
 * SignalR Long-Polling transport.
 * HTTP GET (hangs until data arrives)  →  receive
 * HTTP POST                            →  send
 * HTTP DELETE                          →  disconnect
 *
 * Updated to depend on IHttpClient rather than the concrete HttpClient class
 * so any of the five undici-backed client implementations can be injected.
 */

import type { IHttpClient } from '../interfaces.js';
import { TransferFormat, LogLevel } from '../constants.js';
import type { ITransport, ILogger } from '../interfaces.js';

/** How long a single poll GET can hang before we issue a new one. */
const POLL_TIMEOUT_MS = 100_000;

export class LongPollingTransport implements ITransport {
  readonly name = 'LongPolling' as const;

  readonly #httpClient:          IHttpClient;
  readonly #accessTokenFactory:  (() => Promise<string | null>) | null;
  readonly #logger:              ILogger;
  readonly #extraHeaders:        Record<string, string>;

  #url:           string | null = null;
  #running:       boolean       = false;
  #pollImmediate: ReturnType<typeof setImmediate> | null = null;

  onreceive: ((data: string | Uint8Array) => void) | null = null;
  onclose:   ((error?: Error) => void)             | null = null;

  constructor(
    httpClient:          IHttpClient,
    accessTokenFactory:  (() => Promise<string | null>) | null,
    logger:              ILogger,
    extraHeaders:        Record<string, string> = {},
  ) {
    this.#httpClient         = httpClient;
    this.#accessTokenFactory = accessTokenFactory;
    this.#logger             = logger;
    this.#extraHeaders       = extraHeaders;
  }

  async connect(url: string, _transferFormat: TransferFormat): Promise<void> {
    this.#logger.log(LogLevel.Trace, `(LongPolling transport) Connecting to ${url}.`);
    this.#url     = url;
    this.#running = true;

    // Validate the connection with an initial poll
    await this.#poll(url, /* isConnect */ true);

    // Start the background poll loop
    this.#schedulePoll();
    this.#logger.log(LogLevel.Information, '(LongPolling transport) Connected.');
  }

  async send(data: string | Uint8Array): Promise<void> {
    if (!this.#running || !this.#url) {
      throw new Error('LongPolling transport is not connected.');
    }

    const headers = await this.#buildHeaders({
      'Content-Type': typeof data === 'string'
        ? 'text/plain;charset=UTF-8'
        : 'application/octet-stream',
    });

    const body   = typeof data === 'string' ? data : Buffer.from(data).toString('binary');
    const result = await this.#httpClient.post(this.#url, { headers, body });

    if (result.status < 200 || result.status >= 300) {
      throw new Error(`Long polling send failed with status ${result.status}.`);
    }
  }

  async stop(): Promise<void> {
    this.#running = false;

    if (this.#pollImmediate !== null) {
      clearImmediate(this.#pollImmediate);
      this.#pollImmediate = null;
    }

    if (this.#url) {
      try {
        const headers = await this.#buildHeaders();
        await this.#httpClient.delete(this.#url, { headers });
      } catch (e) {
        this.#logger.log(
          LogLevel.Warning,
          `(LongPolling transport) Error sending DELETE: ${(e as Error).message}`,
        );
      }
    }
  }

  // ─── Internal polling ─────────────────────────────────────────────────────

  #schedulePoll(): void {
    if (!this.#running) return;
    this.#pollImmediate = setImmediate(() => void this.#pollLoop());
  }

  async #pollLoop(): Promise<void> {
    /* istanbul ignore next - #schedulePoll()'s !#running guard and
       clearImmediate() in stop() together prevent #pollLoop from ever
       being called once the transport has stopped; this is a
       belt-and-suspenders defence. */
    if (!this.#running || !this.#url) return;
    try {
      await this.#poll(this.#url, /* isConnect */ false);
    } catch (err) {
      /* istanbul ignore else - between #poll re-throwing and this catch,
         no macrotask can run to set #running=false; the false branch is
         a belt-and-suspenders defence against a theoretical race. */
      if (this.#running) {
        this.#logger.log(
          LogLevel.Error,
          `(LongPolling transport) Poll error: ${(err as Error).message}`,
        );
        this.#running = false;
        this.onclose?.(err instanceof Error ? err : new Error(String(err)));
        return;
      }
    }
    this.#schedulePoll();
  }

  async #poll(url: string, isConnect: boolean): Promise<void> {
    const headers = await this.#buildHeaders();

    let res;
    try {
      res = await this.#httpClient.get(url, { headers, timeout: POLL_TIMEOUT_MS });
    } catch (err) {
      if (!this.#running) return; // normal stop - swallow
      throw err;
    }

    if (res.status === 204) {
      // Server ended connection gracefully
      this.#logger.log(LogLevel.Information, '(LongPolling transport) Server closed (204).');
      this.#running = false;
      this.onclose?.();
      return;
    }

    if (res.status < 200 || res.status >= 300) {
      const err = new Error(`Long polling received unexpected status ${res.status}.`);
      if (!isConnect) {
        this.#running = false;
        this.onclose?.(err);
      } else {
        throw err;
      }
      return;
    }

    if (res.body.length > 0) {
      this.#logger.log(LogLevel.Trace, `(LongPolling transport) Received: ${res.body}`);
      this.onreceive?.(res.body);
    }
  }

  async #buildHeaders(extra: Record<string, string> = {}): Promise<Record<string, string>> {
    const headers: Record<string, string> = { ...this.#extraHeaders, ...extra };
    if (this.#accessTokenFactory) {
      const token = await this.#accessTokenFactory();
      if (token) headers['Authorization'] = `Bearer ${token}`;
    }
    return headers;
  }
}
