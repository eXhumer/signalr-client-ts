/**
 * sse-transport.ts
 *
 * SignalR transport using Server-Sent Events (SSE) for receiving and
 * HTTP POST for sending.
 *
 * SSE spec: https://html.spec.whatwg.org/multipage/server-sent-events.html
 *
 * Updated to use the new StreamResult shape from http-client.ts:
 *   result.body   - Node Readable carrying the SSE byte stream
 *   result.abort  - cancels the in-flight request (replaces req.destroy())
 */

import type { IHttpClient } from '../interfaces.js';
import { TransferFormat, LogLevel } from '../constants.js';
import type { ITransport, ILogger } from '../interfaces.js';

export class ServerSentEventsTransport implements ITransport {
  readonly name = 'ServerSentEvents' as const;

  readonly #httpClient:          IHttpClient;
  readonly #accessTokenFactory:  (() => Promise<string | null>) | null;
  readonly #logger:              ILogger;
  readonly #extraHeaders:        Record<string, string>;

  #url:       string | null             = null;
  #abort:     (() => void) | null       = null;  // replaces the old ClientRequest handle
  #sseBuffer: string                    = '';

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

  async connect(url: string, transferFormat: TransferFormat): Promise<void> {
    if (transferFormat === TransferFormat.Binary) {
      throw new Error('The Server-Sent Events transport only supports the Text transfer format.');
    }

    this.#logger.log(LogLevel.Trace, `(SSE transport) Connecting to ${url}.`);
    this.#url = url;

    const headers: Record<string, string> = {
      Accept: 'text/event-stream',
      ...this.#extraHeaders,
    };

    if (this.#accessTokenFactory) {
      const token = await this.#accessTokenFactory();
      if (token) headers['Authorization'] = `Bearer ${token}`;
    }

    const result = await this.#httpClient.stream('GET', url, { headers });

    if (result.statusCode !== 200) {
      // Attach a no-op error handler before destroying the body to prevent an
      // unhandled 'error' event if the underlying stream emits one on abort.
      result.body.on('error', () => {});
      result.abort();
      throw new Error(
        `Unexpected status code ${result.statusCode} from SSE endpoint.`,
      );
    }

    // Store the abort handle so stop() can cancel the long-lived GET
    this.#abort = result.abort;

    // The body is a Node Readable; setEncoding converts chunks to strings
    result.body.setEncoding('utf8');

    result.body.on('data', (chunk: string) => this.#onData(chunk));
    result.body.on('end',  () => {
      this.#logger.log(LogLevel.Information, '(SSE transport) EventSource closed.');
      this.#abort = null;
      this.onclose?.();
    });
    result.body.on('error', (err: Error) => {
      this.#logger.log(LogLevel.Error, `(SSE transport) Stream error: ${err.message}`);
      this.#abort = null;
      this.onclose?.(err);
    });

    this.#logger.log(LogLevel.Information, '(SSE transport) Connected.');
  }

  async send(data: string | Uint8Array): Promise<void> {
    if (!this.#url) throw new Error('SSE transport is not connected.');

    const headers: Record<string, string> = {
      'Content-Type': typeof data === 'string'
        ? 'text/plain;charset=UTF-8'
        : 'application/octet-stream',
      ...this.#extraHeaders,
    };

    if (this.#accessTokenFactory) {
      const token = await this.#accessTokenFactory();
      if (token) headers['Authorization'] = `Bearer ${token}`;
    }

    const result = await this.#httpClient.post(this.#url, { headers, body: data });

    if (result.status < 200 || result.status >= 300) {
      throw new Error(`SSE send failed with status ${result.status}.`);
    }
  }

  stop(): Promise<void> {
    if (this.#abort) {
      this.#abort();
      this.#abort = null;
    }
    return Promise.resolve();
  }

  // ─── SSE parser ───────────────────────────────────────────────────────────

  #onData(chunk: string): void {
    this.#sseBuffer += chunk;

    let boundary: number;
    while ((boundary = findEventBoundary(this.#sseBuffer)) !== -1) {
      const block        = this.#sseBuffer.slice(0, boundary);
      this.#sseBuffer    = this.#sseBuffer.slice(boundary).replace(/^[\r\n]+/, '');
      this.#dispatchEvent(block);
    }
  }

  #dispatchEvent(block: string): void {
    const lines     = block.split(/\r?\n/);
    const dataParts: string[] = [];

    for (const line of lines) {
      if (line.startsWith('data:')) {
        dataParts.push(line.slice(5).replace(/^ /, ''));
      }
      // event:, id:, retry: are intentionally ignored by SignalR's SSE
    }

    if (dataParts.length === 0) return;

    const message = dataParts.join('\n');
    this.#logger.log(LogLevel.Trace, `(SSE transport) Received data: ${message}`);
    this.onreceive?.(message);
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function findEventBoundary(str: string): number {
  const i1 = str.indexOf('\n\n');
  const i2 = str.indexOf('\r\n\r\n');
  if (i1 === -1 && i2 === -1) return -1;
  if (i1 === -1) return i2 + 4;
  if (i2 === -1) return i1 + 2;
  return Math.min(i1 + 2, i2 + 4);
}
