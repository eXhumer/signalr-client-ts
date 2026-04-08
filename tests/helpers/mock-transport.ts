/**
 * helpers/mock-transport.ts
 *
 * A fully-controllable in-memory ITransport for unit-testing HubConnection
 * without any real network activity.
 */

import type { ITransport } from '../../src/interfaces.js';
import type { TransferFormat } from '../../src/constants.js';

export class MockTransport implements ITransport {
  readonly name = 'MockTransport' as const;

  /** Messages sent by HubConnection (via transport.send). */
  readonly sent: Array<string | Uint8Array> = [];

  /** Set to an Error to make the next connect() call reject. */
  connectError: Error | null = null;

  /** Set to an Error to make the next send() call reject. */
  sendError: Error | null = null;

  private _connected = false;

  onreceive: ((data: string | Uint8Array) => void) | null = null;
  onclose:   ((error?: Error) => void)             | null = null;

  connect(_url: string, _format: TransferFormat): Promise<void> {
    if (this.connectError) return Promise.reject(this.connectError);
    this._connected = true;
    return Promise.resolve();
  }

  send(data: string | Uint8Array): Promise<void> {
    if (this.sendError) return Promise.reject(this.sendError);
    this.sent.push(data);
    return Promise.resolve();
  }

  stop(): Promise<void> {
    this._connected = false;
    return Promise.resolve();
  }

  get isConnected(): boolean { return this._connected; }

  /** Simulate data arriving from the server. */
  receive(data: string | Uint8Array): void {
    this.onreceive?.(data);
  }

  /** Simulate the transport closing (possibly with an error). */
  triggerClose(err?: Error): void {
    this._connected = false;
    this.onclose?.(err);
  }

  /** Return the last message sent as a string (throws if none). */
  lastSent(): string {
    const last = this.sent.at(-1);
    if (last === undefined) throw new Error('No messages sent yet');
    return typeof last === 'string' ? last : Buffer.from(last).toString('utf8');
  }

  /** Return all messages sent as strings. */
  allSentStrings(): string[] {
    return this.sent.map((m) =>
      typeof m === 'string' ? m : Buffer.from(m).toString('utf8')
    );
  }
}
