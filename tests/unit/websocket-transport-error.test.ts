/**
 * websocket-transport-error.test.ts
 *
 * Covers error-event branches in WebSocketTransport.connect() that the real
 * undici WebSocket never exercises naturally:
 *
 * Connect-time error listener (lines 123-143) — fired while the WS is still
 * being opened.  The FakeWebSocket emits 'error' before 'open':
 *
 *   if (e.error instanceof Error) {                              // outer if (L128)
 *     err = e.error.message                                      // L134
 *       ? e.error                              //  TRUE ← non-empty message
 *       : (e.error.cause instanceof Error      // L136
 *           ? e.error.cause                    //  TRUE ← cause is Error
 *           : new Error('WebSocket connection failed'));  // FALSE ← cause not Error
 *   } else {                                                     // L138 else
 *     err = new Error(String(e.message ?? 'WebSocket error'));
 *   }
 *
 * Post-open error listener (lines 183-200) — fired after the WS is open.
 * The FakeWebSocket emits 'open' first, then 'error'.  Two nested
 * setImmediate calls ensure post-open listeners are attached before the
 * error fires (Node drains microtasks between setImmediate callbacks):
 *
 *   if (ws.readyState !== WebSocket.CONNECTING) {               // L186
 *     const err = e.error instanceof Error
 *       ? e.error                              //  already covered by tests above
 *       : new Error(String(e.message ?? 'WebSocket error'));    // L190 Branch #2
 *     ...
 *     this.onclose?.(err);                                      // L197
 *   }
 *   // else: readyState === CONNECTING → error silently ignored // L186 else (L197)
 */

import { describe, it, expect, vi } from 'vitest';

// ── Shared fake class, hoisted before vi.mock and static imports ──────────────
//
// vi.hoisted() runs before *both* static imports and vi.mock() factories,
// so FakeWebSocket is available inside the factory and in every test body.

const { FakeWebSocket, setConnectError, setPostOpenError, setSendError } = vi.hoisted(() => {
  type ErrorEvent = { error?: unknown; message?: string };

  // ── Mode 1: connect-time error (emitted before 'open') ──
  let connectErrCfg: ErrorEvent | null = null;

  // ── Mode 2: post-open error (emitted after 'open') ──
  type PostOpenCfg = {
    event:       ErrorEvent;
    /** ws.readyState when the error fires.  Default 1 (OPEN). */
    readyState?: number;
  };
  let postOpenCfg: PostOpenCfg | null = null;
  let sendError: unknown | null = null;

  function setConnectError(cfg: ErrorEvent | null): void   { connectErrCfg = cfg; }
  function setPostOpenError(cfg: PostOpenCfg | null): void { postOpenCfg   = cfg; }
  function setSendError(error: unknown | null): void       { sendError = error; }

  class FakeWebSocket {
    static readonly CONNECTING = 0;
    static readonly OPEN       = 1;
    static readonly CLOSING    = 2;
    static readonly CLOSED     = 3;

    binaryType: string = 'blob';
    readyState: number = 0;  // CONNECTING

    readonly #listeners = new Map<string, Array<(e: unknown) => void>>();

    constructor(_url: unknown, _opts?: unknown) {
      const errCfg  = connectErrCfg;
      const postCfg = postOpenCfg;

      if (errCfg !== null) {
        // ── Mode 1: emit 'error' immediately (before 'open') ──────────────
        // One setImmediate so that addEventListener('error', …) runs first.
        setImmediate(() => {
          const ev = Object.assign(Object.create(null), { type: 'error' }, errCfg);
          for (const h of (this.#listeners.get('error') ?? [])) h(ev);
        });

      } else if (postCfg !== null) {
        // ── Mode 2: emit 'open', then 'error' ─────────────────────────────
        //
        // Timing guarantee (Node.js event-loop invariant):
        //   • setImmediate #1 fires → calls resolve() → schedules setImmediate #2
        //   • Before setImmediate #2 fires, Node drains the microtask queue
        //     → the async WebSocketTransport.connect() resumes and attaches
        //       the post-open 'error' listener
        //   • setImmediate #2 fires → post-open listener is already registered
        const targetRs = postCfg.readyState ?? 1;  // default OPEN
        setImmediate(() => {
          // Set readyState BEFORE emitting 'open' (mirrors real WebSocket behaviour)
          this.readyState = targetRs;
          const openEv = Object.create(null) as object;
          Object.assign(openEv, { type: 'open' });
          for (const h of (this.#listeners.get('open') ?? [])) h(openEv);

          setImmediate(() => {
            const ev = Object.assign(Object.create(null), { type: 'error' }, postCfg.event);
            for (const h of (this.#listeners.get('error') ?? [])) h(ev);
          });
        });
      } else {
        setImmediate(() => {
          this.readyState = FakeWebSocket.OPEN;
          for (const h of (this.#listeners.get('open') ?? [])) h({ type: 'open' });
        });
      }
    }

    addEventListener(type: string, cb: (e: unknown) => void): void {
      const arr = this.#listeners.get(type) ?? [];
      arr.push(cb);
      this.#listeners.set(type, arr);
    }

    removeEventListener(_type: string, _cb: unknown): void { /* no-op */ }
    close(_code?: number, _reason?: string): void          { /* no-op */ }
    send(_data: unknown): void {
      if (sendError !== null) throw sendError;
    }
  }

  return { FakeWebSocket, setConnectError, setPostOpenError, setSendError };
});

// ── Intercept undici.WebSocket before WebSocketTransport imports it ───────────

vi.mock('undici', async (importOriginal) => {
  const actual = await importOriginal<typeof import('undici')>();
  return { ...actual, WebSocket: FakeWebSocket };
});

// ── Imports after mock is in place ────────────────────────────────────────────

import { WebSocketTransport } from '../../src/transports/websocket-transport.js';
import { TransferFormat }     from '../../src/constants.js';
import { MockLogger }         from '../helpers/mock-logger.js';

// ─────────────────────────────────────────────────────────────────────────────

describe('WebSocketTransport.connect - connect-time error branches (lines 128-143)', () => {

  // ── L134 TRUE ─────────────────────────────────────────────────────────────
  // e.error IS an Error with a non-empty message → transport rejects with e.error.

  it('rejects with e.error directly when e.error has a non-empty message (L134 TRUE branch)', async () => {
    const directErr = new Error('connection actively refused by peer');
    setConnectError({ error: directErr });

    const rejection = await new WebSocketTransport(null, new MockLogger())
      .connect('ws://fake-host', TransferFormat.Text)
      .then(() => null, (e: unknown) => e);

    setConnectError(null);

    expect(rejection).toBe(directErr);
    expect((rejection as Error).message).toBe('connection actively refused by peer');
  });

  // ── L134 FALSE + L136 TRUE ────────────────────────────────────────────────
  // e.error has empty message; cause IS an Error → rejects with e.error.cause.

  it('rejects with e.error.cause when e.error has empty message and cause is an Error (L136 TRUE branch)', async () => {
    const causeErr = new Error('underlying ECONNREFUSED');
    const wrapErr  = new Error('');
    (wrapErr as Error & { cause?: unknown }).cause = causeErr;
    setConnectError({ error: wrapErr });

    const rejection = await new WebSocketTransport(null, new MockLogger())
      .connect('ws://fake-host', TransferFormat.Text)
      .then(() => null, (e: unknown) => e);

    setConnectError(null);

    expect(rejection).toBe(causeErr);
    expect((rejection as Error).message).toBe('underlying ECONNREFUSED');
  });

  // ── L134 FALSE + L136 FALSE ───────────────────────────────────────────────
  // e.error has empty message; cause is NOT an Error → generic fallback.

  it('rejects with generic message when e.error has empty message and cause is not an Error (L136 FALSE branch)', async () => {
    const wrapErr = new Error('');
    (wrapErr as Error & { cause?: unknown }).cause = 'some string cause';
    setConnectError({ error: wrapErr });

    const rejection = await new WebSocketTransport(null, new MockLogger())
      .connect('ws://fake-host', TransferFormat.Text)
      .then(() => null, (e: unknown) => e);

    setConnectError(null);

    expect(rejection).toBeInstanceOf(Error);
    expect((rejection as Error).message).toBe('WebSocket connection failed');
  });

  // ── L138 else branch ─────────────────────────────────────────────────────
  // e.error is NOT an instanceof Error → synthesizes from e.message.

  it('rejects with synthesized Error from e.message when e.error is not an Error (L138 else branch)', async () => {
    setConnectError({ error: undefined, message: 'custom ws failure message' });

    const rejection = await new WebSocketTransport(null, new MockLogger())
      .connect('ws://fake-host', TransferFormat.Text)
      .then(() => null, (e: unknown) => e);

    setConnectError(null);

    expect(rejection).toBeInstanceOf(Error);
    expect((rejection as Error).message).toBe('custom ws failure message');
  });

  // ── L138 else + `?? 'WebSocket error'` fallback ───────────────────────────
  // e.error is not Error, e.message is absent → 'WebSocket error' default.

  it('rejects with "WebSocket error" default when e.error and e.message are both absent (L138 ?? branch)', async () => {
    setConnectError({ error: undefined });

    const rejection = await new WebSocketTransport(null, new MockLogger())
      .connect('ws://fake-host', TransferFormat.Text)
      .then(() => null, (e: unknown) => e);

    setConnectError(null);

    expect(rejection).toBeInstanceOf(Error);
    expect((rejection as Error).message).toBe('WebSocket error');
  });
});

// ─────────────────────────────────────────────────────────────────────────────

describe('WebSocketTransport - post-open error listener branches (lines 183-200)', () => {

  // ── L190 Branch #2: e.error is not an Error (FALSE of instanceof check) ───
  // readyState=OPEN → enters the if block → else branch of instanceof Error.

  it('post-open error with non-Error e.error calls onclose with synthesized Error (L190 Branch #2)', async () => {
    setPostOpenError({
      event:     { error: 'not-an-error-object', message: 'post-open failure' },
      readyState: 1,  // OPEN → if (readyState !== CONNECTING) is TRUE
    });

    const transport = new WebSocketTransport(null, new MockLogger());
    const closedP   = new Promise<Error | undefined>((resolve) => {
      transport.onclose = (err?: Error) => resolve(err);
    });

    await transport.connect('ws://fake-host', TransferFormat.Text);
    const err = await closedP;

    setPostOpenError(null);

    expect(err).toBeInstanceOf(Error);
    expect(err!.message).toBe('post-open failure');
  });

  // ── L190 `?? 'WebSocket error'` Branch #2: e.message is absent ────────────
  // e.error is not an Error AND e.message is undefined → 'WebSocket error' default.

  it('post-open error with no e.message falls back to "WebSocket error" (L190 ?? Branch #2)', async () => {
    setPostOpenError({
      event:     { error: undefined },
      readyState: 1,
    });

    const transport = new WebSocketTransport(null, new MockLogger());
    const closedP   = new Promise<Error | undefined>((resolve) => {
      transport.onclose = (err?: Error) => resolve(err);
    });

    await transport.connect('ws://fake-host', TransferFormat.Text);
    const err = await closedP;

    setPostOpenError(null);

    expect(err).toBeInstanceOf(Error);
    expect(err!.message).toBe('WebSocket error');
  });

  // ── L186/L197 else: readyState === CONNECTING → error silently ignored ─────
  // When the post-open error fires but readyState is still CONNECTING, the
  // entire if block is skipped and onclose is NOT called.

  it('post-open error is silently ignored when readyState is still CONNECTING (L186 else branch)', async () => {
    setPostOpenError({
      event:     { error: new Error('should be ignored') },
      readyState: 0,  // CONNECTING → if (readyState !== CONNECTING) is FALSE → else
    });

    const transport  = new WebSocketTransport(null, new MockLogger());
    let closeCalled  = false;
    transport.onclose = (): void => { closeCalled = true; };

    await transport.connect('ws://fake-host', TransferFormat.Text);

    // Wait for the second setImmediate (error emission) to complete
    await new Promise<void>((r) => setImmediate(r));
    await new Promise<void>((r) => setImmediate(r));

    setPostOpenError(null);

    expect(closeCalled, 'onclose must NOT fire when error is silently ignored').toBe(false);
  });
});

describe('WebSocketTransport.send - synchronous failure', () => {
  it('preserves an Error thrown by WebSocket.send()', async () => {
    const transport = new WebSocketTransport(null, new MockLogger());
    await transport.connect('ws://fake-host', TransferFormat.Text);
    const failure = new Error('send failed');
    setSendError(failure);

    const rejection = await transport.send('payload').catch((error: unknown) => error);
    expect(rejection).toBe(failure);

    setSendError(null);
  });

  it('wraps a non-Error value thrown by WebSocket.send()', async () => {
    const transport = new WebSocketTransport(null, new MockLogger());
    await transport.connect('ws://fake-host', TransferFormat.Text);
    setSendError('send exploded');

    await expect(transport.send('payload')).rejects.toThrow('send exploded');

    setSendError(null);
  });
});
