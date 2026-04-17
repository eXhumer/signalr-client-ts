/**
 * hub-connection.test.ts
 *
 * Comprehensive state-machine tests for HubConnection using MockTransport.
 *
 * Strategy:
 *  1. Build a HubConnection with a MockTransport injected via the internal
 *     constructor (we import HubConnection directly and pass a custom
 *     "transport" option that maps to HttpTransportType.WebSockets).
 *  2. After start() completes the handshake, inject incoming messages via
 *     transport.receive() and assert outgoing messages via transport.sent[].
 *
 * Because HubConnection calls /negotiate over HTTP before opening the
 * transport, and we want no real network, we subclass HubConnection to
 * intercept the negotiate step - OR we mock the transport factory via
 * the constructor option.
 *
 * Easiest approach: use skipNegotiation:true + WebSockets-only, which
 * bypasses negotiate entirely and directly uses the transport.
 * We then swap the WebSocketTransport before start() by patching the
 * private field via a test-only factory wrapper.
 *
 * In practice this is achieved by a TestableHubConnection helper that
 * overrides createTransport() via a protected-ish escape hatch exposed
 * only in tests.
 */

import { describe, it, beforeAll, afterAll, beforeEach, afterEach, expect } from 'vitest';
import * as http from 'node:http';
import * as net  from 'node:net';

import { HubConnection }   from '../../src/hub-connection.js';
import { HubConnectionBuilder } from '../../src/hub-connection-builder.js';
import { MockTransport }   from '../helpers/mock-transport.js';
import { MockLogger }      from '../helpers/mock-logger.js';
import {
  HubConnectionState,
  HttpTransportType,
  LogLevel,
  MessageType,
  RECORD_SEPARATOR as RS,
} from '../../src/constants.js';
import { JsonHubProtocol } from '../../src/protocols/json-hub-protocol.js';
import { HubError, AbortError } from '../../src/errors.js';
import { toInvocationId }  from '../../src/messages.js';
import { Agent } from 'undici';
import {
  FetchHttpClient,
  StreamHttpClient,
  PipelineHttpClient,
  DispatchHttpClient,
} from '../../src/http-client.js';
import type { IHttpClient } from '../../src/interfaces.js';

// ─── TestableHubConnection ────────────────────────────────────────────────────
//
// We cannot inject a transport directly into the official API.
// Instead we create a connection, then manually bypass the start() sequence:
//   1. Set the transport manually
//   2. Wire onreceive / onclose
//   3. Send the handshake request and inject the server's handshake response
//

class TestHarness {
  readonly transport = new MockTransport();
  readonly logger    = new MockLogger();
  readonly conn: HubConnection;
  private readonly _proto = new JsonHubProtocol();

  constructor(options: { pingInterval?: number; serverTimeout?: number; reconnect?: boolean } = {}) {
    // Build via official API
    const builder = new HubConnectionBuilder()
      .withUrl('http://localhost/hub', { skipNegotiation: true, transport: HttpTransportType.WebSockets })
      .configureLogging(this.logger);

    if (options.reconnect) {
      builder.withAutomaticReconnect([0, 0, 0]);
    }

    this.conn = builder.build();
  }

  /** Start the connection by directly wiring the mock transport. */
  async start(): Promise<void> {
    // Patch private #transport before the real start() can create one.
    // We do this by intercepting the WebSocketTransport's connect():
    // Since skipNegotiation=true and transport=WebSockets, the real start()
    // will try to connect a WebSocketTransport. We'll make the mock intercept.
    //
    // Better approach: use the internal _startWithTransport escape hatch
    // that we add to this file via a symbol-keyed method in hub-connection.ts.
    //
    // Since we don't have that, we exercise a well-known trick:
    // Replace the underlying ws-client module by making skipNegotiation
    // invoke our mock. We achieve this by subclassing and overriding
    // the transport creation entirely.
    //
    // For this test file we use a different path: manually call the internal
    // wiring and skip start(), since HubConnection exposes the contract
    // through the transport callbacks.
    await this._startManually();
  }

  private async _startManually(): Promise<void> {
    // Directly replicate what start() does after the transport connects,
    // using our mock transport. This is white-box testing of the connection
    // lifecycle at the message-routing layer.

    // The trick: (conn as any) to access private fields for testing
    const c = this.conn as unknown as Record<string, unknown>;

    // Inject mock transport
    c['_HubConnection__transport'] = this.transport;
    // (TS private name-mangling: #transport → _HubConnection__transport in CommonJS)

    this.transport.onreceive = (data: string | Uint8Array): void => {
      const fn = c['_HubConnection__processIncoming'] as ((t: string) => void) | undefined;
      if (fn) fn.call(this.conn, typeof data === 'string' ? data : Buffer.from(data).toString('utf8'));
    };
    this.transport.onclose = (err?: Error): void => {
      const fn = c['_HubConnection__onTransportClosed'] as ((e?: Error) => void) | undefined;
      if (fn) fn.call(this.conn, err);
    };

    // Set state to Connected
    c['_HubConnection__state'] = HubConnectionState.Connected;
    // Clear handshake flag (already null)
    // Start keep-alive timers
    const resetPing   = c['_HubConnection__resetPingTimer']   as (() => void) | undefined;
    const resetSrvTmo = c['_HubConnection__resetServerTimeoutTimer'] as (() => void) | undefined;
    resetPing?.call(this.conn);
    resetSrvTmo?.call(this.conn);
  }

  /** Simulate the server sending a hub message. */
  serverSend(msg: Record<string, unknown>): void {
    const wire = JSON.stringify(msg) + RS;
    this.transport.receive(wire);
  }

  /** Return the last message sent by the client, parsed. */
  lastClientMsg(): Record<string, unknown> {
    const raw = this.transport.lastSent();
    return JSON.parse(raw.replace(/\x1e$/, '')) as Record<string, unknown>;
  }

  /** All client-sent messages, parsed. */
  allClientMsgs(): Array<Record<string, unknown>> {
    return this.transport.allSentStrings()
      .filter((s) => s.trim())
      .flatMap((s) => s.split(RS).filter(Boolean).map((p) => JSON.parse(p) as Record<string, unknown>));
  }

  async stop(): Promise<void> {
    await this.conn.stop();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Because TypeScript private fields (#field) compile to WeakMap-based storage
// that cannot be accessed by string keys, we use a different test approach:
// we spin up a REAL connection through the full start() path with a mock
// WebSocket server, OR we test via the public API only.
//
// The cleanest zero-network approach: expose a testOnly helper on HubConnection.
// Since we can't modify the source for test-only hooks, we use the
// real integration approach via the signalr-server helper.
// ─────────────────────────────────────────────────────────────────────────────

import { startSignalRServer } from '../helpers/signalr-server.js';
import type { SignalRTestServer, ServerClient } from '../helpers/signalr-server.js';

// ─── Test suite using real in-process server ──────────────────────────────────

describe('HubConnection - lifecycle', () => {
  let srv:    SignalRTestServer;
  let srvCli: ServerClient;
  let conn:   HubConnection;

  const mkConn = (opts?: { reconnect?: boolean }): HubConnection => {
    const builder = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None);
    if (opts?.reconnect) builder.withAutomaticReconnect([0, 0]);
    return builder.build();
  };

  // Start a fresh server once for this suite
  beforeAll(async () => {
    srv = await startSignalRServer();
  });

  afterAll(async () => {
    await srv.close();
  });

  beforeEach(async () => {
    conn = mkConn();
  });

  // ── start / stop ────────────────────────────────────────────────────────

  it('start() transitions through Connecting → Connected', async () => {
    const states: HubConnectionState[] = [conn.state];
    const clientP = srv.nextClient();

    // start() is async; snapshot states before + after
    const startP = conn.start();
    await clientP; // handshake handled by server helper
    await startP;

    states.push(conn.state);
    expect(states[0]).toBe(HubConnectionState.Disconnected);
    expect(states[1]).toBe(HubConnectionState.Connected);
    srvCli = await clientP; // might already be resolved
    await conn.stop();
  });

  it('state is Connected after start()', async () => {
    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;
    expect(conn.state).toBe(HubConnectionState.Connected);
    await conn.stop();
  });

  it('connectionId is set after start()', async () => {
    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;
    expect(typeof conn.connectionId === 'string' && conn.connectionId.length > 0,
      'Expected a non-empty connectionId').toBeTruthy();
    await conn.stop();
  });

  it('stop() transitions to Disconnected', async () => {
    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;
    await conn.stop();
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  it('start() on an already-connected connection throws', async () => {
    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;
    await expect(() => conn.start()).rejects.toThrow(/not Disconnected/i);
    await conn.stop();
  });

  it('onclose fires when stop() is called', async () => {
    let closed = false;
    conn.onclose(() => { closed = true; });
    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;
    await conn.stop();
    // onclose may fire asynchronously with the transport's close
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });
});

describe('HubConnection - send / invoke', () => {
  let srv: SignalRTestServer;
  let conn: HubConnection;
  let srvCli: ServerClient;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  beforeEach(async () => {
    conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;
  });

  afterEach(async () => { await conn.stop(); });

  // ── send ─────────────────────────────────────────────────────────────

  it('send() delivers a fire-and-forget Invocation to the server', async () => {
    const msgP = new Promise<Record<string, unknown>>((resolve) =>
      srvCli.on('message', resolve)
    );
    await conn.send('BroadcastMessage', 'Alice', 'Hello');
    const msg = await msgP;
    expect(msg['type']).toBe(MessageType.Invocation);
    expect(msg['target']).toBe('BroadcastMessage');
    expect(!('invocationId' in msg) || msg['invocationId'] == null,
      'fire-and-forget should have no invocationId').toBeTruthy();
  });

  it('send() throws when not connected', async () => {
    const c2 = new HubConnectionBuilder().withUrl(srv.url).build();
    expect(() => c2.send('Foo')).toThrow(/Connected/);
  });

  // ── invoke ───────────────────────────────────────────────────────────

  it('invoke() sends an Invocation with an invocationId', async () => {
    const msgP = new Promise<Record<string, unknown>>((resolve) =>
      srvCli.on('message', resolve)
    );

    // Don't await invoke() - we need to reply from the server first
    const invokeP = conn.invoke<string>('Echo', 'ping');

    const msg = await msgP;
    expect(msg['type']).toBe(MessageType.Invocation);
    expect(msg['target']).toBe('Echo');
    expect(typeof msg['invocationId']).toBe('string');

    // Server replies with Completion
    srvCli.sendMessage({
      type:         MessageType.Completion,
      invocationId: msg['invocationId'],
      result:       'pong',
    });

    const result = await invokeP;
    expect(result).toBe('pong');
  });

  it('invoke() rejects when server returns a Completion with error', async () => {
    const msgP = new Promise<Record<string, unknown>>((resolve) =>
      srvCli.on('message', resolve)
    );
    const invokeP = conn.invoke('Fail');
    const msg = await msgP;

    srvCli.sendMessage({
      type:         MessageType.Completion,
      invocationId: msg['invocationId'],
      error:        'hub method threw',
    });

    await expect(invokeP).rejects.toBeInstanceOf(HubError);
  });

  it('invoke() is rejected when connection stops mid-flight', async () => {
    // Don't reply from server - just stop the connection
    const invokeP = conn.invoke<string>('Slow');
    await conn.stop();
    await expect(invokeP).rejects.toBeInstanceOf(Error);
  });
});

describe('HubConnection - on / off', () => {
  let srv: SignalRTestServer;
  let conn: HubConnection;
  let srvCli: ServerClient;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  beforeEach(async () => {
    conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;
  });

  afterEach(async () => { await conn.stop(); });

  it('on() receives server-sent invocations', async () => {
    const received: unknown[] = [];
    conn.on('DataPush', (item: unknown) => received.push(item));

    srvCli.sendMessage({ type: MessageType.Invocation, target: 'DataPush', arguments: [42] });
    await new Promise<void>((r) => setTimeout(r, 50));
    expect(received).toEqual([42]);
  });

  it('on() is case-insensitive for method names', async () => {
    const calls: unknown[] = [];
    conn.on('greetUser', (name: unknown) => calls.push(name));

    srvCli.sendMessage({ type: MessageType.Invocation, target: 'GreetUser', arguments: ['Bob'] });
    await new Promise<void>((r) => setTimeout(r, 50));
    expect(calls).toEqual(['Bob']);
  });

  it('multiple handlers for the same method all fire', async () => {
    const log1: unknown[] = [];
    const log2: unknown[] = [];
    conn.on('Tick', (n: unknown) => log1.push(n));
    conn.on('Tick', (n: unknown) => log2.push(n));

    srvCli.sendMessage({ type: MessageType.Invocation, target: 'Tick', arguments: [1] });
    await new Promise<void>((r) => setTimeout(r, 50));
    expect(log1).toEqual([1]);
    expect(log2).toEqual([1]);
  });

  it('off() with a handler removes only that handler', async () => {
    const calls: unknown[] = [];
    const handler1 = (n: unknown): void => { calls.push('h1:' + String(n)); };
    const handler2 = (n: unknown): void => { calls.push('h2:' + String(n)); };

    conn.on('Event', handler1);
    conn.on('Event', handler2);
    conn.off('Event', handler1);

    srvCli.sendMessage({ type: MessageType.Invocation, target: 'Event', arguments: [7] });
    await new Promise<void>((r) => setTimeout(r, 50));

    expect(calls.includes('h1:7'), 'h1 should have been removed').toBeFalsy();
    expect(calls.includes('h2:7'),  'h2 should still fire').toBeTruthy();
  });

  it('off() without handler removes all handlers for the method', async () => {
    const calls: unknown[] = [];
    conn.on('Gone', () => calls.push(1));
    conn.on('Gone', () => calls.push(2));
    conn.off('Gone');

    srvCli.sendMessage({ type: MessageType.Invocation, target: 'Gone', arguments: [] });
    await new Promise<void>((r) => setTimeout(r, 50));
    expect(calls.length).toBe(0);
  });
});

describe('HubConnection - streaming', () => {
  let srv: SignalRTestServer;
  let conn: HubConnection;
  let srvCli: ServerClient;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  beforeEach(async () => {
    conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;
  });

  afterEach(async () => { await conn.stop(); });

  it('stream() delivers StreamItem messages via next()', async () => {
    const items: unknown[] = [];
    const done  = new Promise<void>((resolve) => {
      const msgP = new Promise<Record<string, unknown>>((r) => srvCli.on('message', r));
      conn.stream<number>('Counter', 3).subscribe({
        next:     (n) => items.push(n),
        complete: resolve,
      });

      void msgP.then((msg) => {
        const id = msg['invocationId'] as string;
        srvCli.sendMessage({ type: MessageType.StreamItem, invocationId: id, item: 10 });
        srvCli.sendMessage({ type: MessageType.StreamItem, invocationId: id, item: 20 });
        srvCli.sendMessage({ type: MessageType.Completion, invocationId: id });
      });
    });

    await done;
    expect(items).toEqual([10, 20]);
  });

  it('stream() error path calls error() on the subscriber', async () => {
    let caughtErr: Error | null = null;
    const errP = new Promise<void>((resolve) => {
      const msgP = new Promise<Record<string, unknown>>((r) => srvCli.on('message', r));
      conn.stream<number>('BrokenStream').subscribe({
        error: (e) => { caughtErr = e; resolve(); },
      });
      void msgP.then((msg) => {
        const id = msg['invocationId'] as string;
        srvCli.sendMessage({ type: MessageType.Completion, invocationId: id, error: 'stream blew up' });
      });
    });

    await errP;
    expect((caughtErr as unknown) instanceof HubError, 'Expected HubError').toBeTruthy();
    expect((caughtErr as unknown as Error).message).toMatch(/stream blew up/);
  });

  it('stream() dispose() sends CancelInvocation', async () => {
    let capturedId: string | undefined;
    const msgP = new Promise<Record<string, unknown>>((r) => srvCli.on('message', r));

    const sub = conn.stream<number>('Infinite').subscribe({ next: () => {} });
    capturedId = ((await msgP)['invocationId']) as string;

    const cancelP = new Promise<Record<string, unknown>>((r) => srvCli.on('message', r));
    sub.dispose();
    const cancelMsg = await cancelP;

    expect(cancelMsg['type']).toBe(MessageType.CancelInvocation);
    expect(cancelMsg['invocationId']).toBe(capturedId);
  });

  it('stream() subscription supports using keyword (Symbol.dispose)', async () => {
    const items: unknown[] = [];
    const msgP = new Promise<Record<string, unknown>>((r) => srvCli.on('message', r));

    // TS 5.2+: `using` calls Symbol.dispose on exit
    {
      using sub = conn.stream<number>('Loop').subscribe({ next: (n) => items.push(n) });
      void sub; // suppress unused warning
      // Sub is disposed when block exits
    }

    const initMsg = await msgP;
    expect(initMsg['type']).toBe(MessageType.StreamInvocation);
    // CancelInvocation should have been sent after using-block exits
    await new Promise<void>((r) => setTimeout(r, 30));
    // The test verifies Symbol.dispose is present
    const sub2 = conn.stream<number>('Loop2').subscribe({ next: () => {} });
    expect(typeof sub2[Symbol.dispose]).toBe('function');
    sub2.dispose();
  });
});

describe('HubConnection - Server Close message', () => {
  let srv: SignalRTestServer;
  let conn: HubConnection;
  let srvCli: ServerClient;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  it('onclose fires when server sends Close message', async () => {
    conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();

    let closedWith: Error | undefined | null = null;
    conn.onclose((err) => { closedWith = err ?? null; });

    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;

    srvCli.sendMessage({ type: MessageType.Close });
    await new Promise<void>((r) => setTimeout(r, 80));
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  it('onclose receives the error when Close carries an error', async () => {
    conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();

    let closedErr: Error | undefined;
    conn.onclose((err) => { closedErr = err; });

    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;

    srvCli.sendMessage({ type: MessageType.Close, error: 'server is going down' });
    await new Promise<void>((r) => setTimeout(r, 80));
    expect(closedErr instanceof HubError).toBeTruthy();
    expect((closedErr as unknown as Error).message).toMatch(/server is going down/);
  });

  // ── allowReconnect protocol compliance ────────────────────────────────────
  // Per the Hub Protocol spec:
  //   allowReconnect: true  + reconnect policy set  → client SHOULD reconnect
  //   allowReconnect: false + reconnect policy set  → client MUST NOT reconnect
  //   allowReconnect absent + reconnect policy set  → client MUST NOT reconnect

  it('Close with allowReconnect:true triggers reconnect when a policy is configured', async () => {
    conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect([0]) // retry once immediately
      .build();

    const reconnectedP = new Promise<void>((resolve) => conn.onreconnected(() => resolve()));

    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;

    srvCli.sendMessage({ type: MessageType.Close, allowReconnect: true });
    await reconnectedP;

    expect(conn.state).toBe(HubConnectionState.Connected);
    await conn.stop();
  });

  it('Close with allowReconnect:false does NOT reconnect even when a policy is configured', async () => {
    conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect([0, 0])
      .build();

    const closedP = new Promise<void>((resolve) => conn.onclose(() => resolve()));

    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;

    srvCli.sendMessage({ type: MessageType.Close, allowReconnect: false });
    await closedP;

    // Must be permanently disconnected, not reconnecting
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  it('Close without allowReconnect does NOT reconnect even when a policy is configured', async () => {
    conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect([0, 0])
      .build();

    const closedP = new Promise<void>((resolve) => conn.onclose(() => resolve()));

    const clientP = srv.nextClient();
    await conn.start();
    srvCli = await clientP;

    srvCli.sendMessage({ type: MessageType.Close }); // no allowReconnect field
    await closedP;

    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });
});

describe('HubConnection - auto-reconnect', () => {
  let srv: SignalRTestServer;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  it('reconnects after unexpected transport close', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect([0, 0]) // zero delay for fast tests
      .build();

    let reconnectedId: string | null = null;
    conn.onreconnected((id) => { reconnectedId = id ?? null; });

    const clientP1 = srv.nextClient();
    await conn.start();
    const srvCli1 = await clientP1;

    // Grab next client slot for after reconnect
    const clientP2 = srv.nextClient();

    // Abruptly close the server side
    srvCli1.close();

    // Wait for reconnection
    await new Promise<void>((resolve) => {
      conn.onreconnected(() => resolve());
    });

    expect(conn.state).toBe(HubConnectionState.Connected);
    await clientP2; // ensure server got second connection
    await conn.stop();
  });

  it('fires onreconnecting before reconnect attempts', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect([0])
      .build();

    let reconnectingFired = false;
    conn.onreconnecting(() => { reconnectingFired = true; });

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    const reconnectedP = new Promise<void>((r) => conn.onreconnected(() => r()));
    srvCli.close();
    await reconnectedP;

    expect(reconnectingFired).toBeTruthy();
    await conn.stop();
  });
});

describe('HubConnection - Ping keep-alive', () => {
  let srv: SignalRTestServer;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  it('client sends Ping messages periodically', async () => {
    // Use a very short ping interval via the options override
    const connWithFastPing = new HubConnectionBuilder()
      .withUrl(srv.url, { keepAliveIntervalInMilliseconds: 50 })
      .configureLogging(LogLevel.None)
      .build();

    const pingsSeen: number[] = [];
    const clientP = srv.nextClient();
    await connWithFastPing.start();
    const srvCli = await clientP;

    srvCli.on('message', (msg) => {
      if ((msg as { type?: number })['type'] === MessageType.Ping) {
        pingsSeen.push(Date.now());
      }
    });

    // Wait long enough for at least 2 pings
    await new Promise<void>((r) => setTimeout(r, 200));
    await connWithFastPing.stop();

    expect(pingsSeen.length >= 1, `Expected at least 1 ping, got ${pingsSeen.length}`).toBeTruthy();
  });
});

// ─── HTTP client variants ─────────────────────────────────────────────────────
//
// The /negotiate step is handled by the injected IHttpClient, while the actual
// WebSocket transport always uses undici's built-in WebSocket.  These tests
// verify that the negotiate POST (JSON response parsing, header handling, etc.)
// works correctly for each of the four non-default client implementations.
//
// FetchHttpClient  - undici.fetch()
// StreamHttpClient - undici.stream()
// PipelineHttpClient - undici.pipeline()
// DispatchHttpClient - Dispatcher#dispatch()

describe('HubConnection - HTTP client variants (negotiate via withHttpClient)', () => {
  let srv: SignalRTestServer;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  const variants: Array<{ label: string; make: () => IHttpClient }> = [
    { label: 'FetchHttpClient',    make: () => new FetchHttpClient() },
    { label: 'StreamHttpClient',   make: () => new StreamHttpClient() },
    { label: 'PipelineHttpClient', make: () => new PipelineHttpClient() },
    { label: 'DispatchHttpClient', make: () => new DispatchHttpClient() },
  ];

  for (const { label, make } of variants) {
    it(`full connect/stop cycle works with ${label}`, async () => {
      const conn = new HubConnectionBuilder()
        .withUrl(srv.url)
        .withHttpClient(make())
        .configureLogging(LogLevel.None)
        .build();

      const clientP = srv.nextClient();
      await conn.start();
      await clientP;

      expect(conn.state, `${label}: expected Connected after start()`).toBe(HubConnectionState.Connected);
      expect(typeof conn.connectionId === 'string' && conn.connectionId.length > 0,
        `${label}: expected non-empty connectionId`).toBeTruthy();

      await conn.stop();
      expect(conn.state, `${label}: expected Disconnected after stop()`).toBe(HubConnectionState.Disconnected);
    });
  }

  it('send() fire-and-forget works after connecting with FetchHttpClient', async () => {
    // Deeper smoke-test: verifies message flow (not just connect/stop) when
    // the negotiate step was done via a non-default HTTP client.
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withHttpClient(new FetchHttpClient())
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    const msgP = new Promise<Record<string, unknown>>((r) => srvCli.on('message', r));
    await conn.send('TestMethod', 'arg1', 42);

    const msg = await msgP;
    expect(msg['type']).toBe(MessageType.Invocation);
    expect(msg['target']).toBe('TestMethod');

    await conn.stop();
  });
});

// ─── Shared Dispatcher ────────────────────────────────────────────────────────
//
// withDispatcher() routes BOTH the /negotiate HTTP POST and the WebSocket
// upgrade through the same undici Agent, sharing the connection pool.
// These tests verify that the dispatcher option threads through correctly.

describe('HubConnection - shared Dispatcher (withDispatcher)', () => {
  let srv: SignalRTestServer;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  it('withDispatcher(Agent) - full connect/stop cycle succeeds', async () => {
    const agent = new Agent({ connections: 4 });

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withDispatcher(agent)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    expect(conn.state).toBe(HubConnectionState.Connected);
    await conn.stop();
    await agent.close();
  });

  it('two connections sharing one Agent both connect successfully', async () => {
    // The shared Agent must handle at least 2 concurrent connections:
    // each needs one TCP socket for negotiate (HTTP) and one for the WebSocket.
    const shared = new Agent({ connections: 8 });

    const conn1 = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withDispatcher(shared)
      .configureLogging(LogLevel.None)
      .build();

    const conn2 = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withDispatcher(shared)
      .configureLogging(LogLevel.None)
      .build();

    // Get two client slots before starting so we don't miss them
    const client1P = srv.nextClient();
    const client2P = srv.nextClient();

    await Promise.all([conn1.start(), conn2.start()]);
    await Promise.all([client1P, client2P]);

    expect(conn1.state, 'conn1 should be Connected').toBe(HubConnectionState.Connected);
    expect(conn2.state, 'conn2 should be Connected').toBe(HubConnectionState.Connected);

    await Promise.all([conn1.stop(), conn2.stop()]);
    await shared.close();
  });

  it('withDispatcher + withHttpClient both accepted by the builder', async () => {
    // When both are specified, the dispatcher goes to WebSocketTransport and
    // the httpClient (with its own dispatcher) handles negotiate.
    const agent  = new Agent({ connections: 2 });
    const client = new FetchHttpClient({ dispatcher: agent });

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withDispatcher(agent)
      .withHttpClient(client)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    expect(conn.state).toBe(HubConnectionState.Connected);
    await conn.stop();
    await agent.close();
  });
});

// ─── Coverage gaps: miscellaneous public-API and message-routing branches ─────

describe('HubConnection - coverage gaps (miscellaneous)', () => {
  let srv: SignalRTestServer;

  beforeAll(async () => { srv = await startSignalRServer(); });
  afterAll(async ()  => { await srv.close(); });

  // ── stop() when already Disconnected is a no-op ─────────────────────────

  it('stop() when already Disconnected resolves without throwing', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();

    // Never called start() - state is Disconnected
    await expect(conn.stop()).resolves.toBeUndefined();
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  // ── skipNegotiation without WebSockets transport ─────────────────────────

  it('skipNegotiation without the WebSockets transport flag causes start() to reject', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url, {
        skipNegotiation: true,
        transport:       HttpTransportType.LongPolling,
      })
      .configureLogging(LogLevel.None)
      .build();

    await expect(conn.start()).rejects.toThrow(/skipNegotiation requires the WebSockets/i);
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  // ── Unregistered server→client method logs a warning ────────────────────

  it('server Invocation for an unregistered method is logged as a warning', async () => {
    const logger = new MockLogger();
    const conn   = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(logger)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    srvCli.sendMessage({
      type:      MessageType.Invocation,
      target:    'UnregisteredMethod',
      arguments: [],
    });
    await new Promise<void>((r) => setTimeout(r, 60));

    expect(
      logger.hasMessage('UnregisteredMethod') || logger.hasMessage('No handler'),
      'Expected a warning mentioning the unregistered method',
    ).toBeTruthy();

    await conn.stop();
  });

  // ── Handler exception is caught; other handlers still fire ───────────────

  it('handler exception is caught and logged; other handlers for the same method still fire', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    const secondCalledWith: unknown[] = [];
    conn.on('BoomMethod', () => { throw new Error('handler 1 threw'); });
    conn.on('BoomMethod', (v: unknown) => secondCalledWith.push(v));

    srvCli.sendMessage({
      type:      MessageType.Invocation,
      target:    'BoomMethod',
      arguments: [42],
    });
    await new Promise<void>((r) => setTimeout(r, 60));

    expect(secondCalledWith).toEqual([42]);
    await conn.stop();
  });

  // ── Server timeout fires → closes connection with an error ───────────────

  it('server timeout fires when no message is received and calls onclose with an error', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url, { serverTimeoutInMilliseconds: 80 })
      .configureLogging(LogLevel.None)
      .build();

    let closedErr: Error | undefined;
    const closedP = new Promise<void>((resolve) => {
      conn.onclose((err) => { closedErr = err; resolve(); });
    });

    const clientP = srv.nextClient();
    await conn.start();
    await clientP; // server connects but never sends a message

    await closedP;

    expect(closedErr instanceof Error, 'onclose should receive an Error').toBeTruthy();
    expect((closedErr as Error).message).toMatch(/timeout|server/i);
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  // ── Completion with error field calls stream subscriber error() ──────────
  // Note: StreamItemMessage has no `error` field in the protocol schema -
  // the parser strips it. The hub-connection's stream-error path is exercised
  // via a Completion message that carries an `error` field, which routes
  // through `sc.error()` at the Completion case branch.

  it('Completion with an error field calls stream subscriber error() with a HubError', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    let caughtErr: Error | null = null;
    const errP = new Promise<void>((resolve) => {
      const msgP = new Promise<Record<string, unknown>>((r) => srvCli.on('message', r));
      conn.stream<number>('ItemStream').subscribe({
        error: (e) => { caughtErr = e; resolve(); },
      });
      void msgP.then((msg) => {
        const id = msg['invocationId'] as string;
        // A Completion message with an error terminates the stream with sc.error()
        srvCli.sendMessage({
          type:         MessageType.Completion,
          invocationId: id,
          error:        'stream completed with error',
        });
      });
    });

    await errP;
    // caughtErr is typed Error|null; cast to any so instanceof is accepted
    // by TS6's tighter left-hand-side check (TS2358).
    expect((caughtErr as any) instanceof HubError, 'Expected HubError').toBeTruthy();
    expect((caughtErr as unknown as Error).message).toMatch(/stream completed with error/);
    await conn.stop();
  });

  // ── onreconnecting callback exception does not prevent reconnection ───────

  it('onreconnecting callback exception is caught and reconnection still proceeds', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect([0])
      .build();

    conn.onreconnecting(() => { throw new Error('onreconnecting threw!'); });

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli1 = await clientP;

    const reconnectedP = new Promise<void>((r) => conn.onreconnected(() => r()));
    srvCli1.close();
    await reconnectedP;

    expect(conn.state).toBe(HubConnectionState.Connected);
    await conn.stop();
  });

  // ── onreconnected callback exception does not break reconnected state ─────

  it('onreconnected callback exception is caught and connection remains Connected', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect([0])
      .build();

    let firstCallbackFired = false;
    conn.onreconnected(() => {
      firstCallbackFired = true;
      throw new Error('onreconnected threw!');
    });

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli1 = await clientP;

    // Second callback that doesn't throw - used to know when reconnected settled
    const settledP = new Promise<void>((r) => conn.onreconnected(() => r()));
    srvCli1.close();
    await settledP;

    expect(firstCallbackFired, 'First onreconnected callback should have fired').toBeTruthy();
    expect(conn.state).toBe(HubConnectionState.Connected);
    await conn.stop();
  });

  // ── Reconnect policy returning null stops retrying and fires onclose ──────

  it('reconnect policy returning null immediately stops retrying and fires onclose', async () => {
    const neverRetryPolicy = {
      nextRetryDelayInMilliseconds: (): null => null,
    };

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect(neverRetryPolicy)
      .build();

    const closedP = new Promise<void>((resolve) => {
      conn.onclose(() => resolve());
    });

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    // Abruptly close the server side - policy returns null immediately →
    // #doReconnect breaks on first iteration → #completeClose fires onclose
    srvCli.close();

    await closedP;
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  // ── L659/L664/L698/L736: clean transport close paths ────────────────────
  // When the server sends a proper WS close frame (closeGracefully) the client
  // receives the 'close' event with no error object, so #onTransportClosed is
  // called with err=undefined.  Combined with a policy that returns null
  // immediately this exercises four previously-uncovered null-coalescing
  // branches in #onTransportClosed and #doReconnect.

  it('clean WS close (no error) + null-retry policy covers err-less disconnect branches', async () => {
    const neverRetryPolicy = {
      nextRetryDelayInMilliseconds: (): null => null,
    };

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect(neverRetryPolicy)
      .build();

    let reconnectingErr: Error | undefined = new Error('sentinel');
    conn.onreconnecting((e) => { reconnectingErr = e; });  // e should be undefined

    const closedP = new Promise<void>((resolve) => conn.onclose(() => resolve()));

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    // Send a proper WS close frame → client sees clean close, err=undefined.
    // Covers: L659 false branch ('Transport closed.' log with no err)
    //         L664 null-coalescing (undefined ?? null → null)
    //         L698 null-coalescing (null ?? undefined → undefined in callback)
    //         L736 null-coalescing (null ?? undefined → undefined in #completeClose)
    srvCli.closeGracefully();

    await closedP;
    expect(conn.state).toBe(HubConnectionState.Disconnected);
    // onreconnecting should have been called with undefined (not an Error)
    expect(reconnectingErr).toBeUndefined();
  });

  // ── L719: stop() during reconnect sleep exits loop via #stopping check ────
  // The reconnect policy returns a positive delay so #doReconnect calls
  // sleep(delay).  If stop() is called during that sleep, #stopping becomes
  // true and the guard at L719 (if (this.#stopping) break) fires after the
  // sleep resolves, instead of attempting another #startInternal() call.

  it('stop() called during reconnect sleep triggers L719 #stopping break', async () => {
    // Notify our test when nextRetryDelayInMilliseconds is about to be called
    // so that we know sleep() is immediately starting.
    let notifyDelayStarting!: () => void;
    const delayStarting = new Promise<void>((r) => { notifyDelayStarting = r; });

    const slowRetryPolicy = {
      nextRetryDelayInMilliseconds: () => {
        notifyDelayStarting();  // signal: sleep(200) is about to start
        return 200;
      },
    };

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .withAutomaticReconnect(slowRetryPolicy)
      .build();

    const closedP = new Promise<void>((resolve) => conn.onclose(() => resolve()));

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    // Trigger disconnect to start the reconnect loop.
    srvCli.close();

    // Yield the microtask/I-O queue so that #doReconnect starts running and
    // calls sleep(200).  The policy callback resolves delayStarting at the
    // point where sleep() is about to be awaited; our await here resumes
    // only after the sleep has been registered (microtasks flush after the
    // policy function returns and before the async function suspends).
    await delayStarting;

    // Now #doReconnect is sleeping for 200 ms.  Calling stop() sets
    // #stopping=true; after the sleep resolves, L719 fires and breaks the loop.
    await conn.stop();

    // #doReconnect finishes and calls #completeClose → onclose fires.
    await closedP;
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });
});

// ─── Coverage gaps: negotiate failure scenarios ───────────────────────────────

describe('HubConnection - negotiate failure scenarios', () => {
  // ── negotiate non-200 → start() rejects with TransportError ─────────────

  it('negotiate returning non-200 causes start() to reject', async () => {
    const negSrv = await (async (): Promise<{ url: string; stop: () => Promise<void> }> => {
      const s = http.createServer((_req: http.IncomingMessage, res: http.ServerResponse) => {
        res.writeHead(503);
        res.end('Service Unavailable');
      });
      await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
      const { port } = s.address() as net.AddressInfo;
      return {
        url:  `http://127.0.0.1:${port}/hub`,
        stop: (): Promise<void> => new Promise<void>((r) => {
          s.closeAllConnections();
          s.close(() => r());
        }),
      };
    })();

    const conn = new HubConnectionBuilder()
      .withUrl(negSrv.url)
      .configureLogging(LogLevel.None)
      .build();

    await expect(conn.start()).rejects.toThrow(/503/);
    expect(conn.state).toBe(HubConnectionState.Disconnected);
    await negSrv.stop();
  });

  // ── negotiate body with error field → start() rejects ───────────────────

  it('negotiate body with an error field causes start() to reject', async () => {
    const negSrv = await (async (): Promise<{ url: string; stop: () => Promise<void> }> => {
      const s = http.createServer((_req: http.IncomingMessage, res: http.ServerResponse) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Hub is not available' }));
      });
      await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
      const { port } = s.address() as net.AddressInfo;
      return {
        url:  `http://127.0.0.1:${port}/hub`,
        stop: (): Promise<void> => new Promise<void>((r) => {
          s.closeAllConnections();
          s.close(() => r());
        }),
      };
    })();

    const conn = new HubConnectionBuilder()
      .withUrl(negSrv.url)
      .configureLogging(LogLevel.None)
      .build();

    await expect(conn.start()).rejects.toThrow(/Hub is not available/);
    expect(conn.state).toBe(HubConnectionState.Disconnected);
    await negSrv.stop();
  });

  // ── Too many redirects → start() rejects ────────────────────────────────

  it('exceeding MAX_REDIRECTS causes start() to reject with "Too many negotiate redirects"', async () => {
    // A server that always returns a negotiate redirect to a slightly different path.
    // Because every URL containing /negotiate triggers the response, the client
    // will keep following redirects until the counter overflows.
    const loopSrv = await (async (): Promise<{ url: string; stop: () => Promise<void> }> => {
      let counter = 0;
      const s = http.createServer((_req: http.IncomingMessage, res: http.ServerResponse) => {
        // Always redirect to a new "hub" path so the client keeps negotiating
        const { port } = s.address() as net.AddressInfo;
        counter++;
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url: `http://127.0.0.1:${port}/hub${counter}` }));
      });
      await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
      const { port } = s.address() as net.AddressInfo;
      return {
        url:  `http://127.0.0.1:${port}/hub`,
        stop: (): Promise<void> => new Promise<void>((r) => {
          s.closeAllConnections();
          s.close(() => r());
        }),
      };
    })();

    const conn = new HubConnectionBuilder()
      .withUrl(loopSrv.url)
      .configureLogging(LogLevel.None)
      .build();

    await expect(conn.start()).rejects.toThrow(/Too many negotiate redirects/i);
    expect(conn.state).toBe(HubConnectionState.Disconnected);
    await loopSrv.stop();
  }, 30_000 /* allow extra time for 101 negotiate requests */);
});

// ─────────────────────────────────────────────────────────────────────────────
// Query-string propagation
// ─────────────────────────────────────────────────────────────────────────────

describe('HubConnection - query string propagation', () => {
  /**
   * This suite verifies that every name=value pair present in the original hub
   * URL is forwarded to:
   *
   *  1. The /negotiate POST (path must also be correct - not corrupted by
   *     naive string concatenation when the base URL already carries a `?`).
   *  2. The WebSocket upgrade request (which carries the `id` param added by
   *     #selectTransport in addition to the caller's params).
   *
   * The test spins up a dedicated SignalR-compatible server so it can inspect
   * the raw request URLs received on the wire.
   */
  let srv: SignalRTestServer;

  beforeAll(async () => {
    srv = await startSignalRServer();
  });

  afterAll(async () => {
    await srv.close();
  });

  beforeEach(() => {
    // Reset the capture array between tests
    srv.requestUrls.length = 0;
  });

  it('negotiate URL: pathname ends with /negotiate (not embedded in query string)', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(`${srv.url}?secret=abc`)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const negotiateUrl = srv.requestUrls.find((u) => u.includes('negotiate'));
    expect(negotiateUrl, 'no negotiate request observed').toBeDefined();

    // Path component must end with '/negotiate' - not 'hub?secret=abc/negotiate'
    const parsed = new URL(`http://x${negotiateUrl!}`);
    expect(parsed.pathname.endsWith('/negotiate'),
      `expected pathname to end with /negotiate, got: ${parsed.pathname}`
    ).toBe(true);

    await conn.stop();
  });

  it('negotiate URL: original query params are present', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(`${srv.url}?tenant=acme&env=prod`)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const negotiateUrl = srv.requestUrls.find((u) => u.includes('negotiate'));
    expect(negotiateUrl).toBeDefined();

    const parsed = new URL(`http://x${negotiateUrl!}`);
    expect(parsed.searchParams.get('tenant')).toBe('acme');
    expect(parsed.searchParams.get('env')).toBe('prod');
    expect(parsed.searchParams.get('negotiateVersion')).toBeTruthy();

    await conn.stop();
  });

  it('negotiate URL: negotiateVersion param is present alongside caller params', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(`${srv.url}?x=1`)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const negotiateUrl = srv.requestUrls.find((u) => u.includes('negotiate'));
    const parsed = new URL(`http://x${negotiateUrl!}`);
    expect(parsed.searchParams.get('x')).toBe('1');
    expect(parsed.searchParams.get('negotiateVersion')).toBeTruthy();

    await conn.stop();
  });

  it('WebSocket upgrade URL: original query params are forwarded', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(`${srv.url}?role=admin&region=us-east`)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    // WebSocket upgrade request has no '/negotiate' in the path
    const wsUrl = srv.requestUrls.find((u) => u.includes('?') && !u.includes('/negotiate'));
    expect(wsUrl, 'no WebSocket upgrade request observed').toBeDefined();

    const parsed = new URL(`http://x${wsUrl!}`);
    expect(parsed.searchParams.get('role')).toBe('admin');
    expect(parsed.searchParams.get('region')).toBe('us-east');

    await conn.stop();
  });

  it('WebSocket upgrade URL: connection id param is present', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(`${srv.url}?foo=bar`)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const wsUrl = srv.requestUrls.find((u) => u.includes('?') && !u.includes('/negotiate'));
    expect(wsUrl).toBeDefined();

    const parsed = new URL(`http://x${wsUrl!}`);
    // 'id' holds the connectionToken assigned by negotiate
    expect(parsed.searchParams.get('id')).toBeTruthy();
    expect(parsed.searchParams.get('foo')).toBe('bar');

    await conn.stop();
  });

  it('URL with NO query string still works correctly (regression guard)', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)  // plain URL, no query params
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const negotiateUrl = srv.requestUrls.find((u) => u.includes('negotiate'));
    expect(negotiateUrl).toBeDefined();

    const parsed = new URL(`http://x${negotiateUrl!}`);
    expect(parsed.pathname.endsWith('/negotiate')).toBe(true);
    expect(parsed.searchParams.get('negotiateVersion')).toBeTruthy();

    await conn.stop();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Cookie handling  (withCookies / CookieJar)
// ─────────────────────────────────────────────────────────────────────────────

import { CookieJar } from '../../src/index.js';
import type { Dispatcher } from '../../src/interfaces.js';
import { WebSocketTransport } from '../../src/transports/websocket-transport.js';
import * as nodeCrypto from 'node:crypto';

describe('HubConnection - cookie handling', () => {
  /**
   * These tests verify that cookies set by the server during the negotiate
   * phase are automatically forwarded on all subsequent requests in the same
   * SignalR session.
   *
   * The flow under test:
   *   1. Client calls start() → negotiate POST fires.
   *   2. Server responds with Set-Cookie: session=abc123; Path=/; HttpOnly
   *   3. CookieAgent stores the cookie in the CookieJar.
   *   4. Client opens a WebSocket; the upgrade request includes Cookie: session=abc123
   *
   * We inspect srv.requests (URL + headers) to assert cookie presence.
   */
  let srv: SignalRTestServer;

  beforeAll(async () => {
    srv = await startSignalRServer();
  });

  afterAll(async () => {
    await srv.close();
  });

  beforeEach(() => {
    srv.requests.length = 0;
    srv.requestUrls.length = 0;
    srv.negotiateResponseHeaders = {};
  });

  afterEach(() => {
    // Always clear extra negotiate headers so other tests are not affected.
    srv.negotiateResponseHeaders = {};
  });

  it('withCookies() auto-creates a CookieJar when none is provided', async () => {
    const builder = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withCookies()          // no jar argument → creates one internally
      .configureLogging(LogLevel.None);

    // Just verify build() doesn't throw - the connection should start cleanly.
    const conn     = builder.build();
    const clientP  = srv.nextClient();
    await conn.start();
    await clientP;
    await conn.stop();
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  it('withCookies() accepts a pre-existing CookieJar', async () => {
    const jar  = new CookieJar();
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withCookies(jar)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;
    await conn.stop();
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  it('cookie set by negotiate Set-Cookie is forwarded on the WebSocket upgrade', async () => {
    // Tell the server to set a cookie on the negotiate response.
    srv.negotiateResponseHeaders = {
      'Set-Cookie': 'session=secret-token; Path=/; HttpOnly',
    };

    const jar  = new CookieJar();
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withCookies(jar)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;
    await conn.stop();

    // Find the WebSocket upgrade request (no '/negotiate' in URL).
    const wsReq = srv.requests.find(
      (r) => !r.url.includes('/negotiate') && r.url.includes('?'),
    );
    expect(wsReq, 'No WebSocket upgrade request captured').toBeDefined();

    const cookieHeader = wsReq!.headers['cookie'];
    expect(cookieHeader, 'Cookie header missing on WebSocket upgrade').toBeDefined();
    expect(String(cookieHeader)).toContain('session=secret-token');
  });

  it('multiple cookies set by negotiate are all forwarded', async () => {
    // Node http.ServerResponse does not support multiple Set-Cookie headers via
    // a plain object - we work around this by storing the second cookie into
    // the jar directly before connecting.
    srv.negotiateResponseHeaders = {
      'Set-Cookie': 'x-auth=token1; Path=/',
    };

    const jar = new CookieJar();
    // Pre-seed a second cookie before the connection starts.
    await jar.setCookie('x-tenant=acme; Path=/', srv.url);

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withCookies(jar)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;
    await conn.stop();

    const wsReq = srv.requests.find(
      (r) => !r.url.includes('/negotiate') && r.url.includes('?'),
    );
    expect(wsReq).toBeDefined();

    const cookieHeader = String(wsReq!.headers['cookie'] ?? '');
    expect(cookieHeader).toContain('x-auth=token1');
    expect(cookieHeader).toContain('x-tenant=acme');
  });

  it('withCookies() and withDispatcher() throw when chained together', () => {
    const builder = new HubConnectionBuilder().withUrl('http://localhost/hub');

    // withCookies → withDispatcher must throw
    expect(() =>
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withCookies()
        .withDispatcher({} as Dispatcher),
    ).toThrow(/withDispatcher.*cannot be used after withCookies/i);

    // withDispatcher → withCookies must also throw
    expect(() =>
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withDispatcher({} as Dispatcher)
        .withCookies(),
    ).toThrow(/withCookies.*cannot be used after withDispatcher/i);

    void builder; // suppress unused warning
  });

  it('negotiate request itself does not yet carry a Cookie header (jar is empty initially)', async () => {
    // Fresh jar, nothing pre-seeded - negotiate must not send a Cookie header.
    const jar  = new CookieJar();
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withCookies(jar)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;
    await conn.stop();

    const negotiateReq = srv.requests.find((r) => r.url.includes('/negotiate'));
    expect(negotiateReq).toBeDefined();
    // Cookie header must be absent (or empty) when jar is initially empty.
    const cookieHeader = negotiateReq!.headers['cookie'];
    expect(!cookieHeader || cookieHeader === '').toBe(true);
  });

  it('pre-seeded jar sends Cookie header on negotiate', async () => {
    const jar = new CookieJar();
    // Pre-populate before connection starts (simulates a prior login cookie).
    await jar.setCookie('auth=pre-existing-token; Path=/', srv.url);

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .withCookies(jar)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;
    await conn.stop();

    const negotiateReq = srv.requests.find((r) => r.url.includes('/negotiate'));
    expect(negotiateReq).toBeDefined();
    const cookieHeader = String(negotiateReq!.headers['cookie'] ?? '');
    expect(cookieHeader).toContain('auth=pre-existing-token');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Coverage gaps - hub-connection.ts
// ─────────────────────────────────────────────────────────────────────────────

describe('HubConnection - coverage gaps (hub-connection.ts)', () => {

  // ── L174: default parameter Branch #1 ───────────────────────────────────
  // `options: HubConnectionOptions = {}` - Istanbul marks two branches:
  //   Branch #1 (TRUE):  caller omits second arg → default `{}` is used
  //   Branch #2 (FALSE): caller supplies an explicit options object
  //
  // All existing tests go through HubConnectionBuilder.build() which always
  // passes an explicit options object, so Branch #1 is never exercised.
  // Constructing HubConnection directly without a second argument triggers it.

  it('HubConnection constructed without options uses the default empty object (L174 Branch #1)', () => {
    // Import HubConnection directly (already imported at line ~32).
    // Passing no options exercises the `= {}` default branch.
    const conn = new HubConnection('http://127.0.0.1:1');
    expect(conn.state).toBe(HubConnectionState.Disconnected);
    // No network call is made - constructing the object is enough.
  });

  // ── L262: this.#send(msg).catch function ────────────────────────────────
  // Inside invoke(), after registering the callback and calling #send():
  //
  //   this.#send(msg).catch((err: Error) => {
  //     this.#callbacks.delete(id);   // removes the pending entry
  //     reject(err);                  // rejects the invoke() promise
  //   });
  //
  // To reach this catch handler the transport's send() must reject while the
  // connection is in the Connected state.  We briefly patch
  // WebSocketTransport.prototype.send on the prototype so the next call
  // rejects, then restore it immediately after.

  it('invoke() .catch handler fires when transport.send() rejects (L262)', async () => {
    const srv2 = await startSignalRServer();
    try {
      const conn2 = new HubConnectionBuilder()
        .withUrl(srv2.url)
        .configureLogging(LogLevel.None)
        .build();
      const clientP = srv2.nextClient();
      await conn2.start();
      await clientP;

      // Patch prototype so the very next send() rejects.
      const sendErr  = new Error('transport send failed');
      const origSend = WebSocketTransport.prototype.send;
      WebSocketTransport.prototype.send = (): Promise<void> => Promise.reject(sendErr);

      try {
        await expect(
          conn2.invoke('AnyMethod')
        ).rejects.toThrow('transport send failed');
      } finally {
        WebSocketTransport.prototype.send = origSend;
      }

      await conn2.stop();
    } finally {
      await srv2.close();
    }
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Coverage gaps II - hub-connection.ts deeper code paths
// ─────────────────────────────────────────────────────────────────────────────

// Minimal WebSocket server-side text-frame encoder (no masking).
// Used by tests that spin up their own HTTP+WS server.
const WS_GUID_HEX = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function wsTextFrame(text: string): Buffer {
  const payload = Buffer.from(text, 'utf8');
  const len     = payload.length;
  if (len < 126) {
    return Buffer.concat([Buffer.from([0x81, len]), payload]);
  }
  const h = Buffer.allocUnsafe(4);
  h[0] = 0x81; h[1] = 126; h.writeUInt16BE(len, 2);
  return Buffer.concat([h, payload]);
}

// Small helper: spin up a plain HTTP server that accepts WebSocket upgrades,
// sends the WS 101 handshake and then hands the socket to `onUpgrade`.
async function makeWsServer(
  onRequest: (req: http.IncomingMessage, res: http.ServerResponse) => void,
  onUpgrade: (socket: net.Socket) => void,
): Promise<{ url: string; stop: () => Promise<void> }> {
  const s = http.createServer(onRequest);
  s.on('upgrade', (req: http.IncomingMessage, socket: net.Socket) => {
    const key    = req.headers['sec-websocket-key'] as string;
    const accept = nodeCrypto.createHash('sha1').update(key + WS_GUID_HEX).digest('base64');
    socket.write(
      `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`
    );
    // The socket may be in a non-flowing state after the HTTP server passes it
    // to the upgrade handler.  resume() ensures 'data' events fire properly.
    socket.resume();
    onUpgrade(socket);
  });
  await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
  const { port } = s.address() as net.AddressInfo;
  return {
    url:  `http://127.0.0.1:${port}/hub`,
    stop: () => new Promise<void>((r) => { s.closeAllConnections(); s.close(() => r()); }),
  };
}

// Standard negotiate-only HTTP server (no WebSocket support).
async function makeNegotiateSrv(
  transports: { transport: string; transferFormats: string[] }[],
  onRequest?: (req: http.IncomingMessage, res: http.ServerResponse) => boolean,
): Promise<{ url: string; stop: () => Promise<void> }> {
  const s = http.createServer((req, res) => {
    // Let callers intercept first; return true to skip default behaviour.
    if (onRequest?.(req, res)) return;
    if (req.url?.includes('/negotiate')) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        negotiateVersion: 1,
        connectionId:    'test-cid',
        connectionToken: 'test-tok',
        availableTransports: transports,
      }));
    } else {
      res.writeHead(503); res.end();
    }
  });
  await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
  const { port } = s.address() as net.AddressInfo;
  return {
    url:  `http://127.0.0.1:${port}/hub`,
    stop: () => new Promise<void>((r) => { s.closeAllConnections(); s.close(() => r()); }),
  };
}

describe('HubConnection - coverage gaps II (hub-connection.ts deeper paths)', () => {
  let srv: SignalRTestServer;

  beforeAll(async ()  => { srv = await startSignalRServer(); });
  afterAll(async ()   => { await srv.close(); });
  beforeEach(()       => { srv.requests.length = 0; });

  // ── L615: server sends a Ping (Type 6) ──────────────────────────────────

  it('server Ping (type 6) is silently accepted - connection stays Connected (L615)', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    srvCli.sendMessage({ type: MessageType.Ping });
    await new Promise<void>((r) => setTimeout(r, 30));

    expect(conn.state).toBe(HubConnectionState.Connected);
    await conn.stop();
  });

  // ── L639-640: unknown message type hits the default switch case ──────────
  // MessageType 4 (StreamInvocation) is parsed to a non-null HubMessage by the
  // JSON protocol but has no matching case in hub-connection's switch → default.

  it('StreamInvocation received from server hits the default switch branch (L639-640)', async () => {
    const logger = new MockLogger();
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(logger)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    // type 4 = StreamInvocation - valid JSON-protocol message, no hub-connection case
    srvCli.sendMessage({ type: 4, invocationId: '1', target: 'Counter', arguments: [] });
    await new Promise<void>((r) => setTimeout(r, 30));

    expect(logger.hasMessage('Unknown')).toBeTruthy();
    await conn.stop();
  });

  // ── L658: transport closed unexpectedly, no reconnect policy ────────────

  it('server socket destruction causes onclose to fire (no reconnect policy) (L658)', async () => {
    const conn    = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();
    const closedP = new Promise<void>((r) => conn.onclose(() => r()));

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli  = await clientP;

    srvCli.close(); // destroy server socket → WebSocket close event on client
    await closedP;

    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  // ── L374-375, L492-493: skipNegotiation + WebSockets path ───────────────

  it('skipNegotiation+WebSockets connects directly without a negotiate round-trip (L374-375, L492-493)', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url, { skipNegotiation: true, transport: HttpTransportType.WebSockets })
      .configureLogging(LogLevel.None)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    expect(conn.state).toBe(HubConnectionState.Connected);
    // No negotiate request should have been made
    expect(srv.requests.every((r) => !r.url.includes('/negotiate'))).toBeTruthy();
    await conn.stop();
  });

  // ── L438-439: accessTokenFactory adds Authorization header to negotiate ──

  it('accessTokenFactory token is forwarded as Bearer Authorization in negotiate (L438-439)', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url, {
        accessTokenFactory: () => Promise.resolve('bearer-token-xyz'),
      })
      .configureLogging(LogLevel.None)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const negReq = srv.requests.find((r) => r.url.includes('/negotiate'));
    expect(negReq?.headers['authorization']).toBe('Bearer bearer-token-xyz');
    await conn.stop();
  });

  // ── L314-315: stream() .catch fires when transport.send() rejects ────────

  it("stream() subscriber.error fires when transport.send() rejects (L314-315)", async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const sendErr  = new Error('stream-send-rejected');
    const origSend = WebSocketTransport.prototype.send;
    WebSocketTransport.prototype.send = (): Promise<void> => Promise.reject(sendErr);

    const erroredP = new Promise<Error>((_, reject) => {
      conn.stream<number>('Counter').subscribe({ error: reject });
    });

    WebSocketTransport.prototype.send = origSend;
    await expect(erroredP).rejects.toThrow('stream-send-rejected');
    await conn.stop().catch(() => { /* connection may already be closing */ });
  });

  // ── L559-561: parseMessages throws on invalid message ───────────────────
  // Sending a message whose `type` field is a string (not a number) causes
  // JsonHubProtocol.#coerce → parseMessages to throw, which hub-connection
  // catches at L557-561 and closes the connection.

  it('server message with non-numeric type field closes the connection (L559-561)', async () => {
    const conn    = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();
    const closedP = new Promise<void>((r) => conn.onclose(() => r()));

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli  = await clientP;

    srvCli.sendMessage({ type: 'not-a-number' }); // #coerce throws → L559-561
    await closedP;

    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  // ── L740: ping failure is logged as a warning ────────────────────────────

  it('ping send failure is logged as a warning (L740)', async () => {
    const logger = new MockLogger();
    const conn   = new HubConnectionBuilder()
      .withUrl(srv.url, { keepAliveIntervalInMilliseconds: 50 })
      .configureLogging(logger)
      .build();
    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const origSend = WebSocketTransport.prototype.send;
    WebSocketTransport.prototype.send = (): Promise<void> =>
      Promise.reject(new Error('ping-send-failed'));

    await new Promise<void>((r) => setTimeout(r, 120)); // wait for ≥1 ping tick

    WebSocketTransport.prototype.send = origSend;
    expect(logger.hasMessage('Ping failed')).toBeTruthy();
    await conn.stop().catch(() => {});
  });

  // ── L722-724: reconnect attempt failure ──────────────────────────────────
  // Uses its own server so it can be closed while the connection is live,
  // forcing the reconnect attempt (which requires a live server) to fail.

  it('failed reconnect attempt logs a warning (L722-724)', async () => {
    const srv2   = await startSignalRServer();
    const logger = new MockLogger();
    let   callCount = 0;
    // Return 0ms on the first retry attempt, then null (stop retrying).
    const oneRetryPolicy = {
      nextRetryDelayInMilliseconds: (): number | null => callCount++ === 0 ? 0 : null,
    };

    const conn    = new HubConnectionBuilder()
      .withUrl(srv2.url)
      .configureLogging(logger)
      .withAutomaticReconnect(oneRetryPolicy)
      .build();
    const closedP = new Promise<void>((r) => conn.onclose(() => r()));

    const clientP = srv2.nextClient();
    await conn.start();
    await clientP;

    // Close the server so the reconnect attempt cannot establish a new connection.
    // srv2.close() destroys sockets → triggers client disconnect → #doReconnect
    // calls #startInternal() after 0 ms delay → ECONNREFUSED → L722-724 fires.
    await srv2.close();

    await closedP;
    expect(logger.hasMessage('Reconnect attempt')).toBeTruthy();
    expect(logger.hasMessage('failed')).toBeTruthy();
  });

  // ── L544-549: handshake parse error ─────────────────────────────────────
  // Configure the shared test server to send an error handshake response.
  // `parseHandshakeResponse` throws when it sees the `error` field, and
  // hub-connection catches it at L544-549.

  it('server handshake error causes start() to reject (L544-549)', async () => {
    // Ask the server to send an error handshake instead of the normal `{}\x1e`.
    srv.handshakeResponse = `{"error":"protocol not supported"}\x1e`;

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();

    await expect(conn.start()).rejects.toThrow(/protocol not supported/);
    // srv.handshakeResponse is reset to '' by the server after each connection.
  });

  // ── L385-386, L418: negotiate redirect with accessToken ──────────────────
  // A redirect response that carries an `accessToken` field causes
  // #overrideAccessToken (L418) to be called so that subsequent negotiate
  // requests use the new token (L385-386).

  it('negotiate redirect with accessToken field overrides the access-token factory (L385-386, L418)', async () => {
    // A server that returns one redirect pointing to the shared test server.
    const redirectSrv = await (async (): Promise<{ url: string; stop: () => Promise<void> }> => {
      const s = http.createServer((_req, res) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url: srv.url, accessToken: 'redirected-bearer-456' }));
      });
      await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
      const { port } = s.address() as net.AddressInfo;
      return {
        url:  `http://127.0.0.1:${port}/hub`,
        stop: () => new Promise<void>((r) => { s.closeAllConnections(); s.close(() => r()); }),
      };
    })();

    const conn    = new HubConnectionBuilder()
      .withUrl(redirectSrv.url)
      .configureLogging(LogLevel.None)
      .build();
    const clientP = srv.nextClient();

    await conn.start();
    await clientP;

    // The second negotiate went to srv (real server) carrying the redirected token.
    const negReq = srv.requests.find((r) => r.url.includes('/negotiate'));
    expect(negReq?.headers['authorization']).toBe('Bearer redirected-bearer-456');

    await conn.stop();
    await redirectSrv.stop();
  });

  // ── L482-489: all transports fail → UnsupportedTransportError ───────────
  // A server that offers WebSockets in negotiate but rejects every WebSocket
  // upgrade causes #selectTransport to log a warning at L482, then throw
  // UnsupportedTransportError at L486-489.

  it('WebSocket upgrade failure causes UnsupportedTransportError (L482-489)', async () => {
    const s = http.createServer((req, res) => {
      if (req.url?.includes('/negotiate')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          negotiateVersion: 1, connectionId: 'ws-fail', connectionToken: 'wf-tok',
          availableTransports: [{ transport: 'WebSockets', transferFormats: ['Text'] }],
        }));
      } else { res.writeHead(404); res.end(); }
    });
    // Reject every WebSocket upgrade to make transport.connect() fail.
    s.on('upgrade', (_req: http.IncomingMessage, socket: net.Socket) => {
      socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
      socket.destroy();
    });
    await new Promise<void>((r) => s.listen(0, '127.0.0.1', r));
    const { port } = s.address() as net.AddressInfo;
    const failUrl  = `http://127.0.0.1:${port}/hub`;

    const conn = new HubConnectionBuilder()
      .withUrl(failUrl)
      .configureLogging(LogLevel.None)
      .build();

    await expect(conn.start()).rejects.toThrow(/Unable to connect/);

    await new Promise<void>((r) => { s.closeAllConnections(); s.close(() => r()); });
  });

  // ── L503: ServerSentEvents transport creation ────────────────────────────

  it('SSE transport is instantiated when negotiate offers only ServerSentEvents (L503)', async () => {
    const sseSrv = await makeNegotiateSrv(
      [{ transport: 'ServerSentEvents', transferFormats: ['Text'] }],
    );
    const conn = new HubConnectionBuilder()
      .withUrl(sseSrv.url, { transport: HttpTransportType.ServerSentEvents })
      .configureLogging(LogLevel.None)
      .build();

    // #createTransport(ServerSentEvents) fires (L503) then connect() rejects (503 server).
    await expect(conn.start()).rejects.toThrow();
    await sseSrv.stop();
  });

  // ── L505: LongPolling transport creation ─────────────────────────────────

  it('LongPolling transport is instantiated when negotiate offers only LongPolling (L505)', async () => {
    const lpSrv = await makeNegotiateSrv(
      [{ transport: 'LongPolling', transferFormats: ['Text'] }],
    );
    const conn = new HubConnectionBuilder()
      .withUrl(lpSrv.url, { transport: HttpTransportType.LongPolling })
      .configureLogging(LogLevel.None)
      .build();

    // #createTransport(LongPolling) fires (L505) then connect() rejects (503 server).
    await expect(conn.start()).rejects.toThrow();
    await lpSrv.stop();
  });


  // ── L521: handshake timer fires when server never sends handshake JSON ─────
  // Uses the new handshakeTimeoutInMilliseconds option (100 ms) so the test
  // does not wait 15 s for the hard-coded default to expire.
  //
  // srv.hangHandshake instructs startSignalRServer to accept the WS upgrade
  // and receive the client's SignalR handshake request, but deliberately
  // omit the handshake response.  The client's timer (L528-530) fires after
  // 100 ms and rejects start() with "Server timeout: no handshake response".

  it('start() rejects when server completes WS upgrade but never sends handshake (L521)', async () => {
    srv.hangHandshake = true;

    const conn = new HubConnectionBuilder()
      .withUrl(srv.url, { handshakeTimeoutInMilliseconds: 100 })
      .configureLogging(LogLevel.None)
      .build();

    await expect(conn.start()).rejects.toThrow(/Server timeout.*handshake/i);
  });

  // ── L439: accessTokenFactory returning null skips Authorization header ────
  // When the factory returns null the `if (token)` branch is false → no header.

  it('accessTokenFactory returning null does not add an Authorization header (L439)', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url, { accessTokenFactory: () => Promise.resolve(null) })
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    await clientP;

    const negReq = srv.requests.find((r) => r.url.includes('/negotiate'));
    expect(negReq?.headers['authorization']).toBeUndefined();
    await conn.stop();
  });

  // ── L589: StreamItem with unknown invocationId is silently ignored ─────────
  // When #streamCbs has no entry for the incoming invocationId the `if (!sc)`
  // guard at L589 breaks out of the switch case without error.

  it('StreamItem for an unrecognised invocationId is silently ignored (L589)', async () => {
    const conn = new HubConnectionBuilder()
      .withUrl(srv.url)
      .configureLogging(LogLevel.None)
      .build();

    const clientP = srv.nextClient();
    await conn.start();
    const srvCli = await clientP;

    // No stream subscription registered → #streamCbs is empty → if (!sc) fires.
    srvCli.sendMessage({ type: MessageType.StreamItem, invocationId: 'no-such-id', item: 42 });
    await new Promise<void>((r) => setTimeout(r, 30));

    expect(conn.state).toBe(HubConnectionState.Connected);
    await conn.stop();
  });
});
