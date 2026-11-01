/**
 * Regression tests for the timing between start(), stop(), reconnects and the
 * transport going away.
 */

import { setTimeout as sleep } from 'node:timers/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { HubConnectionBuilder } from '../../src/hub-connection-builder.js';
import { HubConnectionState, HttpTransportType } from '../../src/constants.js';
import { AbortError, TransportError, UnsupportedTransportError } from '../../src/errors.js';
import { HubConnection } from '../../src/hub-connection.js';
import { CookieJar } from 'tough-cookie';
import { MockLogger } from '../helpers/mock-logger.js';
import { startScriptedHub, type HubScript, type ScriptedHub } from '../helpers/scripted-hub-server.js';
import type { IHttpClient, IRetryPolicy } from '../../src/interfaces.js';

const FAST: IRetryPolicy = {
  nextRetryDelayInMilliseconds: ({ previousRetryCount }) => (previousRetryCount < 6 ? 10 : null),
};

let hub: ScriptedHub;
const connections: HubConnection[] = [];

async function setup(script: HubScript = {}): Promise<ScriptedHub> {
  hub = await startScriptedHub(script);
  return hub;
}

function connect(options: { policy?: IRetryPolicy; timeout?: number; logger?: MockLogger } = {}): HubConnection {
  let builder = new HubConnectionBuilder()
    .withUrl(hub.url, { handshakeTimeoutInMilliseconds: options.timeout ?? 500 });
  if (options.policy) builder = builder.withAutomaticReconnect(options.policy);
  if (options.logger) builder = builder.configureLogging(options.logger);
  const connection = builder.build();
  connections.push(connection);
  return connection;
}

afterEach(async () => {
  await Promise.all(connections.splice(0).map((connection) => connection.stop()));
  hub?.close();
});

describe('start() settles', () => {
  it('rejects when the socket dies during the handshake', async () => {
    await setup({ onHandshake: (socket) => socket.destroy() });
    const connection = connect();

    await expect(connection.start()).rejects.toThrow(/./);

    expect(connection.state).toBe(HubConnectionState.Disconnected);
  });

  it('rejects instead of reconnecting when a first start loses its socket', async () => {
    await setup({ onHandshake: (socket) => socket.destroy() });
    const connection = connect({ policy: FAST });
    const reconnecting = vi.fn();
    connection.onreconnecting(reconnecting);

    await expect(connection.start()).rejects.toThrow(/./);

    expect(reconnecting).not.toHaveBeenCalled();
    expect(connection.state).toBe(HubConnectionState.Disconnected);
  });

  it('reports a transport that closes cleanly during the handshake', async () => {
    await setup({ onHandshake: (socket) => socket.closeWith(1000) });

    await expect(connect().start()).rejects.toThrow(/during the handshake/);
  });

  it('rejects when a handshake frame is over the size limit', async () => {
    await setup({ onHandshake: (socket) => socket.raw('x'.repeat(500)) });
    const connection = new HubConnectionBuilder()
      .withUrl(hub.url, { maximumReceiveMessageSize: 300 })
      .build();
    connections.push(connection);

    await expect(connection.start()).rejects.toThrow(/Incoming message exceeds/);
  });

  it('rejects when the handshake request cannot be sent', async () => {
    let polls = 0;
    await setup({
      transports: ['LongPolling'],
      http(req, res) {
        if (req.method === 'GET' && ++polls > 1) return;
        res.writeHead(req.method === 'POST' ? 500 : 200);
        res.end();
      },
    });

    await expect(connect().start()).rejects.toThrow(/send failed with status 500/);
  });

  it('can start again after a failed start', async () => {
    await setup({ onHandshake: (socket) => (socket.index === 1 ? socket.destroy() : socket.handshake()) });
    const connection = connect();

    await expect(connection.start()).rejects.toThrow(/./);
    await connection.start();

    expect(connection.state).toBe(HubConnectionState.Connected);
  });
});

describe('stop() during a start', () => {
  it('rejects a start waiting for the handshake, with no onclose', async () => {
    await setup({ onHandshake: () => {} });
    const connection = connect({ timeout: 5_000 });
    const closed = vi.fn();
    connection.onclose(closed);

    const starting = connection.start();
    await vi.waitFor(() => expect(hub.sockets).toHaveLength(1));
    await sleep(20);
    await connection.stop();

    await expect(starting).rejects.toBeInstanceOf(AbortError);
    expect(connection.state).toBe(HubConnectionState.Disconnected);
    expect(closed).not.toHaveBeenCalled();
    await vi.waitFor(() => expect(hub.openSockets()).toBe(0));
  });

  it('rejects a start waiting for negotiate and opens no socket', async () => {
    await setup({ negotiate: () => sleep(60).then(() => undefined) });
    const connection = connect();

    const starting = connection.start();
    await sleep(10);
    await connection.stop();

    await expect(starting).rejects.toBeInstanceOf(AbortError);
    await sleep(100);
    expect(connection.state).toBe(HubConnectionState.Disconnected);
    expect(hub.sockets).toHaveLength(0);
  });

  it('closes a socket that opens after the attempt gave up', async () => {
    await setup();
    let release!: () => void;
    const token = new Promise<string>((resolve) => { release = () => resolve('t'); });
    const connection = new HubConnectionBuilder()
      .withUrl(hub.url, { accessTokenFactory: () => token, skipNegotiation: true, transport: HttpTransportType.WebSockets })
      .build();
    connections.push(connection);

    const starting = connection.start();
    await sleep(10);
    await connection.stop();
    await expect(starting).rejects.toBeInstanceOf(AbortError);

    release();
    await vi.waitFor(() => expect(hub.sockets).toHaveLength(1));
    await vi.waitFor(() => expect(hub.openSockets()).toBe(0));
  });
});

describe('stop() while a negotiated transport connects', () => {
  it('rejects the start and tries no other transport', async () => {
    await setup({ transports: ['WebSockets', 'LongPolling'] });
    let calls = 0;
    let release!: () => void;
    const held = new Promise<string>((resolve) => { release = () => resolve('t'); });
    const connection = new HubConnectionBuilder()
      .withUrl(hub.url, { accessTokenFactory: () => (++calls === 1 ? Promise.resolve('t') : held) })
      .build();
    connections.push(connection);

    const starting = connection.start();
    await vi.waitFor(() => expect(calls).toBe(2), { interval: 2 });
    await connection.stop();

    await expect(starting).rejects.toBeInstanceOf(AbortError);
    release();
    await vi.waitFor(() => expect(hub.sockets).toHaveLength(1));
    await vi.waitFor(() => expect(hub.openSockets()).toBe(0));
  });
});

describe('reconnecting', () => {
  it('runs one loop and leaves no sockets behind after a failed attempt', async () => {
    await setup({
      onHandshake(socket) {
        if (socket.index === 2) {
          socket.raw('{"error":"nope"}\x1e');
          setTimeout(() => socket.destroy(), 50);
        } else socket.handshake();
      },
    });
    const connection = connect({ policy: FAST });
    const reconnecting = vi.fn();
    const reconnected = vi.fn();
    connection.onreconnecting(reconnecting);
    connection.onreconnected(reconnected);
    await connection.start();

    hub.sockets[0]!.destroy();
    await vi.waitFor(() => expect(reconnected).toHaveBeenCalled());
    await sleep(100);

    expect(reconnecting).toHaveBeenCalledTimes(1);
    expect(reconnected).toHaveBeenCalledTimes(1);
    expect(hub.sockets).toHaveLength(3);
    expect(hub.openSockets()).toBe(1);
  });

  it('retries when the socket dies during a reconnect handshake', async () => {
    await setup({
      onHandshake: (socket) => (socket.index === 2 ? socket.destroy() : socket.handshake()),
    });
    const connection = connect({ policy: FAST });
    const reconnected = vi.fn();
    connection.onreconnected(reconnected);
    await connection.start();

    hub.sockets[0]!.destroy();

    await vi.waitFor(() => expect(reconnected).toHaveBeenCalledTimes(1));
    expect(hub.sockets).toHaveLength(3);
  });

  it('stop() during an attempt ends disconnected with the socket closed', async () => {
    await setup({ negotiate: (attempt) => (attempt > 1 ? sleep(150).then(() => undefined) : undefined) });
    const connection = connect({ policy: FAST });
    const closed = vi.fn();
    connection.onclose(closed);
    await connection.start();

    hub.sockets[0]!.destroy();
    await vi.waitFor(() => expect(connection.state).toBe(HubConnectionState.Reconnecting));
    await connection.stop();
    await sleep(200);

    expect(connection.state).toBe(HubConnectionState.Disconnected);
    expect(closed).toHaveBeenCalledTimes(1);
    expect(hub.sockets).toHaveLength(1);
  });

  it('stop() during a reconnect delay returns without waiting it out', async () => {
    await setup();
    const connection = connect({ policy: { nextRetryDelayInMilliseconds: () => 60_000 } });
    await connection.start();
    hub.sockets[0]!.destroy();
    await vi.waitFor(() => expect(connection.state).toBe(HubConnectionState.Reconnecting));

    const before = process.getActiveResourcesInfo().filter((name) => name === 'Timeout').length;
    await connection.stop();
    await sleep(0);

    expect(process.getActiveResourcesInfo().filter((name) => name === 'Timeout').length).toBeLessThan(before);
  });

  it('closes with the error when the retry policy throws', async () => {
    await setup();
    const connection = connect({
      policy: { nextRetryDelayInMilliseconds() { throw new Error('policy bug'); } },
    });
    const closed = new Promise<Error | undefined>((resolve) => connection.onclose(resolve));
    await connection.start();

    hub.sockets[0]!.destroy();

    expect((await closed)?.message).toBe('policy bug');
    expect(connection.state).toBe(HubConnectionState.Disconnected);
  });

  it('asks accessTokenFactory again after a negotiate redirect', async () => {
    const target = await startScriptedHub();
    await setup({ negotiate: () => ({ url: target.url, accessToken: 'redirect-token' }) });
    const connection = new HubConnectionBuilder()
      .withUrl(hub.url, { accessTokenFactory: async () => 'user-token' })
      .withAutomaticReconnect(FAST)
      .build();
    connections.push(connection);
    const reconnected = new Promise((resolve) => connection.onreconnected(resolve));
    await connection.start();

    target.sockets[0]!.destroy();
    await reconnected;

    expect(hub.negotiateAuth).toEqual(['Bearer user-token', 'Bearer user-token']);
    expect(target.negotiateAuth).toEqual(['Bearer redirect-token', 'Bearer redirect-token']);
    await connection.stop();
    target.close();
  });
});

describe('messages sent with the handshake', () => {
  it('are handled after start() reaches Connected', async () => {
    await setup({ onHandshake: (socket) => socket.raw('{}\x1e{"type":1,"target":"hello","arguments":[]}\x1e') });
    const connection = connect();
    const states: HubConnectionState[] = [];
    connection.on('hello', () => { states.push(connection.state); });

    await connection.start();

    expect(states).toEqual([HubConnectionState.Connected]);
  });

  it('keep their order when they arrive in a following frame', async () => {
    await setup({
      onHandshake(socket) {
        socket.socket.cork();
        socket.raw('{}\x1e{"type":1,"target":"hello","arguments":[1]}\x1e');
        socket.raw('{"type":1,"target":"hello","arguments":[2]}\x1e');
        socket.socket.uncork();
      },
    });
    const connection = connect();
    const seen: unknown[] = [];
    connection.on('hello', (value) => { seen.push(value); });

    await connection.start();
    await vi.waitFor(() => expect(seen).toHaveLength(2));

    expect(seen).toEqual([1, 2]);
  });

  it('a Close among them closes the connection after start()', async () => {
    await setup({ onHandshake: (socket) => socket.raw('{}\x1e{"type":7,"error":"bye"}\x1e') });
    const connection = connect();
    const closed = new Promise<Error | undefined>((resolve) => connection.onclose(resolve));

    await connection.start();

    expect((await closed)?.message).toBe('bye');
    expect(connection.state).toBe(HubConnectionState.Disconnected);
  });

  it('are dropped when onreconnected stops the connection', async () => {
    await setup({
      onHandshake: (socket) => socket.raw(socket.index === 1 ? '{}\x1e' : '{}\x1e{"type":1,"target":"hello","arguments":[]}\x1e'),
    });
    const connection = connect({ policy: FAST });
    const hello = vi.fn();
    connection.on('hello', hello);
    const reconnected = new Promise<void>((resolve) => {
      connection.onreconnected(() => { void connection.stop(); resolve(); });
    });
    await connection.start();

    hub.sockets[0]!.destroy();
    await reconnected;
    await sleep(20);

    expect(hello).not.toHaveBeenCalled();
  });
});

describe('subscribers and handlers', () => {
  it('survives a stream subscriber that throws', async () => {
    await setup();
    const logger = new MockLogger();
    const connection = connect({ logger });
    await connection.start();
    const after = vi.fn();
    connection.on('after', after);
    connection.stream('Numbers').subscribe({ next() { throw new Error('subscriber bug'); } });

    hub.sockets[0]!.raw('{"type":2,"invocationId":"1","item":1}\x1e{"type":1,"target":"after","arguments":[]}\x1e');

    await vi.waitFor(() => expect(after).toHaveBeenCalled());
    expect(connection.state).toBe(HubConnectionState.Connected);
    expect(logger.hasMessage('subscriber bug')).toBe(true);
  });

  it('still closes when a subscriber throws from error()', async () => {
    await setup();
    const connection = connect();
    await connection.start();
    connection.stream('Numbers').subscribe({ error() { throw new Error('subscriber bug'); } });
    const closed = new Promise((resolve) => connection.onclose(resolve));

    hub.sockets[0]!.destroy();
    await closed;

    expect(connection.state).toBe(HubConnectionState.Disconnected);
  });

  it('survives a handler finishing after the server closed the connection', async () => {
    await setup();
    const unhandled = vi.fn();
    process.on('unhandledRejection', unhandled);
    const logger = new MockLogger();
    const connection = connect({ logger });
    connection.on('ask', async () => { await sleep(30); return 1; });
    await connection.start();

    hub.sockets[0]!.send({ type: 1, invocationId: '9', target: 'ask', arguments: [] });
    await sleep(5);
    hub.sockets[0]!.send({ type: 7 });
    await sleep(60);

    process.off('unhandledRejection', unhandled);
    expect(unhandled).not.toHaveBeenCalled();
    expect(logger.hasMessage('Could not send a completion')).toBe(true);
  });

  it('reports a handler result that cannot be serialized', async () => {
    await setup();
    const connection = connect();
    connection.on('ask', () => 1n);
    await connection.start();
    const received = new Promise<string>((resolve) => {
      hub.sockets[0]!.socket.on('data', (data: Buffer) => resolve(unmask(data)));
    });

    hub.sockets[0]!.send({ type: 1, invocationId: '9', target: 'ask', arguments: [] });

    expect(await received).toMatch(/could not be serialized/);
  });
});

describe('errors and calls when not connected', () => {
  it('names the failure of each transport', async () => {
    await setup({ rejectUpgrade: () => true });

    const error = await connect().start().catch((err: unknown) => err as UnsupportedTransportError);

    expect(error).toBeInstanceOf(UnsupportedTransportError);
    expect((error as Error).message).toBe(
      'Unable to connect with any of the available transports. WebSockets: WebSocket upgrade failed with status code 403.',
    );
    expect((error as Error).cause).toBeInstanceOf(TransportError);
    expect(((error as Error).cause as TransportError).statusCode).toBe(403);
  });

  it('gives no cause when no transport was tried', async () => {
    await setup({ transports: [] });

    const error = (await connect().start().catch((err: unknown) => err)) as Error;

    expect(error.message).toBe('Unable to connect with any of the available transports.');
    expect(error.cause).toBeUndefined();
  });

  it.each([[1011, 'server error'], [4000, '']])('reports close code %i as an error', async (code, reason) => {
    await setup();
    const connection = connect();
    const closed = new Promise<Error | undefined>((resolve) => connection.onclose(resolve));
    await connection.start();

    hub.sockets[0]!.closeWith(code, reason);

    expect((await closed)?.message).toContain(`status code ${code} (${reason || 'no reason given'})`);
  });

  it('treats close code 1000 as a clean close', async () => {
    await setup();
    const connection = connect();
    const closed = new Promise<Error | undefined>((resolve) => connection.onclose(resolve));
    await connection.start();

    hub.sockets[0]!.closeWith(1000);

    expect(await closed).toBeUndefined();
  });

  it('rejects invoke() instead of throwing', async () => {
    await setup();

    await expect(connect().invoke('Echo')).rejects.toThrow(/not Connected/);
  });

  it('reports stream() through the subscriber', async () => {
    await setup();
    const error = vi.fn();

    connect().stream('Numbers').subscribe({ error });

    expect(error.mock.calls[0]![0].message).toMatch(/not Connected/);
  });

  it('rejects invoke() when arguments cannot be serialized', async () => {
    await setup();
    const connection = connect();
    await connection.start();

    await expect(connection.invoke('Echo', 1n)).rejects.toThrow(TypeError);
  });

  it('clears connectionId when the connection ends', async () => {
    await setup();
    const connection = connect();
    await connection.start();
    expect(connection.connectionId).toBe('id-1');

    await connection.stop();

    expect(connection.connectionId).toBeNull();
  });
});

describe('stop() called twice', () => {
  it('fires onclose once, and both calls wait for it', async () => {
    await setup();
    const connection = connect();
    const closed = vi.fn();
    connection.onclose(closed);
    await connection.start();

    await Promise.all([connection.stop(), connection.stop()]);

    expect(closed).toHaveBeenCalledTimes(1);
    expect(connection.state).toBe(HubConnectionState.Disconnected);
  });

  it('can stop a later connection after an earlier stop', async () => {
    await setup();
    const connection = connect();
    await connection.start();
    await connection.stop();
    await connection.start();

    await connection.stop();

    expect(connection.state).toBe(HubConnectionState.Disconnected);
    await vi.waitFor(() => expect(hub.openSockets()).toBe(0));
  });
});

describe('withCookies()', () => {
  it('can start again after stop()', async () => {
    await setup();
    const connection = new HubConnectionBuilder().withUrl(hub.url).withCookies().build();
    connections.push(connection);
    await connection.start();
    await connection.stop();

    await connection.start();

    expect(connection.state).toBe(HubConnectionState.Connected);
  });

  it('can start again from onclose when the server closes', async () => {
    await setup();
    const connection = new HubConnectionBuilder().withUrl(hub.url).withCookies().build();
    connections.push(connection);
    let closes = 0;
    const restarted = new Promise<void>((resolve) => {
      // Only the first time: the stop() in afterEach fires onclose as well.
      connection.onclose(() => { if (++closes === 1) void connection.start().then(resolve); });
    });
    await connection.start();

    hub.sockets[0]!.destroy();
    await restarted;

    expect(connection.state).toBe(HubConnectionState.Connected);
  });

  it('keeps a custom HTTP client when it renews the dispatcher', async () => {
    await setup();
    const post = vi.fn(async () => ({
      status: 200,
      headers: {},
      body: JSON.stringify({
        connectionId: 'id', connectionToken: 'token',
        availableTransports: [{ transport: 'WebSockets', transferFormats: ['Text'] }],
      }),
    }));
    const logger = new MockLogger();
    const connection = new HubConnectionBuilder()
      .withUrl(hub.url)
      .withCookies(new CookieJar())
      .withHttpClient({ post } as unknown as IHttpClient)
      .configureLogging(logger)
      .build();
    connections.push(connection);
    await connection.start();
    await connection.stop();
    await connection.start();

    expect(post).toHaveBeenCalledTimes(2);
    expect(logger.hasMessage('does not reach a client from withHttpClient')).toBe(true);
  });
});

/** Decode one masked client frame. */
function unmask(data: Buffer): string {
  const short = (data[1]! & 0x7f) < 126;
  const offset = short ? 2 : 4;
  const key = data.subarray(offset, offset + 4);
  return Buffer.from(data.subarray(offset + 4).map((byte, i) => byte ^ key[i % 4]!)).toString('utf8');
}
