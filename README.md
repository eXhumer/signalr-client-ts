# @exhumer/signalr-client

[![CI](https://github.com/eXhumer/signalr-client-ts/actions/workflows/ci.yml/badge.svg)](https://github.com/eXhumer/signalr-client-ts/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/eXhumer/signalr-client-ts/branch/main/graph/badge.svg)](https://codecov.io/gh/eXhumer/signalr-client-ts)
[![npm](https://img.shields.io/npm/v/@exhumer/signalr-client)](https://www.npmjs.com/package/@exhumer/signalr-client)


ASP.NET Core SignalR client for Node.js. HTTP and WebSocket layers are backed entirely by [undici](https://github.com/nodejs/undici) - no `node-fetch`, no `ws`, no browser shims required.

## Requirements

- Node.js ≥ 22.19.0
- TypeScript ≥ 5.9 (for consumers using types)

## Installation

```sh
npm install @exhumer/signalr-client
```

## Quick start

```ts
import { HubConnectionBuilder, LogLevel } from '@exhumer/signalr-client';

const connection = new HubConnectionBuilder()
  .withUrl('https://example.com/chathub')
  .configureLogging(LogLevel.Information)
  .withAutomaticReconnect()
  .build();

connection.on('ReceiveMessage', (user: string, message: string) => {
  console.log(`${user}: ${message}`);
});

await connection.start();

await connection.send('SendMessage', 'Alice', 'Hello!');
const result = await connection.invoke<string>('Echo', 'hello');

await connection.stop();
```

## `HubConnectionBuilder`

All configuration is done through the fluent builder before calling `build()`.

### `.withUrl(url, options?)`

Required. Sets the hub endpoint URL.

```ts
builder.withUrl('https://example.com/hub', {
  // Bearer token factory - called before each negotiate attempt
  accessTokenFactory: () => fetchToken(),

  // Bitmask of transports to allow (default: all three)
  transport: HttpTransportType.WebSockets | HttpTransportType.ServerSentEvents,

  // Extra headers appended to every request
  headers: { 'X-Tenant': 'acme' },

  // Skip /negotiate and connect directly via WebSocket
  // (only valid when transport is exclusively WebSockets)
  skipNegotiation: true,

  // How long without a server message before the connection is considered dead
  serverTimeoutInMilliseconds: 60_000,   // default: 30 000

  // How often to send a keep-alive ping to the server
  keepAliveIntervalInMilliseconds: 10_000, // default: 15 000

  // How long to wait for the SignalR protocol handshake
  handshakeTimeoutInMilliseconds: 15_000, // default: 15 000

  // Reject an individual incoming transport payload above this size
  maximumReceiveMessageSize: 32 * 1024 * 1024, // default: 32 MiB
});
```

### `.configureLogging(logLevelOrLogger)`

Pass a `LogLevel` constant to use the built-in `ConsoleLogger`, or an object implementing `ILogger` to use your own.

```ts
builder.configureLogging(LogLevel.Warning);

// Custom logger
builder.configureLogging({
  log(level, message) { myLogger.write(level, message); },
});
```

### `.withAutomaticReconnect()`

Enable automatic reconnection on unexpected disconnects.

```ts
// Default delays: 0 ms, 2 s, 10 s, 30 s - then give up
builder.withAutomaticReconnect();

// Custom delay sequence (ms)
builder.withAutomaticReconnect([0, 1_000, 5_000, 10_000, 30_000]);

// Custom policy - return null to stop retrying
builder.withAutomaticReconnect({
  nextRetryDelayInMilliseconds({ previousRetryCount, elapsedMilliseconds }) {
    if (elapsedMilliseconds > 60_000) return null; // give up after 1 min
    return Math.min(1_000 * 2 ** previousRetryCount, 30_000); // exponential backoff
  },
});
```

### `.withCookies(jar?)`

Enable automatic cookie handling. Every request in the session (negotiate, WebSocket upgrade, SSE, long-polling) shares the same `CookieJar`.

```ts
import { HubConnectionBuilder, CookieJar } from '@exhumer/signalr-client';

// Automatic jar - server cookies are collected and replayed automatically
builder.withCookies();

// Pre-seeded jar - inject an auth cookie obtained before connecting
const jar = new CookieJar();
await jar.setCookie('session=abc123', 'https://example.com');
builder.withCookies(jar);
```

`withCookies()` and `withDispatcher()` are mutually exclusive. To combine cookie support with a custom dispatcher, compose the `cookie` interceptor instead - see [Cookie handling with a custom dispatcher](#cookie-handling-with-a-custom-dispatcher).

### `.withDispatcher(dispatcher)`

Set an undici `Dispatcher` for the entire connection session. Accepts any subclass - `Agent`, `Pool`, `Client`, `ProxyAgent`, `MockAgent`, etc.

```ts
import { ProxyAgent } from 'undici';

builder.withDispatcher(new ProxyAgent('http://proxy.corp:8080'));
```

### `.withHttpClient(client)`

Substitute the HTTP client used for negotiate, SSE, and long-polling requests. Useful for swapping to a different undici primitive or injecting a mock in tests.

```ts
import { FetchHttpClient } from '@exhumer/signalr-client';

builder.withHttpClient(new FetchHttpClient());
```

### `.withHubProtocol(protocol)`

Select the hub wire protocol. JSON is used by default; MessagePack provides a
compact binary representation and requires server-side MessagePack support.

```ts
import { HubConnectionBuilder, MsgpackHubProtocol } from '@exhumer/signalr-client';

const connection = new HubConnectionBuilder()
  .withUrl('https://example.com/hub')
  .withHubProtocol(new MsgpackHubProtocol())
  .build();
```

### `.build()`

Returns a `HubConnection`. Throws if `.withUrl()` was never called.

---

## `HubConnection`

### Lifecycle

```ts
await connection.start();   // Negotiate, pick transport, perform handshake
await connection.stop();    // Graceful shutdown
```

`start()` throws if the connection is not in the `Disconnected` state.

#### State

```ts
connection.state        // HubConnectionState string
connection.connectionId // string | null - assigned after negotiate
```

`HubConnectionState` values: `"Disconnected"`, `"Connecting"`, `"Connected"`, `"Disconnecting"`, `"Reconnecting"`.

### Invoking hub methods

```ts
// invoke - awaits the server's return value
const result = await connection.invoke<string>('Echo', 'hello');

// send - fire-and-forget; server sends no completion message
await connection.send('Broadcast', 'hello everyone');
```

### Server streaming

```ts
const stream = connection.stream<number>('Counter', 10);

const sub = stream.subscribe({
  next:     (value)  => console.log(value),
  error:    (err)    => console.error(err),
  complete: ()       => console.log('done'),
});

// Cancel early
sub.dispose();

// Or with the `using` keyword (TS 5.2+)
using sub = connection.stream<number>('Counter', 10).subscribe({ next: console.log });
```

`stream()` sends the invocation immediately. Its result may be subscribed to
once; items received before `subscribe()` are queued and delivered in order.

### Receiving hub method calls

```ts
// Register a handler - multiple handlers per method are supported
connection.on('ReceiveMessage', (user: string, msg: string) => {
  console.log(`${user}: ${msg}`);
});

// If the server requests a result, a handler's return value is sent back.
connection.on('GetClientName', () => 'node-worker-1');

// Remove a specific handler
connection.off('ReceiveMessage', handler);

// Remove all handlers for a method
connection.off('ReceiveMessage');
```

### Connection lifecycle callbacks

```ts
connection.onclose((error) => {
  if (error) console.error('Connection closed with error:', error);
  else       console.log('Connection closed cleanly.');
});

connection.onreconnecting((error) => {
  console.warn('Reconnecting...', error?.message);
});

connection.onreconnected((connectionId) => {
  console.log('Reconnected. New connection ID:', connectionId);
});
```

---

## Transports

The client negotiates the best available transport automatically, trying them in this order of preference:

1. **WebSockets** - full-duplex, lowest latency
2. **Server-Sent Events** - server-push only; client sends via separate HTTP POSTs
3. **Long Polling** - maximum compatibility; fallback of last resort

You can restrict which transports are attempted via the `transport` bitmask in `withUrl()`:

```ts
import { HttpTransportType } from '@exhumer/signalr-client';

builder.withUrl(url, {
  transport: HttpTransportType.WebSockets | HttpTransportType.LongPolling,
});

// Skip negotiate and force WebSocket directly
builder.withUrl(url, {
  transport: HttpTransportType.WebSockets,
  skipNegotiation: true,
});
```

All three transport classes are also exported for advanced direct use: `WebSocketTransport`, `ServerSentEventsTransport`, `LongPollingTransport`.

---

## Cookie handling with a custom dispatcher

`withCookies()` and `withDispatcher()` cannot be used together. To combine both - for example, routing through a proxy while also handling cookies - compose the `cookie` interceptor onto your dispatcher:

```ts
import { ProxyAgent } from 'undici';
import { HubConnectionBuilder, CookieJar, cookie } from '@exhumer/signalr-client';

const jar   = new CookieJar();
const agent = new ProxyAgent('http://proxy:8080').compose(cookie({ jar }));

const connection = new HubConnectionBuilder()
  .withUrl('https://example.com/hub')
  .withDispatcher(agent)
  .build();
```

`CookieJar` and `cookie` are re-exported from `@exhumer/signalr-client` - no separate install of `tough-cookie` or `@exhumer/undici-cookie-agent` is needed.

---

## Protocols

The library currently ships two hub protocol implementations.

**JSON** (default):

```ts
import { JsonHubProtocol } from '@exhumer/signalr-client';
```

`HubConnection` uses `JsonHubProtocol` when no protocol is configured.

**MessagePack**:

```ts
import { HubConnectionBuilder, MsgpackHubProtocol } from '@exhumer/signalr-client';

const connection = new HubConnectionBuilder()
  .withUrl('https://example.com/hub')
  .withHubProtocol(new MsgpackHubProtocol())
  .build();
```

The server must have the ASP.NET Core SignalR MessagePack protocol enabled.
`MsgpackHubProtocol` depends on `@msgpack/msgpack`, which is a regular
dependency of this package. Custom protocols can implement `IHubProtocol` and
be supplied through the same builder method.

---

## HTTP clients

Five undici-backed HTTP client implementations are exported. The default (`DispatchHttpClient`) is created automatically and fits most use cases. The others are available if you need a specific undici primitive.

| Export | Undici primitive | Notes |
|---|---|---|
| `DispatchHttpClient` | `Dispatcher#dispatch()` | Default; explicit pause/resume backpressure for streaming responses. Also aliased as `HttpClient`. |
| `RequestHttpClient` | `undici.request()` | |
| `FetchHttpClient` | `undici.fetch()` | WHATWG-compatible. |
| `StreamHttpClient` | `undici.stream()` | Factory-callback pattern. |
| `PipelineHttpClient` | `undici.pipeline()` | Duplex pipe pattern. |

Inject via `.withHttpClient()` on the builder, or use standalone:

```ts
import { DispatchHttpClient } from '@exhumer/signalr-client';

const client = new DispatchHttpClient();
const res = await client.get('https://example.com/api/data');
```

Every client constructor accepts `HttpClientOptions`, including a dispatcher,
default timeout, headers, and a buffered-response limit:

```ts
const client = new DispatchHttpClient({
  maximumResponseBodySize: 8 * 1024 * 1024,
});

await client.post(url, {
  body: new Uint8Array([0x00, 0xff]),
  headers: { 'Content-Type': 'application/octet-stream' },
});
```

For streaming responses, `DispatchHttpClient` pauses its undici dispatcher
when the consumer applies backpressure and resumes it after the stream drains.
Local benchmarks can be run with `npm run bench:transport`; results depend on
the runtime and workload. The HTTP client affects WebSocket negotiation and
startup only—steady-state WebSocket frames bypass it.

---

## Errors

All error classes are exported and support both `instanceof` checks and type guard helpers.

| Class | When thrown |
|---|---|
| `HubError` | Server-side hub method error |
| `AbortError` | In-flight operation cancelled (e.g. `stop()` called) |
| `TransportError` | Network-level failure; has `.statusCode` |
| `HandshakeError` | Server rejected the SignalR protocol handshake |
| `UnsupportedTransportError` | No acceptable transport could be negotiated |

```ts
import { isHubError, isAbortError, isTransportError } from '@exhumer/signalr-client';

try {
  await connection.invoke('DoWork');
} catch (err) {
  if (isHubError(err))       console.error('Server error:', err.message);
  else if (isAbortError(err)) console.warn('Cancelled');
  else if (isTransportError(err)) console.error('HTTP', err.statusCode);
  else throw err;
}
```

---

## Logging

```ts
import { LogLevel, ConsoleLogger, NullLogger } from '@exhumer/signalr-client';
```

`LogLevel` values (ascending severity): `Trace`, `Debug`, `Information`, `Warning`, `Error`, `Critical`, `None`.

`ConsoleLogger` routes messages to `console.debug` / `console.log` / `console.warn` / `console.error` depending on severity, prefixed with an ISO timestamp and level label.

`NullLogger` discards everything. It is the default when no logger is configured.

To plug in a third-party logger, implement `ILogger`:

```ts
import type { ILogger, LogLevel } from '@exhumer/signalr-client';
import pino from 'pino';

const logger = pino();

const signalrLogger: ILogger = {
  log(level: LogLevel, message: string) {
    logger.info({ level }, message);
  },
};

builder.configureLogging(signalrLogger);
```

---

## Advanced: implementing a custom retry policy

```ts
import type { IRetryPolicy, RetryContext } from '@exhumer/signalr-client';

class ExponentialBackoff implements IRetryPolicy {
  nextRetryDelayInMilliseconds({ previousRetryCount, elapsedMilliseconds }: RetryContext): number | null {
    if (elapsedMilliseconds > 2 * 60 * 1000) return null; // give up after 2 min
    return Math.min(500 * 2 ** previousRetryCount, 30_000);
  }
}

builder.withAutomaticReconnect(new ExponentialBackoff());
```

---

## Advanced: implementing a custom transport

Implement `ITransport` to use a completely different underlying mechanism (e.g. a raw TCP socket):

```ts
import type { ITransport } from '@exhumer/signalr-client';
import { TransferFormat } from '@exhumer/signalr-client';

class MyTransport implements ITransport {
  readonly name = 'MyTransport';
  onreceive: ((data: string | Uint8Array) => void) | null = null;
  onclose:   ((error?: Error) => void) | null = null;

  async connect(url: string, transferFormat: TransferFormat): Promise<void> { /* ... */ }
  async send(data: string | Uint8Array): Promise<void> { /* ... */ }
  async stop(): Promise<void> { /* ... */ }
}
```

---

## Build & development

```sh
# Build ESM + CJS bundles and type declarations
npm run build

# Type-check source files
npm run typecheck

# Type-check test files
npm run typecheck:test

# Run tests once
npm test

# Run tests in watch mode
npm run test:watch

# Run all benchmarks
npm run bench

# Run a specific benchmark suite
npm run bench:protocol
npm run bench:transport
```

The build uses [tsdown](https://tsdown.dev/) and produces:

| File | Format | Purpose |
|---|---|---|
| `dist/index.mjs` | ESM | `import` condition |
| `dist/index.cjs` | CJS | `require` condition |
| `dist/index.d.mts` | Types | ESM consumers |
| `dist/index.d.cts` | Types | CJS consumers |

Source maps are emitted for all four files.

---

## License

MIT
