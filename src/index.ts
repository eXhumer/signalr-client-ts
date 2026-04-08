/**
 * @signalr/client
 *
 * ASP.NET Core SignalR client for Node.js.
 * HTTP and WebSocket layers are backed by undici.
 *
 * @example
 * ```ts
 * import { HubConnectionBuilder, LogLevel } from '@signalr/client';
 *
 * const connection = new HubConnectionBuilder()
 *   .withUrl('https://example.com/chathub')
 *   .configureLogging(LogLevel.Information)
 *   .withAutomaticReconnect()
 *   .build();
 *
 * connection.on('ReceiveMessage', (user: string, message: string) => {
 *   console.log(`${user}: ${message}`);
 * });
 *
 * await connection.start();
 * const result = await connection.invoke<string>('Echo', 'hello');
 * await connection.stop();
 * ```
 */

// ── Connection ────────────────────────────────────────────────────────────────
export { HubConnection }                          from './hub-connection.js';
export type { HubConnectionOptions }              from './hub-connection.js';
export { HubConnectionBuilder, DefaultReconnectPolicy } from './hub-connection-builder.js';
export type { UrlOptions }                        from './hub-connection-builder.js';

// ── Protocol ──────────────────────────────────────────────────────────────────
export { JsonHubProtocol, parseHandshakeResponse, HANDSHAKE_REQUEST } from './protocols/json-hub-protocol.js';
export type { HandshakeResponse, HandshakeParseResult }               from './protocols/json-hub-protocol.js';

export {
  MsgpackHubProtocol,
  HANDSHAKE_REQUEST as MSGPACK_HANDSHAKE_REQUEST,
} from './protocols/msgpack-hub-protocol.js';

// ── Messages (discriminated union + type guards) ───────────────────────────────
export {
  toInvocationId,
  assertNever,
  isInvocationMessage,
  isStreamItemMessage,
  isCompletionMessage,
  isStreamInvocationMessage,
  isCancelInvocationMessage,
  isPingMessage,
  isCloseMessage,
} from './messages.js';
export type {
  InvocationId,
  MessageHeaders,
  HubMessage,
  InvocationMessage,
  StreamItemMessage,
  CompletionMessage,
  StreamInvocationMessage,
  CancelInvocationMessage,
  PingMessage,
  CloseMessage,
} from './messages.js';

// ── Enumerations & constants ──────────────────────────────────────────────────
export {
  MessageType,
  HubConnectionState,
  LogLevel,
  HttpTransportType,
  TransferFormat,
  RECORD_SEPARATOR,
  NEGOTIATE_VERSION,
  DEFAULT_TIMEOUT_IN_MS,
  DEFAULT_PING_INTERVAL_IN_MS,
  DEFAULT_SERVER_TIMEOUT_IN_MS,
} from './constants.js';
export type { MessageTypeValue } from './constants.js';

// ── Interfaces (consumers can implement / augment these) ──────────────────────
export type {
  ILogger,
  ITransport,
  IHubProtocol,
  IHttpClient,
  IRetryPolicy,
  RetryContext,
  IStreamResult,
  IStreamSubscriber,
  ISubscription,
  NegotiateResponse,
  AvailableTransport,
  HttpResponse,
  StreamResult,
  /** undici Dispatcher - re-exported so callers need not import undici directly */
  Dispatcher,
} from './interfaces.js';

// ── Loggers ───────────────────────────────────────────────────────────────────
export { ConsoleLogger, NullLogger } from './logger.js';

// ── Errors ────────────────────────────────────────────────────────────────────
export {
  HubError,
  AbortError,
  TransportError,
  HandshakeError,
  UnsupportedTransportError,
  isHubError,
  isAbortError,
  isTransportError,
} from './errors.js';

// ── HTTP clients ──────────────────────────────────────────────────────────────
//
//  Five implementations, each using a different undici primitive.
//  `HttpClient` is an alias for `DispatchHttpClient` (backward compat).
//
export {
  /** Default client - uses undici.request() */
  RequestHttpClient,
  /** WHATWG-compatible client - uses undici.fetch() */
  FetchHttpClient,
  /** Factory-callback client - uses undici.stream() */
  StreamHttpClient,
  /** Duplex-pipe client - uses undici.pipeline() */
  PipelineHttpClient,
  /** Low-level handler client - uses Dispatcher#dispatch() */
  DispatchHttpClient,
  /** Alias for DispatchHttpClient (backward compatibility) */
  HttpClient,
} from './http-client.js';
export type { RequestOptions, HttpClientOptions, HttpMethod } from './http-client.js';

// ── Transports (exposed for advanced / custom use) ────────────────────────────
export { WebSocketTransport }        from './transports/websocket-transport.js';
export { ServerSentEventsTransport } from './transports/sse-transport.js';
export { LongPollingTransport }      from './transports/long-polling-transport.js';

// ── Low-level hand-rolled WebSocket client (kept for direct use if needed) ────
export { WebSocketClient, WebSocketReadyState } from './ws-client.js';

// ── Cookie support ────────────────────────────────────────────────────────────
//
// Re-exported so consumers can use CookieJar, CookieAgent, and the low-level
// `cookie` interceptor without a separate `npm install tough-cookie @exhumer/undici-cookie-agent`.
//
// Typical usage:
//
//   import { HubConnectionBuilder, CookieJar } from '@exhumer/signalr-client';
//
//   const jar  = new CookieJar();
//   const conn = new HubConnectionBuilder()
//     .withUrl('https://example.com/hub')
//     .withCookies(jar)          // ← enables automatic cookie handling
//     .build();
//
//   await conn.start();
//   // jar now contains any cookies set by the server during negotiate
//
// To combine cookie handling with a custom dispatcher (e.g. ProxyAgent):
//
//   import { ProxyAgent } from 'undici';
//   import { cookie, CookieJar } from '@exhumer/signalr-client';
//
//   const jar   = new CookieJar();
//   const agent = new ProxyAgent('http://proxy:8080');
//   const agentWithCookies = agent.compose(cookie({ jar }));
//
//   const conn = new HubConnectionBuilder()
//     .withUrl('https://example.com/hub')
//     .withDispatcher(agentWithCookies)
//     .build();
//
export { CookieJar }             from 'tough-cookie';
export { CookieAgent, cookie }   from '@exhumer/undici-cookie-agent';
