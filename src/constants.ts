/**
 * constants.ts
 *
 * All SignalR protocol constants, enumerations, and timing defaults.
 * Uses `as const satisfies` (TS 4.9+) so values carry their narrow literal
 * types while being verified against a constraint at compile time.
 */

// ─── MessageType ──────────────────────────────────────────────────────────────
// Each value doubles as both a runtime constant and a narrow numeric literal
// type (used in the discriminated-union HubMessage in messages.ts).

export const MessageType = Object.freeze({
  Invocation:       1,
  StreamItem:       2,
  Completion:       3,
  StreamInvocation: 4,
  CancelInvocation: 5,
  Ping:             6,
  Close:            7,
} as const satisfies Record<string, number>);

/** Union of all valid message type numbers. */
export type MessageTypeValue = (typeof MessageType)[keyof typeof MessageType];

// ─── HubConnectionState ───────────────────────────────────────────────────────

export const HubConnectionState = Object.freeze({
  Disconnected:  'Disconnected',
  Connecting:    'Connecting',
  Connected:     'Connected',
  Disconnecting: 'Disconnecting',
  Reconnecting:  'Reconnecting',
} as const satisfies Record<string, string>);

/** Union of all valid connection state strings. */
export type HubConnectionState = (typeof HubConnectionState)[keyof typeof HubConnectionState];

// ─── LogLevel ─────────────────────────────────────────────────────────────────

export const LogLevel = {
  Trace:       0,
  Debug:       1,
  Information: 2,
  Warning:     3,
  Error:       4,
  Critical:    5,
  None:        6,
} as const satisfies Record<string, number>;

export type LogLevel = (typeof LogLevel)[keyof typeof LogLevel];

// ─── HttpTransportType ────────────────────────────────────────────────────────

export const HttpTransportType = {
  None:             0,
  WebSockets:       1,
  ServerSentEvents: 2,
  LongPolling:      4,
} as const satisfies Record<string, number>;

/** Bitmask of enabled transports - values can be OR-ed together. */
export type HttpTransportType = (typeof HttpTransportType)[keyof typeof HttpTransportType];

// ─── TransferFormat ───────────────────────────────────────────────────────────

export const TransferFormat = {
  Text:   1,
  Binary: 2,
} as const satisfies Record<string, number>;

export type TransferFormat = (typeof TransferFormat)[keyof typeof TransferFormat];

// ─── Protocol constants ───────────────────────────────────────────────────────

/** ASCII 30 - the byte that terminates every SignalR JSON message. */
export const RECORD_SEPARATOR = '\x1e' as const;

/** Negotiate version we advertise to the server. */
export const NEGOTIATE_VERSION = 1 as const;

// ─── Timing defaults ──────────────────────────────────────────────────────────

export const DEFAULT_TIMEOUT_IN_MS        = 30_000 as const;
export const DEFAULT_PING_INTERVAL_IN_MS  = 15_000 as const;
export const DEFAULT_SERVER_TIMEOUT_IN_MS = 30_000 as const;
