/**
 * messages.ts
 *
 * Full discriminated-union type definitions for every SignalR hub protocol
 * message.  The `type` field on each interface carries a numeric literal type
 * derived from the `MessageType` constants so TypeScript narrows correctly
 * inside `switch` / `if` statements.
 *
 * Reference:
 *   https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md
 */

import type { MessageType } from './constants.js';

// ─── Branding ─────────────────────────────────────────────────────────────────

declare const __invocationIdBrand: unique symbol;

/**
 * Opaque wrapper around `string` for invocation IDs.
 * Prevents mixing arbitrary strings with IDs at the type level.
 */
export type InvocationId = string & { readonly [__invocationIdBrand]: void };

/** Cast a plain string to an InvocationId (only used inside the library). */
export function toInvocationId(s: string): InvocationId {
  return s as InvocationId;
}

// ─── Common metadata ──────────────────────────────────────────────────────────

/** Optional headers that may appear on any hub message. */
export type MessageHeaders = Readonly<Record<string, string>>;

interface BaseMessage {
  readonly headers?: MessageHeaders;
}

// ─── Individual message shapes ────────────────────────────────────────────────

type MT = typeof MessageType;

/** Type 1 - Server→Client or Client→Server method call. */
export interface InvocationMessage extends BaseMessage {
  readonly type:         MT['Invocation'];
  /** Absent on fire-and-forget invocations. */
  readonly invocationId?: InvocationId;
  readonly target:       string;
  readonly arguments:    readonly unknown[];
  readonly streamIds?:   readonly string[];
}

/** Type 2 - One item from a server-streaming method. */
export interface StreamItemMessage extends BaseMessage {
  readonly type:         MT['StreamItem'];
  readonly invocationId: InvocationId;
  readonly item:         unknown;
}

/** Type 3 - Terminal result or error for an invocation / stream. */
export interface CompletionMessage extends BaseMessage {
  readonly type:         MT['Completion'];
  readonly invocationId: InvocationId;
  readonly error?:       string;
  readonly result?:      unknown;
}

/** Type 4 - Client initiates a server-streaming call. */
export interface StreamInvocationMessage extends BaseMessage {
  readonly type:         MT['StreamInvocation'];
  readonly invocationId: InvocationId;
  readonly target:       string;
  readonly arguments:    readonly unknown[];
  readonly streamIds?:   readonly string[];
}

/** Type 5 - Client cancels an outstanding server stream. */
export interface CancelInvocationMessage extends BaseMessage {
  readonly type:         MT['CancelInvocation'];
  readonly invocationId: InvocationId;
}

/** Type 6 - Keep-alive ping; expects no response from the other side. */
export interface PingMessage extends BaseMessage {
  readonly type: MT['Ping'];
}

/** Type 7 - Graceful close notification from the server. */
export interface CloseMessage extends BaseMessage {
  readonly type:             MT['Close'];
  readonly error?:           string;
  readonly allowReconnect?:  boolean;
}

// ─── Union ────────────────────────────────────────────────────────────────────

/** Exhaustive discriminated union of all SignalR hub protocol messages. */
export type HubMessage =
  | InvocationMessage
  | StreamItemMessage
  | CompletionMessage
  | StreamInvocationMessage
  | CancelInvocationMessage
  | PingMessage
  | CloseMessage;

// ─── Utility: exhaustiveness helper ──────────────────────────────────────────

/**
 * Asserts a code path is unreachable at compile time.
 * Use at the `default` branch of a `switch` over `HubMessage['type']`.
 */
export function assertNever(_x: never, label?: string): never {
  throw new Error(`Unexpected value${label ? ` for ${label}` : ''}: ${JSON.stringify(_x)}`);
}

// ─── Type guards ─────────────────────────────────────────────────────────────

export function isInvocationMessage(msg: HubMessage): msg is InvocationMessage {
  return msg.type === 1;
}

export function isStreamItemMessage(msg: HubMessage): msg is StreamItemMessage {
  return msg.type === 2;
}

export function isCompletionMessage(msg: HubMessage): msg is CompletionMessage {
  return msg.type === 3;
}

export function isStreamInvocationMessage(msg: HubMessage): msg is StreamInvocationMessage {
  return msg.type === 4;
}

export function isCancelInvocationMessage(msg: HubMessage): msg is CancelInvocationMessage {
  return msg.type === 5;
}

export function isPingMessage(msg: HubMessage): msg is PingMessage {
  return msg.type === 6;
}

export function isCloseMessage(msg: HubMessage): msg is CloseMessage {
  return msg.type === 7;
}
