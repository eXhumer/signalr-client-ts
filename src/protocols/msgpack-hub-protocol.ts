/**
 * msgpack-hub-protocol.ts
 *
 * Implements the SignalR Hub Protocol over MessagePack (version 1).
 *
 * Wire format
 * ───────────
 * • Handshake: JSON text terminated by ASCII 0x1e - identical to the JSON
 *   protocol, only the "protocol" field changes to "messagepack".
 * • All subsequent messages: binary MessagePack, each frame prefixed by a
 *   base-128 VarInt carrying the byte-length of the following MessagePack
 *   payload.  A single transport payload may contain multiple such frames.
 *
 * MessagePack array layout (per message type)
 * ────────────────────────────────────────────
 *   Invocation       [1, headers, invocationId|null, target, args, streamIds]
 *   StreamItem       [2, headers, invocationId, item]
 *   Completion       [3, headers, invocationId, resultKind, value?]
 *     resultKind: 1 = error string, 2 = void, 3 = non-void result value
 *   StreamInvocation [4, headers, invocationId, target, args, streamIds]
 *   CancelInvocation [5, headers, invocationId]
 *   Ping             [6]
 *   Close            [7, error|null, allowReconnect]
 *
 * Reference:
 *   https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md
 */

import { encode, decode }          from '@msgpack/msgpack';
import { MessageType, TransferFormat, RECORD_SEPARATOR } from '../constants.js';
import type { IHubProtocol, ILogger } from '../interfaces.js';
import {
  type HubMessage,
  type InvocationMessage,
  type StreamInvocationMessage,
  type CancelInvocationMessage,
  type CompletionMessage,
  type StreamItemMessage,
  type PingMessage,
  type CloseMessage,
  type InvocationId,
  toInvocationId,
} from '../messages.js';

// ─── Handshake constants ──────────────────────────────────────────────────────

/**
 * The JSON-formatted handshake request the client sends once the transport
 * is open.  The server always responds with JSON ({} on success), regardless
 * of the chosen hub protocol.
 */
export const HANDSHAKE_REQUEST =
  JSON.stringify({ protocol: 'messagepack', version: 1 }) + RECORD_SEPARATOR;

// The handshake *response* is always JSON - re-export the same parser that
// JsonHubProtocol uses.
export {
  parseHandshakeResponse,
  type HandshakeResponse,
  type HandshakeParseResult,
} from './json-hub-protocol.js';

// ─── Completion result-kind constants ─────────────────────────────────────────

const ResultKind = Object.freeze({
  Error:   1,
  Void:    2,
  NonVoid: 3,
} as const);

type ResultKindValue = (typeof ResultKind)[keyof typeof ResultKind];

// ─── VarInt framing (base-128 variable-length integer) ───────────────────────

/** Write `n` as a base-128 VarInt (little-endian groups of 7 bits). */
function writeVarInt(n: number): Uint8Array {
  const bytes: number[] = [];
  do {
    let b = n & 0x7f;
    n >>>= 7;
    if (n > 0) b |= 0x80;
    bytes.push(b);
  } while (n > 0);
  return new Uint8Array(bytes);
}

/** Read a VarInt from `data` starting at `offset`.  Returns value + bytes consumed. */
function readVarInt(data: Uint8Array, offset: number): { value: number; bytesRead: number } {
  let value = 0;
  let shift = 0;
  let bytesRead = 0;
  let hasMore = true;
  while (hasMore) {
    if (offset + bytesRead >= data.length) {
      throw new Error('MessagePack: unexpected end of data reading VarInt frame length.');
    }
    const byte = data[offset + bytesRead]!;
    hasMore = (byte & 0x80) !== 0;
    if (bytesRead === 4 && hasMore) {
      throw new Error('MessagePack: VarInt frame length prefix is longer than 5 bytes.');
    }
    if (bytesRead === 4 && byte > 0x07) {
      throw new Error('MessagePack: VarInt frame length exceeds the 2 GB SignalR limit.');
    }
    value  |= (byte & 0x7f) << shift;
    shift  += 7;
    bytesRead++;
  }
  return { value, bytesRead };
}

// ─── Frame helper: encode a MessagePack array + prepend VarInt length ─────────

function pack(arr: unknown[]): ArrayBuffer {
  const payload   = encode(arr);
  const lenPrefix = writeVarInt(payload.length);
  const result    = new Uint8Array(lenPrefix.length + payload.length);
  result.set(lenPrefix, 0);
  result.set(payload, lenPrefix.length);
  // Return a standalone ArrayBuffer (not a shared typed-array view).
  return result.buffer.slice(0, result.byteLength);
}

// ─── Protocol class ───────────────────────────────────────────────────────────

export class MsgpackHubProtocol implements IHubProtocol {
  readonly name           = 'messagepack' as const;
  readonly version        = 1 as const;
  readonly transferFormat = TransferFormat.Binary;

  // ─── IHubProtocol: parseMessages ─────────────────────────────────────

  parseMessages(input: string | ArrayBuffer, logger: ILogger): HubMessage[] {
    // The binary transport always delivers ArrayBuffer; accept string as a
    // convenience for testing (treat as UTF-8 encoded bytes).
    const raw: ArrayBuffer =
      typeof input === 'string'
        ? new TextEncoder().encode(input).buffer
        : input;

    const data = new Uint8Array(raw);
    if (data.length === 0) return [];

    const messages: HubMessage[] = [];
    let offset = 0;

    while (offset < data.length) {
      const { value: msgLen, bytesRead } = readVarInt(data, offset);
      offset += bytesRead;

      if (offset + msgLen > data.length) {
        throw new Error(
          `MessagePack hub message: declared length ${msgLen} B exceeds ` +
          `remaining data (${data.length - offset} B).`,
        );
      }

      const frame  = data.subarray(offset, offset + msgLen);
      offset      += msgLen;

      const rawArr = decode(frame);
      if (!Array.isArray(rawArr) || rawArr.length === 0) {
        throw new Error(
          'Invalid MessagePack hub message: top-level value must be a non-empty array.',
        );
      }

      const msg = this.#coerce(rawArr, logger);
      if (msg !== null) messages.push(msg);
    }

    return messages;
  }

  // ─── IHubProtocol: writeMessage ───────────────────────────────────────

  writeMessage(message: HubMessage): ArrayBuffer {
    switch (message.type) {

      case MessageType.Invocation:
        return pack([
          MessageType.Invocation,
          message.headers   ?? {},
          message.invocationId ?? null,
          message.target,
          message.arguments,
          message.streamIds  ?? [],
        ]);

      case MessageType.StreamItem:
        return pack([
          MessageType.StreamItem,
          message.headers ?? {},
          message.invocationId,
          message.item,
        ]);

      case MessageType.Completion: {
        const hasError  = message.error  != null;
        const hasResult = Object.prototype.hasOwnProperty.call(message, 'result');
        if (hasError) {
          return pack([
            MessageType.Completion,
            message.headers ?? {},
            message.invocationId,
            ResultKind.Error,
            message.error,
          ]);
        } else if (hasResult) {
          return pack([
            MessageType.Completion,
            message.headers ?? {},
            message.invocationId,
            ResultKind.NonVoid,
            message.result,
          ]);
        } else {
          return pack([
            MessageType.Completion,
            message.headers ?? {},
            message.invocationId,
            ResultKind.Void,
          ]);
        }
      }

      case MessageType.StreamInvocation:
        return pack([
          MessageType.StreamInvocation,
          message.headers   ?? {},
          message.invocationId,
          message.target,
          message.arguments,
          message.streamIds  ?? [],
        ]);

      case MessageType.CancelInvocation:
        return pack([
          MessageType.CancelInvocation,
          message.headers ?? {},
          message.invocationId,
        ]);

      case MessageType.Ping:
        return pack([MessageType.Ping]);

      case MessageType.Close: {
        const arr: unknown[] = [MessageType.Close, message.error ?? null];
        if (message.allowReconnect !== undefined) arr.push(message.allowReconnect);
        return pack(arr);
      }
    }
  }

  // ─── Coercion / narrowing ─────────────────────────────────────────────

  /**
   * Validate and narrow a raw decoded array into a typed `HubMessage`.
   * Returns `null` for unknown types - they are logged as warnings.
   */
  #coerce(arr: unknown[], logger: ILogger): HubMessage | null {
    const type = arr[0];
    if (typeof type !== 'number') {
      throw new Error(
        `Invalid MessagePack hub message: first element (type) must be a number, ` +
        `got ${JSON.stringify(type)}.`,
      );
    }

    switch (type) {
      case MessageType.Invocation:       return this.#coerceInvocation(arr);
      case MessageType.StreamItem:       return this.#coerceStreamItem(arr);
      case MessageType.Completion:       return this.#coerceCompletion(arr);
      case MessageType.StreamInvocation: return this.#coerceStreamInvocation(arr);
      case MessageType.CancelInvocation: return this.#coerceCancelInvocation(arr);
      case MessageType.Ping:             return this.#coercePing();
      case MessageType.Close:            return this.#coerceClose(arr);
      default:
        logger.log(3 /* Warning */, `Ignoring MessagePack message with unknown type ${String(type)}.`);
        return null;
    }
  }

  // ── [1, headers, invocationId|null, target, arguments, streamIds] ─────

  #coerceInvocation(arr: unknown[]): InvocationMessage {
    const target = arr[3];
    const args   = arr[4];
    if (typeof target !== 'string') {
      throw new Error("Invalid Invocation message: index 3 ('target') must be a string.");
    }
    if (!Array.isArray(args)) {
      throw new Error("Invalid Invocation message: index 4 ('arguments') must be an array.");
    }
    const rawId     = arr[2];
    const rawSids   = arr[5];
    const rawHdrs   = arr[1];
    const streamIds = rawSids == null ? [] : parseStreamIds(rawSids);
    return {
      type:      MessageType.Invocation,
      target,
      arguments: args as unknown[],
      ...(rawId != null && rawId !== '' && { invocationId: parseInvocationId(rawId) }),
      ...(streamIds.length > 0 && { streamIds }),
      ...(isNonEmptyObject(rawHdrs) && { headers: parseHeaders(rawHdrs) }),
    };
  }

  // ── [2, headers, invocationId, item] ─────────────────────────────────

  #coerceStreamItem(arr: unknown[]): StreamItemMessage {
    const rawId = arr[2];
    if (rawId == null) {
      throw new Error("Invalid StreamItem message: index 2 ('invocationId') is required.");
    }
    return {
      type:         MessageType.StreamItem,
      invocationId: parseInvocationId(rawId),
      item:         arr[3],
      ...(isNonEmptyObject(arr[1]) && { headers: parseHeaders(arr[1]) }),
    };
  }

  // ── [3, headers, invocationId, resultKind, value?] ───────────────────

  #coerceCompletion(arr: unknown[]): CompletionMessage {
    const rawId = arr[2];
    if (rawId == null) {
      throw new Error("Invalid Completion message: index 2 ('invocationId') is required.");
    }
    const invocationId = parseInvocationId(rawId);
    const resultKind   = arr[3] as ResultKindValue;
    const hdrs         = isNonEmptyObject(arr[1]) ? { headers: parseHeaders(arr[1]) } : {};

    switch (resultKind) {
      case ResultKind.Error:
        return { type: MessageType.Completion, invocationId, error: String(arr[4] ?? ''), ...hdrs };
      case ResultKind.Void:
        return { type: MessageType.Completion, invocationId, ...hdrs };
      case ResultKind.NonVoid:
        return { type: MessageType.Completion, invocationId, result: arr[4], ...hdrs };
      default:
        throw new Error(`Invalid Completion message: unknown resultKind ${String(resultKind)}.`);
    }
  }

  // ── [4, headers, invocationId, target, arguments, streamIds] ─────────

  #coerceStreamInvocation(arr: unknown[]): StreamInvocationMessage {
    const rawId  = arr[2];
    const target = arr[3];
    const args   = arr[4];
    if (rawId == null) {
      throw new Error("Invalid StreamInvocation message: index 2 ('invocationId') is required.");
    }
    if (typeof target !== 'string') {
      throw new Error("Invalid StreamInvocation message: index 3 ('target') must be a string.");
    }
    if (!Array.isArray(args)) {
      throw new Error("Invalid StreamInvocation message: index 4 ('arguments') must be an array.");
    }
    const rawSids = arr[5];
    const streamIds = rawSids == null ? [] : parseStreamIds(rawSids);
    return {
      type:         MessageType.StreamInvocation,
      invocationId: parseInvocationId(rawId),
      target,
      arguments:    args as unknown[],
      ...(streamIds.length > 0 && { streamIds }),
      ...(isNonEmptyObject(arr[1]) && { headers: parseHeaders(arr[1]) }),
    };
  }

  // ── [5, headers, invocationId] ───────────────────────────────────────

  #coerceCancelInvocation(arr: unknown[]): CancelInvocationMessage {
    const rawId = arr[2];
    if (rawId == null) {
      throw new Error("Invalid CancelInvocation message: index 2 ('invocationId') is required.");
    }
    return {
      type:         MessageType.CancelInvocation,
      invocationId: parseInvocationId(rawId),
      ...(isNonEmptyObject(arr[1]) && { headers: parseHeaders(arr[1]) }),
    };
  }

  // ── [6] ──────────────────────────────────────────────────────────────

  #coercePing(): PingMessage { return { type: MessageType.Ping }; }

  // ── [7, error|null, allowReconnect] ──────────────────────────────────

  #coerceClose(arr: unknown[]): CloseMessage {
    if (arr[2] != null && typeof arr[2] !== 'boolean') {
      throw new Error("Invalid Close message: index 2 ('allowReconnect') must be a boolean.");
    }
    return {
      type: MessageType.Close,
      ...(arr[1] != null && { error:          String(arr[1])  }),
      ...(arr[2] != null && { allowReconnect: arr[2] }),
    };
  }

  // ─── Static message factories (mirrors JsonHubProtocol API) ──────────

  static invocation(
    invocationId: InvocationId,
    target:       string,
    args:         readonly unknown[],
    streamIds?:   readonly string[],
  ): InvocationMessage {
    return {
      type:         MessageType.Invocation,
      invocationId,
      target,
      arguments:    args,
      ...(streamIds != null && streamIds.length > 0 ? { streamIds } : {}),
    };
  }

  /** Fire-and-forget (no `invocationId`). */
  static send(
    target:     string,
    args:       readonly unknown[],
    streamIds?: readonly string[],
  ): InvocationMessage {
    return {
      type:      MessageType.Invocation,
      target,
      arguments: args,
      ...(streamIds != null && streamIds.length > 0 ? { streamIds } : {}),
    };
  }

  static streamInvocation(
    invocationId: InvocationId,
    target:       string,
    args:         readonly unknown[],
    streamIds?:   readonly string[],
  ): StreamInvocationMessage {
    return {
      type:         MessageType.StreamInvocation,
      invocationId,
      target,
      arguments:    args,
      ...(streamIds != null && streamIds.length > 0 ? { streamIds } : {}),
    };
  }

  static cancelInvocation(invocationId: InvocationId): CancelInvocationMessage {
    return { type: MessageType.CancelInvocation, invocationId };
  }

  static streamItem(invocationId: InvocationId, item: unknown): StreamItemMessage {
    return { type: MessageType.StreamItem, invocationId, item };
  }

  static completion(
    invocationId: InvocationId,
    result:       unknown,
    error:        string | null,
  ): CompletionMessage {
    return {
      type: MessageType.Completion,
      invocationId,
      ...(error  != null && { error  }),
      ...(result != null && { result }),
    };
  }

  static ping(): PingMessage { return { type: MessageType.Ping }; }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function isNonEmptyObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v) &&
    Object.keys(v as object).length > 0;
}

function parseInvocationId(value: unknown): InvocationId {
  if (typeof value !== 'string') throw new Error('Invalid MessagePack message: invocationId must be a string.');
  return toInvocationId(value);
}

function parseStreamIds(value: unknown): string[] {
  if (!Array.isArray(value) || !value.every((entry) => typeof entry === 'string')) {
    throw new Error('Invalid MessagePack message: streamIds must be an array of strings.');
  }
  return value;
}

function parseHeaders(value: unknown): Record<string, string> {
  if (!isNonEmptyObject(value) || !Object.values(value).every((entry) => typeof entry === 'string')) {
    throw new Error('Invalid MessagePack message: headers must contain string values.');
  }
  return value as Record<string, string>;
}
