/**
 * json-hub-protocol.ts
 *
 * Implements the SignalR Hub Protocol over JSON (version 1).
 *
 * Wire format: every message is JSON followed by ASCII 0x1e (RECORD_SEPARATOR).
 * A single transport payload may contain multiple messages.
 *
 * Reference:
 *   https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md
 */

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

export const HANDSHAKE_REQUEST =
  JSON.stringify({ protocol: 'json', version: 1 }) + RECORD_SEPARATOR;

export interface HandshakeResponse {
  readonly error?: string;
  readonly minorVersion?: number;
}

export interface HandshakeParseResult {
  /** Remainder of the input string after the handshake record (may be non-empty). */
  readonly remainder: string;
}

/**
 * Parse the first record from `data` as the server handshake response.
 * @throws {Error} if the record separator is missing or the server returned an error.
 */
export function parseHandshakeResponse(data: string): HandshakeParseResult {
  const idx = data.indexOf(RECORD_SEPARATOR);
  if (idx === -1) {
    throw new Error('Handshake response is missing the record separator.');
  }

  const raw:      string            = data.slice(0, idx);
  const response: HandshakeResponse = JSON.parse(raw) as HandshakeResponse;

  if (response.error) {
    throw new Error(`Server rejected the handshake with the error: "${response.error}"`);
  }

  return { remainder: data.slice(idx + 1) };
}

// ─── Raw message shape (before validation/narrowing) ─────────────────────────

interface RawMessage {
  type:          unknown;
  invocationId?: unknown;
  target?:       unknown;
  arguments?:    unknown;
  streamIds?:    unknown;
  item?:         unknown;
  result?:       unknown;
  error?:        unknown;
  allowReconnect?: unknown;
  headers?:      unknown;
}

// ─── Protocol class ───────────────────────────────────────────────────────────

export class JsonHubProtocol implements IHubProtocol {
  readonly name           = 'json' as const;
  readonly version        = 1 as const;
  readonly transferFormat = TransferFormat.Text;

  // ─── IHubProtocol: parseMessages ─────────────────────────────────────

  parseMessages(input: string | ArrayBuffer, logger: ILogger): HubMessage[] {
    const text = typeof input === 'string' ? input : new TextDecoder().decode(input);
    if (!text) return [];

    const messages: HubMessage[] = [];
    const parts = text.split(RECORD_SEPARATOR);

    for (const part of parts) {
      const trimmed = part.trim();
      if (!trimmed) continue;

      let raw: RawMessage;
      try {
        raw = JSON.parse(trimmed) as RawMessage;
      } catch (e) {
        throw new Error(`Error parsing message: ${(e as Error).message}. Raw value: "${trimmed}"`);
      }

      const msg = this.#coerce(raw);
      if (msg !== null) {
        messages.push(msg);
      } else {
        logger.log(3 /* Warning */, `Ignoring message with unknown type ${String(raw.type)}.`);
      }
    }

    return messages;
  }

  // ─── IHubProtocol: writeMessage ───────────────────────────────────────

  writeMessage(message: HubMessage): string {
    return JSON.stringify(message) + RECORD_SEPARATOR;
  }

  // ─── Coercion / narrowing ─────────────────────────────────────────────

  /**
   * Validate and narrow a raw parsed object into a typed `HubMessage`.
   * Returns `null` for unknown types (not an error - new protocol versions
   * may add types we don't know about).
   */
  #coerce(raw: RawMessage): HubMessage | null {
    const type = raw.type;
    if (typeof type !== 'number') {
      throw new Error(`Invalid message: 'type' must be a number, got ${JSON.stringify(type)}.`);
    }

    switch (type) {
      case MessageType.Invocation:
        return this.#coerceInvocation(raw);
      case MessageType.StreamItem:
        return this.#coerceStreamItem(raw);
      case MessageType.Completion:
        return this.#coerceCompletion(raw);
      case MessageType.StreamInvocation:
        return this.#coerceStreamInvocation(raw);
      case MessageType.CancelInvocation:
        return this.#coerceCancelInvocation(raw);
      case MessageType.Ping:
        return this.#coercePing();
      case MessageType.Close:
        return this.#coerceClose(raw);
      default:
        return null;
    }
  }

  #coerceInvocation(raw: RawMessage): InvocationMessage {
    if (typeof raw.target !== 'string') {
      throw new Error("Invalid Invocation message: 'target' must be a string.");
    }
    if (!Array.isArray(raw.arguments)) {
      throw new Error("Invalid Invocation message: 'arguments' must be an array.");
    }
    return {
      type:      MessageType.Invocation,
      target:    raw.target,
      arguments: raw.arguments as unknown[],
      ...(raw.invocationId != null && { invocationId: toInvocationId(String(raw.invocationId)) }),
      ...(Array.isArray(raw.streamIds)  && { streamIds: raw.streamIds as string[] }),
      ...(raw.headers      != null      && { headers:   raw.headers as Record<string, string> }),
    };
  }

  #coerceStreamItem(raw: RawMessage): StreamItemMessage {
    if (raw.invocationId == null) {
      throw new Error("Invalid StreamItem message: 'invocationId' is required.");
    }
    return {
      type:         MessageType.StreamItem,
      invocationId: toInvocationId(String(raw.invocationId)),
      item:         raw.item,
      ...(raw.headers != null && { headers: raw.headers as Record<string, string> }),
    };
  }

  #coerceCompletion(raw: RawMessage): CompletionMessage {
    if (raw.invocationId == null) {
      throw new Error("Invalid Completion message: 'invocationId' is required.");
    }
    if (raw.error != null && raw.result != null) {
      throw new Error("Invalid Completion message: 'error' and 'result' are mutually exclusive.");
    }
    return {
      type:         MessageType.Completion,
      invocationId: toInvocationId(String(raw.invocationId)),
      ...(raw.error  != null && { error:  String(raw.error)  }),
      ...(raw.result != null && { result: raw.result         }),
      ...(raw.headers != null && { headers: raw.headers as Record<string, string> }),
    };
  }

  #coerceStreamInvocation(raw: RawMessage): StreamInvocationMessage {
    if (raw.invocationId == null) {
      throw new Error("Invalid StreamInvocation message: 'invocationId' is required.");
    }
    if (typeof raw.target !== 'string') {
      throw new Error("Invalid StreamInvocation message: 'target' must be a string.");
    }
    if (!Array.isArray(raw.arguments)) {
      throw new Error("Invalid StreamInvocation message: 'arguments' must be an array.");
    }
    return {
      type:         MessageType.StreamInvocation,
      invocationId: toInvocationId(String(raw.invocationId)),
      target:       raw.target,
      arguments:    raw.arguments as unknown[],
      ...(Array.isArray(raw.streamIds) && { streamIds: raw.streamIds as string[] }),
      ...(raw.headers != null          && { headers:   raw.headers as Record<string, string> }),
    };
  }

  #coerceCancelInvocation(raw: RawMessage): CancelInvocationMessage {
    if (raw.invocationId == null) {
      throw new Error("Invalid CancelInvocation message: 'invocationId' is required.");
    }
    return {
      type:         MessageType.CancelInvocation,
      invocationId: toInvocationId(String(raw.invocationId)),
      ...(raw.headers != null && { headers: raw.headers as Record<string, string> }),
    };
  }

  #coercePing(): PingMessage {
    return { type: MessageType.Ping };
  }

  #coerceClose(raw: RawMessage): CloseMessage {
    return {
      type:            MessageType.Close,
      ...(raw.error         != null && { error:          String(raw.error)         }),
      ...(raw.allowReconnect != null && { allowReconnect: Boolean(raw.allowReconnect) }),
      ...(raw.headers        != null && { headers:        raw.headers as Record<string, string> }),
    };
  }

  // ─── Static message factories ─────────────────────────────────────────

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

  /**
   * Fire-and-forget (no `invocationId` → server won't send a Completion).
   */
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

  static ping(): PingMessage {
    return { type: MessageType.Ping };
  }
}
