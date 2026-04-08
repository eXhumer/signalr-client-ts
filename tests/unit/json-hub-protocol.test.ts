/**
 * json-hub-protocol.test.ts
 *
 * Comprehensive tests for the SignalR JSON hub protocol:
 *   - Handshake parsing
 *   - Encoding all 7 message types
 *   - Decoding single + multi-message payloads
 *   - Error cases (bad JSON, missing required fields, unknown types)
 *   - Static factory methods
 */

import { describe, it, expect } from 'vitest';

import {
  JsonHubProtocol,
  HANDSHAKE_REQUEST,
  parseHandshakeResponse,
} from '../../src/protocols/json-hub-protocol.js';
import { MessageType, RECORD_SEPARATOR } from '../../src/constants.js';
import { NullLogger } from '../../src/logger.js';
import { toInvocationId } from '../../src/messages.js';
import type { HubMessage } from '../../src/messages.js';

const RS   = RECORD_SEPARATOR;
const log  = NullLogger.instance;
const p    = new JsonHubProtocol();

// ─── Handshake ────────────────────────────────────────────────────────────────

describe('HANDSHAKE_REQUEST', () => {
  it('is a JSON object followed by RS', () => {
    expect(HANDSHAKE_REQUEST.endsWith(RS)).toBeTruthy();
    const parsed = JSON.parse(HANDSHAKE_REQUEST.slice(0, -1)) as Record<string, unknown>;
    expect(parsed['protocol']).toBe('json');
    expect(parsed['version']).toBe(1);
  });
});

describe('parseHandshakeResponse', () => {
  it('accepts an empty-object success response', () => {
    const result = parseHandshakeResponse(`{}${RS}`);
    expect(result.remainder).toBe('');
  });

  it('returns remainder bytes after the RS', () => {
    const extra  = `{"type":6}${RS}`;
    const result = parseHandshakeResponse(`{}${RS}${extra}`);
    expect(result.remainder).toBe(extra);
  });

  it('throws when RS is missing', () => {
    expect(() => parseHandshakeResponse('{}')).toThrow(/record separator/i);
  });

  it('throws when server returns an error', () => {
    expect(
      () => parseHandshakeResponse(`{"error":"unsupported protocol"}${RS}`),
    ).toThrow(/unsupported protocol/);
  });
});

// ─── writeMessage / parseMessages round-trip ──────────────────────────────────

describe('JsonHubProtocol.writeMessage', () => {
  it('terminates every message with RS', () => {
    const msg: HubMessage = { type: MessageType.Ping };
    expect((p.writeMessage(msg) as string).endsWith(RS)).toBeTruthy();
  });

  it('serialises all fields', () => {
    const id  = toInvocationId('42');
    const msg = JsonHubProtocol.invocation(id, 'SendMessage', ['Alice', 'Hi']);
    const wire = JSON.parse((p.writeMessage(msg) as string).slice(0, -1)) as Record<string, unknown>;
    expect(wire['type']).toBe(1);
    expect(wire['invocationId']).toBe('42');
    expect(wire['target']).toBe('SendMessage');
    expect(wire['arguments']).toEqual(['Alice', 'Hi']);
  });
});

describe('JsonHubProtocol.parseMessages', () => {
  function roundtrip(msg: HubMessage): HubMessage {
    const wire = p.writeMessage(msg) as string;
    const msgs = p.parseMessages(wire, log);
    expect(msgs.length).toBe(1);
    return msgs[0]!;
  }

  it('parses Ping (type 6)', () => {
    const out = roundtrip({ type: MessageType.Ping });
    expect(out.type).toBe(MessageType.Ping);
  });

  it('parses Close (type 7) without error', () => {
    const out = roundtrip({ type: MessageType.Close });
    expect(out.type).toBe(MessageType.Close);
  });

  it('parses Close (type 7) with error', () => {
    const msg: HubMessage = { type: MessageType.Close, error: 'server crashed' };
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Close);
    expect('error' in out && out.error === 'server crashed').toBeTruthy();
  });

  it('parses Invocation (type 1) with invocationId', () => {
    const id  = toInvocationId('1');
    const msg = JsonHubProtocol.invocation(id, 'Greet', ['World']);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Invocation);
    expect('target' in out && out.target === 'Greet').toBeTruthy();
    expect('arguments' in out).toBeTruthy();
    expect([...(out as unknown as { arguments: unknown[] }).arguments]).toEqual(['World']);
  });

  it('parses fire-and-forget Invocation (no invocationId)', () => {
    const msg = JsonHubProtocol.send('Broadcast', ['hello']);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Invocation);
    expect(!('invocationId' in out) || (out as { invocationId?: unknown }).invocationId === undefined).toBeTruthy();
  });

  it('parses StreamItem (type 2)', () => {
    const id  = toInvocationId('5');
    const msg = JsonHubProtocol.streamItem(id, { x: 1 });
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.StreamItem);
    expect('item' in out).toBeTruthy();
    expect((out as { item: unknown }).item).toEqual({ x: 1 });
  });

  it('parses Completion with result (type 3)', () => {
    const id  = toInvocationId('3');
    const msg = JsonHubProtocol.completion(id, 'done', null);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Completion);
    expect('result' in out && (out as { result: unknown }).result === 'done').toBeTruthy();
  });

  it('parses Completion with error (type 3)', () => {
    const id  = toInvocationId('3');
    const msg = JsonHubProtocol.completion(id, null, 'oops');
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Completion);
    expect('error' in out && (out as { error: unknown }).error === 'oops').toBeTruthy();
  });

  it('parses StreamInvocation (type 4)', () => {
    const id  = toInvocationId('7');
    const msg = JsonHubProtocol.streamInvocation(id, 'GetStream', [10]);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.StreamInvocation);
    expect('target' in out && (out as { target: string }).target === 'GetStream').toBeTruthy();
  });

  it('parses CancelInvocation (type 5)', () => {
    const id  = toInvocationId('9');
    const msg = JsonHubProtocol.cancelInvocation(id);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.CancelInvocation);
    expect('invocationId' in out).toBeTruthy();
  });

  it('parses multiple messages in a single chunk', () => {
    const wire =
      p.writeMessage({ type: MessageType.Ping }) as string +
      p.writeMessage(JsonHubProtocol.completion(toInvocationId('1'), 42, null)) as string;
    const msgs = p.parseMessages(wire, log);
    expect(msgs.length).toBe(2);
    expect(msgs[0]!.type).toBe(MessageType.Ping);
    expect(msgs[1]!.type).toBe(MessageType.Completion);
  });

  it('ignores empty parts between RS delimiters', () => {
    const wire = `${RS}${RS}{"type":6}${RS}${RS}`;
    const msgs = p.parseMessages(wire, log);
    expect(msgs.length).toBe(1);
  });

  it('returns [] for empty input', () => {
    expect(p.parseMessages('', log)).toEqual([]);
  });

  it('returns [] for ArrayBuffer input (empty)', () => {
    expect(p.parseMessages(new ArrayBuffer(0), log)).toEqual([]);
  });

  it('silently skips unknown message types (returns [] entry via logger)', () => {
    // Unknown type 99 - should be ignored, not throw
    const wire = `{"type":99}${RS}`;
    const msgs = p.parseMessages(wire, log);
    expect(msgs.length).toBe(0);
  });

  it('throws on malformed JSON', () => {
    expect(() => p.parseMessages(`{bad json}${RS}`, log)).toThrow(/parsing/i);
  });

  it('throws when Invocation has no target', () => {
    expect(
      () => p.parseMessages(`{"type":1,"arguments":[]}${RS}`, log),
    ).toThrow(/target/i);
  });

  it('throws when Invocation has no arguments array', () => {
    expect(
      () => p.parseMessages(`{"type":1,"target":"X"}${RS}`, log),
    ).toThrow(/arguments/i);
  });

  it('throws when Completion has both error and result', () => {
    const wire = `{"type":3,"invocationId":"1","error":"oops","result":42}${RS}`;
    expect(() => p.parseMessages(wire, log)).toThrow(/mutually exclusive/i);
  });

  it('throws when message type is not a number', () => {
    expect(
      () => p.parseMessages(`{"type":"bad"}${RS}`, log),
    ).toThrow(/number/i);
  });

  it('parses Invocation with streamIds', () => {
    const id  = toInvocationId('2');
    const msg = JsonHubProtocol.invocation(id, 'Upload', [], ['s1', 's2']);
    const out = roundtrip(msg);
    expect('streamIds' in out).toBeTruthy();
    expect([...(out as unknown as { streamIds: string[] }).streamIds]).toEqual(['s1', 's2']);
  });
});

describe('JsonHubProtocol metadata', () => {
  it('name is "json"', () => {
    expect(p.name).toBe('json');
  });

  it('version is 1', () => {
    expect(p.version).toBe(1);
  });

  it('transferFormat is Text (1)', () => {
    expect(p.transferFormat).toBe(1);
  });
});

// ─── Protocol compliance: required-field validation ───────────────────────────
// Verifies that the parser throws a descriptive error for every message type
// whose spec-required fields are absent, rather than silently producing a
// malformed typed message.

describe('JsonHubProtocol.parseMessages - required-field validation (protocol compliance)', () => {
  // ── StreamItem (type 2) ────────────────────────────────────────────────────

  it('throws when StreamItem is missing invocationId', () => {
    // invocationId is required on StreamItem per the Hub Protocol spec.
    expect(
      () => p.parseMessages(`{"type":2}${RS}`, log),
    ).toThrow(/invocationId/i);
  });

  // ── Completion (type 3) ───────────────────────────────────────────────────

  it('throws when Completion is missing invocationId', () => {
    // invocationId is required on Completion per the Hub Protocol spec.
    expect(
      () => p.parseMessages(`{"type":3}${RS}`, log),
    ).toThrow(/invocationId/i);
  });

  // ── StreamInvocation (type 4) ────────────────────────────────────────────

  it('throws when StreamInvocation is missing invocationId', () => {
    expect(
      () => p.parseMessages(`{"type":4,"target":"Foo","arguments":[]}${RS}`, log),
    ).toThrow(/invocationId/i);
  });

  it('throws when StreamInvocation is missing target', () => {
    expect(
      () => p.parseMessages(`{"type":4,"invocationId":"1","arguments":[]}${RS}`, log),
    ).toThrow(/target/i);
  });

  it('throws when StreamInvocation is missing arguments array', () => {
    expect(
      () => p.parseMessages(`{"type":4,"invocationId":"1","target":"Foo"}${RS}`, log),
    ).toThrow(/arguments/i);
  });

  // ── CancelInvocation (type 5) ────────────────────────────────────────────

  it('throws when CancelInvocation is missing invocationId', () => {
    expect(
      () => p.parseMessages(`{"type":5}${RS}`, log),
    ).toThrow(/invocationId/i);
  });

  // ── ArrayBuffer input ─────────────────────────────────────────────────────

  it('decodes a non-empty ArrayBuffer payload correctly', () => {
    // The protocol spec requires the parser to accept both string and binary
    // (ArrayBuffer) input; text transport uses string, binary uses ArrayBuffer.
    const wire   = `{"type":6}${RS}`; // a Ping message
    const encoded = new TextEncoder().encode(wire);
    const buf     = encoded.buffer.slice(
      encoded.byteOffset,
      encoded.byteOffset + encoded.byteLength,
    ) as ArrayBuffer;
    const msgs = p.parseMessages(buf, log);
    expect(msgs.length).toBe(1);
    expect(msgs[0]!.type).toBe(MessageType.Ping);
  });

  // ── Close.allowReconnect field ───────────────────────────────────────────

  it('preserves allowReconnect:true on Close message', () => {
    // Per spec, the server can set allowReconnect to signal the client it may
    // reconnect.  The parser must preserve this field for hub-connection to
    // act on it.
    const wire = `{"type":7,"allowReconnect":true}${RS}`;
    const msgs = p.parseMessages(wire, log);
    expect(msgs.length).toBe(1);
    const msg = msgs[0]!;
    expect(msg.type).toBe(MessageType.Close);
    expect((msg as { allowReconnect?: boolean }).allowReconnect).toBe(true);
  });

  it('preserves allowReconnect:false on Close message', () => {
    const wire = `{"type":7,"allowReconnect":false}${RS}`;
    const msgs = p.parseMessages(wire, log);
    expect(msgs.length).toBe(1);
    const msg = msgs[0]!;
    expect(msg.type).toBe(MessageType.Close);
    expect((msg as { allowReconnect?: boolean }).allowReconnect).toBe(false);
  });

  it('allowReconnect is absent when not sent by server', () => {
    // When the server omits allowReconnect, the client must NOT reconnect.
    const wire = `{"type":7}${RS}`;
    const msgs = p.parseMessages(wire, log);
    expect(msgs.length).toBe(1);
    const msg = msgs[0]!;
    expect(msg.type).toBe(MessageType.Close);
    expect(!('allowReconnect' in msg) || (msg as { allowReconnect?: boolean }).allowReconnect === undefined).toBeTruthy();
  });
});
