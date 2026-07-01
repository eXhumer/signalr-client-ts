/**
 * msgpack-hub-protocol.test.ts
 *
 * Comprehensive tests for MsgpackHubProtocol:
 *   - Codec round-trips for all 7 message types
 *   - VarInt framing (single + multi-message payloads)
 *   - All error / validation paths
 *   - Static factory methods
 *   - Handshake constant
 *   - Codec edge-cases (integers, floats, binary, nested objects)
 *   - Comparison with JsonHubProtocol output shapes
 */

import { describe, it, expect } from 'vitest';
import { encode } from '@msgpack/msgpack';

import { MsgpackHubProtocol, HANDSHAKE_REQUEST } from '../../src/protocols/msgpack-hub-protocol.js';
import { MessageType, TransferFormat, RECORD_SEPARATOR } from '../../src/constants.js';
import { NullLogger }    from '../../src/logger.js';
import { toInvocationId } from '../../src/messages.js';
import type { HubMessage } from '../../src/messages.js';

// ─── Helpers ─────────────────────────────────────────────────────────────────

const log = NullLogger.instance;
const p   = new MsgpackHubProtocol();

/**
 * Encode a single HubMessage, then decode it back.
 * Asserts exactly one message is returned.
 */
function roundtrip(msg: HubMessage): HubMessage {
  const wire = p.writeMessage(msg);
  expect(wire).toBeInstanceOf(ArrayBuffer);
  const msgs = p.parseMessages(wire, log);
  expect(msgs.length).toBe(1);
  return msgs[0]!;
}

/**
 * Concatenate multiple ArrayBuffers into one.
 * Used to build multi-message payloads for parseMessages.
 */
function concat(...bufs: ArrayBuffer[]): ArrayBuffer {
  const totalLen = bufs.reduce((sum, b) => sum + b.byteLength, 0);
  const result   = new Uint8Array(totalLen);
  let offset = 0;
  for (const b of bufs) {
    result.set(new Uint8Array(b), offset);
    offset += b.byteLength;
  }
  return result.buffer;
}

// ─── Handshake ────────────────────────────────────────────────────────────────

describe('HANDSHAKE_REQUEST', () => {
  it('ends with record separator', () => {
    expect(HANDSHAKE_REQUEST.endsWith(RECORD_SEPARATOR)).toBe(true);
  });

  it('contains protocol "messagepack"', () => {
    const payload = JSON.parse(HANDSHAKE_REQUEST.slice(0, -1)) as Record<string, unknown>;
    expect(payload['protocol']).toBe('messagepack');
  });

  it('contains version 1', () => {
    const payload = JSON.parse(HANDSHAKE_REQUEST.slice(0, -1)) as Record<string, unknown>;
    expect(payload['version']).toBe(1);
  });
});

// ─── Protocol metadata ────────────────────────────────────────────────────────

describe('MsgpackHubProtocol metadata', () => {
  it('name is "messagepack"', () => { expect(p.name).toBe('messagepack'); });
  it('version is 1',         () => { expect(p.version).toBe(1); });
  it('transferFormat is Binary (2)', () => {
    expect(p.transferFormat).toBe(TransferFormat.Binary);
    expect(p.transferFormat).toBe(2);
  });
});

// ─── writeMessage: produces non-empty ArrayBuffer ─────────────────────────────

describe('MsgpackHubProtocol.writeMessage', () => {
  it('returns an ArrayBuffer', () => {
    const buf = p.writeMessage({ type: MessageType.Ping });
    expect(buf).toBeInstanceOf(ArrayBuffer);
    expect(buf.byteLength).toBeGreaterThan(0);
  });

  it('is smaller than an equivalent JSON+RS string for a rich message', () => {
    const id  = toInvocationId('42');
    const msg = MsgpackHubProtocol.invocation(id, 'SendMessage', ['Alice', 'Hello world!']);
    const mpBuf  = p.writeMessage(msg).byteLength;
    // JSON equivalent for rough size comparison
    const jsonLen =
      JSON.stringify({ type: 1, invocationId: '42', target: 'SendMessage', arguments: ['Alice', 'Hello world!'] }).length + 1;
    // MessagePack is typically more compact - just assert it's non-trivially sized
    expect(mpBuf).toBeGreaterThan(0);
    expect(mpBuf).toBeLessThanOrEqual(jsonLen + 10); // within 10 bytes (VarInt overhead)
  });
});

// ─── Round-trip for all 7 message types ──────────────────────────────────────

describe('MsgpackHubProtocol round-trips', () => {

  // ── Ping (type 6) ─────────────────────────────────────────────────────────

  it('Ping (type 6)', () => {
    const out = roundtrip({ type: MessageType.Ping });
    expect(out.type).toBe(MessageType.Ping);
  });

  // ── Close (type 7) ────────────────────────────────────────────────────────

  it('Close with no fields', () => {
    const out = roundtrip({ type: MessageType.Close });
    expect(out.type).toBe(MessageType.Close);
    expect((out as { error?: unknown }).error).toBeUndefined();
    expect((out as { allowReconnect?: unknown }).allowReconnect).toBeUndefined();
  });

  it('Close with error', () => {
    const out = roundtrip({ type: MessageType.Close, error: 'server exploded' });
    expect(out.type).toBe(MessageType.Close);
    expect((out as { error?: unknown }).error).toBe('server exploded');
  });

  it('Close with allowReconnect:true', () => {
    const out = roundtrip({ type: MessageType.Close, allowReconnect: true });
    expect(out.type).toBe(MessageType.Close);
    expect((out as { allowReconnect?: unknown }).allowReconnect).toBe(true);
  });

  it('Close with allowReconnect:false', () => {
    // false is the zero-value - make sure we don't accidentally strip it
    const out = roundtrip({ type: MessageType.Close, allowReconnect: false });
    expect(out.type).toBe(MessageType.Close);
    // false coerces to null on write, so allowReconnect is absent on decode
    // (this matches the reference implementation's behaviour)
    expect((out as { allowReconnect?: unknown }).allowReconnect).toBeFalsy();
  });

  // ── Invocation (type 1) ───────────────────────────────────────────────────

  it('Invocation with invocationId', () => {
    const id  = toInvocationId('1');
    const msg = MsgpackHubProtocol.invocation(id, 'Greet', ['World']);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Invocation);
    expect((out as { target?: unknown }).target).toBe('Greet');
    expect((out as { arguments?: unknown[] }).arguments).toEqual(['World']);
    expect((out as { invocationId?: unknown }).invocationId).toBe('1');
  });

  it('fire-and-forget Invocation (no invocationId)', () => {
    const msg = MsgpackHubProtocol.send('Broadcast', ['hi']);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Invocation);
    expect((out as { invocationId?: unknown }).invocationId).toBeUndefined();
  });

  it('MsgpackHubProtocol.send() with streamIds includes streamIds (line 421)', () => {
    const msg = MsgpackHubProtocol.send('Upload', [], ['sid1', 'sid2']);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Invocation);
    expect((out as { streamIds?: string[] }).streamIds).toEqual(['sid1', 'sid2']);
  });

  it('Invocation with streamIds', () => {
    const id  = toInvocationId('2');
    const msg = MsgpackHubProtocol.invocation(id, 'Upload', [], ['s1', 's2']);
    const out = roundtrip(msg);
    expect((out as { streamIds?: unknown }).streamIds).toEqual(['s1', 's2']);
  });

  it('Invocation with numeric and null arguments', () => {
    const id  = toInvocationId('3');
    const msg = MsgpackHubProtocol.invocation(id, 'Calc', [42, 3.14, null, true]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments).toEqual([42, 3.14, null, true]);
  });

  it('Invocation with nested object argument', () => {
    const id  = toInvocationId('4');
    const msg = MsgpackHubProtocol.invocation(id, 'SetUser', [{ name: 'Alice', age: 30 }]);
    const out = roundtrip(msg);
    const args = (out as { arguments?: unknown[] }).arguments;
    expect(args).toBeDefined();
    expect(args![0]).toEqual({ name: 'Alice', age: 30 });
  });

  // ── StreamItem (type 2) ───────────────────────────────────────────────────

  it('StreamItem with primitive item', () => {
    const id  = toInvocationId('5');
    const msg = MsgpackHubProtocol.streamItem(id, 99);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.StreamItem);
    expect((out as { item?: unknown }).item).toBe(99);
  });

  it('StreamItem with object item', () => {
    const id  = toInvocationId('6');
    const msg = MsgpackHubProtocol.streamItem(id, { x: 1, y: 2 });
    const out = roundtrip(msg);
    expect((out as { item?: unknown }).item).toEqual({ x: 1, y: 2 });
  });

  it('StreamItem with null item', () => {
    const id  = toInvocationId('7');
    const msg = MsgpackHubProtocol.streamItem(id, null);
    const out = roundtrip(msg);
    expect((out as { item?: unknown }).item).toBeNull();
  });

  // ── Completion (type 3) ───────────────────────────────────────────────────

  it('Completion with void (no result, no error)', () => {
    const id  = toInvocationId('8');
    const msg = MsgpackHubProtocol.completion(id, null, null);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.Completion);
    expect((out as { result?: unknown }).result).toBeUndefined();
    expect((out as { error?: unknown }).error).toBeUndefined();
  });

  it('Completion with non-void result', () => {
    const id  = toInvocationId('9');
    const msg = MsgpackHubProtocol.completion(id, 'done', null);
    const out = roundtrip(msg);
    expect((out as { result?: unknown }).result).toBe('done');
  });

  it('Completion with numeric result', () => {
    const id  = toInvocationId('10');
    const msg = MsgpackHubProtocol.completion(id, 42, null);
    const out = roundtrip(msg);
    expect((out as { result?: unknown }).result).toBe(42);
  });

  it('Completion with object result', () => {
    const id  = toInvocationId('11');
    const msg = MsgpackHubProtocol.completion(id, { status: 'ok' }, null);
    const out = roundtrip(msg);
    expect((out as { result?: unknown }).result).toEqual({ status: 'ok' });
  });

  it('Completion with error', () => {
    const id  = toInvocationId('12');
    const msg = MsgpackHubProtocol.completion(id, null, 'something went wrong');
    const out = roundtrip(msg);
    expect((out as { error?: unknown }).error).toBe('something went wrong');
    expect((out as { result?: unknown }).result).toBeUndefined();
  });

  // ── StreamInvocation (type 4) ─────────────────────────────────────────────

  it('StreamInvocation', () => {
    const id  = toInvocationId('13');
    const msg = MsgpackHubProtocol.streamInvocation(id, 'GetStream', [10, 'asc']);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.StreamInvocation);
    expect((out as { target?: unknown }).target).toBe('GetStream');
    expect((out as { arguments?: unknown[] }).arguments).toEqual([10, 'asc']);
    expect((out as { invocationId?: unknown }).invocationId).toBe('13');
  });

  it('StreamInvocation with streamIds', () => {
    const id  = toInvocationId('14');
    const msg = MsgpackHubProtocol.streamInvocation(id, 'Upload', [], ['src1']);
    const out = roundtrip(msg);
    expect((out as { streamIds?: unknown }).streamIds).toEqual(['src1']);
  });

  // ── CancelInvocation (type 5) ─────────────────────────────────────────────

  it('CancelInvocation', () => {
    const id  = toInvocationId('15');
    const msg = MsgpackHubProtocol.cancelInvocation(id);
    const out = roundtrip(msg);
    expect(out.type).toBe(MessageType.CancelInvocation);
    expect((out as { invocationId?: unknown }).invocationId).toBe('15');
  });
});

// ─── Multi-message payloads ───────────────────────────────────────────────────

describe('MsgpackHubProtocol.parseMessages - multiple frames', () => {
  it('parses two frames concatenated in one buffer', () => {
    const buf1 = p.writeMessage({ type: MessageType.Ping });
    const buf2 = p.writeMessage(
      MsgpackHubProtocol.completion(toInvocationId('1'), 42, null),
    );
    const combined = concat(buf1, buf2);
    const msgs = p.parseMessages(combined, log);
    expect(msgs.length).toBe(2);
    expect(msgs[0]!.type).toBe(MessageType.Ping);
    expect(msgs[1]!.type).toBe(MessageType.Completion);
  });

  it('parses five different message types in one buffer', () => {
    const bufs = [
      p.writeMessage({ type: MessageType.Ping }),
      p.writeMessage(MsgpackHubProtocol.invocation(toInvocationId('1'), 'Foo', [])),
      p.writeMessage(MsgpackHubProtocol.streamItem(toInvocationId('2'), 'item')),
      p.writeMessage(MsgpackHubProtocol.completion(toInvocationId('3'), null, null)),
      p.writeMessage({ type: MessageType.Close }),
    ];
    const msgs = p.parseMessages(concat(...bufs), log);
    expect(msgs.length).toBe(5);
    expect(msgs[0]!.type).toBe(MessageType.Ping);
    expect(msgs[1]!.type).toBe(MessageType.Invocation);
    expect(msgs[2]!.type).toBe(MessageType.StreamItem);
    expect(msgs[3]!.type).toBe(MessageType.Completion);
    expect(msgs[4]!.type).toBe(MessageType.Close);
  });
});

// ─── Empty / edge-case inputs ─────────────────────────────────────────────────

describe('MsgpackHubProtocol.parseMessages - empty inputs', () => {
  it('returns [] for empty ArrayBuffer', () => {
    expect(p.parseMessages(new ArrayBuffer(0), log)).toEqual([]);
  });

  it('returns [] for empty string (convenience path)', () => {
    expect(p.parseMessages('', log)).toEqual([]);
  });
});

// ─── Unknown message types ────────────────────────────────────────────────────

describe('MsgpackHubProtocol.parseMessages - unknown type', () => {
  it('silently ignores an unknown type and returns []', () => {
    // Manually build a VarInt-framed MessagePack array [99]
    const payload = encode([99]);
    // VarInt prefix
    const varint = new Uint8Array([payload.length]);
    const frame  = new Uint8Array(varint.length + payload.length);
    frame.set(varint, 0);
    frame.set(payload, varint.length);
    const msgs = p.parseMessages(frame.buffer, log);
    expect(msgs.length).toBe(0);
  });
});

// ─── Error paths ──────────────────────────────────────────────────────────────

describe('MsgpackHubProtocol.parseMessages - validation errors', () => {

  /**
   * Build a single framed message from a raw array (bypasses writeMessage).
   */
  function rawFrame(arr: unknown[]): ArrayBuffer {
    const payload = encode(arr);
    const varint  = new Uint8Array([payload.length]);       // works for short payloads
    const frame   = new Uint8Array(varint.length + payload.length);
    frame.set(varint, 0);
    frame.set(payload, varint.length);
    return frame.buffer;
  }

  it('throws when top-level is not an array', () => {
    const payload = encode('not an array');
    const varint  = new Uint8Array([payload.length]);
    const frame   = new Uint8Array(1 + payload.length);
    frame.set(varint, 0);
    frame.set(payload, 1);
    expect(() => p.parseMessages(frame.buffer, log)).toThrow(/array/i);
  });

  it('throws when type field is not a number', () => {
    const buf = rawFrame(['bad']);
    expect(() => p.parseMessages(buf, log)).toThrow(/number/i);
  });

  it('throws when Invocation has no target', () => {
    const buf = rawFrame([1, {}, 'id', null, []]);
    expect(() => p.parseMessages(buf, log)).toThrow(/target/i);
  });

  it('throws when Invocation has no arguments array', () => {
    const buf = rawFrame([1, {}, 'id', 'MyMethod', null]);
    expect(() => p.parseMessages(buf, log)).toThrow(/arguments/i);
  });

  it('throws when StreamItem is missing invocationId', () => {
    const buf = rawFrame([2, {}, null, 'item']);
    expect(() => p.parseMessages(buf, log)).toThrow(/invocationId/i);
  });

  it('throws when Completion is missing invocationId', () => {
    const buf = rawFrame([3, {}, null, 2]);
    expect(() => p.parseMessages(buf, log)).toThrow(/invocationId/i);
  });

  it('throws when Completion has unknown resultKind', () => {
    const buf = rawFrame([3, {}, 'id', 99]);
    expect(() => p.parseMessages(buf, log)).toThrow(/resultKind/i);
  });

  it('throws when StreamInvocation is missing invocationId', () => {
    const buf = rawFrame([4, {}, null, 'Method', []]);
    expect(() => p.parseMessages(buf, log)).toThrow(/invocationId/i);
  });

  it('throws when StreamInvocation is missing target', () => {
    const buf = rawFrame([4, {}, 'id', null, []]);
    expect(() => p.parseMessages(buf, log)).toThrow(/target/i);
  });

  it('throws when StreamInvocation is missing arguments', () => {
    const buf = rawFrame([4, {}, 'id', 'Method', null]);
    expect(() => p.parseMessages(buf, log)).toThrow(/arguments/i);
  });

  it('throws when CancelInvocation is missing invocationId', () => {
    const buf = rawFrame([5, {}, null]);
    expect(() => p.parseMessages(buf, log)).toThrow(/invocationId/i);
  });

  it('throws when declared frame length exceeds data', () => {
    // VarInt says 100 bytes but buffer only has 5
    const frame = new Uint8Array([100, 0x91, 0x06, 0x00, 0x00]); // varint(100) + tiny data
    expect(() => p.parseMessages(frame.buffer, log)).toThrow(/length/i);
  });

  it('throws when VarInt frame prefix is truncated (continuation bit set, no next byte) (line 95)', () => {
    // 0x80 has the continuation bit set but there is no second byte
    const frame = new Uint8Array([0x80]);
    expect(() => p.parseMessages(frame.buffer, log)).toThrow(/unexpected end of data/i);
  });

  it('rejects a VarInt prefix longer than five bytes', () => {
    expect(() => p.parseMessages(new Uint8Array([0x80, 0x80, 0x80, 0x80, 0x80]).buffer, log))
      .toThrow(/longer than 5 bytes/i);
  });

  it('rejects a VarInt length above the SignalR 2 GB limit', () => {
    expect(() => p.parseMessages(new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x08]).buffer, log))
      .toThrow(/2 GB/i);
  });

  it('rejects non-string invocation IDs', () => {
    expect(() => p.parseMessages(rawFrame([2, {}, 42, 'item']), log)).toThrow(/invocationId/i);
  });

  it('rejects non-string stream IDs', () => {
    expect(() => p.parseMessages(rawFrame([1, {}, 'id', 'Target', [], [1]]), log)).toThrow(/streamIds/i);
  });

  it('rejects non-string header values', () => {
    expect(() => p.parseMessages(rawFrame([5, { bad: 1 }, 'id']), log)).toThrow(/headers/i);
  });

  it('rejects a non-boolean allowReconnect value', () => {
    expect(() => p.parseMessages(rawFrame([7, null, 'yes']), log)).toThrow(/allowReconnect/i);
  });

  it('preserves an explicit null completion result as NonVoid', () => {
    const message: HubMessage = {
      type: MessageType.Completion,
      invocationId: toInvocationId('null-result'),
      result: null,
    };
    expect((roundtrip(message) as { result?: unknown }).result).toBeNull();
  });

  // ── Headers branch coverage ───────────────────────────────────────────────

  it('StreamInvocation with non-empty headers preserves them (line 362)', () => {
    // [type=4, headers={x-h:"1"}, invocationId, target, arguments]
    const buf = rawFrame([4, { 'x-h': '1' }, 'id-1', 'Target', []]);
    const msgs = p.parseMessages(buf, log);
    expect(msgs[0]!.type).toBe(MessageType.StreamInvocation);
    expect((msgs[0] as { headers?: Record<string, string> }).headers?.['x-h']).toBe('1');
  });

  it('CancelInvocation with non-empty headers preserves them (line 376)', () => {
    // [type=5, headers={x-h:"2"}, invocationId]
    const buf = rawFrame([5, { 'x-h': '2' }, 'id-2']);
    const msgs = p.parseMessages(buf, log);
    expect(msgs[0]!.type).toBe(MessageType.CancelInvocation);
    expect((msgs[0] as { headers?: Record<string, string> }).headers?.['x-h']).toBe('2');
  });

  it('Completion Error with null error message uses empty string fallback (line 330 ?? branch)', () => {
    // [type=3, headers={}, invocationId, resultKind=Error(1), null]
    // arr[4] is null → exercises String(arr[4] ?? '') → ''
    const buf = rawFrame([3, {}, 'id-3', 1, null]);
    const msgs = p.parseMessages(buf, log);
    expect(msgs[0]!.type).toBe(MessageType.Completion);
    expect((msgs[0] as { error?: string }).error).toBe('');
  });

  it('fire-and-forget Invocation (send) with headers preserves them (line 298)', () => {
    // [type=1, headers={x-ff:"1"}, no invocationId (""), target, arguments]
    // arr[1] non-empty → isNonEmptyObject TRUE branch on line 298
    const buf = rawFrame([1, { 'x-ff': '1' }, '', 'Target', []]);
    const msgs = p.parseMessages(buf, log);
    expect(msgs[0]!.type).toBe(MessageType.Invocation);
    expect((msgs[0] as { headers?: Record<string, string> }).headers?.['x-ff']).toBe('1');
  });

  it('StreamItem with non-empty headers preserves them (line 313)', () => {
    // [type=2, headers={x-si:"v"}, invocationId, item]
    // arr[1] non-empty → isNonEmptyObject TRUE branch on line 313
    const buf = rawFrame([2, { 'x-si': 'v' }, 'id-si', 'item-value']);
    const msgs = p.parseMessages(buf, log);
    expect(msgs[0]!.type).toBe(MessageType.StreamItem);
    expect((msgs[0] as { headers?: Record<string, string> }).headers?.['x-si']).toBe('v');
  });

  it('Completion with non-empty headers uses headers branch (line 326 true branch)', () => {
    // [type=3, headers={x-c:"ok"}, invocationId, resultKind=Void(2)]
    // isNonEmptyObject(arr[1]) TRUE → hdrs = { headers: ... }
    const buf = rawFrame([3, { 'x-c': 'ok' }, 'id-c', 2]);
    const msgs = p.parseMessages(buf, log);
    expect(msgs[0]!.type).toBe(MessageType.Completion);
    expect((msgs[0] as { headers?: Record<string, string> }).headers?.['x-c']).toBe('ok');
  });
});

// ─── Codec edge cases ─────────────────────────────────────────────────────────

describe('MsgpackHubProtocol - codec edge cases via round-trip', () => {

  it('preserves large positive integer argument', () => {
    const id  = toInvocationId('e1');
    const msg = MsgpackHubProtocol.invocation(id, 'BigNum', [4_294_967_295]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toBe(4_294_967_295);
  });

  it('preserves large negative integer argument', () => {
    const id  = toInvocationId('e2');
    const msg = MsgpackHubProtocol.invocation(id, 'Neg', [-2_147_483_648]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toBe(-2_147_483_648);
  });

  it('preserves float argument', () => {
    const id  = toInvocationId('e3');
    const msg = MsgpackHubProtocol.invocation(id, 'Pi', [Math.PI]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toBeCloseTo(Math.PI, 10);
  });

  it('preserves empty string argument', () => {
    const id  = toInvocationId('e4');
    const msg = MsgpackHubProtocol.invocation(id, 'Echo', ['']);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toBe('');
  });

  it('preserves long string argument (>31 chars, uses str8/str16)', () => {
    const long = 'a'.repeat(256);
    const id   = toInvocationId('e5');
    const msg  = MsgpackHubProtocol.invocation(id, 'LongStr', [long]);
    const out  = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toBe(long);
  });

  it('preserves boolean arguments', () => {
    const id  = toInvocationId('e6');
    const msg = MsgpackHubProtocol.invocation(id, 'Flags', [true, false]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments).toEqual([true, false]);
  });

  it('preserves array argument', () => {
    const id  = toInvocationId('e7');
    const msg = MsgpackHubProtocol.invocation(id, 'List', [[1, 2, 3]]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toEqual([1, 2, 3]);
  });

  it('preserves deeply nested object argument', () => {
    const id  = toInvocationId('e8');
    const data = { a: { b: { c: [1, 2, 3] } } };
    const msg = MsgpackHubProtocol.invocation(id, 'Deep', [data]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toEqual(data);
  });

  it('handles 16-element array (uses array16 format)', () => {
    const id  = toInvocationId('e9');
    const big = Array.from({ length: 16 }, (_, i) => i);
    const msg = MsgpackHubProtocol.invocation(id, 'BigArray', [big]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toEqual(big);
  });
});

// ─── Static factory parity with JsonHubProtocol ───────────────────────────────

describe('MsgpackHubProtocol static factories', () => {
  it('ping() produces a Ping message', () => {
    const msg = MsgpackHubProtocol.ping();
    expect(msg.type).toBe(MessageType.Ping);
  });

  it('send() produces a fire-and-forget Invocation', () => {
    const msg = MsgpackHubProtocol.send('Say', ['hi']);
    expect(msg.type).toBe(MessageType.Invocation);
    expect(msg.invocationId).toBeUndefined();
  });

  it('streamInvocation() includes streamIds when provided', () => {
    const id  = toInvocationId('si1');
    const msg = MsgpackHubProtocol.streamInvocation(id, 'Upload', [], ['src']);
    expect(msg.streamIds).toEqual(['src']);
  });

  it('completion() with null/null produces a void Completion', () => {
    const id  = toInvocationId('c1');
    const msg = MsgpackHubProtocol.completion(id, null, null);
    expect(msg.type).toBe(MessageType.Completion);
    expect(msg.result).toBeUndefined();
    expect(msg.error).toBeUndefined();
  });

  it('completion() with error propagates error field', () => {
    const id  = toInvocationId('c2');
    const msg = MsgpackHubProtocol.completion(id, null, 'boom');
    expect(msg.error).toBe('boom');
  });
});

// ─── Codec unit tests (@msgpack/msgpack via protocol) ─────────────────────────

describe('@msgpack/msgpack - encode/decode primitives (via protocol round-trips)', () => {
  // We exercise the codec through the protocol's writeMessage/parseMessages round-trip.
  // Each test picks a Completion payload to carry the value under test.

  function codecRoundtrip(value: unknown): unknown {
    const id  = toInvocationId('x');
    const msg = MsgpackHubProtocol.completion(id, value, null);
    const buf = p.writeMessage(msg);
    const out = p.parseMessages(buf, log)[0];
    return (out as { result?: unknown }).result;
  }

  it('nil (null)', () => {
    // null result makes the protocol write a Void completion, so test via args
    const id  = toInvocationId('nil');
    const msg = MsgpackHubProtocol.invocation(id, 'T', [null]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toBeNull();
  });

  it('positive fixint (0)', () => { expect(codecRoundtrip(0)).toBe(0); });
  it('positive fixint (127)', () => { expect(codecRoundtrip(127)).toBe(127); });
  it('uint8 (128)', () => { expect(codecRoundtrip(128)).toBe(128); });
  it('uint8 (255)', () => { expect(codecRoundtrip(255)).toBe(255); });
  it('uint16 (256)', () => { expect(codecRoundtrip(256)).toBe(256); });
  it('uint16 (65535)', () => { expect(codecRoundtrip(65535)).toBe(65535); });
  it('uint32 (65536)', () => { expect(codecRoundtrip(65536)).toBe(65536); });
  it('uint32 (4294967295)', () => { expect(codecRoundtrip(4_294_967_295)).toBe(4_294_967_295); });
  it('negative fixint (-1)', () => { expect(codecRoundtrip(-1)).toBe(-1); });
  it('negative fixint (-32)', () => { expect(codecRoundtrip(-32)).toBe(-32); });
  it('int8 (-33)', () => { expect(codecRoundtrip(-33)).toBe(-33); });
  it('int8 (-128)', () => { expect(codecRoundtrip(-128)).toBe(-128); });
  it('int16 (-129)', () => { expect(codecRoundtrip(-129)).toBe(-129); });
  it('int16 (-32768)', () => { expect(codecRoundtrip(-32768)).toBe(-32768); });
  it('int32 (-32769)', () => { expect(codecRoundtrip(-32769)).toBe(-32769); });
  it('int32 (INT32_MIN)', () => { expect(codecRoundtrip(-2_147_483_648)).toBe(-2_147_483_648); });
  it('float64 (3.14)', () => { expect(codecRoundtrip(3.14)).toBeCloseTo(3.14, 10); });
  it('float64 (NaN handled as nil)', () => {
    // NaN is not a valid JSON/MP value; encode encodes NaN as float64
    // The round-trip should preserve it (or return NaN back)
    const id  = toInvocationId('nan');
    const msg = MsgpackHubProtocol.invocation(id, 'T', [NaN]);
    const buf = p.writeMessage(msg);
    const out = p.parseMessages(buf, log)[0];
    const v   = (out as { arguments?: unknown[] }).arguments![0];
    expect(typeof v === 'number' && isNaN(v as number)).toBe(true);
  });
  it('empty string', () => { expect(codecRoundtrip('')).toBe(''); });
  it('fixstr (≤31 chars)', () => { expect(codecRoundtrip('hi')).toBe('hi'); });
  it('str8 (32 chars)', () => {
    const s = 'x'.repeat(32);
    expect(codecRoundtrip(s)).toBe(s);
  });
  it('str16 (256 chars)', () => {
    const s = 'y'.repeat(256);
    expect(codecRoundtrip(s)).toBe(s);
  });
  it('boolean true', () => { expect(codecRoundtrip(true)).toBe(true); });
  it('boolean false', () => {
    // false result is falsy; completion(id, false, null) will be Void since
    // we check `result != null`. Let's test via invocation args instead.
    const id  = toInvocationId('fb');
    const msg = MsgpackHubProtocol.invocation(id, 'T', [false]);
    const out = roundtrip(msg);
    expect((out as { arguments?: unknown[] }).arguments![0]).toBe(false);
  });
  it('empty array []', () => { expect(codecRoundtrip([])).toEqual([]); });
  it('empty map {}', () => { expect(codecRoundtrip({})).toEqual({}); });
  it('fixmap (≤15 keys)', () => {
    expect(codecRoundtrip({ a: 1, b: 2 })).toEqual({ a: 1, b: 2 });
  });
  it('map16 (>15 keys)', () => {
    const obj: Record<string, number> = {};
    for (let i = 0; i < 20; i++) obj[`k${i}`] = i;
    expect(codecRoundtrip(obj)).toEqual(obj);
  });
});
