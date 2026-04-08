/**
 * tests/bench/protocol.bench.ts
 *
 * Micro-benchmark: JSON vs MessagePack hub protocol
 *
 * Measures:
 *   • writeMessage()  - encode a HubMessage to its wire representation
 *   • parseMessages() - decode a pre-encoded wire payload back to HubMessage[]
 *
 * For every message type (Ping, Close, Invocation, StreamItem, Completion,
 * StreamInvocation, CancelInvocation) we run both directions so you can see
 * which protocol wins on encode, which wins on decode, and by how much.
 *
 * Run with:
 *   npx vitest bench tests/bench/protocol.bench.ts
 */

import { bench, describe, beforeAll } from 'vitest';

import { JsonHubProtocol }    from '../../src/protocols/json-hub-protocol.js';
import { MsgpackHubProtocol } from '../../src/protocols/msgpack-hub-protocol.js';
import { MessageType }        from '../../src/constants.js';
import { NullLogger }         from '../../src/logger.js';
import { toInvocationId }     from '../../src/messages.js';
import type { HubMessage }    from '../../src/messages.js';

const log  = NullLogger.instance;
const json = new JsonHubProtocol();
const mp   = new MsgpackHubProtocol();

// ─── Fixture messages ─────────────────────────────────────────────────────────

const id = toInvocationId('bench-1');

const PING:    HubMessage = { type: MessageType.Ping };
const CLOSE:   HubMessage = { type: MessageType.Close, error: 'shutdown', allowReconnect: false };
const INV:     HubMessage = JsonHubProtocol.invocation(id, 'SendMessage', ['Alice', 'Hello, World!']);
const INV_BIG: HubMessage = JsonHubProtocol.invocation(
  id, 'BulkInsert',
  [{ id: 1, name: 'Alice', email: 'alice@example.com', score: 98.5, active: true }],
);
const STREAM_ITEM: HubMessage = JsonHubProtocol.streamItem(id, { index: 42, value: 3.14 });
const COMPLETION:  HubMessage = JsonHubProtocol.completion(id, { status: 'ok', code: 200 }, null);
const COMPLETION_ERR: HubMessage = JsonHubProtocol.completion(id, null, 'Method not found');
const STREAM_INV:  HubMessage = JsonHubProtocol.streamInvocation(id, 'GetFeed', ['sports', 10]);
const CANCEL:      HubMessage = JsonHubProtocol.cancelInvocation(id);

// ─── Pre-encode wire payloads once (used in decode benches) ──────────────────

let jsonPing:       string;
let jsonInv:        string;
let jsonInvBig:     string;
let jsonStreamItem: string;
let jsonCompletion: string;
let jsonCompErr:    string;
let jsonStreamInv:  string;
let jsonCancel:     string;
let jsonClose:      string;

let mpPing:         ArrayBuffer;
let mpInv:          ArrayBuffer;
let mpInvBig:       ArrayBuffer;
let mpStreamItem:   ArrayBuffer;
let mpCompletion:   ArrayBuffer;
let mpCompErr:      ArrayBuffer;
let mpStreamInv:    ArrayBuffer;
let mpCancel:       ArrayBuffer;
let mpClose:        ArrayBuffer;

beforeAll(() => {
  jsonPing       = json.writeMessage(PING)         as string;
  jsonInv        = json.writeMessage(INV)          as string;
  jsonInvBig     = json.writeMessage(INV_BIG)      as string;
  jsonStreamItem = json.writeMessage(STREAM_ITEM)  as string;
  jsonCompletion = json.writeMessage(COMPLETION)   as string;
  jsonCompErr    = json.writeMessage(COMPLETION_ERR) as string;
  jsonStreamInv  = json.writeMessage(STREAM_INV)   as string;
  jsonCancel     = json.writeMessage(CANCEL)        as string;
  jsonClose      = json.writeMessage(CLOSE)         as string;

  mpPing       = mp.writeMessage(PING);
  mpInv        = mp.writeMessage(INV);
  mpInvBig     = mp.writeMessage(INV_BIG);
  mpStreamItem = mp.writeMessage(STREAM_ITEM);
  mpCompletion = mp.writeMessage(COMPLETION);
  mpCompErr    = mp.writeMessage(COMPLETION_ERR);
  mpStreamInv  = mp.writeMessage(STREAM_INV);
  mpCancel     = mp.writeMessage(CANCEL);
  mpClose      = mp.writeMessage(CLOSE);
});

// ─── Payload size report ─────────────────────────────────────────────────────
// (Runs as a setup bench, never counted in results)

describe('Payload sizes (bytes) - JSON vs MessagePack', () => {
  bench('Ping       JSON  size', () => { void jsonPing.length; });
  bench('Ping       MP    size', () => { void mpPing.byteLength; });
  bench('Invocation JSON  size', () => { void jsonInv.length; });
  bench('Invocation MP    size', () => { void mpInv.byteLength; });
  bench('InvBig     JSON  size', () => { void jsonInvBig.length; });
  bench('InvBig     MP    size', () => { void mpInvBig.byteLength; });
});

// ─── WRITE (encode) benchmarks ────────────────────────────────────────────────

describe('writeMessage - Ping', () => {
  bench('JSON',    () => { json.writeMessage(PING); });
  bench('MsgPack', () => {   mp.writeMessage(PING); });
});

describe('writeMessage - Close', () => {
  bench('JSON',    () => { json.writeMessage(CLOSE); });
  bench('MsgPack', () => {   mp.writeMessage(CLOSE); });
});

describe('writeMessage - Invocation (simple)', () => {
  bench('JSON',    () => { json.writeMessage(INV); });
  bench('MsgPack', () => {   mp.writeMessage(INV); });
});

describe('writeMessage - Invocation (rich object)', () => {
  bench('JSON',    () => { json.writeMessage(INV_BIG); });
  bench('MsgPack', () => {   mp.writeMessage(INV_BIG); });
});

describe('writeMessage - StreamItem', () => {
  bench('JSON',    () => { json.writeMessage(STREAM_ITEM); });
  bench('MsgPack', () => {   mp.writeMessage(STREAM_ITEM); });
});

describe('writeMessage - Completion (with result)', () => {
  bench('JSON',    () => { json.writeMessage(COMPLETION); });
  bench('MsgPack', () => {   mp.writeMessage(COMPLETION); });
});

describe('writeMessage - Completion (error)', () => {
  bench('JSON',    () => { json.writeMessage(COMPLETION_ERR); });
  bench('MsgPack', () => {   mp.writeMessage(COMPLETION_ERR); });
});

describe('writeMessage - StreamInvocation', () => {
  bench('JSON',    () => { json.writeMessage(STREAM_INV); });
  bench('MsgPack', () => {   mp.writeMessage(STREAM_INV); });
});

describe('writeMessage - CancelInvocation', () => {
  bench('JSON',    () => { json.writeMessage(CANCEL); });
  bench('MsgPack', () => {   mp.writeMessage(CANCEL); });
});

// ─── PARSE (decode) benchmarks ────────────────────────────────────────────────

describe('parseMessages - Ping', () => {
  bench('JSON',    () => { json.parseMessages(jsonPing,    log); });
  bench('MsgPack', () => {   mp.parseMessages(mpPing,     log); });
});

describe('parseMessages - Close', () => {
  bench('JSON',    () => { json.parseMessages(jsonClose,   log); });
  bench('MsgPack', () => {   mp.parseMessages(mpClose,    log); });
});

describe('parseMessages - Invocation (simple)', () => {
  bench('JSON',    () => { json.parseMessages(jsonInv,     log); });
  bench('MsgPack', () => {   mp.parseMessages(mpInv,      log); });
});

describe('parseMessages - Invocation (rich object)', () => {
  bench('JSON',    () => { json.parseMessages(jsonInvBig,  log); });
  bench('MsgPack', () => {   mp.parseMessages(mpInvBig,   log); });
});

describe('parseMessages - StreamItem', () => {
  bench('JSON',    () => { json.parseMessages(jsonStreamItem, log); });
  bench('MsgPack', () => {   mp.parseMessages(mpStreamItem,  log); });
});

describe('parseMessages - Completion (with result)', () => {
  bench('JSON',    () => { json.parseMessages(jsonCompletion, log); });
  bench('MsgPack', () => {   mp.parseMessages(mpCompletion,  log); });
});

describe('parseMessages - Completion (error)', () => {
  bench('JSON',    () => { json.parseMessages(jsonCompErr,  log); });
  bench('MsgPack', () => {   mp.parseMessages(mpCompErr,   log); });
});

describe('parseMessages - StreamInvocation', () => {
  bench('JSON',    () => { json.parseMessages(jsonStreamInv, log); });
  bench('MsgPack', () => {   mp.parseMessages(mpStreamInv,  log); });
});

describe('parseMessages - CancelInvocation', () => {
  bench('JSON',    () => { json.parseMessages(jsonCancel,  log); });
  bench('MsgPack', () => {   mp.parseMessages(mpCancel,   log); });
});

// ─── Full round-trip (write + parse) ─────────────────────────────────────────

describe('round-trip (write+parse) - Ping', () => {
  bench('JSON',    () => { json.parseMessages(json.writeMessage(PING) as string, log); });
  bench('MsgPack', () => {   mp.parseMessages(mp.writeMessage(PING),             log); });
});

describe('round-trip (write+parse) - Invocation (rich)', () => {
  bench('JSON',    () => { json.parseMessages(json.writeMessage(INV_BIG) as string, log); });
  bench('MsgPack', () => {   mp.parseMessages(mp.writeMessage(INV_BIG),             log); });
});

describe('round-trip (write+parse) - Completion', () => {
  bench('JSON',    () => { json.parseMessages(json.writeMessage(COMPLETION) as string, log); });
  bench('MsgPack', () => {   mp.parseMessages(mp.writeMessage(COMPLETION),             log); });
});
