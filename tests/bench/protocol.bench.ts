/**
 * tests/bench/protocol.bench.ts
 *
 * Micro-benchmark: JSON hub protocol
 *
 * Measures:
 *   • writeMessage()  - encode a HubMessage to its wire representation
 *   • parseMessages() - decode a pre-encoded wire payload back to HubMessage[]
 *
 * For every message type (Ping, Close, Invocation, StreamItem, Completion,
 * StreamInvocation, CancelInvocation) we compare encode, decode and the full
 * round-trip side by side.
 *
 * Every bench function returns its result so the engine cannot eliminate
 * the work as dead code.
 *
 * Run with:
 *   npx vitest bench tests/bench/protocol.bench.ts
 */

import { test } from 'vitest';

import { JsonHubProtocol }    from '../../src/protocols/json-hub-protocol.js';
import { MessageType }        from '../../src/constants.js';
import { NullLogger }         from '../../src/logger.js';
import { toInvocationId }     from '../../src/messages.js';
import type { HubMessage }    from '../../src/messages.js';

const log  = NullLogger.instance;
const json = new JsonHubProtocol();

// ─── Fixture messages ─────────────────────────────────────────────────────────

const id = toInvocationId('bench-1');

interface Fixture {
  label:   string;
  message: HubMessage;
  /** Pre-encoded wire payloads, used by the decode benches. */
  json:    string;
}

function fixture(label: string, message: HubMessage): Fixture {
  return { label, message, json: json.writeMessage(message) as string };
}

const PING           = fixture('Ping', { type: MessageType.Ping });
const CLOSE          = fixture('Close', { type: MessageType.Close, error: 'shutdown', allowReconnect: false });
const INV            = fixture('Invocation (simple)', JsonHubProtocol.invocation(id, 'SendMessage', ['Alice', 'Hello, World!']));
const INV_BIG        = fixture('Invocation (rich object)', JsonHubProtocol.invocation(
  id, 'BulkInsert',
  [{ id: 1, name: 'Alice', email: 'alice@example.com', score: 98.5, active: true }],
));
const STREAM_ITEM    = fixture('StreamItem', JsonHubProtocol.streamItem(id, { index: 42, value: 3.14 }));
const COMPLETION     = fixture('Completion (with result)', JsonHubProtocol.completion(id, { status: 'ok', code: 200 }, null));
const COMPLETION_ERR = fixture('Completion (error)', JsonHubProtocol.completion(id, null, 'Method not found'));
const STREAM_INV     = fixture('StreamInvocation', JsonHubProtocol.streamInvocation(id, 'GetFeed', ['sports', 10]));
const CANCEL         = fixture('CancelInvocation', JsonHubProtocol.cancelInvocation(id));

const ALL = [PING, CLOSE, INV, INV_BIG, STREAM_ITEM, COMPLETION, COMPLETION_ERR, STREAM_INV, CANCEL];

// ─── Payload size report ─────────────────────────────────────────────────────
// Not a benchmark: prints the encoded size of each message.

test('Payload sizes (bytes)', () => {
  const encoder = new TextEncoder();
  console.table(Object.fromEntries(ALL.map((f) => [f.label, {
    JSON: encoder.encode(f.json).byteLength,
  }])));
});

// ─── Encode vs decode vs round-trip ───────────────────────────────────────────

test.for(ALL)('JSON protocol - $label', async (f, { bench }) => {
  await bench.compare(
    bench('writeMessage', () => json.writeMessage(f.message)),
    bench('parseMessages', () => json.parseMessages(f.json, log)),
    bench('round-trip',    () => json.parseMessages(json.writeMessage(f.message), log)),
  );
});
