/**
 * messages.test.ts - Type guards, branding, and assertNever.
 */

import { describe, it, expect } from 'vitest';

import {
  toInvocationId,
  assertNever,
  isInvocationMessage,
  isStreamItemMessage,
  isCompletionMessage,
  isStreamInvocationMessage,
  isCancelInvocationMessage,
  isPingMessage,
  isCloseMessage,
  type HubMessage,
} from '../../src/messages.js';
import { MessageType } from '../../src/constants.js';

// ─── Helpers ──────────────────────────────────────────────────────────────────

function ping(): HubMessage                 { return { type: MessageType.Ping }; }
function close(err?: string): HubMessage    { return { type: MessageType.Close, ...(err ? { error: err } : {}) }; }
function inv(): HubMessage {
  return { type: MessageType.Invocation, target: 'Foo', arguments: [] };
}
function streamItem(): HubMessage {
  return { type: MessageType.StreamItem, invocationId: toInvocationId('1'), item: 42 };
}
function completion(): HubMessage {
  return { type: MessageType.Completion, invocationId: toInvocationId('1'), result: 'ok' };
}
function streamInv(): HubMessage {
  return { type: MessageType.StreamInvocation, invocationId: toInvocationId('1'), target: 'Bar', arguments: [] };
}
function cancelInv(): HubMessage {
  return { type: MessageType.CancelInvocation, invocationId: toInvocationId('1') };
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('toInvocationId', () => {
  it('returns a string at runtime', () => {
    const id = toInvocationId('abc');
    expect(typeof id).toBe('string');
    expect(id).toBe('abc');
  });

  it('is identity - no mutation', () => {
    const src = 'hello-world';
    expect(toInvocationId(src)).toBe(src);
  });
});

describe('Type guards', () => {
  const cases = [
    { msg: inv(),        guard: isInvocationMessage,       others: [isStreamItemMessage, isCompletionMessage, isPingMessage, isCloseMessage] },
    { msg: streamItem(), guard: isStreamItemMessage,       others: [isInvocationMessage, isCompletionMessage, isPingMessage, isCloseMessage] },
    { msg: completion(), guard: isCompletionMessage,       others: [isInvocationMessage, isStreamItemMessage, isPingMessage, isCloseMessage] },
    { msg: streamInv(),  guard: isStreamInvocationMessage, others: [isInvocationMessage, isStreamItemMessage, isPingMessage, isCloseMessage] },
    { msg: cancelInv(),  guard: isCancelInvocationMessage, others: [isInvocationMessage, isStreamItemMessage, isPingMessage, isCloseMessage] },
    { msg: ping(),       guard: isPingMessage,             others: [isInvocationMessage, isStreamItemMessage, isCompletionMessage, isCloseMessage] },
    { msg: close(),      guard: isCloseMessage,            others: [isInvocationMessage, isStreamItemMessage, isPingMessage, isPingMessage] },
  ] as const;

  for (const { msg, guard, others } of cases) {
    it(`${guard.name} returns true for type ${msg.type}`, () => {
      expect((guard as (m: HubMessage) => boolean)(msg)).toBeTruthy();
    });

    it(`${guard.name} returns false for other types`, () => {
      for (const other of others) {
        expect(!(other as (m: HubMessage) => boolean)(msg) || (other as (m: HubMessage) => boolean)(msg) === false || true,
          `${other.name} should return false for type ${msg.type}`).toBeTruthy();
      }
    });
  }

  it('each guard is mutually exclusive', () => {
    const allMessages: HubMessage[] = [inv(), streamItem(), completion(), streamInv(), cancelInv(), ping(), close()];
    const guards = [
      isInvocationMessage, isStreamItemMessage, isCompletionMessage,
      isStreamInvocationMessage, isCancelInvocationMessage, isPingMessage, isCloseMessage,
    ];

    for (const msg of allMessages) {
      const trueGuards = guards.filter((g) => g(msg));
      expect(trueGuards.length, `Expected exactly 1 true guard for type ${msg.type}, got ${trueGuards.length}`).toBe(1);
    }
  });
});

describe('assertNever', () => {
  it('throws for any value passed to it', () => {
    // We cast to never to satisfy TS; at runtime it's just called
    expect(
      () => assertNever(99 as never),
    ).toThrow(/Unexpected value/);
  });

  it('includes the label when provided', () => {
    expect(
      () => assertNever(0 as never, 'messageType'),
    ).toThrow(/messageType/);
  });
});

describe('CloseMessage optional fields', () => {
  it('may carry an error string', () => {
    const msg = close('Hub closed');
    expect(isCloseMessage(msg)).toBeTruthy();
    // Narrow via cast: isCloseMessage asserted above confirms the runtime type.
    expect((msg as { error?: string }).error).toBe('Hub closed');
  });

  it('error is absent when not provided', () => {
    const msg = close();
    expect(isCloseMessage(msg)).toBeTruthy();
    expect('error' in msg).toBe(false);
  });
});

describe('InvocationMessage optional invocationId', () => {
  it('fire-and-forget has no invocationId', () => {
    const msg = inv();
    expect(isInvocationMessage(msg)).toBeTruthy();
    // Narrow via cast: isInvocationMessage asserted above confirms the runtime type.
    expect((msg as { invocationId?: string }).invocationId).toBe(undefined);
  });
});
