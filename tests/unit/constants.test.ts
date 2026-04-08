/**
 * constants.test.ts - Verify enum shapes, values, and type-level behaviour.
 */

import { describe, it, expect } from 'vitest';

import {
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
} from '../../src/constants.js';

describe('MessageType', () => {
  it('has exactly 7 entries', () => {
    expect(Object.keys(MessageType).length).toBe(7);
  });

  it('values match the SignalR spec', () => {
    expect(MessageType.Invocation).toBe(1);
    expect(MessageType.StreamItem).toBe(2);
    expect(MessageType.Completion).toBe(3);
    expect(MessageType.StreamInvocation).toBe(4);
    expect(MessageType.CancelInvocation).toBe(5);
    expect(MessageType.Ping).toBe(6);
    expect(MessageType.Close).toBe(7);
  });

  it('is frozen (immutable)', () => {
    expect(Object.isFrozen(MessageType)).toBeTruthy();
  });
});

describe('HubConnectionState', () => {
  it('exposes all five lifecycle states', () => {
    const states = Object.values(HubConnectionState) as string[];
    expect(states.includes('Disconnected')).toBeTruthy();
    expect(states.includes('Connecting')).toBeTruthy();
    expect(states.includes('Connected')).toBeTruthy();
    expect(states.includes('Disconnecting')).toBeTruthy();
    expect(states.includes('Reconnecting')).toBeTruthy();
  });

  it('is frozen', () => {
    expect(Object.isFrozen(HubConnectionState)).toBeTruthy();
  });
});

describe('LogLevel', () => {
  it('has ascending numeric values', () => {
    expect(LogLevel.Trace < LogLevel.Debug).toBeTruthy();
    expect(LogLevel.Debug < LogLevel.Information).toBeTruthy();
    expect(LogLevel.Information < LogLevel.Warning).toBeTruthy();
    expect(LogLevel.Warning < LogLevel.Error).toBeTruthy();
    expect(LogLevel.Error < LogLevel.Critical).toBeTruthy();
    expect(LogLevel.Critical < LogLevel.None).toBeTruthy();
  });
});

describe('HttpTransportType', () => {
  it('values are distinct powers-of-two bitmask flags', () => {
    expect(HttpTransportType.None).toBe(0);
    expect(HttpTransportType.WebSockets).toBe(1);
    expect(HttpTransportType.ServerSentEvents).toBe(2);
    expect(HttpTransportType.LongPolling).toBe(4);
  });

  it('supports OR-combination for transport selection', () => {
    const all = HttpTransportType.WebSockets |
                HttpTransportType.ServerSentEvents |
                HttpTransportType.LongPolling;
    expect(all).toBe(7);
    expect((all & HttpTransportType.WebSockets)       !== 0).toBeTruthy();
    expect((all & HttpTransportType.ServerSentEvents) !== 0).toBeTruthy();
    expect((all & HttpTransportType.LongPolling)      !== 0).toBeTruthy();
    expect((all & HttpTransportType.None)             === 0).toBeTruthy();
  });
});

describe('TransferFormat', () => {
  it('has Text=1, Binary=2', () => {
    expect(TransferFormat.Text).toBe(1);
    expect(TransferFormat.Binary).toBe(2);
  });
});

describe('Protocol constants', () => {
  it('RECORD_SEPARATOR is ASCII 30', () => {
    expect(RECORD_SEPARATOR.charCodeAt(0)).toBe(30);
    expect(RECORD_SEPARATOR.length).toBe(1);
  });

  it('NEGOTIATE_VERSION is 1', () => {
    expect(NEGOTIATE_VERSION).toBe(1);
  });
});

describe('Timing defaults', () => {
  it('DEFAULT_TIMEOUT_IN_MS is 30 seconds', () => {
    expect(DEFAULT_TIMEOUT_IN_MS).toBe(30_000);
  });

  it('DEFAULT_PING_INTERVAL_IN_MS is 15 seconds', () => {
    expect(DEFAULT_PING_INTERVAL_IN_MS).toBe(15_000);
  });

  it('DEFAULT_SERVER_TIMEOUT_IN_MS is 30 seconds', () => {
    expect(DEFAULT_SERVER_TIMEOUT_IN_MS).toBe(30_000);
  });
});
