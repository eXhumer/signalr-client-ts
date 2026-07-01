/**
 * hub-connection-builder.test.ts
 *
 * Tests the fluent builder API, validation, and DefaultReconnectPolicy.
 */

import { describe, it, afterEach, expect } from 'vitest';

import { HubConnectionBuilder, DefaultReconnectPolicy } from '../../src/hub-connection-builder.js';
import { HubConnection }        from '../../src/hub-connection.js';
import { LogLevel, HubConnectionState } from '../../src/constants.js';
import type { IRetryPolicy, RetryContext } from '../../src/interfaces.js';
import { Agent } from 'undici';
import {
  RequestHttpClient,
  FetchHttpClient,
  StreamHttpClient,
  PipelineHttpClient,
  DispatchHttpClient,
} from '../../src/http-client.js';
import { closeTrackedDispatchers, trackDispatcher } from '../helpers/dispatcher-tracker.js';

afterEach(closeTrackedDispatchers);

// ─── Builder basics ───────────────────────────────────────────────────────────

describe('HubConnectionBuilder.build()', () => {
  it('throws when withUrl was not called', () => {
    expect(
      () => new HubConnectionBuilder().build(),
    ).toThrow(/withUrl/i);
  });

  it('returns a HubConnection instance', () => {
    const conn = new HubConnectionBuilder()
      .withUrl('http://localhost/hub')
      .build();
    expect(conn instanceof HubConnection).toBeTruthy();
  });

  it('initial state is Disconnected', () => {
    const conn = new HubConnectionBuilder()
      .withUrl('http://localhost/hub')
      .build();
    expect(conn.state).toBe(HubConnectionState.Disconnected);
  });

  it('initial connectionId is null', () => {
    const conn = new HubConnectionBuilder()
      .withUrl('http://localhost/hub')
      .build();
    expect(conn.connectionId).toBe(null);
  });
});

describe('HubConnectionBuilder.withUrl()', () => {
  it('throws for empty string', () => {
    expect(
      () => new HubConnectionBuilder().withUrl(''),
    ).toThrow(/non-empty/i);
  });

  it('throws for non-string', () => {
    expect(
      () => new HubConnectionBuilder().withUrl(null as unknown as string),
    ).toThrow(/non-empty/i);
  });

  it('is chainable (returns this)', () => {
    const builder = new HubConnectionBuilder();
    const result  = builder.withUrl('http://localhost/hub');
    expect(result).toBe(builder);
  });

  it('accepts UrlOptions.accessTokenFactory', () => {
    const factory = async (): Promise<string> => 'tok';
    const conn = new HubConnectionBuilder()
      .withUrl('http://localhost/hub', { accessTokenFactory: factory })
      .build();
    expect(conn instanceof HubConnection).toBeTruthy();
  });

  it('accepts UrlOptions.headers', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub', { headers: { 'X-Foo': 'bar' } })
        .build();
    }).not.toThrow();
  });

  it('accepts UrlOptions.skipNegotiation=true', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub', { skipNegotiation: true })
        .build();
    }).not.toThrow();
  });
});

describe('HubConnectionBuilder.configureLogging()', () => {
  it('accepts a LogLevel number', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .configureLogging(LogLevel.Warning)
        .build();
    }).not.toThrow();
  });

  it('accepts a custom ILogger', () => {
    const custom = { log: (): void => {} };
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .configureLogging(custom)
        .build();
    }).not.toThrow();
  });

  it('throws for invalid input', () => {
    expect(
      () => new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .configureLogging(null as unknown as LogLevel),
    ).toThrow(/log.*method/i);
  });

  it('is chainable', () => {
    const builder = new HubConnectionBuilder().withUrl('http://localhost/hub');
    expect(builder.configureLogging(LogLevel.None)).toBe(builder);
  });
});

describe('HubConnectionBuilder.withAutomaticReconnect()', () => {
  it('accepts no arguments - uses default delays', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withAutomaticReconnect()
        .build();
    }).not.toThrow();
  });

  it('accepts a number[] of retry delays', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withAutomaticReconnect([0, 1000, 5000])
        .build();
    }).not.toThrow();
  });

  it('accepts a custom IRetryPolicy', () => {
    const policy: IRetryPolicy = {
      nextRetryDelayInMilliseconds: () => 1000,
    };
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withAutomaticReconnect(policy)
        .build();
    }).not.toThrow();
  });

  it('throws for invalid argument', () => {
    expect(
      () => new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withAutomaticReconnect(42 as unknown as number[]),
    ).toThrow(/IRetryPolicy/i);
  });

  it('is chainable', () => {
    const builder = new HubConnectionBuilder().withUrl('http://localhost/hub');
    expect(builder.withAutomaticReconnect()).toBe(builder);
  });
});

// ─── DefaultReconnectPolicy ───────────────────────────────────────────────────

describe('DefaultReconnectPolicy', () => {
  function ctx(previousRetryCount: number): RetryContext {
    return { previousRetryCount, elapsedMilliseconds: 0, retryReason: null };
  }

  it('returns the delay at the given index', () => {
    const policy = new DefaultReconnectPolicy([0, 2_000, 10_000]);
    expect(policy.nextRetryDelayInMilliseconds(ctx(0))).toBe(0);
    expect(policy.nextRetryDelayInMilliseconds(ctx(1))).toBe(2_000);
    expect(policy.nextRetryDelayInMilliseconds(ctx(2))).toBe(10_000);
  });

  it('returns null when delays are exhausted', () => {
    const policy = new DefaultReconnectPolicy([100, 200]);
    expect(policy.nextRetryDelayInMilliseconds(ctx(2))).toBe(null);
    expect(policy.nextRetryDelayInMilliseconds(ctx(9))).toBe(null);
  });

  it('default constructor uses [0, 2000, 10000, 30000]', () => {
    const policy = new DefaultReconnectPolicy();
    expect(policy.nextRetryDelayInMilliseconds(ctx(0))).toBe(0);
    expect(policy.nextRetryDelayInMilliseconds(ctx(1))).toBe(2_000);
    expect(policy.nextRetryDelayInMilliseconds(ctx(2))).toBe(10_000);
    expect(policy.nextRetryDelayInMilliseconds(ctx(3))).toBe(30_000);
    expect(policy.nextRetryDelayInMilliseconds(ctx(4))).toBe(null);
  });

  it('works with an empty delay array → always returns null', () => {
    const policy = new DefaultReconnectPolicy([]);
    expect(policy.nextRetryDelayInMilliseconds(ctx(0))).toBe(null);
  });
});

// ─── withDispatcher() ─────────────────────────────────────────────────────────

describe('HubConnectionBuilder.withDispatcher()', () => {
  it('is chainable (returns this)', () => {
    const agent   = trackDispatcher(new Agent());
    const builder = new HubConnectionBuilder().withUrl('http://localhost/hub');
    expect(builder.withDispatcher(agent)).toBe(builder);
  });

  it('does not throw when a valid Dispatcher is provided', () => {
    const agent = trackDispatcher(new Agent({ connections: 4 }));
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withDispatcher(agent)
        .build();
    }).not.toThrow();
  });

  it('produces a HubConnection instance when combined with other options', () => {
    const agent = trackDispatcher(new Agent());
    const conn  = new HubConnectionBuilder()
      .withUrl('http://localhost/hub')
      .withDispatcher(agent)
      .configureLogging(LogLevel.None)
      .build();
    expect(conn instanceof HubConnection).toBeTruthy();
  });
});

// ─── withHttpClient() ─────────────────────────────────────────────────────────

describe('HubConnectionBuilder.withHttpClient()', () => {
  it('is chainable (returns this)', () => {
    const builder = new HubConnectionBuilder().withUrl('http://localhost/hub');
    expect(builder.withHttpClient(new RequestHttpClient())).toBe(builder);
  });

  it('accepts RequestHttpClient', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withHttpClient(new RequestHttpClient())
        .build();
    }).not.toThrow();
  });

  it('accepts FetchHttpClient', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withHttpClient(new FetchHttpClient())
        .build();
    }).not.toThrow();
  });

  it('accepts StreamHttpClient', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withHttpClient(new StreamHttpClient())
        .build();
    }).not.toThrow();
  });

  it('accepts PipelineHttpClient', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withHttpClient(new PipelineHttpClient())
        .build();
    }).not.toThrow();
  });

  it('accepts DispatchHttpClient', () => {
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withHttpClient(new DispatchHttpClient())
        .build();
    }).not.toThrow();
  });

  it('can be combined with withDispatcher()', () => {
    const agent = trackDispatcher(new Agent());
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withDispatcher(agent)
        .withHttpClient(new FetchHttpClient({ dispatcher: agent }))
        .build();
    }).not.toThrow();
  });
});

// ─── Coverage gaps ────────────────────────────────────────────────────────────

describe('HubConnectionBuilder.withAutomaticReconnect() - object without nextRetryDelayInMilliseconds', () => {
  it('throws when passed a plain object that lacks the nextRetryDelayInMilliseconds method', () => {
    // The existing test passes the number 42, which is not an object at all.
    // This test passes {} - typeof "object" is true but the method is absent -
    // exercising the inner else-throw branch inside the typeof-object guard.
    expect(
      () => new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withAutomaticReconnect({} as unknown as readonly number[]),
    ).toThrow(/IRetryPolicy/i);
  });

  it('does not throw when passed an object with nextRetryDelayInMilliseconds → null (always stop)', () => {
    const alwaysStop = { nextRetryDelayInMilliseconds: (): null => null };
    expect(() => {
      new HubConnectionBuilder()
        .withUrl('http://localhost/hub')
        .withAutomaticReconnect(alwaysStop)
        .build();
    }).not.toThrow();
  });
});
