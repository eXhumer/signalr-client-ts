/**
 * errors.test.ts - Custom error classes and type-guard helpers.
 */

import { describe, it, expect } from 'vitest';

import {
  HubError,
  AbortError,
  TransportError,
  HandshakeError,
  UnsupportedTransportError,
  isHubError,
  isAbortError,
  isTransportError,
} from '../../src/errors.js';

describe('HubError', () => {
  it('is an instance of Error', () => {
    expect(new HubError('x') instanceof Error).toBeTruthy();
  });

  it('name is "HubError"', () => {
    expect(new HubError('x').name).toBe('HubError');
  });

  it('preserves the message', () => {
    expect(new HubError('boom').message).toBe('boom');
  });

  it('instanceof works across new.target prototype fix', () => {
    const err = new HubError('test');
    expect(err instanceof HubError).toBeTruthy();
    expect(err instanceof Error).toBeTruthy();
  });
});

describe('AbortError', () => {
  it('has a sensible default message', () => {
    expect(new AbortError().message).toMatch(/aborted/i);
  });

  it('accepts a custom message', () => {
    expect(new AbortError('stopped').message).toBe('stopped');
  });

  it('name is "AbortError"', () => {
    expect(new AbortError().name).toBe('AbortError');
  });
});

describe('TransportError', () => {
  it('stores the status code', () => {
    const err = new TransportError('Not Found', 404);
    expect(err.statusCode).toBe(404);
  });

  it('statusCode is undefined when omitted', () => {
    const err = new TransportError('network error');
    expect(err.statusCode).toBe(undefined);
  });

  it('name is "TransportError"', () => {
    expect(new TransportError('x').name).toBe('TransportError');
  });
});

describe('HandshakeError', () => {
  it('name is "HandshakeError"', () => {
    expect(new HandshakeError('rejected').name).toBe('HandshakeError');
  });
});

describe('UnsupportedTransportError', () => {
  it('stores the transport value', () => {
    const err = new UnsupportedTransportError('no ws', 1);
    expect(err.transport).toBe(1);
    expect(err.name).toBe('UnsupportedTransportError');
  });

  it('accepts null transport', () => {
    const err = new UnsupportedTransportError('none', null);
    expect(err.transport).toBe(null);
  });
});

describe('Type-guard helpers', () => {
  it('isHubError narrows correctly', () => {
    expect(isHubError(new HubError('x'))).toBeTruthy();
    expect(isHubError(new Error('x'))).toBeFalsy();
    expect(isHubError(null)).toBeFalsy();
    expect(isHubError('string')).toBeFalsy();
  });

  it('isAbortError narrows correctly', () => {
    expect(isAbortError(new AbortError())).toBeTruthy();
    expect(isAbortError(new Error())).toBeFalsy();
    expect(isAbortError(undefined)).toBeFalsy();
  });

  it('isTransportError narrows correctly', () => {
    expect(isTransportError(new TransportError('x'))).toBeTruthy();
    expect(isTransportError(new HubError('x'))).toBeFalsy();
    expect(isTransportError(42)).toBeFalsy();
  });

  it('error classes are distinct (not cross-matching)', () => {
    const hub       = new HubError('x');
    const abort     = new AbortError();
    const transport = new TransportError('x');

    expect(isAbortError(hub)).toBeFalsy();
    expect(isTransportError(hub)).toBeFalsy();
    expect(isHubError(abort)).toBeFalsy();
    expect(isHubError(transport)).toBeFalsy();
  });
});
