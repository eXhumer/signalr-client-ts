/**
 * logger.test.ts - ConsoleLogger, NullLogger, resolveLogger.
 */

import { describe, it, vi, expect } from 'vitest';

import { ConsoleLogger, NullLogger, resolveLogger } from '../../src/logger.js';
import { LogLevel } from '../../src/constants.js';
import type { ILogger } from '../../src/interfaces.js';

describe('NullLogger', () => {
  it('is a singleton', () => {
    expect(NullLogger.instance).toBe(NullLogger.instance);
  });

  it('log() does nothing and does not throw', () => {
    expect(() => NullLogger.instance.log(LogLevel.Error, 'oops')).not.toThrow();
  });

  it('satisfies ILogger', () => {
    const logger: ILogger = NullLogger.instance;
    expect(typeof logger.log).toBe('function');
  });
});

describe('ConsoleLogger', () => {
  it('respects minimumLevel - suppresses messages below threshold', () => {
    const logged: string[] = [];
    const logger = new ConsoleLogger(LogLevel.Warning);

    // Capture console.debug / console.log / console.warn / console.error
    const spies = [
      vi.spyOn(console, 'debug').mockImplementation((...args: unknown[]) => { logged.push(String(args[0])); }),
      vi.spyOn(console, 'log').mockImplementation((...args: unknown[])   => { logged.push(String(args[0])); }),
      vi.spyOn(console, 'warn').mockImplementation((...args: unknown[])  => { logged.push(String(args[0])); }),
      vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => { logged.push(String(args[0])); }),
    ];

    logger.log(LogLevel.Trace,       'trace msg');   // should be suppressed
    logger.log(LogLevel.Information, 'info msg');    // suppressed
    logger.log(LogLevel.Warning,     'warn msg');    // passes
    logger.log(LogLevel.Error,       'error msg');   // passes

    spies.forEach((s) => s.mockRestore());

    expect(logged.some((l) => l.includes('trace msg'))).toBeFalsy();
    expect(logged.some((l) => l.includes('info msg'))).toBeFalsy();
    expect(logged.some((l) => l.includes('warn msg'))).toBeTruthy();
    expect(logged.some((l) => l.includes('error msg'))).toBeTruthy();
  });

  it('default level is Information', () => {
    const logger = new ConsoleLogger();
    const logged: string[] = [];
    const spy = vi.spyOn(console, 'log').mockImplementation((...args: unknown[]) => { logged.push(String(args[0])); });

    logger.log(LogLevel.Debug,       'debug');   // suppressed
    logger.log(LogLevel.Information, 'info');    // passes

    spy.mockRestore();
    expect(logged.some((l) => l.includes('debug'))).toBeFalsy();
    expect(logged.some((l) => l.includes('info'))).toBeTruthy();
  });

  it('log entries include a timestamp and level label', () => {
    const logged: string[] = [];
    const spy = vi.spyOn(console, 'warn').mockImplementation((...args: unknown[]) => { logged.push(String(args[0])); });
    const logger = new ConsoleLogger(LogLevel.Warning);
    logger.log(LogLevel.Warning, 'test message');
    spy.mockRestore();

    expect(logged[0]?.includes('Warning'), `Expected "Warning" in "${logged[0]}"`).toBeTruthy();
    expect(logged[0]?.includes('test message')).toBeTruthy();
    // ISO date pattern
    expect(logged[0]!).toMatch(/\d{4}-\d{2}-\d{2}T/);
  });

  it('routes Error/Critical to console.error', () => {
    const errors: string[] = [];
    const spy = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => { errors.push(String(args[0])); });
    const logger = new ConsoleLogger(LogLevel.Trace);
    logger.log(LogLevel.Error,    'err1');
    logger.log(LogLevel.Critical, 'crit1');
    spy.mockRestore();
    expect(errors.some((l) => l.includes('err1'))).toBeTruthy();
    expect(errors.some((l) => l.includes('crit1'))).toBeTruthy();
  });

  it('routes Trace/Debug to console.debug', () => {
    const debugs: string[] = [];
    const spy = vi.spyOn(console, 'debug').mockImplementation((...args: unknown[]) => { debugs.push(String(args[0])); });
    const logger = new ConsoleLogger(LogLevel.Trace);
    logger.log(LogLevel.Trace, 'trace1');
    logger.log(LogLevel.Debug, 'debug1');
    spy.mockRestore();
    expect(debugs.some((l) => l.includes('trace1'))).toBeTruthy();
    expect(debugs.some((l) => l.includes('debug1'))).toBeTruthy();
  });

  it('falls back to the numeric string for an unknown level (line 66 fallback path)', () => {
    const logged: string[] = [];
    const spy = vi.spyOn(console, 'log').mockImplementation((...args: unknown[]) => {
      logged.push(String(args[0]));
    });
    const logger = new ConsoleLogger(LogLevel.Trace);
    logger.log(99 as LogLevel, 'unknown-level-message');
    spy.mockRestore();
    expect(logged.some((l) => l.includes('99'))).toBeTruthy();
    expect(logged.some((l) => l.includes('unknown-level-message'))).toBeTruthy();
  });
});

describe('resolveLogger', () => {
  it('accepts a LogLevel number → ConsoleLogger', () => {
    const logger = resolveLogger(LogLevel.Warning);
    expect(logger instanceof ConsoleLogger).toBeTruthy();
  });

  it('accepts a custom ILogger object', () => {
    const custom: ILogger = { log: () => {} };
    expect(resolveLogger(custom)).toBe(custom);
  });

  it('throws for invalid input', () => {
    expect(() => resolveLogger(null as unknown as ILogger)).toThrow(/log.*method/i);
    expect(() => resolveLogger({} as ILogger)).toThrow(/log.*method/i);
  });
});
