/**
 * logger.ts
 *
 * Built-in ILogger implementations.
 * ConsoleLogger - writes to console with timestamps and severity labels.
 * NullLogger     - discards everything (default when no logger is configured).
 */

import { LogLevel } from './constants.js';
import type { ILogger } from './interfaces.js';

// ─── ConsoleLogger ────────────────────────────────────────────────────────────

export class ConsoleLogger implements ILogger {
  readonly #minimumLevel: LogLevel;

  constructor(minimumLevel: LogLevel = LogLevel.Information) {
    this.#minimumLevel = minimumLevel;
  }

  log(level: LogLevel, message: string): void {
    if (level < this.#minimumLevel) return;

    const ts    = new Date().toISOString();
    const label = logLevelName(level);
    const line  = `[${ts}] [${label}] ${message}`;

    switch (level) {
      case LogLevel.Trace:
      case LogLevel.Debug:
        console.debug(line);
        break;
      case LogLevel.Warning:
        console.warn(line);
        break;
      case LogLevel.Error:
      case LogLevel.Critical:
        console.error(line);
        break;
      default:
        console.log(line);
        break;
    }
  }
}

// ─── NullLogger ───────────────────────────────────────────────────────────────

export class NullLogger implements ILogger {
  /** Singleton - no state, so one instance is enough. */
  static readonly instance: NullLogger = new NullLogger();

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  log(_logLevel: LogLevel, _message: string): void {
    // intentionally empty
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Reverse-map a LogLevel number to its string label. */
function logLevelName(level: LogLevel): string {
  const entry = (Object.entries(LogLevel) as [string, LogLevel][]).find(
    ([, v]) => v === level
  );
  return entry?.[0] ?? String(level);
}

/** Normalise a LogLevel number or ILogger into an ILogger. */
export function resolveLogger(logLevelOrLogger: LogLevel | ILogger): ILogger {
  if (typeof logLevelOrLogger === 'number') {
    return new ConsoleLogger(logLevelOrLogger);
  }
  if (
    logLevelOrLogger != null &&
    typeof logLevelOrLogger === 'object' &&
    typeof (logLevelOrLogger as ILogger).log === 'function'
  ) {
    return logLevelOrLogger as ILogger;
  }
  throw new TypeError(
    'configureLogging expects a LogLevel number or an object with a log(level, message) method.'
  );
}
