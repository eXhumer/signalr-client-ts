/**
 * helpers/mock-logger.ts
 *
 * Captures log entries for assertion in tests.
 */

import type { ILogger } from '../../src/interfaces.js';
import type { LogLevel } from '../../src/constants.js';

export interface LogEntry {
  readonly level:   LogLevel;
  readonly message: string;
}

export class MockLogger implements ILogger {
  readonly entries: LogEntry[] = [];

  log(level: LogLevel, message: string): void {
    this.entries.push({ level, message });
  }

  /** Return all messages at or above `minLevel`. */
  messagesAt(minLevel: LogLevel): string[] {
    return this.entries
      .filter((e) => e.level >= minLevel)
      .map((e) => e.message);
  }

  /** True if any log entry contains `substring`. */
  hasMessage(substring: string): boolean {
    return this.entries.some((e) => e.message.includes(substring));
  }

  clear(): void {
    this.entries.length = 0;
  }
}
