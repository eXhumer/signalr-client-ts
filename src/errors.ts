/**
 * errors.ts
 *
 * Typed error classes for the SignalR client.  Each has a distinct `name`
 * so callers can use `instanceof` or switch on `error.name` to distinguish
 * error categories at runtime.
 */

// ─── Base ────────────────────────────────────────────────────────────────────

/** Thrown when a hub method invocation fails with a server-side error. */
export class HubError extends Error {
  override readonly name = 'HubError' as const;

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown when an in-flight operation is cancelled (e.g. connection closed). */
export class AbortError extends Error {
  override readonly name = 'AbortError' as const;

  constructor(message = 'The operation was aborted.') {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown on network-level transport failures. */
export class TransportError extends Error {
  override readonly name = 'TransportError' as const;
  readonly statusCode: number | undefined;

  constructor(message: string, statusCode?: number) {
    super(message);
    this.statusCode = statusCode;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown when the SignalR protocol handshake is rejected by the server. */
export class HandshakeError extends Error {
  override readonly name = 'HandshakeError' as const;

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** Thrown when no acceptable transport could be negotiated. */
export class UnsupportedTransportError extends Error {
  override readonly name = 'UnsupportedTransportError' as const;
  readonly transport: number | null;

  constructor(message: string, transport: number | null) {
    super(message);
    this.transport = transport;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Thrown internally when negotiate returns an Azure SignalR redirect.
 * Handled transparently by HubConnection - not exposed to callers.
 * @internal
 */
export class RedirectError extends Error {
  override readonly name = 'RedirectError' as const;
  readonly url: string;
  readonly accessToken: string | undefined;

  constructor(url: string, accessToken?: string) {
    super(`Redirected to ${url}`);
    this.url         = url;
    this.accessToken = accessToken;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ─── Type guard helpers ───────────────────────────────────────────────────────

export function isHubError(e: unknown): e is HubError {
  return e instanceof HubError;
}

export function isAbortError(e: unknown): e is AbortError {
  return e instanceof AbortError;
}

export function isTransportError(e: unknown): e is TransportError {
  return e instanceof TransportError;
}
