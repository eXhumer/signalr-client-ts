import type { Dispatcher } from 'undici';

const dispatchers: Dispatcher[] = [];

/** Register a dispatcher so the current test can reliably close it in teardown. */
export function trackDispatcher<T extends Dispatcher>(dispatcher: T): T {
  dispatchers.push(dispatcher);
  return dispatcher;
}

/** Force-close every dispatcher created by the current test, reporting all failures. */
export async function closeTrackedDispatchers(): Promise<void> {
  const tracked = dispatchers.splice(0);
  const results = await Promise.allSettled(
    // Failure-path tests intentionally leave timed-out operations in flight;
    // close() waits for those operations, whereas destroy() tears them down.
    tracked.map((dispatcher) => dispatcher.destroy()),
  );
  const errors = results
    .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
    .map((result) => result.reason);

  if (errors.length > 0) {
    throw new AggregateError(errors, 'Dispatcher cleanup failed');
  }
}
