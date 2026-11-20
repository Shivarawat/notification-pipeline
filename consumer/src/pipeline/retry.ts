export interface RetryOptions {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

export class RetriesExhaustedError extends Error {
  constructor(readonly attempts: number, readonly lastError: unknown) {
    super(`failed after ${attempts} attempt(s): ${String(lastError)}`);
  }
}

// Errors can opt out of retrying (e.g. HTTP 400: sending it again won't help).
export function isRetryable(err: unknown): boolean {
  return !(err instanceof Error && (err as { retryable?: boolean }).retryable === false);
}

// Exponential backoff with full jitter: a random delay in [0, min(max, base * 2^(attempt-1))].
// Jitter stops many failing consumers from retrying in lockstep.
export function backoffDelay(attempt: number, opts: RetryOptions): number {
  const ceiling = Math.min(opts.maxDelayMs, opts.baseDelayMs * 2 ** (attempt - 1));
  return Math.floor((opts.random ?? Math.random)() * ceiling);
}

const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// Runs fn until it succeeds, a non-retryable error is thrown, or maxAttempts is reached.
// Resolves with the number of attempts used.
export async function withRetry(fn: () => Promise<void>, opts: RetryOptions): Promise<number> {
  const sleep = opts.sleep ?? defaultSleep;
  for (let attempt = 1; ; attempt++) {
    try {
      await fn();
      return attempt;
    } catch (err) {
      if (attempt >= opts.maxAttempts || !isRetryable(err)) throw new RetriesExhaustedError(attempt, err);
      await sleep(backoffDelay(attempt, opts));
    }
  }
}
