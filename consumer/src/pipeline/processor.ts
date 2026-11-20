import type { NotificationEvent } from '../types';
import { RetriesExhaustedError, RetryOptions, withRetry } from './retry';

export type Outcome = 'delivered' | 'duplicate' | 'rate_limited' | 'dead_lettered';

export interface ProcessResult {
  event: NotificationEvent;
  outcome: Outcome;
  attempts: number;
  error?: string;
}

// Everything the processor needs from the outside world, so tests can pass in fakes.
export interface ProcessorDeps {
  isDelivered(messageId: string): Promise<boolean>;
  markDelivered(messageId: string): Promise<void>;
  isRateLimited(tenantId: string, messageId: string): Promise<boolean>;
  deliver(event: NotificationEvent): Promise<void>;
  deadLetter(event: NotificationEvent, error: string, attempts: number): Promise<void>;
  retry: RetryOptions;
}

export async function processEvent(event: NotificationEvent, deps: ProcessorDeps): Promise<ProcessResult> {
  if (await deps.isDelivered(event.messageId)) {
    return { event, outcome: 'duplicate', attempts: 0 };
  }
  if (await deps.isRateLimited(event.tenantId, event.messageId)) {
    return { event, outcome: 'rate_limited', attempts: 0 };
  }
  try {
    const attempts = await withRetry(() => deps.deliver(event), deps.retry);
    await deps.markDelivered(event.messageId);
    return { event, outcome: 'delivered', attempts };
  } catch (err) {
    const attempts = err instanceof RetriesExhaustedError ? err.attempts : 1;
    const error = err instanceof RetriesExhaustedError ? String(err.lastError) : String(err);
    await deps.deadLetter(event, error, attempts);
    return { event, outcome: 'dead_lettered', attempts, error };
  }
}

// Ordering is kept per user: one user's events run one after another, different users run in parallel
// (at most maxConcurrency at a time). Duplicates share a messageId and therefore a user, so the
// check-then-mark idempotency in processEvent never races with itself.
export async function processBatch(
  events: NotificationEvent[],
  deps: ProcessorDeps,
  maxConcurrency = 50
): Promise<ProcessResult[]> {
  const byUser = new Map<string, NotificationEvent[]>();
  for (const event of events) {
    const key = `${event.tenantId}:${event.userId}`;
    const queue = byUser.get(key) ?? [];
    queue.push(event);
    byUser.set(key, queue);
  }

  const queues = [...byUser.values()];
  const results: ProcessResult[][] = new Array(queues.length);
  let next = 0;

  async function worker(): Promise<void> {
    while (next < queues.length) {
      const index = next++;
      const queueResults: ProcessResult[] = [];
      for (const event of queues[index]) queueResults.push(await processEvent(event, deps));
      results[index] = queueResults;
    }
  }

  await Promise.all(Array.from({ length: Math.min(maxConcurrency, queues.length) }, worker));
  return results.flat();
}
