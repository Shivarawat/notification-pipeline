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

// Tenants are processed in parallel; each tenant's events stay in their original order.
export async function processBatch(events: NotificationEvent[], deps: ProcessorDeps): Promise<ProcessResult[]> {
  const byTenant = new Map<string, NotificationEvent[]>();
  for (const event of events) {
    const queue = byTenant.get(event.tenantId) ?? [];
    queue.push(event);
    byTenant.set(event.tenantId, queue);
  }

  const perTenant = await Promise.all(
    [...byTenant.values()].map(async (queue) => {
      const results: ProcessResult[] = [];
      for (const event of queue) results.push(await processEvent(event, deps));
      return results;
    })
  );
  return perTenant.flat();
}
