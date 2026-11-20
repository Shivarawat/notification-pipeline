import type { NotificationEvent } from '../types';

export type Outcome = 'delivered' | 'duplicate' | 'rate_limited' | 'dead_lettered';

export interface ProcessResult {
  event: NotificationEvent;
  outcome: Outcome;
  attempts: number;
  error?: string;
}

// Everything the processor needs from the outside world, so tests can pass in fakes.
export interface ProcessorDeps {
  isDuplicate(messageId: string): Promise<boolean>;
  isRateLimited(tenantId: string): Promise<boolean>;
  deliver(event: NotificationEvent): Promise<void>;
}

export async function processEvent(event: NotificationEvent, deps: ProcessorDeps): Promise<ProcessResult> {
  if (await deps.isDuplicate(event.messageId)) {
    return { event, outcome: 'duplicate', attempts: 0 };
  }
  if (await deps.isRateLimited(event.tenantId)) {
    return { event, outcome: 'rate_limited', attempts: 0 };
  }
  await deps.deliver(event);
  return { event, outcome: 'delivered', attempts: 1 };
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
