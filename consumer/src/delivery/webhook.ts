import type { NotificationEvent } from '../types';
import { deliveryAttempts, webhookDuration } from '../metrics';

export class DeliveryError extends Error {
  constructor(message: string, readonly retryable: boolean) {
    super(message);
  }
}

// POSTs the event to the tenant-facing webhook. 5xx, 429 and network errors are retryable; other 4xx are not.
export function createWebhookDelivery(url: string, timeoutMs = 2000) {
  return async function deliver(event: NotificationEvent): Promise<void> {
    const endTimer = webhookDuration.startTimer();
    let res: Response;
    try {
      res = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': event.messageId },
        body: JSON.stringify(event),
        signal: AbortSignal.timeout(timeoutMs),
      });
      await res.arrayBuffer();
    } catch (err) {
      deliveryAttempts.inc({ result: 'retryable_error' });
      throw new DeliveryError(`webhook unreachable: ${String(err)}`, true);
    } finally {
      endTimer();
    }

    if (res.ok) {
      deliveryAttempts.inc({ result: 'success' });
      return;
    }
    const retryable = res.status >= 500 || res.status === 429;
    deliveryAttempts.inc({ result: retryable ? 'retryable_error' : 'permanent_error' });
    throw new DeliveryError(`webhook returned ${res.status}`, retryable);
  };
}
