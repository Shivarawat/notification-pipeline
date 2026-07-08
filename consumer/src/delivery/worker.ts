import type { NotificationEvent } from '../types';

export function deliver(event: NotificationEvent): void {
  console.log(JSON.stringify({
    event: 'notification_delivered',
    tenantId: event.tenantId,
    userId: event.userId,
    messageId: event.messageId,
    channel: event.channel,
    message: event.message,
    timestamp: new Date().toISOString(),
  }));
}
