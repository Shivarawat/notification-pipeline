import { Kafka } from 'kafkajs';
import type { NotificationEvent } from '../types';
import { isDuplicate } from '../redis/idempotency';
import { isRateLimited } from '../redis/ratelimit';
import { logEvent } from '../db/logger';
import { deliver } from '../delivery/worker';

const kafka = new Kafka({
  clientId: 'notification-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: { retries: 20, initialRetryTime: 1000 },
});

export async function startConsumer(): Promise<void> {
  const consumer = kafka.consumer({ groupId: 'notification-group' });
  await consumer.connect();

  await consumer.subscribe({ topic: /^notifications\..+/, fromBeginning: false });

  console.log(JSON.stringify({ event: 'kafka_consumer_connected' }));

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const event: NotificationEvent = JSON.parse(message.value.toString());
      const start = Date.now();

      if (await isDuplicate(event.messageId)) {
        console.log(JSON.stringify({ event: 'duplicate_skipped', messageId: event.messageId }));
        await logEvent(event, 'duplicate');
        return;
      }

      if (await isRateLimited(event.tenantId)) {
        console.log(JSON.stringify({ event: 'rate_limited', tenantId: event.tenantId, messageId: event.messageId }));
        await logEvent(event, 'rate_limited');
        return;
      }

      deliver(event);
      await logEvent(event, 'delivered');

      console.log(JSON.stringify({
        event: 'message_processed',
        messageId: event.messageId,
        tenantId: event.tenantId,
        durationMs: Date.now() - start,
      }));
    },
  });
}
