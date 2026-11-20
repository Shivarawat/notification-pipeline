import { Kafka } from 'kafkajs';
import type { NotificationEvent } from '../types';
import { isDuplicate } from '../redis/idempotency';
import { isRateLimited } from '../redis/ratelimit';
import { logEvent } from '../db/logger';
import { deliver } from '../delivery/worker';
import { endToEndLatency, eventsConsumed, eventsProcessed } from '../metrics';
import { startLagReporter } from './lag';

const kafka = new Kafka({
  clientId: 'notification-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: { retries: 20, initialRetryTime: 1000 },
});

const GROUP_ID = 'notification-group';

export async function startConsumer(): Promise<void> {
  const consumer = kafka.consumer({ groupId: GROUP_ID });
  await consumer.connect();

  const admin = kafka.admin();
  await admin.connect();
  startLagReporter(admin, GROUP_ID);

  await consumer.subscribe({ topic: /^notifications\..+/, fromBeginning: false });

  console.log(JSON.stringify({ event: 'kafka_consumer_connected' }));

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const event: NotificationEvent = JSON.parse(message.value.toString());
      const start = Date.now();
      eventsConsumed.inc();

      if (await isDuplicate(event.messageId)) {
        console.log(JSON.stringify({ event: 'duplicate_skipped', messageId: event.messageId }));
        await logEvent(event, 'duplicate');
        eventsProcessed.inc({ outcome: 'duplicate' });
        return;
      }

      if (await isRateLimited(event.tenantId)) {
        console.log(JSON.stringify({ event: 'rate_limited', tenantId: event.tenantId, messageId: event.messageId }));
        await logEvent(event, 'rate_limited');
        eventsProcessed.inc({ outcome: 'rate_limited' });
        return;
      }

      deliver(event);
      await logEvent(event, 'delivered');
      eventsProcessed.inc({ outcome: 'delivered' });
      endToEndLatency.observe((Date.now() - Date.parse(event.timestamp)) / 1000);

      console.log(JSON.stringify({
        event: 'message_processed',
        messageId: event.messageId,
        tenantId: event.tenantId,
        durationMs: Date.now() - start,
      }));
    },
  });
}
