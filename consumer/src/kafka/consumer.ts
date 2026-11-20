import { Kafka } from 'kafkajs';
import type { NotificationEvent } from '../types';
import { redis } from '../redis/client';
import { createIdempotencyStore } from '../redis/idempotency';
import { createRateLimiter } from '../redis/ratelimit';
import { logResults } from '../db/logger';
import { createWebhookDelivery } from '../delivery/webhook';
import { createDeadLetterPublisher } from './deadletter';
import { ensureSchema } from '../db/schema';
import { processBatch, ProcessorDeps } from '../pipeline/processor';
import { batchDuration, endToEndLatency, eventsConsumed, eventsProcessed } from '../metrics';
import { startLagReporter } from './lag';
import { NOTIFICATIONS_PARTITIONS, NOTIFICATIONS_TOPIC, ensureTopics } from './topics';

const kafka = new Kafka({
  clientId: 'notification-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: { retries: 20, initialRetryTime: 1000 },
});

const GROUP_ID = 'notification-group';

const idempotency = createIdempotencyStore(redis);
const rateLimiter = createRateLimiter(redis, {
  limit: Number(process.env.RATE_LIMIT || 100),
  windowMs: 60_000,
});


function parse(value: Buffer | null): NotificationEvent | null {
  if (!value) return null;
  try {
    return JSON.parse(value.toString()) as NotificationEvent;
  } catch {
    console.error(JSON.stringify({ event: 'invalid_message_skipped', value: value.toString().slice(0, 200) }));
    return null;
  }
}

export async function startConsumer(): Promise<void> {
  await ensureTopics(kafka);
  await ensureSchema();

  const deps: ProcessorDeps = {
    isDelivered: idempotency.isDelivered,
    markDelivered: idempotency.markDelivered,
    isRateLimited: rateLimiter.isRateLimited,
    deliver: createWebhookDelivery(process.env.WEBHOOK_URL || 'http://localhost:8081/webhook'),
    deadLetter: await createDeadLetterPublisher(kafka),
    retry: {
      maxAttempts: Number(process.env.DELIVERY_MAX_ATTEMPTS || 4),
      baseDelayMs: 100,
      maxDelayMs: 2000,
    },
  };

  const consumer = kafka.consumer({ groupId: GROUP_ID });
  await consumer.connect();

  const admin = kafka.admin();
  await admin.connect();
  startLagReporter(admin, GROUP_ID);

  // fromBeginning: a partition with no committed offset starts at the oldest message,
  // so nothing published before the group's first commit is skipped.
  await consumer.subscribe({ topic: NOTIFICATIONS_TOPIC, fromBeginning: true });
  console.log(JSON.stringify({ event: 'kafka_consumer_connected' }));

  await consumer.run({
    partitionsConsumedConcurrently: NOTIFICATIONS_PARTITIONS,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      if (!isRunning() || isStale() || batch.messages.length === 0) return;

      const endTimer = batchDuration.startTimer();
      const events = batch.messages.map((m) => parse(m.value)).filter((e): e is NotificationEvent => e !== null);
      eventsConsumed.inc(batch.messages.length);

      const results = await processBatch(events, deps, Number(process.env.DELIVERY_CONCURRENCY || 50));
      await logResults(results);

      const now = Date.now();
      for (const r of results) {
        eventsProcessed.inc({ outcome: r.outcome });
        if (r.outcome === 'delivered') endToEndLatency.observe((now - Date.parse(r.event.timestamp)) / 1000);
      }

      // Only mark the batch done after it's logged: a crash before this line replays the batch (at-least-once).
      resolveOffset(batch.messages[batch.messages.length - 1].offset);
      await heartbeat();
      endTimer();
    },
  });
}
