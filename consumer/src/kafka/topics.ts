import type { Kafka } from 'kafkajs';

// One topic for all tenants. The tenant ID is the message key, so each tenant's events
// land on the same partition (ordered), and a new tenant needs no new topic.
export const NOTIFICATIONS_TOPIC = 'notifications';
export const NOTIFICATIONS_PARTITIONS = 6;

export async function ensureTopics(kafka: Kafka): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: NOTIFICATIONS_TOPIC, numPartitions: NOTIFICATIONS_PARTITIONS }],
    });
  } finally {
    await admin.disconnect();
  }
}
