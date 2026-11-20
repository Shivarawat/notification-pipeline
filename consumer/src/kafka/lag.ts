import type { Admin } from 'kafkajs';
import { consumerLag } from '../metrics';

const TOPIC_PATTERN = /^notifications\..+/;

// Lag = latest offset in each partition minus the offset this group has committed.
export async function updateConsumerLag(admin: Admin, groupId: string): Promise<void> {
  const topics = (await admin.listTopics()).filter((t) => TOPIC_PATTERN.test(t));
  if (topics.length === 0) {
    consumerLag.set(0);
    return;
  }

  const committed = await admin.fetchOffsets({ groupId, topics });
  let lag = 0;

  for (const { topic, partitions } of committed) {
    const latest = await admin.fetchTopicOffsets(topic);
    for (const p of partitions) {
      const end = Number(latest.find((l) => l.partition === p.partition)?.offset ?? 0);
      const done = Number(p.offset) < 0 ? 0 : Number(p.offset);
      lag += Math.max(0, end - done);
    }
  }
  consumerLag.set(lag);
}

export function startLagReporter(admin: Admin, groupId: string, intervalMs = 5000): void {
  setInterval(() => {
    updateConsumerLag(admin, groupId).catch((err) =>
      console.error(JSON.stringify({ event: 'lag_check_failed', error: String(err) }))
    );
  }, intervalMs);
}
