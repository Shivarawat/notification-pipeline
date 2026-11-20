import type Redis from 'ioredis';

const DELIVERED_TTL_SECONDS = 24 * 60 * 60;

// A message is only marked after it was delivered, so a failed delivery can be retried.
// Check-then-mark is safe here: a tenant's events share one partition and are processed one at a time,
// so a duplicate is always seen after the first copy has finished.
export function createIdempotencyStore(redis: Redis) {
  const key = (messageId: string) => `delivered:${messageId}`;
  return {
    async isDelivered(messageId: string): Promise<boolean> {
      return (await redis.exists(key(messageId))) === 1;
    },
    async markDelivered(messageId: string): Promise<void> {
      await redis.set(key(messageId), '1', 'EX', DELIVERED_TTL_SECONDS);
    },
  };
}
