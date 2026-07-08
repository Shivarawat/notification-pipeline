import Redis from 'ioredis';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

// Returns true if already processed (duplicate), false if new
export async function isDuplicate(messageId: string): Promise<boolean> {
  const key = `msg:${messageId}`;
  const result = await redis.set(key, '1', 'EX', 86400, 'NX');
  return result === null; // null means key already existed
}

export { redis };
