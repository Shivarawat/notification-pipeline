import type Redis from 'ioredis';

// Sliding-window log: each allowed event is a member of a sorted set scored by its timestamp.
// The script runs atomically in Redis, so concurrent consumers can't both slip past the limit.
const SLIDING_WINDOW_SCRIPT = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
if redis.call('ZCARD', key) >= limit then
  return 1
end
redis.call('ZADD', key, now, ARGV[4])
redis.call('PEXPIRE', key, window)
return 0
`;

export interface RateLimiterOptions {
  limit: number;
  windowMs: number;
  now?: () => number;
}

export function createRateLimiter(redis: Redis, { limit, windowMs, now = Date.now }: RateLimiterOptions) {
  return {
    // Returns true if this event would exceed the tenant's limit in the last windowMs.
    async isRateLimited(tenantId: string, messageId: string): Promise<boolean> {
      const result = await redis.eval(
        SLIDING_WINDOW_SCRIPT,
        1,
        `ratelimit:${tenantId}`,
        now(),
        windowMs,
        limit,
        messageId
      );
      return result === 1;
    },
  };
}
