import { redis } from './idempotency';

const RATE_LIMIT = parseInt(process.env.RATE_LIMIT || '100');
const WINDOW_SECONDS = 60;

// Returns true if tenant is over rate limit
export async function isRateLimited(tenantId: string): Promise<boolean> {
  const key = `ratelimit:${tenantId}`;
  const count = await redis.incr(key);
  if (count === 1) {
    await redis.expire(key, WINDOW_SECONDS);
  }
  return count > RATE_LIMIT;
}
