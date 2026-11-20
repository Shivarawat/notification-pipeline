// Runs against a real Redis (REDIS_URL, default localhost:6379). Skipped if Redis isn't reachable.
import { after, before, describe, test } from 'node:test';
import assert from 'node:assert/strict';
import Redis from 'ioredis';
import { createRateLimiter } from './ratelimit';
import { createIdempotencyStore } from './idempotency';

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  lazyConnect: true,
  maxRetriesPerRequest: 1,
  retryStrategy: () => null,
});
let available = false;

before(async () => {
  try {
    await redis.connect();
    available = (await redis.ping()) === 'PONG';
  } catch {
    available = false;
  }
});
after(() => redis.disconnect());

const unique = (name: string) => `${name}-${Date.now()}-${Math.random().toString(36).slice(2)}`;

describe('sliding-window rate limiter', () => {
  test('allows up to the limit inside the window, then blocks', async (t) => {
    if (!available) return t.skip('Redis not available');
    let now = 1_000_000;
    const limiter = createRateLimiter(redis, { limit: 3, windowMs: 1000, now: () => now });
    const tenant = unique('tenant');
    const results: boolean[] = [];
    for (let i = 0; i < 4; i++) {
      results.push(await limiter.isRateLimited(tenant, `m${i}`));
      now += 100;
    }
    assert.deepEqual(results, [false, false, false, true]);
  });

  test('frees capacity as old events slide out (unlike a fixed window)', async (t) => {
    if (!available) return t.skip('Redis not available');
    let now = 2_000_000;
    const limiter = createRateLimiter(redis, { limit: 3, windowMs: 1000, now: () => now });
    const tenant = unique('tenant');
    for (const at of [0, 400, 800]) {
      now = 2_000_000 + at;
      assert.equal(await limiter.isRateLimited(tenant, `m${at}`), false);
    }
    now = 2_000_000 + 900;
    assert.equal(await limiter.isRateLimited(tenant, 'm900'), true, 'still 3 events in the last second');
    now = 2_000_000 + 1001;
    assert.equal(await limiter.isRateLimited(tenant, 'm1001'), false, 'the event at t=0 has slid out');
    now = 2_000_000 + 1002;
    assert.equal(await limiter.isRateLimited(tenant, 'm1002'), true, 'only one slot was freed');
  });

  test('limits each tenant separately', async (t) => {
    if (!available) return t.skip('Redis not available');
    const limiter = createRateLimiter(redis, { limit: 1, windowMs: 60_000 });
    const [a, b] = [unique('a'), unique('b')];
    assert.equal(await limiter.isRateLimited(a, '1'), false);
    assert.equal(await limiter.isRateLimited(a, '2'), true);
    assert.equal(await limiter.isRateLimited(b, '1'), false);
  });
});

describe('idempotency store', () => {
  test('a message is only a duplicate after it was marked delivered', async (t) => {
    if (!available) return t.skip('Redis not available');
    const store = createIdempotencyStore(redis);
    const id = unique('msg');
    assert.equal(await store.isDelivered(id), false);
    assert.equal(await store.isDelivered(id), false, 'checking does not mark it');
    await store.markDelivered(id);
    assert.equal(await store.isDelivered(id), true);
    assert.ok((await redis.ttl(`delivered:${id}`)) > 0, 'the marker expires');
  });
});
