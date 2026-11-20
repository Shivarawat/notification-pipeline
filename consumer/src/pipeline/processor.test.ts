import { test } from 'node:test';
import assert from 'node:assert/strict';
import { processBatch, processEvent, ProcessorDeps } from './processor';
import type { NotificationEvent } from '../types';

function event(id: string, userId = 'u1', tenantId = 't1'): NotificationEvent {
  return { messageId: id, tenantId, userId, message: `msg ${id}`, channel: 'email', timestamp: new Date().toISOString() };
}

// In-memory stand-ins for Redis, the webhook and the dead-letter topic.
function fakeDeps(overrides: Partial<ProcessorDeps> = {}) {
  const delivered = new Set<string>();
  const deliveredOrder: string[] = [];
  const deadLettered: string[] = [];
  const deps: ProcessorDeps = {
    isDelivered: async (id) => delivered.has(id),
    markDelivered: async (id) => { delivered.add(id); },
    isRateLimited: async () => false,
    deliver: async (e) => { deliveredOrder.push(e.messageId); },
    deadLetter: async (e) => { deadLettered.push(e.messageId); },
    retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 1, sleep: async () => {} },
    ...overrides,
  };
  return { deps, delivered, deliveredOrder, deadLettered };
}

test('delivers a new event and marks it delivered', async () => {
  const { deps, delivered } = fakeDeps();
  const result = await processEvent(event('a'), deps);
  assert.equal(result.outcome, 'delivered');
  assert.equal(result.attempts, 1);
  assert.ok(delivered.has('a'));
});

test('skips an event that was already delivered', async () => {
  const { deps, deliveredOrder } = fakeDeps();
  await processEvent(event('a'), deps);
  const second = await processEvent(event('a'), deps);
  assert.equal(second.outcome, 'duplicate');
  assert.deepEqual(deliveredOrder, ['a']);
});

test('does not deliver rate-limited events', async () => {
  const { deps, deliveredOrder } = fakeDeps({ isRateLimited: async () => true });
  const result = await processEvent(event('a'), deps);
  assert.equal(result.outcome, 'rate_limited');
  assert.deepEqual(deliveredOrder, []);
});

test('retries a failing delivery and records the attempts', async () => {
  let calls = 0;
  const { deps } = fakeDeps({ deliver: async () => { if (++calls < 3) throw new Error('503'); } });
  const result = await processEvent(event('a'), deps);
  assert.equal(result.outcome, 'delivered');
  assert.equal(result.attempts, 3);
});

test('dead-letters after all retries fail and does not mark it delivered', async () => {
  const { deps, delivered, deadLettered } = fakeDeps({ deliver: async () => { throw new Error('503'); } });
  const result = await processEvent(event('a'), deps);
  assert.equal(result.outcome, 'dead_lettered');
  assert.equal(result.attempts, 3);
  assert.deepEqual(deadLettered, ['a']);
  // Not marked, so a later replay of this message is retried instead of treated as a duplicate.
  assert.equal(delivered.has('a'), false);
});

test('keeps each user\'s events in order while users run in parallel', async () => {
  const { deps, deliveredOrder } = fakeDeps({
    // The first event of each user is slow, so a parallel-but-unordered design would reorder them.
    deliver: async (e) => {
      if (e.message.endsWith('1')) await new Promise((r) => setTimeout(r, 20));
      deliveredOrder.push(e.messageId);
    },
  });
  const events = [event('alice-1', 'alice'), event('bob-1', 'bob'), event('alice-2', 'alice'), event('bob-2', 'bob')];
  const results = await processBatch(events, deps);
  assert.equal(results.length, 4);
  assert.ok(deliveredOrder.indexOf('alice-1') < deliveredOrder.indexOf('alice-2'));
  assert.ok(deliveredOrder.indexOf('bob-1') < deliveredOrder.indexOf('bob-2'));
});

test('a duplicate inside the same batch is delivered once', async () => {
  const { deps, deliveredOrder } = fakeDeps();
  const results = await processBatch([event('a'), event('a')], deps);
  assert.deepEqual(results.map((r) => r.outcome), ['delivered', 'duplicate']);
  assert.deepEqual(deliveredOrder, ['a']);
});
