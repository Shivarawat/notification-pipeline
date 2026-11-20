import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RetriesExhaustedError, backoffDelay, withRetry } from './retry';

const noSleep = { sleep: async () => {} };
const opts = { maxAttempts: 4, baseDelayMs: 100, maxDelayMs: 1000, ...noSleep };

test('backoff ceiling doubles each attempt and is capped', () => {
  const max = { ...opts, random: () => 0.9999 };
  assert.equal(backoffDelay(1, max), 99);
  assert.equal(backoffDelay(2, max), 199);
  assert.equal(backoffDelay(3, max), 399);
  assert.equal(backoffDelay(10, max), 999);
});

test('full jitter can pick zero delay', () => {
  assert.equal(backoffDelay(3, { ...opts, random: () => 0 }), 0);
});

test('returns the attempt number that succeeded', async () => {
  let calls = 0;
  const attempts = await withRetry(async () => {
    calls++;
    if (calls < 3) throw new Error('temporary');
  }, opts);
  assert.equal(attempts, 3);
});

test('gives up after maxAttempts', async () => {
  let calls = 0;
  await assert.rejects(
    withRetry(async () => {
      calls++;
      throw new Error('down');
    }, opts),
    (err: unknown) => err instanceof RetriesExhaustedError && err.attempts === 4
  );
  assert.equal(calls, 4);
});

test('does not retry errors marked non-retryable', async () => {
  let calls = 0;
  const permanent = Object.assign(new Error('400 bad request'), { retryable: false });
  await assert.rejects(withRetry(async () => {
    calls++;
    throw permanent;
  }, opts));
  assert.equal(calls, 1);
});

test('waits between attempts using the backoff delay', async () => {
  const waits: number[] = [];
  await assert.rejects(withRetry(async () => { throw new Error('down'); }, {
    ...opts,
    random: () => 0.5,
    sleep: async (ms) => { waits.push(ms); },
  }));
  assert.deepEqual(waits, [50, 100, 200]);
});
