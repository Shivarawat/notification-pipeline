import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateNotifyRequest } from './validate';

const valid = { tenantId: 'acme', userId: 'u1', message: 'hi', channel: 'email' };

test('accepts a valid request', () => {
  assert.equal(validateNotifyRequest(valid), null);
});

test('accepts a client messageId', () => {
  assert.equal(validateNotifyRequest({ ...valid, messageId: 'order-42:retry.1' }), null);
});

test('rejects missing fields and an empty body', () => {
  assert.match(validateNotifyRequest({ ...valid, userId: undefined }) ?? '', /required/);
  assert.match(validateNotifyRequest(undefined) ?? '', /required/);
});

test('rejects an unknown channel', () => {
  assert.match(validateNotifyRequest({ ...valid, channel: 'fax' }) ?? '', /channel must be one of/);
});

test('rejects a malformed messageId', () => {
  assert.ok(validateNotifyRequest({ ...valid, messageId: 'has spaces' }));
  assert.ok(validateNotifyRequest({ ...valid, messageId: 42 }));
  assert.ok(validateNotifyRequest({ ...valid, messageId: 'x'.repeat(129) }));
});
