// Sends events to the producer as fast as possible and exits: node loadtest/send-only.mjs [events] [concurrency] [tenants]
const [EVENTS = 60000, CONCURRENCY = 100, TENANTS = 10] = process.argv.slice(2).map(Number);
const PRODUCER = process.env.PRODUCER_URL || 'http://localhost:3000';
let sent = 0;
let failed = 0;
async function worker() {
  while (sent < EVENTS) {
    const i = sent++;
    const res = await fetch(`${PRODUCER}/notify`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ tenantId: `tenant${i % TENANTS}`, userId: `user${i % 1000}`, message: `drain ${i}`, channel: 'email' }),
    }).catch(() => null);
    if (!res || res.status !== 202) failed++;
    await res?.arrayBuffer();
  }
}
await Promise.all(Array.from({ length: CONCURRENCY }, worker));
if (failed) console.error(`${failed} sends failed`);
