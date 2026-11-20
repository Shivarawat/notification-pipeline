// Load test: node loadtest/run.mjs [events=20000] [concurrency=50] [tenants=10]
// Sends events to the producer, then waits until the consumer has processed all of them.
// Run the stack with a high RATE_LIMIT so the limiter doesn't drop test traffic:
//   RATE_LIMIT=1000000 docker compose up -d

const [EVENTS = 20000, CONCURRENCY = 50, TENANTS = 10] = process.argv.slice(2).map(Number);
const PRODUCER = process.env.PRODUCER_URL || 'http://localhost:3000';
const CONSUMER_METRICS = process.env.CONSUMER_METRICS_URL || 'http://localhost:9100/metrics';

const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];

async function readConsumerMetrics() {
  const text = await (await fetch(CONSUMER_METRICS)).text();
  let processed = 0;
  const buckets = new Map();
  for (const line of text.split('\n')) {
    if (line.startsWith('consumer_events_processed_total{')) processed += Number(line.split(' ').pop());
    const m = line.match(/^consumer_end_to_end_latency_seconds_bucket\{le="([^"]+)"\} (\d+)/);
    if (m) buckets.set(m[1], Number(m[2]));
  }
  return { processed, buckets };
}

// p95 from the change in histogram buckets during this run (upper bound of the bucket).
function histogramPercentile(before, after, p) {
  const entries = [...after.buckets].map(([le, n]) => [le, n - (before.buckets.get(le) || 0)]);
  const total = entries.find(([le]) => le === '+Inf')?.[1] || 0;
  const target = (p / 100) * total;
  for (const [le, n] of entries) if (n >= target) return le === '+Inf' ? '>60' : `<=${le}`;
  return 'n/a';
}

async function main() {
  const before = await readConsumerMetrics();
  const latencies = [];
  let sent = 0;
  let failed = 0;

  const started = performance.now();
  async function worker(id) {
    while (sent < EVENTS) {
      const i = sent++;
      const body = JSON.stringify({
        tenantId: `tenant${i % TENANTS}`,
        userId: `user${i % 1000}`,
        message: `load test ${i}`,
        channel: 'email',
      });
      const t0 = performance.now();
      try {
        const res = await fetch(`${PRODUCER}/notify`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body,
        });
        await res.arrayBuffer();
        if (res.status !== 202) failed++;
      } catch {
        failed++;
      }
      latencies.push(performance.now() - t0);
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, (_, id) => worker(id)));
  const sendSeconds = (performance.now() - started) / 1000;
  const accepted = EVENTS - failed;

  // Wait for the consumer to catch up (or stop after 10 minutes).
  let after = before;
  while (after.processed - before.processed < accepted && performance.now() - started < 600_000) {
    await new Promise((r) => setTimeout(r, 500));
    after = await readConsumerMetrics();
  }
  const totalSeconds = (performance.now() - started) / 1000;
  latencies.sort((a, b) => a - b);

  const result = {
    events: EVENTS,
    concurrency: CONCURRENCY,
    tenants: TENANTS,
    accepted,
    failed,
    producer_req_per_sec: Math.round(accepted / sendSeconds),
    producer_http_p50_ms: Math.round(percentile(latencies, 50)),
    producer_http_p95_ms: Math.round(percentile(latencies, 95)),
    processed: after.processed - before.processed,
    end_to_end_events_per_sec: Math.round((after.processed - before.processed) / totalSeconds),
    end_to_end_events_per_min: Math.round(((after.processed - before.processed) / totalSeconds) * 60),
    end_to_end_p95_seconds: histogramPercentile(before, after, 95),
    total_seconds: Number(totalSeconds.toFixed(1)),
  };
  console.log(JSON.stringify(result, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
