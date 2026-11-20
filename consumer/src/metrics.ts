import http from 'http';
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry, prefix: 'consumer_' });

export type Outcome = 'delivered' | 'duplicate' | 'rate_limited' | 'dead_lettered';

export const eventsConsumed = new Counter({
  name: 'consumer_events_consumed_total',
  help: 'Messages read from Kafka',
  registers: [registry],
});

export const eventsProcessed = new Counter({
  name: 'consumer_events_processed_total',
  help: 'Messages fully handled, by outcome',
  labelNames: ['outcome'] as const,
  registers: [registry],
});

export const endToEndLatency = new Histogram({
  name: 'consumer_end_to_end_latency_seconds',
  help: 'Time from the producer accepting an event to it being delivered',
  buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60],
  registers: [registry],
});

export const batchDuration = new Histogram({
  name: 'consumer_batch_duration_seconds',
  help: 'Time to process one Kafka batch',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [registry],
});

export const consumerLag = new Gauge({
  name: 'consumer_lag_messages',
  help: 'Messages written to Kafka but not yet committed by this consumer group',
  registers: [registry],
});

export function startMetricsServer(port: number): void {
  http
    .createServer(async (req, res) => {
      if (req.url !== '/metrics') {
        res.writeHead(404).end();
        return;
      }
      res.writeHead(200, { 'Content-Type': registry.contentType });
      res.end(await registry.metrics());
    })
    .listen(port, () => console.log(JSON.stringify({ event: 'metrics_server_started', port })));
}
