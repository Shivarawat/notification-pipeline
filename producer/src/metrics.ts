import { Counter, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry, prefix: 'producer_' });

export const notifyRequests = new Counter({
  name: 'producer_notify_requests_total',
  help: 'POST /notify requests by HTTP status code',
  labelNames: ['status'] as const,
  registers: [registry],
});

export const eventsPublished = new Counter({
  name: 'producer_events_published_total',
  help: 'Events successfully written to Kafka',
  registers: [registry],
});

export const publishDuration = new Histogram({
  name: 'producer_publish_duration_seconds',
  help: 'Time to write one event to Kafka',
  buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
  registers: [registry],
});
