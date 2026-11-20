import { startMetricsServer } from './metrics';
import { startConsumer } from './kafka/consumer';

startMetricsServer(Number(process.env.METRICS_PORT || 9100));

startConsumer().catch((err) => {
  console.error(JSON.stringify({ event: 'consumer_fatal_error', error: String(err) }));
  process.exit(1);
});
