import { startConsumer } from './kafka/consumer';

startConsumer().catch((err) => {
  console.error(JSON.stringify({ event: 'consumer_fatal_error', error: String(err) }));
  process.exit(1);
});
