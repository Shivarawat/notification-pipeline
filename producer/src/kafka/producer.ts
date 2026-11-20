import { Kafka, Producer } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'notification-producer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  retry: { retries: 10, initialRetryTime: 300 },
});

// Cache the connection promise, not the producer, so concurrent requests share one connect().
// If the broker goes away, the cache is cleared and the next request reconnects.
let connecting: Promise<Producer> | null = null;

export function getProducer(): Promise<Producer> {
  if (!connecting) {
    const producer = kafka.producer({ allowAutoTopicCreation: true });
    producer.on(producer.events.DISCONNECT, () => {
      console.log(JSON.stringify({ event: 'kafka_producer_disconnected' }));
      connecting = null;
    });
    connecting = producer
      .connect()
      .then(() => {
        console.log(JSON.stringify({ event: 'kafka_producer_connected' }));
        return producer;
      })
      .catch((err) => {
        connecting = null;
        throw err;
      });
  }
  return connecting;
}

export async function publishNotification(tenantId: string, payload: string): Promise<void> {
  const p = await getProducer();
  try {
    await p.send({
      topic: `notifications.${tenantId}`,
      messages: [{ value: payload }],
    });
  } catch (err) {
    // A dead connection won't heal by itself: drop it so the next request reconnects.
    if (String(err).includes('disconnected')) connecting = null;
    throw err;
  }
}
