import { Kafka, Producer } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'notification-producer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

let producer: Producer;

export async function getProducer(): Promise<Producer> {
  if (!producer) {
    producer = kafka.producer();
    await producer.connect();
    console.log(JSON.stringify({ event: 'kafka_producer_connected' }));
  }
  return producer;
}

export async function publishNotification(tenantId: string, payload: string): Promise<void> {
  const p = await getProducer();
  await p.send({
    topic: `notifications.${tenantId}`,
    messages: [{ value: payload }],
  });
}
