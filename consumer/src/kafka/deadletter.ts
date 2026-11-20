import type { Kafka, Producer } from 'kafkajs';
import type { NotificationEvent } from '../types';
import { DEAD_LETTER_TOPIC } from './topics';

// Events that still fail after all retries go here with the reason, so they can be inspected and replayed.
export async function createDeadLetterPublisher(kafka: Kafka) {
  const producer: Producer = kafka.producer();
  await producer.connect();
  return async function deadLetter(event: NotificationEvent, error: string, attempts: number): Promise<void> {
    await producer.send({
      topic: DEAD_LETTER_TOPIC,
      messages: [{
        key: event.tenantId,
        value: JSON.stringify({ event, error, attempts, failedAt: new Date().toISOString() }),
      }],
    });
  };
}
