import { Pool } from 'pg';
import type { NotificationEvent } from '../types';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL ||
    'postgresql://notif_user:notif_pass@localhost:5432/notifications',
});

export async function logEvent(
  event: NotificationEvent,
  status: 'delivered' | 'rate_limited' | 'duplicate'
): Promise<void> {
  await pool.query(
    `INSERT INTO notification_events (tenant_id, user_id, message, channel, status, message_id)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [event.tenantId, event.userId, event.message, event.channel, status, event.messageId]
  );
}
