import { Pool } from 'pg';
import type { ProcessResult } from '../pipeline/processor';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL ||
    'postgresql://notif_user:notif_pass@localhost:5432/notifications',
});

// One INSERT for the whole batch: unnest() turns the parallel arrays into rows.
export async function logResults(results: ProcessResult[]): Promise<void> {
  if (results.length === 0) return;
  await pool.query(
    `INSERT INTO notification_events (tenant_id, user_id, message, channel, status, message_id)
     SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])`,
    [
      results.map((r) => r.event.tenantId),
      results.map((r) => r.event.userId),
      results.map((r) => r.event.message),
      results.map((r) => r.event.channel),
      results.map((r) => r.outcome),
      results.map((r) => r.event.messageId),
    ]
  );
}
