import { pool } from './logger';

// Brings an existing database up to date (init.sql only runs when the volume is first created).
export async function ensureSchema(): Promise<void> {
  await pool.query(`
    ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
    ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS error TEXT;
    CREATE INDEX IF NOT EXISTS notification_events_message_id_idx ON notification_events (message_id);
  `);
}
