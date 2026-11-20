CREATE TABLE IF NOT EXISTS notification_events (
  id         SERIAL PRIMARY KEY,
  tenant_id  VARCHAR(100) NOT NULL,
  user_id    VARCHAR(100) NOT NULL,
  message    TEXT NOT NULL,
  channel    VARCHAR(50) NOT NULL,
  status     VARCHAR(50) NOT NULL,
  message_id VARCHAR(200),
  attempts   INT NOT NULL DEFAULT 0,
  error      TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS notification_events_message_id_idx ON notification_events (message_id);
