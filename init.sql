CREATE TABLE IF NOT EXISTS notification_events (
  id         SERIAL PRIMARY KEY,
  tenant_id  VARCHAR(100) NOT NULL,
  user_id    VARCHAR(100) NOT NULL,
  message    TEXT NOT NULL,
  channel    VARCHAR(50) NOT NULL,
  status     VARCHAR(50) NOT NULL,
  message_id VARCHAR(200),
  created_at TIMESTAMP DEFAULT NOW()
);
