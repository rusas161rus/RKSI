CREATE TABLE IF NOT EXISTS telegram_bot_settings (
  id SMALLINT PRIMARY KEY,
  bot_token TEXT,
  bot_username VARCHAR(128),
  site_base_url TEXT,
  polling_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  notifications_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  last_update_id BIGINT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO telegram_bot_settings(id, polling_enabled, notifications_enabled)
VALUES (1, FALSE, FALSE)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS telegram_notification_deliveries (
  user_id BIGINT NOT NULL,
  event_id BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  error_text TEXT,
  delivered_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, event_id)
);
