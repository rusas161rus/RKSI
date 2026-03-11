ALTER TABLE IF EXISTS site_users
  ADD COLUMN IF NOT EXISTS ai_enabled BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE IF EXISTS site_users
  ADD COLUMN IF NOT EXISTS ai_daily_limit INTEGER NOT NULL DEFAULT 20;

CREATE TABLE IF NOT EXISTS ai_usage_daily (
  user_id BIGINT NOT NULL REFERENCES site_users(id) ON DELETE CASCADE,
  usage_date DATE NOT NULL,
  requests_count INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, usage_date)
);

CREATE INDEX IF NOT EXISTS idx_ai_usage_daily_date ON ai_usage_daily(usage_date, user_id);
