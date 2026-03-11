CREATE TABLE IF NOT EXISTS online_user_presence (
  user_id BIGINT PRIMARY KEY REFERENCES site_users(id) ON DELETE CASCADE,
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_online_user_presence_last_seen
  ON online_user_presence(last_seen DESC);
