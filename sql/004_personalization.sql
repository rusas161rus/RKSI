ALTER TABLE IF EXISTS site_users
  ADD COLUMN IF NOT EXISTS telegram_chat_id BIGINT UNIQUE;

ALTER TABLE IF EXISTS site_users
  ADD COLUMN IF NOT EXISTS telegram_link_code VARCHAR(32);

ALTER TABLE IF EXISTS site_users
  ADD COLUMN IF NOT EXISTS telegram_link_code_created_at TIMESTAMPTZ;

ALTER TABLE IF EXISTS site_users
  ADD COLUMN IF NOT EXISTS telegram_notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE IF EXISTS site_users
  ADD COLUMN IF NOT EXISTS telegram_linked_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS user_favorite_teachers (
  user_id BIGINT NOT NULL REFERENCES site_users(id) ON DELETE CASCADE,
  teacher_id BIGINT NOT NULL REFERENCES teachers(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, teacher_id)
);

CREATE TABLE IF NOT EXISTS user_notes (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES site_users(id) ON DELETE CASCADE,
  title VARCHAR(180) NOT NULL,
  note_text TEXT,
  due_date DATE,
  is_done BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS group_announcements (
  id BIGSERIAL PRIMARY KEY,
  group_id BIGINT REFERENCES study_groups(id) ON DELETE CASCADE,
  title VARCHAR(180) NOT NULL,
  body TEXT NOT NULL,
  created_by_user_id BIGINT REFERENCES site_users(id) ON DELETE SET NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS group_schedule_snapshots (
  group_id BIGINT PRIMARY KEY REFERENCES study_groups(id) ON DELETE CASCADE,
  snapshot_hash VARCHAR(64) NOT NULL,
  snapshot_data JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS group_schedule_change_events (
  id BIGSERIAL PRIMARY KEY,
  group_id BIGINT NOT NULL REFERENCES study_groups(id) ON DELETE CASCADE,
  event_type VARCHAR(16) NOT NULL,
  source_name VARCHAR(32) NOT NULL,
  event_text TEXT NOT NULL,
  lesson_date DATE,
  start_time TIME,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
  user_id BIGINT NOT NULL REFERENCES site_users(id) ON DELETE CASCADE,
  event_id BIGINT NOT NULL REFERENCES group_schedule_change_events(id) ON DELETE CASCADE,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  error_text TEXT,
  delivered_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, event_id)
);
