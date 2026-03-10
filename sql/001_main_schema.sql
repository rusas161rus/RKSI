-- Основная схема сайта "Расписание занятий РКСИ" (БД apprksi)
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS study_groups (
  id BIGSERIAL PRIMARY KEY,
  group_name VARCHAR(64) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS subjects (
  id BIGSERIAL PRIMARY KEY,
  subject_name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS teachers (
  id BIGSERIAL PRIMARY KEY,
  full_name TEXT NOT NULL,
  room VARCHAR(32),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_teachers_name_room UNIQUE (full_name, room)
);

CREATE TABLE IF NOT EXISTS site_users (
  id BIGSERIAL PRIMARY KEY,
  username VARCHAR(64) NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  full_name TEXT,
  preferred_group_id BIGINT REFERENCES study_groups(id) ON DELETE SET NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS site_admins (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL UNIQUE REFERENCES site_users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Основная таблица ручного расписания (редактируется админом)
CREATE TABLE IF NOT EXISTS schedule_entries (
  id BIGSERIAL PRIMARY KEY,
  lesson_date DATE NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  subject_id BIGINT NOT NULL REFERENCES subjects(id) ON DELETE RESTRICT,
  teacher_id BIGINT REFERENCES teachers(id) ON DELETE SET NULL,
  room VARCHAR(32),
  group_id BIGINT NOT NULL REFERENCES study_groups(id) ON DELETE CASCADE,
  created_by_user_id BIGINT REFERENCES site_users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_manual_time_valid CHECK (end_time > start_time),
  CONSTRAINT uq_manual_slot UNIQUE (group_id, lesson_date, start_time, end_time)
);

-- Отдельное read-only расписание, загружаемое из parser-БД
CREATE TABLE IF NOT EXISTS parsed_schedule_entries (
  id BIGSERIAL PRIMARY KEY,
  lesson_date DATE NOT NULL,
  start_time TIME,
  end_time TIME,
  subject_id BIGINT NOT NULL REFERENCES subjects(id) ON DELETE RESTRICT,
  teacher_id BIGINT REFERENCES teachers(id) ON DELETE SET NULL,
  room VARCHAR(32),
  raw_teacher_name TEXT,
  group_id BIGINT NOT NULL REFERENCES study_groups(id) ON DELETE CASCADE,
  source_hash VARCHAR(64) NOT NULL UNIQUE,
  source_group_name VARCHAR(64) NOT NULL,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_parsed_time_valid CHECK (
    (start_time IS NULL AND end_time IS NULL)
    OR (start_time IS NOT NULL AND end_time IS NOT NULL AND end_time > start_time)
  )
);

CREATE INDEX IF NOT EXISTS idx_schedule_group_date ON schedule_entries(group_id, lesson_date);
CREATE INDEX IF NOT EXISTS idx_schedule_date_time ON schedule_entries(lesson_date, start_time);
CREATE INDEX IF NOT EXISTS idx_schedule_subject ON schedule_entries(subject_id);
CREATE INDEX IF NOT EXISTS idx_schedule_teacher ON schedule_entries(teacher_id);
CREATE INDEX IF NOT EXISTS idx_users_group ON site_users(preferred_group_id);

CREATE INDEX IF NOT EXISTS idx_parsed_group_date ON parsed_schedule_entries(group_id, lesson_date);
CREATE INDEX IF NOT EXISTS idx_parsed_date_time ON parsed_schedule_entries(lesson_date, start_time);

ALTER TABLE schedule_entries
  DROP CONSTRAINT IF EXISTS ex_manual_schedule_no_overlap;

ALTER TABLE schedule_entries
  ADD CONSTRAINT ex_manual_schedule_no_overlap
  EXCLUDE USING gist (
    group_id WITH =,
    lesson_date WITH =,
    tsrange((lesson_date + start_time)::timestamp, (lesson_date + end_time)::timestamp, '[)') WITH &&
  );

CREATE OR REPLACE VIEW vw_schedule_resolved AS
SELECT
  s.id,
  s.lesson_date,
  s.start_time,
  s.end_time,
  g.group_name,
  subj.subject_name,
  t.full_name AS teacher_name,
  COALESCE(s.room, t.room) AS room,
  'manual'::text AS source
FROM schedule_entries s
JOIN study_groups g ON g.id = s.group_id
JOIN subjects subj ON subj.id = s.subject_id
LEFT JOIN teachers t ON t.id = s.teacher_id
UNION ALL
SELECT
  p.id,
  p.lesson_date,
  p.start_time,
  p.end_time,
  g.group_name,
  subj.subject_name,
  COALESCE(t.full_name, p.raw_teacher_name) AS teacher_name,
  COALESCE(p.room, t.room) AS room,
  'parsed'::text AS source
FROM parsed_schedule_entries p
JOIN study_groups g ON g.id = p.group_id
JOIN subjects subj ON subj.id = p.subject_id
LEFT JOIN teachers t ON t.id = p.teacher_id;

INSERT INTO study_groups(group_name) VALUES ('2-ИС-З') ON CONFLICT DO NOTHING;
INSERT INTO subjects(subject_name) VALUES ('Классный час') ON CONFLICT DO NOTHING;
