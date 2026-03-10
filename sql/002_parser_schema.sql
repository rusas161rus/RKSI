-- Схема parser-таблиц (выполнять в той же БД apprksi)
BEGIN;

DROP TABLE IF EXISTS parser_lessons CASCADE;
DROP TABLE IF EXISTS parser_days CASCADE;

CREATE TABLE parser_days (
  id BIGSERIAL PRIMARY KEY,
  source_url TEXT NOT NULL,
  group_name VARCHAR(64) NOT NULL,
  lesson_date DATE,
  day_label TEXT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE parser_lessons (
  id BIGSERIAL PRIMARY KEY,
  day_id BIGINT NOT NULL REFERENCES parser_days(id) ON DELETE CASCADE,
  start_time TIME,
  end_time TIME,
  subject_name TEXT NOT NULL,
  teacher_name TEXT,
  room VARCHAR(32),
  raw_text TEXT,
  source_hash VARCHAR(64) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_hash)
);

CREATE INDEX idx_parser_days_group_date ON parser_days(group_name, lesson_date);
CREATE INDEX idx_parser_lessons_day ON parser_lessons(day_id);
CREATE INDEX idx_parser_lessons_subject ON parser_lessons(subject_name);

COMMIT;
