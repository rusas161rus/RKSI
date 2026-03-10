-- Схема для источника "Планшетка" (Google Drive)

CREATE TABLE IF NOT EXISTS parser_tabletka_sources (
  id SMALLINT PRIMARY KEY,
  folder_url TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO parser_tabletka_sources(id, folder_url)
VALUES (1, 'https://drive.google.com/drive/folders/1kUYiSAafghhYR0ARyXwPW1HZPpHcFIag')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS parsed_tabletka_schedule_entries (
  id BIGSERIAL PRIMARY KEY,
  lesson_date DATE NOT NULL,
  start_time TIME,
  end_time TIME,
  subject_id BIGINT NOT NULL REFERENCES subjects(id) ON DELETE RESTRICT,
  teacher_id BIGINT REFERENCES teachers(id) ON DELETE SET NULL,
  group_id BIGINT NOT NULL REFERENCES study_groups(id) ON DELETE CASCADE,
  room VARCHAR(32),
  source_hash VARCHAR(64) NOT NULL UNIQUE,
  source_group_name VARCHAR(64) NOT NULL,
  source_doc_url TEXT,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_tabletka_time_valid CHECK (
    (start_time IS NULL AND end_time IS NULL)
    OR (start_time IS NOT NULL AND end_time IS NOT NULL AND end_time > start_time)
  )
);

CREATE INDEX IF NOT EXISTS idx_tabletka_group_date
ON parsed_tabletka_schedule_entries(group_id, lesson_date);
