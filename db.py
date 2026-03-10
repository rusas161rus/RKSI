import os
import re
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv

load_dotenv()

MULTI_TEACHER_TOKEN_RE = re.compile(r"\b[А-ЯЁ][а-яё-]+(?:\s+[А-Я]\.\s*[А-Я]\.?)")


def build_dsn(prefix: str = "DB") -> str:
    host = os.getenv(f"{prefix}_HOST", "localhost")
    port = os.getenv(f"{prefix}_PORT", "5432")
    name = os.getenv(f"{prefix}_NAME")
    user = os.getenv(f"{prefix}_USER")
    password = os.getenv(f"{prefix}_PASSWORD")

    if not all([name, user, password]):
        raise RuntimeError(f"Missing {prefix}_NAME/{prefix}_USER/{prefix}_PASSWORD in .env")

    return f"host={host} port={port} dbname={name} user={user} password={password}"


@contextmanager
def get_db_conn():
    conn = psycopg2.connect(build_dsn("DB"))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Алиасы для читаемости кода
get_main_conn = get_db_conn
get_parser_conn = get_db_conn


def is_composite_teacher_name(name: str | None) -> bool:
    cleaned = re.sub(r"\s+", " ", (name or "").strip())
    if not cleaned:
        return False
    return len(MULTI_TEACHER_TOKEN_RE.findall(cleaned)) >= 2


def ensure_schedule_room_columns() -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE IF EXISTS schedule_entries
                ADD COLUMN IF NOT EXISTS room VARCHAR(32)
                """
            )
            cur.execute(
                """
                ALTER TABLE IF EXISTS parsed_schedule_entries
                ADD COLUMN IF NOT EXISTS room VARCHAR(32)
                """
            )
            cur.execute(
                """
                ALTER TABLE IF EXISTS parsed_tabletka_schedule_entries
                ADD COLUMN IF NOT EXISTS room VARCHAR(32)
                """
            )
            cur.execute(
                """
                ALTER TABLE IF EXISTS parsed_schedule_entries
                ADD COLUMN IF NOT EXISTS raw_teacher_name TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE IF EXISTS parsed_tabletka_schedule_entries
                ADD COLUMN IF NOT EXISTS raw_teacher_name TEXT
                """
            )

            cur.execute("SELECT id, full_name FROM teachers")
            composite_teacher_ids = [row[0] for row in cur.fetchall() if is_composite_teacher_name(row[1])]
            if composite_teacher_ids:
                cur.execute(
                    """
                    UPDATE parsed_schedule_entries p
                    SET raw_teacher_name = COALESCE(NULLIF(p.raw_teacher_name, ''), t.full_name),
                        teacher_id = NULL
                    FROM teachers t
                    WHERE p.teacher_id = t.id
                      AND p.teacher_id = ANY(%s)
                    """,
                    (composite_teacher_ids,),
                )
                cur.execute(
                    """
                    UPDATE parsed_tabletka_schedule_entries p
                    SET raw_teacher_name = COALESCE(NULLIF(p.raw_teacher_name, ''), t.full_name),
                        teacher_id = NULL
                    FROM teachers t
                    WHERE p.teacher_id = t.id
                      AND p.teacher_id = ANY(%s)
                    """,
                    (composite_teacher_ids,),
                )
                cur.execute(
                    """
                    DELETE FROM teachers t
                    WHERE t.id = ANY(%s)
                      AND NOT EXISTS (
                        SELECT 1
                        FROM schedule_entries s
                        WHERE s.teacher_id = t.id
                      )
                    """,
                    (composite_teacher_ids,),
                )
