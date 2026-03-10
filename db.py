import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv

load_dotenv()


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
