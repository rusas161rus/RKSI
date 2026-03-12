import hashlib
import secrets
import time
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urljoin

import psycopg2
import requests
from psycopg2.extras import Json

from db import build_dsn, get_bot_conn, get_main_conn

SOURCE_TITLES = {"manual": "ручное расписание", "rksi": "сайт РКСИ", "planshetka": "Planshetka"}
CHANGE_LOOKAHEAD_DAYS = 21
MAX_EVENTS_PER_REFRESH = 16
TELEGRAM_POLL_LOCK_ID = 812341
TELEGRAM_NOTIFY_LOCK_ID = 812342
LESSON_REMINDER_WINDOW_MINUTES = 15
LESSON_SOURCE_PRIORITY = {"rksi": 0, "planshetka": 1, "manual": 2}


def ensure_personalization_tables() -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE IF EXISTS site_users ADD COLUMN IF NOT EXISTS telegram_chat_id BIGINT UNIQUE")
            cur.execute("ALTER TABLE IF EXISTS site_users ADD COLUMN IF NOT EXISTS telegram_link_code VARCHAR(32)")
            cur.execute("ALTER TABLE IF EXISTS site_users ADD COLUMN IF NOT EXISTS telegram_link_code_created_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE IF EXISTS site_users ADD COLUMN IF NOT EXISTS telegram_notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE")
            cur.execute("ALTER TABLE IF EXISTS site_users ADD COLUMN IF NOT EXISTS telegram_lesson_notifications_enabled BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE IF EXISTS site_users ADD COLUMN IF NOT EXISTS telegram_linked_at TIMESTAMPTZ")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_favorite_teachers (
                    user_id BIGINT NOT NULL REFERENCES site_users(id) ON DELETE CASCADE,
                    teacher_id BIGINT NOT NULL REFERENCES teachers(id) ON DELETE CASCADE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (user_id, teacher_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_notes (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES site_users(id) ON DELETE CASCADE,
                    title VARCHAR(180) NOT NULL,
                    note_text TEXT,
                    due_date DATE,
                    is_done BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS group_announcements (
                    id BIGSERIAL PRIMARY KEY,
                    group_id BIGINT REFERENCES study_groups(id) ON DELETE CASCADE,
                    title VARCHAR(180) NOT NULL,
                    body TEXT NOT NULL,
                    created_by_user_id BIGINT REFERENCES site_users(id) ON DELETE SET NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS group_schedule_snapshots (
                    group_id BIGINT PRIMARY KEY REFERENCES study_groups(id) ON DELETE CASCADE,
                    snapshot_hash VARCHAR(64) NOT NULL,
                    snapshot_data JSONB NOT NULL DEFAULT '[]'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
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
                )
                """
            )


def ensure_bot_tables() -> None:
    with get_bot_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_bot_settings (
                    id SMALLINT PRIMARY KEY,
                    bot_token TEXT,
                    bot_username VARCHAR(128),
                    site_base_url TEXT,
                    polling_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    notifications_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    last_update_id BIGINT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute("INSERT INTO telegram_bot_settings(id, polling_enabled, notifications_enabled) VALUES (1, FALSE, FALSE) ON CONFLICT (id) DO NOTHING")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_notification_deliveries (
                    user_id BIGINT NOT NULL,
                    event_id BIGINT NOT NULL,
                    status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    error_text TEXT,
                    delivered_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (user_id, event_id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_lesson_reminder_deliveries (
                    user_id BIGINT NOT NULL,
                    reminder_key VARCHAR(128) NOT NULL,
                    status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    error_text TEXT,
                    delivered_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (user_id, reminder_key)
                )
                """
            )


def load_telegram_settings() -> dict[str, Any]:
    ensure_bot_tables()
    with get_bot_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT bot_token, bot_username, site_base_url, polling_enabled,
                       notifications_enabled, last_update_id, updated_at
                FROM telegram_bot_settings
                WHERE id = 1
                """
            )
            row = cur.fetchone()
    return {
        "bot_token": (row[0] or "") if row else "",
        "bot_username": (row[1] or "") if row else "",
        "site_base_url": ((row[2] or "").rstrip("/")) if row else "",
        "polling_enabled": bool(row[3]) if row else False,
        "notifications_enabled": bool(row[4]) if row else False,
        "last_update_id": row[5] if row else None,
        "updated_at": row[6] if row else None,
    }


def save_telegram_settings(bot_token: str, bot_username: str, site_base_url: str, polling_enabled: bool, notifications_enabled: bool) -> None:
    ensure_bot_tables()
    with get_bot_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE telegram_bot_settings
                SET bot_token = %s,
                    bot_username = %s,
                    site_base_url = %s,
                    polling_enabled = %s,
                    notifications_enabled = %s,
                    updated_at = now()
                WHERE id = 1
                """,
                (
                    bot_token.strip() or None,
                    bot_username.strip().lstrip("@") or None,
                    site_base_url.strip().rstrip("/") or None,
                    polling_enabled,
                    notifications_enabled,
                ),
            )


def update_last_telegram_update_id(last_update_id: int) -> None:
    ensure_bot_tables()
    with get_bot_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE telegram_bot_settings SET last_update_id = %s, updated_at = now() WHERE id = 1", (last_update_id,))


def generate_telegram_link_code(user_id: int) -> str:
    code = secrets.token_hex(4).upper()
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE site_users
                SET telegram_link_code = %s,
                    telegram_link_code_created_at = now(),
                    updated_at = now()
                WHERE id = %s
                """,
                (code, user_id),
            )
    return code


def unlink_telegram_account(user_id: int) -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE site_users
                SET telegram_chat_id = NULL,
                    telegram_link_code = NULL,
                    telegram_link_code_created_at = NULL,
                    telegram_linked_at = NULL,
                    updated_at = now()
                WHERE id = %s
                """,
                (user_id,),
            )


def consume_telegram_link_code(code: str, chat_id: int) -> tuple[str, dict[str, Any] | None]:
    normalized = (code or "").strip().upper()
    if not normalized:
        return "empty", None
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, username, COALESCE(full_name, ''), preferred_group_id, telegram_chat_id
                FROM site_users
                WHERE telegram_link_code = %s
                  AND telegram_link_code_created_at >= now() - interval '30 minutes'
                  AND is_active = TRUE
                FOR UPDATE
                """,
                (normalized,),
            )
            row = cur.fetchone()
            if not row:
                return "invalid", None
            user_id, username, full_name, preferred_group_id, existing_chat_id = row
            if existing_chat_id and int(existing_chat_id) == int(chat_id):
                cur.execute(
                    """
                    UPDATE site_users
                    SET telegram_link_code = NULL,
                        telegram_link_code_created_at = NULL,
                        telegram_linked_at = COALESCE(telegram_linked_at, now()),
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (user_id,),
                )
                return "already_linked", {"id": user_id, "username": username, "full_name": full_name, "preferred_group_id": preferred_group_id}
            cur.execute("SELECT id FROM site_users WHERE telegram_chat_id = %s AND id <> %s LIMIT 1", (chat_id, user_id))
            if cur.fetchone():
                return "chat_in_use", None
            cur.execute(
                """
                UPDATE site_users
                SET telegram_chat_id = %s,
                    telegram_link_code = NULL,
                    telegram_link_code_created_at = NULL,
                    telegram_linked_at = now(),
                    updated_at = now()
                WHERE id = %s
                """,
                (chat_id, user_id),
            )
    return "linked", {"id": user_id, "username": username, "full_name": full_name, "preferred_group_id": preferred_group_id}


def get_user_by_chat_id(chat_id: int) -> dict[str, Any] | None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.id, u.username, COALESCE(u.full_name, ''), u.preferred_group_id, COALESCE(g.group_name, '')
                FROM site_users u
                LEFT JOIN study_groups g ON g.id = u.preferred_group_id
                WHERE u.telegram_chat_id = %s AND u.is_active = TRUE
                """,
                (chat_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "username": row[1], "full_name": row[2], "preferred_group_id": row[3], "group_name": row[4]}


def fetch_user_notes(user_id: int) -> list[dict[str, Any]]:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, COALESCE(note_text, ''), due_date, is_done, created_at
                FROM user_notes
                WHERE user_id = %s
                ORDER BY is_done ASC, due_date NULLS LAST, created_at DESC
                LIMIT 30
                """,
                (user_id,),
            )
            rows = cur.fetchall()
    return [{"id": row[0], "title": row[1], "note_text": row[2], "due_date": row[3], "is_done": row[4], "created_at": row[5]} for row in rows]


def create_user_note(user_id: int, title: str, note_text: str, due_date_text: str | None) -> None:
    due_date = datetime.strptime(due_date_text, "%Y-%m-%d").date() if (due_date_text or "").strip() else None
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO user_notes(user_id, title, note_text, due_date) VALUES (%s, %s, %s, %s)", (user_id, title.strip(), note_text.strip() or None, due_date))


def toggle_user_note(user_id: int, note_id: int) -> bool:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE user_notes SET is_done = NOT is_done, updated_at = now() WHERE id = %s AND user_id = %s", (note_id, user_id))
            return cur.rowcount > 0


def delete_user_note(user_id: int, note_id: int) -> bool:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_notes WHERE id = %s AND user_id = %s", (note_id, user_id))
            return cur.rowcount > 0


def toggle_favorite_teacher(user_id: int, teacher_id: int) -> bool:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM user_favorite_teachers WHERE user_id = %s AND teacher_id = %s", (user_id, teacher_id))
            exists = bool(cur.fetchone())
            if exists:
                cur.execute("DELETE FROM user_favorite_teachers WHERE user_id = %s AND teacher_id = %s", (user_id, teacher_id))
                return False
            cur.execute("INSERT INTO user_favorite_teachers(user_id, teacher_id) VALUES (%s, %s) ON CONFLICT (user_id, teacher_id) DO NOTHING", (user_id, teacher_id))
            return True


def fetch_favorite_teachers(user_id: int) -> list[dict[str, Any]]:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.id, t.full_name, COALESCE(t.room, '')
                FROM user_favorite_teachers f
                JOIN teachers t ON t.id = f.teacher_id
                WHERE f.user_id = %s
                ORDER BY t.full_name
                """,
                (user_id,),
            )
            rows = cur.fetchall()
    return [{"id": row[0], "full_name": row[1], "room": row[2]} for row in rows]


def fetch_announcements_for_user(user_id: int) -> list[dict[str, Any]]:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT preferred_group_id FROM site_users WHERE id = %s", (user_id,))
            row = cur.fetchone()
            preferred_group_id = row[0] if row else None
            cur.execute(
                """
                SELECT a.id, a.title, a.body, a.created_at, COALESCE(g.group_name, 'Все группы')
                FROM group_announcements a
                LEFT JOIN study_groups g ON g.id = a.group_id
                WHERE a.is_active = TRUE AND (a.group_id IS NULL OR a.group_id = %s)
                ORDER BY a.created_at DESC
                LIMIT 12
                """,
                (preferred_group_id,),
            )
            rows = cur.fetchall()
    return [{"id": row[0], "title": row[1], "body": row[2], "created_at": row[3], "group_name": row[4]} for row in rows]


def fetch_admin_announcements() -> list[dict[str, Any]]:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id, a.title, a.body, a.is_active, a.created_at, COALESCE(g.group_name, 'Все группы')
                FROM group_announcements a
                LEFT JOIN study_groups g ON g.id = a.group_id
                ORDER BY a.created_at DESC
                LIMIT 40
                """
            )
            rows = cur.fetchall()
    return [{"id": row[0], "title": row[1], "body": row[2], "is_active": row[3], "created_at": row[4], "group_name": row[5]} for row in rows]


def create_announcement(group_id: int | None, title: str, body: str, created_by_user_id: int) -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO group_announcements(group_id, title, body, created_by_user_id) VALUES (%s, %s, %s, %s)", (group_id, title.strip(), body.strip(), created_by_user_id))


def toggle_announcement(announcement_id: int) -> bool:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE group_announcements SET is_active = NOT is_active WHERE id = %s", (announcement_id,))
            return cur.rowcount > 0


def delete_announcement(announcement_id: int) -> bool:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM group_announcements WHERE id = %s", (announcement_id,))
            return cur.rowcount > 0


def fetch_recent_change_events_for_user(user_id: int, limit: int = 12) -> list[dict[str, Any]]:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT preferred_group_id FROM site_users WHERE id = %s", (user_id,))
            row = cur.fetchone()
            preferred_group_id = row[0] if row else None
            if not preferred_group_id:
                return []
            cur.execute(
                """
                SELECT id, event_type, source_name, event_text, lesson_date, start_time, detected_at
                FROM group_schedule_change_events
                WHERE group_id = %s
                ORDER BY detected_at DESC, id DESC
                LIMIT %s
                """,
                (preferred_group_id, limit),
            )
            rows = cur.fetchall()
    return [
        {
            "id": row[0],
            "event_type": row[1],
            "source_name": row[2],
            "event_text": row[3],
            "lesson_date": row[4],
            "start_time": row[5],
            "detected_at": row[6],
        }
        for row in rows
    ]


def fetch_source_conflicts(group_id: int | None, start_day: date | None = None, end_day: date | None = None) -> list[dict[str, Any]]:
    if not group_id:
        return []
    start_day = start_day or date.today()
    end_day = end_day or (start_day + timedelta(days=6))
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.lesson_date,
                    p.start_time,
                    subj_p.subject_name,
                    COALESCE(tp.full_name, p.raw_teacher_name, ''),
                    COALESCE(p.room, tp.room, ''),
                    subj_t.subject_name,
                    COALESCE(tt.full_name, t.raw_teacher_name, ''),
                    COALESCE(t.room, tt.room, '')
                FROM parsed_schedule_entries p
                JOIN parsed_tabletka_schedule_entries t
                  ON t.group_id = p.group_id
                 AND t.lesson_date = p.lesson_date
                 AND COALESCE(t.start_time, TIME '00:00') = COALESCE(p.start_time, TIME '00:00')
                JOIN subjects subj_p ON subj_p.id = p.subject_id
                JOIN subjects subj_t ON subj_t.id = t.subject_id
                LEFT JOIN teachers tp ON tp.id = p.teacher_id
                LEFT JOIN teachers tt ON tt.id = t.teacher_id
                WHERE p.group_id = %s
                  AND p.lesson_date BETWEEN %s AND %s
                  AND (
                    subj_p.subject_name <> subj_t.subject_name
                    OR COALESCE(tp.full_name, p.raw_teacher_name, '') <> COALESCE(tt.full_name, t.raw_teacher_name, '')
                    OR COALESCE(p.room, tp.room, '') <> COALESCE(t.room, tt.room, '')
                  )
                ORDER BY p.lesson_date, p.start_time NULLS LAST
                LIMIT 12
                """,
                (group_id, start_day, end_day),
            )
            rows = cur.fetchall()
    return [
        {
            "lesson_date": row[0],
            "start_time": row[1],
            "rksi_subject": row[2],
            "rksi_teacher": row[3],
            "rksi_room": row[4],
            "planshetka_subject": row[5],
            "planshetka_teacher": row[6],
            "planshetka_room": row[7],
        }
        for row in rows
    ]


def fetch_user_schedule(group_id: int | None, start_day: date, end_day: date) -> list[dict[str, Any]]:
    if not group_id:
        return []
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT x.lesson_date, x.start_time, x.end_time, x.subject_name, x.teacher_name, x.room, x.source
                FROM (
                  SELECT s.lesson_date, s.start_time, s.end_time, subj.subject_name,
                         COALESCE(t.full_name, '') AS teacher_name,
                         COALESCE(s.room, t.room, '') AS room,
                         'manual' AS source
                  FROM schedule_entries s
                  JOIN subjects subj ON subj.id = s.subject_id
                  LEFT JOIN teachers t ON t.id = s.teacher_id
                  WHERE s.group_id = %s
                  UNION ALL
                  SELECT p.lesson_date, p.start_time, p.end_time, subj.subject_name,
                         COALESCE(t.full_name, p.raw_teacher_name, '') AS teacher_name,
                         COALESCE(p.room, t.room, '') AS room,
                         'rksi' AS source
                  FROM parsed_schedule_entries p
                  JOIN subjects subj ON subj.id = p.subject_id
                  LEFT JOIN teachers t ON t.id = p.teacher_id
                  WHERE p.group_id = %s
                  UNION ALL
                  SELECT p.lesson_date, p.start_time, p.end_time, subj.subject_name,
                         COALESCE(t.full_name, p.raw_teacher_name, '') AS teacher_name,
                         COALESCE(p.room, t.room, '') AS room,
                         'planshetka' AS source
                  FROM parsed_tabletka_schedule_entries p
                  JOIN subjects subj ON subj.id = p.subject_id
                  LEFT JOIN teachers t ON t.id = p.teacher_id
                  WHERE p.group_id = %s
                ) x
                WHERE x.lesson_date BETWEEN %s AND %s
                ORDER BY x.lesson_date, x.start_time NULLS LAST, x.end_time NULLS LAST, x.subject_name
                """,
                (group_id, group_id, group_id, start_day, end_day),
            )
            rows = cur.fetchall()
    return [
        {
            "lesson_date": row[0],
            "start_time": row[1],
            "end_time": row[2],
            "subject_name": row[3],
            "teacher_name": row[4],
            "room": row[5],
            "source": row[6],
        }
        for row in rows
    ]


def build_today_summary(group_id: int | None) -> dict[str, Any]:
    today = date.today()
    rows = fetch_user_schedule(group_id, today, today)
    now_time = datetime.now().time()
    next_lesson = None
    for row in rows:
        if row["start_time"] and row["start_time"] >= now_time:
            next_lesson = row
            break
    return {
        "today_count": len(rows),
        "next_lesson": next_lesson,
        "sources": sorted({row["source"] for row in rows}),
        "rows": rows[:6],
    }


def _snapshot_payload(group_id: int) -> list[dict[str, Any]]:
    rows = fetch_user_schedule(group_id, date.today(), date.today() + timedelta(days=CHANGE_LOOKAHEAD_DAYS))
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "source": row["source"],
                "lesson_date": row["lesson_date"].isoformat(),
                "start_time": row["start_time"].strftime("%H:%M") if row["start_time"] else "",
                "end_time": row["end_time"].strftime("%H:%M") if row["end_time"] else "",
                "subject_name": row["subject_name"],
                "teacher_name": row["teacher_name"] or "",
                "room": row["room"] or "",
            }
        )
    payload.sort(key=lambda item: (item["lesson_date"], item["start_time"], item["end_time"], item["subject_name"].lower(), item["teacher_name"].lower(), item["source"]))
    return payload


def _format_event_item(item: dict[str, Any]) -> str:
    time_text = item["start_time"]
    if item.get("end_time"):
        time_text = f"{time_text}-{item['end_time']}" if time_text else item["end_time"]
    pieces = [SOURCE_TITLES.get(item["source"], item["source"]), item["lesson_date"]]
    if time_text:
        pieces.append(time_text)
    pieces.append(item["subject_name"])
    if item.get("teacher_name"):
        pieces.append(item["teacher_name"])
    if item.get("room"):
        pieces.append(f"ауд. {item['room']}")
    return ", ".join(pieces)


def _weak_key(item: dict[str, Any]) -> tuple[str, str, str, str]:
    return (item["source"], item["lesson_date"], item["subject_name"].strip().lower(), item.get("teacher_name", "").strip().lower())


def _strong_key(item: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (item["source"], item["lesson_date"], item["start_time"], item["end_time"], item["subject_name"].strip().lower(), item.get("teacher_name", "").strip().lower())


def _build_change_events(old_items: list[dict[str, Any]], new_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    old_map = {_strong_key(item): item for item in old_items}
    new_map = {_strong_key(item): item for item in new_items}
    added_keys = set(new_map) - set(old_map)
    removed_keys = set(old_map) - set(new_map)
    events: list[dict[str, Any]] = []
    old_by_weak: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    new_by_weak: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for key in removed_keys:
        old_by_weak.setdefault(_weak_key(old_map[key]), []).append(old_map[key])
    for key in added_keys:
        new_by_weak.setdefault(_weak_key(new_map[key]), []).append(new_map[key])

    matched_removed: set[tuple[str, str, str, str, str, str]] = set()
    matched_added: set[tuple[str, str, str, str, str, str]] = set()
    for weak_key, old_group in old_by_weak.items():
        new_group = new_by_weak.get(weak_key, [])
        if len(old_group) == 1 and len(new_group) == 1:
            old_item = old_group[0]
            new_item = new_group[0]
            matched_removed.add(_strong_key(old_item))
            matched_added.add(_strong_key(new_item))
            details = []
            if old_item["start_time"] != new_item["start_time"] or old_item["end_time"] != new_item["end_time"]:
                details.append(f"время {old_item['start_time'] or 'без времени'} -> {new_item['start_time'] or 'без времени'}")
            if old_item.get("room", "") != new_item.get("room", ""):
                details.append(f"аудитория {old_item.get('room', 'не указана')} -> {new_item.get('room', 'не указана')}")
            if not details:
                details.append("обновлены детали занятия")
            events.append({"event_type": "updated", "source_name": old_item["source"], "event_text": f"Изменение: {_format_event_item(new_item)} ({'; '.join(details)})", "lesson_date": new_item["lesson_date"], "start_time": new_item["start_time"] or None, "payload": {"old": old_item, "new": new_item}})

    for key in sorted(added_keys - matched_added):
        item = new_map[key]
        events.append({"event_type": "added", "source_name": item["source"], "event_text": f"Добавлено: {_format_event_item(item)}", "lesson_date": item["lesson_date"], "start_time": item["start_time"] or None, "payload": {"new": item}})
    for key in sorted(removed_keys - matched_removed):
        item = old_map[key]
        events.append({"event_type": "removed", "source_name": item["source"], "event_text": f"Удалено: {_format_event_item(item)}", "lesson_date": item["lesson_date"], "start_time": item["start_time"] or None, "payload": {"old": item}})
    return events[:MAX_EVENTS_PER_REFRESH]


def detect_and_record_schedule_changes(group_ids: list[int] | None = None) -> int:
    ensure_personalization_tables()
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            if group_ids:
                cur.execute("SELECT id FROM study_groups WHERE id = ANY(%s) ORDER BY id", (group_ids,))
            else:
                cur.execute("SELECT id FROM study_groups ORDER BY id")
            target_group_ids = [row[0] for row in cur.fetchall()]
            inserted = 0
            for group_id in target_group_ids:
                new_payload = _snapshot_payload(group_id)
                new_hash = hashlib.sha256(repr(new_payload).encode("utf-8")).hexdigest()
                cur.execute("SELECT snapshot_hash, snapshot_data FROM group_schedule_snapshots WHERE group_id = %s", (group_id,))
                row = cur.fetchone()
                old_hash = row[0] if row else None
                old_payload = list(row[1] or []) if row else []
                if old_hash and old_hash == new_hash:
                    continue
                if row:
                    for event in _build_change_events(old_payload, new_payload):
                        cur.execute(
                            """
                            INSERT INTO group_schedule_change_events(group_id, event_type, source_name, event_text, lesson_date, start_time, payload)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (group_id, event["event_type"], event["source_name"], event["event_text"], event["lesson_date"] or None, event["start_time"], Json(event["payload"])),
                        )
                        inserted += 1
                    cur.execute("UPDATE group_schedule_snapshots SET snapshot_hash = %s, snapshot_data = %s, updated_at = now() WHERE group_id = %s", (new_hash, Json(new_payload), group_id))
                else:
                    cur.execute("INSERT INTO group_schedule_snapshots(group_id, snapshot_hash, snapshot_data) VALUES (%s, %s, %s)", (group_id, new_hash, Json(new_payload)))
            cur.execute("DELETE FROM group_schedule_change_events WHERE detected_at < now() - interval '30 days'")
    return inserted


def format_schedule_rows_for_bot(rows: list[dict[str, Any]], title: str) -> str:
    if not rows:
        return f"{title}\n\nЗанятий не найдено."

    merged_rows: list[dict[str, Any]] = []
    merged_map: dict[tuple[Any, Any, Any, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            row["lesson_date"],
            row["start_time"],
            row["end_time"],
            row["subject_name"],
            row.get("teacher_name") or "",
            row.get("room") or "",
        )
        source_title = SOURCE_TITLES.get(row["source"], row["source"])
        if key not in merged_map:
            merged_map[key] = {
                "lesson_date": row["lesson_date"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "subject_name": row["subject_name"],
                "teacher_name": row.get("teacher_name") or "",
                "room": row.get("room") or "",
                "sources": [source_title],
            }
            merged_rows.append(merged_map[key])
        elif source_title not in merged_map[key]["sources"]:
            merged_map[key]["sources"].append(source_title)

    lines = [title]
    current_day = None
    for idx, row in enumerate(merged_rows):
        if idx > 0:
            lines.append("")
        day_text = row["lesson_date"].strftime("%d.%m.%Y")
        if day_text != current_day:
            current_day = day_text
            if lines[-1] != "":
                lines.append("")
            lines.append(day_text)
        if row["start_time"] and row["end_time"]:
            time_text = f"{row['start_time'].strftime('%H:%M')}-{row['end_time'].strftime('%H:%M')}"
        elif row["start_time"]:
            time_text = row["start_time"].strftime("%H:%M")
        else:
            time_text = "без времени"
        lines.append(f"• {time_text}")
        lines.append(f"  {row['subject_name']}")
        details: list[str] = []
        if row["teacher_name"]:
            details.append(row["teacher_name"])
        if row["room"]:
            details.append(f"ауд. {row['room']}")
        if details:
            lines.append(f"  {' | '.join(details)}")
        lines.append(f"  Источник: {', '.join(row['sources'])}")
    return "\n".join(lines)


def _telegram_api(settings: dict[str, Any], method: str, payload: dict[str, Any], timeout: int = 30) -> dict[str, Any]:
    response = requests.post(f"https://api.telegram.org/bot{settings['bot_token']}/{method}", json=payload, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(data.get("description") or f"Telegram API {method} failed")
    return data


def _build_site_links_text(settings: dict[str, Any]) -> str:
    base_url = settings.get("site_base_url") or ""
    if not base_url:
        return ""
    return "\n".join(
        [
            "",
            f"Личный кабинет: {urljoin(base_url + '/', 'me')}",
            f"Вход на сайт: {urljoin(base_url + '/', 'login')}",
        ]
    )


def _build_reply_keyboard(linked: bool) -> dict[str, Any]:
    if not linked:
        return {"keyboard": [[{"text": "Как привязать аккаунт"}]], "resize_keyboard": True}
    return {"keyboard": [[{"text": "Сегодня"}, {"text": "Завтра"}], [{"text": "Неделя"}, {"text": "Изменения"}], [{"text": "Личный кабинет"}]], "resize_keyboard": True}


def _send_telegram_message(settings: dict[str, Any], chat_id: int, text: str, linked: bool = True, append_site_links: bool = False) -> None:
    if append_site_links:
        text = f"{text}{_build_site_links_text(settings)}"
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text[:4096],
        "reply_markup": _build_reply_keyboard(linked),
        "disable_web_page_preview": True,
    }
    _telegram_api(settings, "sendMessage", payload)


def _send_unlinked_help(settings: dict[str, Any], chat_id: int) -> None:
    base_url = settings.get("site_base_url") or ""
    lines = ["Доступ к боту открыт после привязки аккаунта из личного кабинета."]
    if base_url:
        lines.append(f"1. Откройте сайт: {urljoin(base_url + '/', 'login')}")
        lines.append("2. В кабинете нажмите кнопку генерации кода для Telegram.")
        lines.append("3. Отправьте код в этот бот одним сообщением.")
    else:
        lines.append("Попросите администратора заполнить ссылку на сайт и токен бота в админке.")
    _send_telegram_message(settings, chat_id, "\n".join(lines), linked=False)


def _handle_link_code(settings: dict[str, Any], chat_id: int, text: str) -> None:
    status, user = consume_telegram_link_code(text, chat_id)
    if status == "linked":
        display_name = user["full_name"] or user["username"]
        _send_telegram_message(settings, chat_id, f"Аккаунт привязан. Теперь бот доступен для {display_name}.", linked=True)
        return
    if status == "already_linked":
        _send_telegram_message(settings, chat_id, "Этот Telegram уже привязан к вашему аккаунту.", linked=True)
        return
    if status == "chat_in_use":
        _send_telegram_message(settings, chat_id, "Этот Telegram уже привязан к другому аккаунту. Сначала отвяжите его в личном кабинете.", linked=False)
        return
    _send_unlinked_help(settings, chat_id)


def _schedule_title(prefix: str, user: dict[str, Any]) -> str:
    return f"{prefix}\nГруппа: {user.get('group_name') or 'не выбрана'}"


def _send_schedule_two_messages(
    settings: dict[str, Any],
    chat_id: int,
    user: dict[str, Any],
    title_prefix: str,
    start_day: date,
    end_day: date,
) -> None:
    rows = fetch_user_schedule(user.get("preferred_group_id"), start_day, end_day)
    planshetka_rows = [row for row in rows if row["source"] == "planshetka"]
    rksi_rows = [row for row in rows if row["source"] == "rksi"]

    _send_telegram_message(
        settings,
        chat_id,
        format_schedule_rows_for_bot(planshetka_rows, _schedule_title(f"{title_prefix} (Planshetka)", user)),
        linked=True,
    )
    _send_telegram_message(
        settings,
        chat_id,
        format_schedule_rows_for_bot(rksi_rows, _schedule_title(f"{title_prefix} (РКСИ)", user)),
        linked=True,
    )


def _handle_linked_command(settings: dict[str, Any], chat_id: int, user: dict[str, Any], text: str) -> None:
    lowered = (text or "").strip().lower()
    if lowered in {"/start", "/help"}:
        _send_telegram_message(
            settings,
            chat_id,
            f"Здравствуйте, {user['full_name'] or user['username']}. Бот готов показывать расписание и уведомления.",
            linked=True,
            append_site_links=True,
        )
        return
    if lowered == "личный кабинет":
        base_url = settings.get("site_base_url") or ""
        if base_url:
            _send_telegram_message(settings, chat_id, f"Личный кабинет: {urljoin(base_url + '/', 'me')}", linked=True)
        else:
            _send_telegram_message(settings, chat_id, "Ссылка на сайт пока не настроена администратором.", linked=True)
        return
    if lowered == "изменения":
        events = fetch_recent_change_events_for_user(user["id"], limit=8)
        if not events:
            _send_telegram_message(settings, chat_id, "Свежих изменений по вашей группе пока нет.", linked=True)
            return
        lines = ["Последние изменения:"]
        for event in events:
            detected_at = event["detected_at"].strftime("%d.%m %H:%M") if event["detected_at"] else ""
            if detected_at:
                lines.append(f"• {detected_at} — {event['event_text']}")
            else:
                lines.append(f"• {event['event_text']}")
        _send_telegram_message(settings, chat_id, "\n".join(lines), linked=True)
        return
    if not user.get("preferred_group_id"):
        _send_telegram_message(settings, chat_id, "Сначала выберите учебную группу в личном кабинете.", linked=True)
        return
    if lowered == "сегодня":
        day = date.today()
        _send_schedule_two_messages(settings, chat_id, user, "Расписание на сегодня", day, day)
        return
    if lowered == "завтра":
        day = date.today() + timedelta(days=1)
        _send_schedule_two_messages(settings, chat_id, user, "Расписание на завтра", day, day)
        return
    if lowered == "неделя":
        start_day = date.today() - timedelta(days=date.today().weekday())
        _send_schedule_two_messages(settings, chat_id, user, "Расписание на неделю", start_day, start_day + timedelta(days=6))
        return
    if lowered == "как привязать аккаунт":
        _send_unlinked_help(settings, chat_id)
        return
    _send_telegram_message(settings, chat_id, "Не понял команду. Используйте кнопки бота или /help.", linked=True)


def process_telegram_update(settings: dict[str, Any], update: dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message") or {}
    chat = message.get("chat") or {}
    text = (message.get("text") or "").strip()
    chat_id = chat.get("id")
    if not chat_id or not text:
        return
    user = get_user_by_chat_id(int(chat_id))
    if not user:
        if text.startswith("/start") or text.lower() == "как привязать аккаунт":
            _send_unlinked_help(settings, int(chat_id))
            return
        _handle_link_code(settings, int(chat_id), text)
        return
    _handle_linked_command(settings, int(chat_id), user, text)


def _claim_delivery(user_id: int, event_id: int) -> bool:
    ensure_bot_tables()
    with get_bot_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO telegram_notification_deliveries(user_id, event_id, status) VALUES (%s, %s, 'pending') ON CONFLICT (user_id, event_id) DO NOTHING", (user_id, event_id))
            return cur.rowcount > 0


def _mark_delivery(user_id: int, event_id: int, status: str, error_text: str | None = None) -> None:
    ensure_bot_tables()
    with get_bot_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE telegram_notification_deliveries
                SET status = %s,
                    error_text = %s,
                    delivered_at = CASE WHEN %s = 'sent' THEN now() ELSE delivered_at END
                WHERE user_id = %s AND event_id = %s
                """,
                (status, error_text, status, user_id, event_id),
            )


def _build_lesson_reminder_key(lesson: dict[str, Any]) -> str:
    payload = "|".join(
        [
            lesson["source"],
            lesson["lesson_date"].isoformat(),
            lesson["start_time"].strftime("%H:%M") if lesson.get("start_time") else "",
            lesson.get("subject_name", ""),
            lesson.get("teacher_name", ""),
            lesson.get("room", ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _claim_lesson_reminder_delivery(user_id: int, reminder_key: str) -> bool:
    ensure_bot_tables()
    with get_bot_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO telegram_lesson_reminder_deliveries(user_id, reminder_key, status)
                VALUES (%s, %s, 'pending')
                ON CONFLICT (user_id, reminder_key) DO NOTHING
                """,
                (user_id, reminder_key),
            )
            return cur.rowcount > 0


def _mark_lesson_reminder_delivery(user_id: int, reminder_key: str, status: str, error_text: str | None = None) -> None:
    ensure_bot_tables()
    with get_bot_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE telegram_lesson_reminder_deliveries
                SET status = %s,
                    error_text = %s,
                    delivered_at = CASE WHEN %s = 'sent' THEN now() ELSE delivered_at END
                WHERE user_id = %s AND reminder_key = %s
                """,
                (status, error_text, status, user_id, reminder_key),
            )


def _format_next_lesson_message(lesson: dict[str, Any]) -> str:
    if lesson.get("start_time") and lesson.get("end_time"):
        time_text = f"{lesson['start_time'].strftime('%H:%M')}-{lesson['end_time'].strftime('%H:%M')}"
    elif lesson.get("start_time"):
        time_text = lesson["start_time"].strftime("%H:%M")
    else:
        time_text = "без времени"
    lines = [
        "Следующая пара",
        f"• {time_text}",
        f"  {lesson['subject_name']}",
    ]
    details: list[str] = []
    if lesson.get("teacher_name"):
        details.append(lesson["teacher_name"])
    if lesson.get("room"):
        details.append(f"ауд. {lesson['room']}")
    if details:
        lines.append(f"  {' | '.join(details)}")
    lines.append(f"  Источник: {SOURCE_TITLES.get(lesson['source'], lesson['source'])}")
    return "\n".join(lines)


def send_upcoming_lesson_reminders_once(settings: dict[str, Any]) -> int:
    if not settings.get("bot_token") or not settings.get("notifications_enabled"):
        return 0

    now_dt = datetime.now()
    window_end = now_dt + timedelta(minutes=LESSON_REMINDER_WINDOW_MINUTES)
    today = now_dt.date()
    tomorrow = today + timedelta(days=1)

    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, telegram_chat_id, preferred_group_id
                FROM site_users
                WHERE telegram_chat_id IS NOT NULL
                  AND telegram_lesson_notifications_enabled = TRUE
                  AND is_active = TRUE
                """
            )
            users = cur.fetchall()

    sent_count = 0
    for user_id, chat_id, group_id in users:
        if not group_id:
            continue
        rows = fetch_user_schedule(group_id, today, tomorrow)
        candidates: list[tuple[datetime, dict[str, Any]]] = []
        for row in rows:
            if not row.get("start_time"):
                continue
            start_dt = datetime.combine(row["lesson_date"], row["start_time"])
            if now_dt < start_dt <= window_end:
                candidates.append((start_dt, row))
        if not candidates:
            continue

        candidates.sort(key=lambda item: (item[0], LESSON_SOURCE_PRIORITY.get(item[1]["source"], 99), item[1]["subject_name"].lower()))
        next_lesson = candidates[0][1]
        reminder_key = _build_lesson_reminder_key(next_lesson)
        if not _claim_lesson_reminder_delivery(user_id, reminder_key):
            continue
        try:
            _send_telegram_message(settings, int(chat_id), _format_next_lesson_message(next_lesson), linked=True)
            _mark_lesson_reminder_delivery(user_id, reminder_key, "sent")
            sent_count += 1
        except Exception as exc:
            _mark_lesson_reminder_delivery(user_id, reminder_key, "failed", str(exc)[:500])
    return sent_count


def send_pending_notifications_once(settings: dict[str, Any]) -> int:
    if not settings.get("bot_token") or not settings.get("notifications_enabled"):
        return 0
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.id, u.telegram_chat_id, e.id, e.event_text
                FROM group_schedule_change_events e
                JOIN site_users u ON u.preferred_group_id = e.group_id
                WHERE u.telegram_chat_id IS NOT NULL
                  AND u.telegram_notifications_enabled = TRUE
                  AND u.is_active = TRUE
                  AND e.detected_at >= now() - interval '7 days'
                ORDER BY e.id ASC
                LIMIT 25
                """
            )
            rows = cur.fetchall()
    sent_count = 0
    for user_id, chat_id, event_id, event_text in rows:
        if not _claim_delivery(user_id, event_id):
            continue
        try:
            _send_telegram_message(settings, int(chat_id), f"Изменение в расписании:\n{event_text}", linked=True)
            _mark_delivery(user_id, event_id, "sent")
            sent_count += 1
        except Exception as exc:
            _mark_delivery(user_id, event_id, "failed", str(exc)[:500])
    return sent_count


def _service_lock_loop(lock_id: int, worker) -> None:
    while True:
        conn = None
        try:
            try:
                conn = psycopg2.connect(build_dsn("BOT_DB"))
            except RuntimeError:
                conn = psycopg2.connect(build_dsn("DB"))
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
                locked = bool(cur.fetchone()[0])
            if not locked:
                time.sleep(10)
                continue
            worker()
        except Exception as exc:
            print(f"[personalization] background worker error: {exc}")
            time.sleep(10)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


def telegram_polling_worker() -> None:
    def worker():
        while True:
            settings = load_telegram_settings()
            if not settings.get("polling_enabled") or not settings.get("bot_token"):
                time.sleep(10)
                continue
            params = {"timeout": 20}
            if settings.get("last_update_id") is not None:
                params["offset"] = int(settings["last_update_id"]) + 1
            response = requests.get(f"https://api.telegram.org/bot{settings['bot_token']}/getUpdates", params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            if not payload.get("ok"):
                raise RuntimeError(payload.get("description") or "Telegram getUpdates failed")
            for update in payload.get("result", []):
                update_id = int(update["update_id"])
                try:
                    process_telegram_update(settings, update)
                except Exception as exc:
                    # Advance the offset even if replying to this update failed,
                    # otherwise the same incoming message gets processed forever.
                    print(f"[personalization] telegram update {update_id} failed: {exc}")
                finally:
                    update_last_telegram_update_id(update_id)
            time.sleep(1)

    _service_lock_loop(TELEGRAM_POLL_LOCK_ID, worker)


def telegram_notification_worker() -> None:
    def worker():
        while True:
            settings = load_telegram_settings()
            if settings.get("notifications_enabled") and settings.get("bot_token"):
                send_pending_notifications_once(settings)
                send_upcoming_lesson_reminders_once(settings)
            time.sleep(20)

    _service_lock_loop(TELEGRAM_NOTIFY_LOCK_ID, worker)
