import os
import re
from datetime import date, datetime, timedelta
from typing import Any

import requests
from psycopg2.extras import Json

from db import get_llm_conn, get_main_conn

SEARCH_TRIGGERS = ("найди ", "поищи ", "ищи ", "поиск ", "search ")
NOTE_TRIGGERS = ("/note ", "создай заметку", "добавь заметку", "сделай заметку")
QUICK_COMMANDS = {
    "/today": "today",
    "/quick today": "today",
    "/tomorrow": "tomorrow",
    "/quick tomorrow": "tomorrow",
    "/week": "week",
    "/quick week": "week",
    "/changes": "changes",
    "/quick changes": "changes",
}
STUDY_TRIGGERS = (
    "план подготовки по ",
    "план по предмету ",
    "учебный план по ",
)
DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")


def ensure_llm_tables() -> None:
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_chat_sessions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    title VARCHAR(180),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_chat_messages (
                    id BIGSERIAL PRIMARY KEY,
                    session_id BIGINT NOT NULL REFERENCES ai_chat_sessions(id) ON DELETE CASCADE,
                    role VARCHAR(16) NOT NULL,
                    content TEXT NOT NULL,
                    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT chk_ai_chat_messages_role CHECK (role IN ('user', 'assistant', 'system'))
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ai_chat_sessions_user_updated
                ON ai_chat_sessions(user_id, updated_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ai_chat_messages_session_created
                ON ai_chat_messages(session_id, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_user_settings (
                    user_id BIGINT PRIMARY KEY,
                    allow_note_creation BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_pending_notes (
                    user_id BIGINT PRIMARY KEY,
                    session_id BIGINT NOT NULL REFERENCES ai_chat_sessions(id) ON DELETE CASCADE,
                    title VARCHAR(180) NOT NULL,
                    note_text TEXT,
                    due_date DATE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ai_pending_notes_session
                ON ai_pending_notes(session_id, updated_at DESC)
                """
            )


def _normalize_text(raw_text: str) -> str:
    return re.sub(r"\s+", " ", (raw_text or "").strip())


def _normalize_message_text(raw_text: str) -> str:
    text = (raw_text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return ""

    cleaned_lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    compact_lines: list[str] = []
    last_was_blank = False
    for line in cleaned_lines:
        if not line:
            if last_was_blank:
                continue
            compact_lines.append("")
            last_was_blank = True
        else:
            compact_lines.append(line)
            last_was_blank = False

    return "\n".join(compact_lines).strip()


def get_or_create_chat_session(user_id: int) -> int:
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM ai_chat_sessions
                WHERE user_id = %s
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (user_id,),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])
            cur.execute(
                """
                INSERT INTO ai_chat_sessions(user_id, title)
                VALUES (%s, %s)
                RETURNING id
                """,
                (user_id, "Диалог с ИИ"),
            )
            return int(cur.fetchone()[0])


def fetch_chat_messages(session_id: int, limit: int = 40) -> list[dict[str, Any]]:
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT role, content, COALESCE(meta, '{}'::jsonb), created_at
                FROM ai_chat_messages
                WHERE session_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (session_id, max(1, limit)),
            )
            rows = cur.fetchall()
    rows.reverse()
    return [{"role": row[0], "content": row[1], "meta": row[2], "created_at": row[3]} for row in rows]


def save_chat_message(session_id: int, role: str, content: str, meta: dict[str, Any] | None = None) -> None:
    cleaned = _normalize_message_text(content)
    if not cleaned:
        return
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ai_chat_messages(session_id, role, content, meta)
                VALUES (%s, %s, %s, %s)
                """,
                (session_id, role, cleaned, Json(meta or {})),
            )
            cur.execute("UPDATE ai_chat_sessions SET updated_at = now() WHERE id = %s", (session_id,))


def get_ai_user_settings(user_id: int) -> dict[str, Any]:
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO ai_user_settings(user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (user_id,))
            cur.execute("SELECT allow_note_creation FROM ai_user_settings WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
    return {"allow_note_creation": bool(row[0]) if row else False}


def update_ai_user_settings(user_id: int, allow_note_creation: bool) -> dict[str, Any]:
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ai_user_settings(user_id, allow_note_creation, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (user_id)
                DO UPDATE SET allow_note_creation = EXCLUDED.allow_note_creation, updated_at = now()
                """,
                (user_id, allow_note_creation),
            )
    return {"allow_note_creation": bool(allow_note_creation)}


def upsert_pending_note(user_id: int, session_id: int, note_payload: dict[str, str]) -> dict[str, Any]:
    title = _normalize_text(note_payload.get("title", ""))[:180]
    note_text = (note_payload.get("note_text") or "").strip() or None
    due_date_raw = (note_payload.get("due_date") or "").strip()
    due_date = due_date_raw or None
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ai_pending_notes(user_id, session_id, title, note_text, due_date, updated_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (user_id)
                DO UPDATE SET
                    session_id = EXCLUDED.session_id,
                    title = EXCLUDED.title,
                    note_text = EXCLUDED.note_text,
                    due_date = EXCLUDED.due_date,
                    updated_at = now()
                """,
                (user_id, session_id, title, note_text, due_date),
            )
    return {"title": title, "note_text": note_text or "", "due_date": due_date or ""}


def get_pending_note(user_id: int) -> dict[str, Any] | None:
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT title, COALESCE(note_text, ''), due_date, updated_at
                FROM ai_pending_notes
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {"title": row[0], "note_text": row[1], "due_date": row[2].isoformat() if row[2] else "", "updated_at": row[3]}


def clear_pending_note(user_id: int) -> None:
    with get_llm_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ai_pending_notes WHERE user_id = %s", (user_id,))


def _load_user_group(user_id: int) -> tuple[int | None, str | None]:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.preferred_group_id, g.group_name
                FROM site_users u
                LEFT JOIN study_groups g ON g.id = u.preferred_group_id
                WHERE u.id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def fetch_group_subject_options(user_id: int, limit: int = 300) -> list[str]:
    group_id, _ = _load_user_group(user_id)
    if not group_id:
        return []

    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT x.subject_name
                FROM (
                  SELECT subj.subject_name
                  FROM schedule_entries s
                  JOIN subjects subj ON subj.id = s.subject_id
                  WHERE s.group_id = %s
                  UNION
                  SELECT subj.subject_name
                  FROM parsed_schedule_entries p
                  JOIN subjects subj ON subj.id = p.subject_id
                  WHERE p.group_id = %s
                ) x
                WHERE x.subject_name IS NOT NULL
                  AND btrim(x.subject_name) <> ''
                ORDER BY x.subject_name
                LIMIT %s
                """,
                (group_id, group_id, max(1, limit)),
            )
            rows = cur.fetchall()
    return [row[0] for row in rows if (row[0] or "").strip()]


def _format_time_range(start_time: Any, end_time: Any) -> str:
    if start_time and end_time:
        return f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}"
    if start_time:
        return f"{start_time.strftime('%H:%M')}"
    return "время не указано"


def _format_schedule_rows(rows: list[tuple]) -> str:
    if not rows:
        return "Записей не найдено."
    lines: list[str] = []
    for row in rows:
        lesson_date, start_time, end_time, subject_name, teacher_name, room, source, group_name = row
        date_text = lesson_date.strftime("%d.%m.%Y")
        teacher_text = teacher_name or "преподаватель не указан"
        room_text = room or "ауд. не указана"
        source_label = {"manual": "ручное", "rksi": "РКСИ", "planshetka": "Planshetka"}.get(source, source)
        lines.append(
            f"- {date_text} {_format_time_range(start_time, end_time)} | {subject_name} | {teacher_text} | {room_text} | {group_name} | {source_label}"
        )
    return "\n".join(lines)


def fetch_schedule_context(user_id: int, start_day: date, end_day: date, limit: int, keyword: str | None = None) -> str:
    group_id, group_name = _load_user_group(user_id)
    if not group_id:
        return "У пользователя не выбрана основная группа. Дай инструкцию выбрать группу в личном кабинете."

    sql = """
        SELECT x.lesson_date, x.start_time, x.end_time, x.subject_name, x.teacher_name, x.room, x.source, x.group_name
        FROM (
          SELECT s.lesson_date, s.start_time, s.end_time, subj.subject_name,
                 COALESCE(t.full_name, '') AS teacher_name, COALESCE(s.room, t.room, '') AS room,
                 'manual' AS source, s.group_id, g.group_name
          FROM schedule_entries s
          JOIN subjects subj ON subj.id = s.subject_id
          LEFT JOIN teachers t ON t.id = s.teacher_id
          JOIN study_groups g ON g.id = s.group_id
          UNION ALL
          SELECT p.lesson_date, p.start_time, p.end_time, subj.subject_name,
                 COALESCE(t.full_name, p.raw_teacher_name, '') AS teacher_name, COALESCE(p.room, t.room, '') AS room,
                 'rksi' AS source, p.group_id, g.group_name
          FROM parsed_schedule_entries p
          JOIN subjects subj ON subj.id = p.subject_id
          LEFT JOIN teachers t ON t.id = p.teacher_id
          JOIN study_groups g ON g.id = p.group_id
          UNION ALL
          SELECT pt.lesson_date, pt.start_time, pt.end_time, subj.subject_name,
                 COALESCE(t.full_name, pt.raw_teacher_name, '') AS teacher_name, COALESCE(pt.room, t.room, '') AS room,
                 'planshetka' AS source, pt.group_id, g.group_name
          FROM parsed_tabletka_schedule_entries pt
          JOIN subjects subj ON subj.id = pt.subject_id
          LEFT JOIN teachers t ON t.id = pt.teacher_id
          JOIN study_groups g ON g.id = pt.group_id
        ) x
        WHERE x.lesson_date BETWEEN %s AND %s
          AND x.group_id = %s
    """
    params: list[Any] = [start_day, end_day, group_id]
    if keyword:
        sql += " AND (x.subject_name ILIKE %s OR COALESCE(x.teacher_name, '') ILIKE %s OR COALESCE(x.room, '') ILIKE %s)"
        like = f"%{keyword}%"
        params.extend([like, like, like])
    sql += " ORDER BY x.lesson_date, x.start_time NULLS LAST, x.end_time NULLS LAST, x.subject_name LIMIT %s"
    params.append(max(1, limit))

    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
    title = f"Группа пользователя: {group_name or 'не указана'}"
    return f"{title}\n{_format_schedule_rows(rows)}"


def extract_quick_command(user_text: str) -> str | None:
    text = _normalize_text(user_text).lower()
    if not text:
        return None
    if text in QUICK_COMMANDS:
        return QUICK_COMMANDS[text]
    if "что изменилось" in text or ("изменен" in text and "вчера" in text):
        return "changes"
    return None


def _week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def _weekday_label(day: date) -> str:
    names = {
        0: "понедельник",
        1: "вторник",
        2: "среда",
        3: "четверг",
        4: "пятница",
        5: "суббота",
        6: "воскресенье",
    }
    return names.get(day.weekday(), "")


def build_recent_changes_summary(user_id: int, hours: int = 24, limit: int = 25) -> str:
    group_id, group_name = _load_user_group(user_id)
    if not group_id:
        return "Не могу показать изменения: в личном кабинете не выбрана основная группа."
    since_dt = datetime.now() - timedelta(hours=max(1, hours))
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT detected_at, event_text, source_name, lesson_date, start_time
                FROM group_schedule_change_events
                WHERE group_id = %s
                  AND detected_at >= %s
                ORDER BY detected_at DESC
                LIMIT %s
                """,
                (group_id, since_dt, max(1, limit)),
            )
            rows = cur.fetchall()
    if not rows:
        return f"Группа {group_name or '-'}: за последние {hours} часов изменений нет."
    lines = [f"Группа {group_name or '-'}: изменения за последние {hours} часов:"]
    for detected_at, event_text, source_name, lesson_date, start_time in rows:
        detected_text = detected_at.strftime("%d.%m %H:%M") if detected_at else "--"
        lesson_text = lesson_date.strftime("%d.%m.%Y") if lesson_date else "без даты"
        if start_time:
            lesson_text += f" {start_time.strftime('%H:%M')}"
        lines.append(f"- [{detected_text}] {event_text} ({lesson_text}, источник: {source_name})")
    return "\n".join(lines)


def build_quick_reply(user_id: int, quick_command: str) -> tuple[str, dict[str, Any]]:
    today = date.today()
    if quick_command == "today":
        text = "Расписание на сегодня:\n" + fetch_schedule_context(user_id, today, today, limit=80)
        return text, {"quick_command": "today", "date_from": today.isoformat(), "date_to": today.isoformat()}
    if quick_command == "tomorrow":
        next_day = today + timedelta(days=1)
        text = "Расписание на завтра:\n" + fetch_schedule_context(user_id, next_day, next_day, limit=80)
        return text, {"quick_command": "tomorrow", "date_from": next_day.isoformat(), "date_to": next_day.isoformat()}
    if quick_command == "week":
        start_day = _week_start(today)
        end_day = start_day + timedelta(days=6)
        text = "Расписание на текущую неделю:\n" + fetch_schedule_context(user_id, start_day, end_day, limit=200)
        return text, {"quick_command": "week", "date_from": start_day.isoformat(), "date_to": end_day.isoformat()}
    if quick_command == "changes":
        text = build_recent_changes_summary(user_id, hours=24, limit=30)
        return text, {"quick_command": "changes", "window_hours": 24}
    return "Не понял быструю команду.", {"quick_command": "unknown"}


def extract_study_subject(user_text: str) -> str | None:
    text = _normalize_text(user_text)
    if not text:
        return None
    lower = text.lower()

    if lower.startswith("/study "):
        return text[7:].strip()
    if lower == "/study":
        return ""

    for trigger in STUDY_TRIGGERS:
        idx = lower.find(trigger)
        if idx >= 0:
            return text[idx + len(trigger):].strip(" .,:;!?")

    return None


def _fetch_subject_lessons_for_week(user_id: int, subject_query: str, days: int = 7, limit: int = 120) -> tuple[date, date, list[tuple]]:
    group_id, _ = _load_user_group(user_id)
    if not group_id:
        return date.today(), date.today(), []

    start_day = date.today()
    end_day = start_day + timedelta(days=max(1, days) - 1)
    sql = """
        SELECT x.lesson_date, x.start_time, x.end_time, x.subject_name, x.teacher_name, x.room
        FROM (
          SELECT s.lesson_date, s.start_time, s.end_time, subj.subject_name,
                 COALESCE(t.full_name, '') AS teacher_name, COALESCE(s.room, t.room, '') AS room, s.group_id
          FROM schedule_entries s
          JOIN subjects subj ON subj.id = s.subject_id
          LEFT JOIN teachers t ON t.id = s.teacher_id
          UNION ALL
          SELECT p.lesson_date, p.start_time, p.end_time, subj.subject_name,
                 COALESCE(t.full_name, p.raw_teacher_name, '') AS teacher_name, COALESCE(p.room, t.room, '') AS room, p.group_id
          FROM parsed_schedule_entries p
          JOIN subjects subj ON subj.id = p.subject_id
          LEFT JOIN teachers t ON t.id = p.teacher_id
        ) x
        WHERE x.lesson_date BETWEEN %s AND %s
          AND x.group_id = %s
          AND x.subject_name ILIKE %s
        ORDER BY x.lesson_date, x.start_time NULLS LAST, x.end_time NULLS LAST, x.subject_name
        LIMIT %s
    """
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_day, end_day, group_id, f"%{subject_query}%", max(1, limit)))
            rows = cur.fetchall()

    deduped: list[tuple] = []
    seen: set[tuple] = set()
    for row in rows:
        key = (row[0], row[1], row[2], row[3], row[4], row[5])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return start_day, end_day, deduped


def build_study_plan(user_id: int, subject_query: str) -> tuple[str, dict[str, Any]]:
    subject_query = _normalize_text(subject_query)
    if not subject_query:
        return (
            "Укажите предмет после команды, например: /study Математика",
            {"study_plan": False, "study_subject": "", "study_error": "subject_missing"},
        )

    group_id, group_name = _load_user_group(user_id)
    if not group_id:
        return (
            "Не могу построить план: в личном кабинете не выбрана основная группа.",
            {"study_plan": False, "study_subject": subject_query, "study_error": "group_missing"},
        )

    start_day, end_day, rows = _fetch_subject_lessons_for_week(user_id, subject_query, days=7, limit=150)
    if not rows:
        text = (
            f"По предмету «{subject_query}» на период {start_day.strftime('%d.%m.%Y')} - {end_day.strftime('%d.%m.%Y')} "
            "занятий не найдено. Попробуйте более точное название предмета."
        )
        return text, {
            "study_plan": False,
            "study_subject": subject_query,
            "date_from": start_day.isoformat(),
            "date_to": end_day.isoformat(),
            "lessons_count": 0,
        }

    subject_name = rows[0][3]
    lessons_by_day: dict[date, list[tuple]] = {}
    for row in rows:
        lessons_by_day.setdefault(row[0], []).append(row)

    days = [start_day + timedelta(days=i) for i in range((end_day - start_day).days + 1)]
    lesson_days = sorted(lessons_by_day.keys())
    first_lesson_day = lesson_days[0]

    lines: list[str] = []
    lines.append(f"План подготовки на неделю по предмету «{subject_name}» (группа {group_name or '-'}).")
    lines.append(f"Период: {start_day.strftime('%d.%m.%Y')} - {end_day.strftime('%d.%m.%Y')}.")
    lines.append("")

    for day in days:
        header = f"{day.strftime('%d.%m.%Y')} ({_weekday_label(day)})"
        day_lessons = lessons_by_day.get(day, [])
        if day_lessons:
            lesson_parts: list[str] = []
            for lesson in day_lessons:
                time_text = _format_time_range(lesson[1], lesson[2])
                room_text = f", ауд. {lesson[5]}" if lesson[5] else ""
                lesson_parts.append(f"{time_text}{room_text}")
            lines.append(f"{header}: пары по предмету ({'; '.join(lesson_parts)}).")
            lines.append("- До пары: 20-25 минут на повторение теории и формул.")
            lines.append("- После пары: 30-40 минут закрепления (конспект + 3-5 задач).")
            lines.append("- Вечером: короткий чек-лист вопросов, которые остались непонятны.")
        else:
            if day < first_lesson_day:
                lines.append(f"{header}: базовая подготовка 30-40 минут (теория + термины).")
            elif day == end_day:
                lines.append(f"{header}: итоговое повторение недели 40-60 минут и мини-самопроверка.")
            else:
                lines.append(f"{header}: поддерживающая сессия 20-30 минут (повтор и 2-3 задачи).")
        lines.append("")

    lines.append("Если хотите, могу на основе этого плана сразу подготовить набор заметок в ЛК.")
    text = "\n".join(lines).strip()
    meta = {
        "study_plan": True,
        "study_subject": subject_name,
        "date_from": start_day.isoformat(),
        "date_to": end_day.isoformat(),
        "lessons_count": len(rows),
    }
    return text, meta


def extract_search_query(user_text: str) -> str | None:
    text = _normalize_text(user_text)
    if not text:
        return None
    lower = text.lower()
    if lower.startswith("/search "):
        query = text[8:].strip()
        return query if query else None
    for trigger in SEARCH_TRIGGERS:
        idx = lower.find(trigger)
        if idx >= 0:
            query = text[idx + len(trigger):].strip(" :")
            return query if len(query) >= 2 else None
    return None


def extract_note_payload(user_text: str) -> dict[str, str] | None:
    text = _normalize_text(user_text)
    if not text:
        return None
    lower = text.lower()
    payload = ""
    note_intent_detected = False

    if lower.startswith("/note "):
        note_intent_detected = True
        payload = text[6:].strip()
    else:
        for trigger in NOTE_TRIGGERS[1:]:
            idx = lower.find(trigger)
            if idx >= 0:
                note_intent_detected = True
                payload = text[idx + len(trigger):].strip(" :,-")
                break

    if not note_intent_detected:
        return None
    if not payload:
        return {"title": "", "note_text": "", "due_date": ""}

    parts = [part.strip() for part in payload.split("|")]
    title = parts[0][:180] if parts else ""
    note_text = parts[1] if len(parts) > 1 else ""
    due_date = ""

    if len(parts) > 2:
        due_date = normalize_due_date(parts[2])
    else:
        date_match = DATE_RE.search(payload)
        if date_match:
            due_date = normalize_due_date(date_match.group(1))

    if not title:
        return {"title": "", "note_text": "", "due_date": ""}
    return {"title": title, "note_text": note_text, "due_date": due_date}


def normalize_due_date(raw_text: str) -> str:
    value = _normalize_text(raw_text).strip(" .")
    if not value:
        return ""
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def build_ollama_messages(
    user_text: str,
    recent_messages: list[dict[str, Any]],
    schedule_context: str,
    search_context: str | None,
    can_create_note: bool,
) -> list[dict[str, str]]:
    system_prompt = (
        "Ты ассистент приложения расписания РКСИ. Отвечай кратко, по делу и дружелюбно. "
        "Опирайся только на переданный контекст расписания. Если данных нет, честно скажи об этом. "
        "Если пользователь просит создать заметку, напомни формат: /note Заголовок | Текст | YYYY-MM-DD "
        "и то, что после этого нужно подтвердить действие. "
        "Для недельного плана подготовки по предмету используй команду /study Название предмета. "
        f"Разрешение на создание заметок: {'включено' if can_create_note else 'выключено'}."
    )
    context_prompt = "Контекст расписания:\n" + schedule_context
    if search_context:
        context_prompt += "\n\nРезультат поиска:\n" + search_context

    messages: list[dict[str, str]] = [{"role": "system", "content": system_prompt}, {"role": "system", "content": context_prompt}]
    for item in recent_messages[-10:]:
        role = item.get("role")
        if role in {"user", "assistant"}:
            messages.append({"role": role, "content": str(item.get("content", ""))})
    if not messages or messages[-1].get("role") != "user" or messages[-1].get("content") != user_text:
        messages.append({"role": "user", "content": user_text})
    return messages


def call_ollama_chat(messages: list[dict[str, str]]) -> str:
    ollama_url = (os.getenv("OLLAMA_URL") or "http://127.0.0.1:11434").rstrip("/")
    ollama_model = (os.getenv("OLLAMA_MODEL") or "qwen2.5:7b-instruct").strip()
    response = requests.post(
        f"{ollama_url}/api/chat",
        json={"model": ollama_model, "stream": False, "messages": messages, "options": {"temperature": 0.2}},
        timeout=90,
    )
    response.raise_for_status()
    payload = response.json()
    answer = ((payload.get("message") or {}).get("content") or "").strip()
    if not answer:
        raise RuntimeError("Пустой ответ от Ollama.")
    return answer


def build_default_schedule_context(user_id: int) -> str:
    today = date.today()
    return fetch_schedule_context(user_id, today, today + timedelta(days=7), limit=40)
