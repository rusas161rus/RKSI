import os
import re
from datetime import date, datetime, timedelta
from typing import Any

import requests
from psycopg2.extras import Json

from db import get_llm_conn, get_main_conn

SEARCH_TRIGGERS = ("найди ", "поищи ", "ищи ", "поиск ", "search ")
NOTE_TRIGGERS = ("/note ", "создай заметку", "добавь заметку", "сделай заметку")
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


def _normalize_text(raw_text: str) -> str:
    return re.sub(r"\s+", " ", (raw_text or "").strip())


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
    cleaned = _normalize_text(content)
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
        "Если пользователь просит создать заметку, напомни формат: /note Заголовок | Текст | YYYY-MM-DD. "
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
