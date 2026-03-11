import os
import platform
import re
import time
from datetime import date, datetime, timedelta
from functools import wraps
from threading import Lock, Thread

from dotenv import load_dotenv
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from psycopg2.extras import Json
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

from db import ensure_schedule_room_columns, get_main_conn, is_composite_teacher_name
from llm_assistant import (
    build_quick_reply,
    build_study_plan,
    build_default_schedule_context,
    build_ollama_messages,
    call_ollama_chat,
    clear_chat_messages,
    clear_pending_note,
    ensure_llm_tables,
    extract_add_notes_request,
    extract_note_commands,
    extract_quick_command,
    extract_note_payload,
    extract_recent_assistant_note_commands,
    extract_search_query,
    extract_study_subject,
    fetch_group_subject_options,
    fetch_chat_messages,
    get_pending_note,
    fetch_schedule_context,
    get_ai_user_settings,
    get_or_create_chat_session,
    save_chat_message,
    upsert_pending_note,
    update_ai_user_settings,
)
from personalization import (
    build_today_summary,
    create_announcement,
    create_user_note,
    delete_announcement,
    delete_user_note,
    detect_and_record_schedule_changes,
    ensure_bot_tables,
    ensure_personalization_tables,
    fetch_admin_announcements,
    fetch_announcements_for_user,
    fetch_favorite_teachers,
    fetch_recent_change_events_for_user,
    fetch_source_conflicts,
    fetch_user_notes,
    generate_telegram_link_code,
    load_telegram_settings,
    save_telegram_settings,
    telegram_notification_worker,
    telegram_polling_worker,
    toggle_announcement,
    toggle_favorite_teacher,
    toggle_user_note,
    unlink_telegram_account,
)
from scripts.parse_and_sync import fetch_group_map, run as run_parser_group
from scripts.parse_tabletka_sync import (
    clear_planshetka_data,
    ensure_planshetka_tables,
    get_planshetka_folder_url,
    get_planshetka_recent_files_limit,
    run_planshetka_sync,
    update_planshetka_folder_url,
    update_planshetka_recent_files_limit,
)

load_dotenv()
app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
if os.getenv("TRUST_PROXY", "1").strip().lower() in {"1", "true", "yes"}:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

DAY_NAMES = {0: "понедельник", 1: "вторник", 2: "среда", 3: "четверг", 4: "пятница", 5: "суббота", 6: "воскресенье"}
MONTHS_RU = {1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня", 7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"}
PERIOD_OPTIONS = {"day": "День", "day_next": "Следующий день", "week_current": "Текущая неделя", "week_next": "Следующая неделя", "week_prev": "Прошлая неделя", "two_weeks": "2 недели (текущая + следующая)"}
WEEKDAY_OPTIONS = [(0, "Понедельник"), (1, "Вторник"), (2, "Среда"), (3, "Четверг"), (4, "Пятница"), (5, "Суббота"), (6, "Воскресенье")]
AUTO_SCHEDULER_ENABLED = os.getenv("ENABLE_BACKGROUND_PARSER", "0").strip().lower() in {"1", "true", "yes"}
RKSI_SCHEDULE_URL = "https://www.rksi.ru/schedules"
RKSI_MOBILE_SCHEDULE_URL = "https://www.rksi.ru/mobileschedule"
DASHBOARD_WIDGETS = [
    {"id": "announcements", "title": "Объявления", "default_size": "full"},
    {"id": "profile", "title": "Личный кабинет", "default_size": "half"},
    {"id": "telegram", "title": "Привязка Telegram", "default_size": "quarter"},
    {"id": "favorites", "title": "Избранные преподаватели", "default_size": "quarter"},
    {"id": "changes", "title": "Лента изменений", "default_size": "half"},
    {"id": "today", "title": "Сегодня", "default_size": "half"},
    {"id": "notes", "title": "Мои заметки и дедлайны", "default_size": "half"},
    {"id": "conflicts", "title": "Расхождения источников", "default_size": "half"},
    {"id": "schedule", "title": "Расписание", "default_size": "full"},
]

RUNTIME_STATE_DEFAULTS = {
    "parser": {
        "summary": "Парсер ещё не запускался из админки.",
        "log_lines": [],
        "failed_groups": [],
        "is_running": False,
        "stop_requested": False,
        "failed_files": 0,
        "scanned_files": 0,
        "last_run_at": None,
    },
    "planshetka": {
        "summary": "Парсер Planshetka ещё не запускался.",
        "log_lines": [],
        "failed_groups": [],
        "is_running": False,
        "stop_requested": False,
        "failed_files": 0,
        "scanned_files": 0,
        "last_run_at": None,
    },
}
RUNTIME_STATE_LOCKS = {"parser": Lock(), "planshetka": Lock()}
APP_STARTED_AT = datetime.now()
ONLINE_USERS_WINDOW_MINUTES = max(1, int((os.getenv("ONLINE_USERS_WINDOW_MINUTES") or "15").strip() or "15"))
MONITOR_STATE_LOCK = Lock()
MONITOR_CPU_STATE = {"total": None, "idle": None, "ts": None}
MONITOR_NET_STATE = {"rx": None, "tx": None, "ts": None}

INVALID_TEACHER_NAME_RE = re.compile(r"^[\s_.-]+$")


def normalize_teacher_name(raw_name: str | None) -> str | None:
    normalized = re.sub(r"\\s+", " ", (raw_name or "").strip())
    if not normalized:
        return None
    if INVALID_TEACHER_NAME_RE.fullmatch(normalized):
        return None
    if is_composite_teacher_name(normalized):
        return None
    return normalized


def parse_int_ids(values: list[str]) -> list[int]:
    return sorted({int(v) for v in values if (v or "").strip().isdigit()})


def delete_rows_with_savepoints(cur, table_name: str, row_ids: list[int]) -> tuple[int, list[int], list[int]]:
    if table_name not in {"teachers", "study_groups", "subjects", "site_users"}:
        raise ValueError(f"Unsupported table: {table_name}")

    deleted = 0
    missing: list[int] = []
    failed: list[int] = []

    for row_id in row_ids:
        cur.execute("SAVEPOINT bulk_delete_sp")
        try:
            cur.execute(f"DELETE FROM {table_name} WHERE id = %s", (row_id,))
            if cur.rowcount:
                deleted += 1
            else:
                missing.append(row_id)
            cur.execute("RELEASE SAVEPOINT bulk_delete_sp")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT bulk_delete_sp")
            cur.execute("RELEASE SAVEPOINT bulk_delete_sp")
            failed.append(row_id)

    return deleted, missing, failed


def format_day_header(day: date) -> str:
    return f"{day.day} {MONTHS_RU[day.month]}, {DAY_NAMES[day.weekday()]}"


def week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def resolve_period_range(anchor_day: date, period: str) -> tuple[date, date]:
    if period == "day":
        return anchor_day, anchor_day
    if period == "day_next":
        next_day = anchor_day + timedelta(days=1)
        return next_day, next_day
    start = week_start(anchor_day)
    if period == "week_prev":
        start -= timedelta(days=7)
        return start, start + timedelta(days=6)
    if period == "week_next":
        start += timedelta(days=7)
        return start, start + timedelta(days=6)
    if period == "two_weeks":
        return start, start + timedelta(days=13)
    return start, start + timedelta(days=6)


def build_days(start_day: date, end_day: date) -> list[date]:
    return [start_day + timedelta(days=i) for i in range((end_day - start_day).days + 1)]


def should_start_background_threads() -> bool:
    debug_mode = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes"}
    if debug_mode and os.getenv("WERKZEUG_RUN_MAIN") != "true":
        return False
    return True


def ensure_llm_ready() -> tuple[bool, str]:
    try:
        ensure_llm_tables()
        return True, ""
    except Exception as exc:
        return False, str(exc)


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        if not session.get("is_admin"):
            flash("Доступ только для админа.", "error")
            return redirect(url_for("me"))
        return view(*args, **kwargs)
    return wrapped


@app.before_request
def mark_online_user():
    user_id = session.get("user_id")
    if not user_id:
        return
    try:
        touch_user_presence(int(user_id))
    except Exception:
        # Online-метрики не должны ломать основной запрос.
        pass


def load_user(user_id: int):
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.username, u.full_name, u.preferred_group_id,
                       u.telegram_chat_id, u.telegram_link_code, u.telegram_link_code_created_at,
                       u.telegram_notifications_enabled, u.telegram_lesson_notifications_enabled, u.telegram_linked_at,
                       EXISTS(SELECT 1 FROM site_admins a WHERE a.user_id = u.id) AS is_admin
                FROM site_users u
                WHERE u.id = %s AND u.is_active = TRUE
            """, (user_id,))
            row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "username": row[1],
        "full_name": row[2],
        "preferred_group_id": row[3],
        "telegram_chat_id": row[4],
        "telegram_link_code": row[5],
        "telegram_link_code_created_at": row[6],
        "telegram_notifications_enabled": row[7],
        "telegram_lesson_notifications_enabled": row[8],
        "telegram_linked_at": row[9],
        "is_admin": row[10],
    }


def ensure_monitoring_tables() -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS online_user_presence (
                    user_id BIGINT PRIMARY KEY REFERENCES site_users(id) ON DELETE CASCADE,
                    last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_online_user_presence_last_seen
                ON online_user_presence(last_seen DESC)
                """
            )


def touch_user_presence(user_id: int) -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO online_user_presence(user_id, last_seen)
                VALUES (%s, now())
                ON CONFLICT (user_id)
                DO UPDATE SET last_seen = EXCLUDED.last_seen
                """,
                (user_id,),
            )


def count_online_users(window_minutes: int = ONLINE_USERS_WINDOW_MINUTES) -> int:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM online_user_presence
                WHERE last_seen >= now() - (%s * interval '1 minute')
                """,
                (max(1, int(window_minutes)),),
            )
            row = cur.fetchone()
    return int(row[0]) if row else 0


def _read_linux_cpu_totals() -> tuple[int, int] | None:
    try:
        with open("/proc/stat", "r", encoding="utf-8") as fh:
            first = fh.readline().strip()
        if not first.startswith("cpu "):
            return None
        parts = [int(value) for value in first.split()[1:] if value.isdigit()]
        if len(parts) < 4:
            return None
        total = sum(parts)
        idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
        return total, idle
    except Exception:
        return None


def _cpu_percent() -> float | None:
    now_ts = time.time()
    totals = _read_linux_cpu_totals()
    if not totals:
        return None
    total, idle = totals
    with MONITOR_STATE_LOCK:
        prev_total = MONITOR_CPU_STATE["total"]
        prev_idle = MONITOR_CPU_STATE["idle"]
        MONITOR_CPU_STATE.update({"total": total, "idle": idle, "ts": now_ts})
    if prev_total is None or prev_idle is None:
        return None
    delta_total = total - int(prev_total)
    delta_idle = idle - int(prev_idle)
    if delta_total <= 0:
        return None
    busy_ratio = max(0.0, min(1.0, 1.0 - (delta_idle / delta_total)))
    return round(busy_ratio * 100.0, 2)


def _memory_stats_bytes() -> dict[str, int] | None:
    try:
        values: dict[str, int] = {}
        with open("/proc/meminfo", "r", encoding="utf-8") as fh:
            for line in fh:
                if ":" not in line:
                    continue
                key, raw = line.split(":", 1)
                token = raw.strip().split()[0]
                if token.isdigit():
                    values[key] = int(token) * 1024
        total = values.get("MemTotal")
        available = values.get("MemAvailable")
        if total is None:
            return None
        if available is None:
            available = values.get("MemFree", 0) + values.get("Buffers", 0) + values.get("Cached", 0)
        used = max(0, total - available)
        return {"total": int(total), "used": int(used), "available": int(available)}
    except Exception:
        return None


def _read_linux_network_bytes() -> tuple[int, int] | None:
    try:
        rx_total = 0
        tx_total = 0
        with open("/proc/net/dev", "r", encoding="utf-8") as fh:
            for raw in fh.readlines()[2:]:
                if ":" not in raw:
                    continue
                iface, data = raw.split(":", 1)
                iface = iface.strip()
                if iface == "lo":
                    continue
                fields = data.split()
                if len(fields) < 16:
                    continue
                rx_total += int(fields[0])
                tx_total += int(fields[8])
        return rx_total, tx_total
    except Exception:
        return None


def _network_rates_bps() -> tuple[float | None, float | None]:
    now_ts = time.time()
    counters = _read_linux_network_bytes()
    if not counters:
        return None, None
    rx_now, tx_now = counters
    with MONITOR_STATE_LOCK:
        prev_rx = MONITOR_NET_STATE["rx"]
        prev_tx = MONITOR_NET_STATE["tx"]
        prev_ts = MONITOR_NET_STATE["ts"]
        MONITOR_NET_STATE.update({"rx": rx_now, "tx": tx_now, "ts": now_ts})
    if prev_rx is None or prev_tx is None or prev_ts is None:
        return None, None
    delta_t = now_ts - float(prev_ts)
    if delta_t <= 0:
        return None, None
    rx_rate = max(0.0, (rx_now - int(prev_rx)) / delta_t)
    tx_rate = max(0.0, (tx_now - int(prev_tx)) / delta_t)
    return rx_rate, tx_rate


def _read_linux_uptime_seconds() -> int | None:
    try:
        with open("/proc/uptime", "r", encoding="utf-8") as fh:
            token = fh.read().split()[0]
        return int(float(token))
    except Exception:
        return None


def collect_monitor_payload() -> dict[str, object]:
    now_dt = datetime.now()
    cpu_percent = _cpu_percent()
    if cpu_percent is None and hasattr(os, "getloadavg"):
        try:
            load_1m = os.getloadavg()[0]
            cpu_count = max(1, int(os.cpu_count() or 1))
            cpu_percent = round(max(0.0, min(100.0, (load_1m / cpu_count) * 100.0)), 2)
        except Exception:
            cpu_percent = None
    mem = _memory_stats_bytes() or {}
    rx_bps, tx_bps = _network_rates_bps()
    app_uptime_seconds = int(max(0.0, (now_dt - APP_STARTED_AT).total_seconds()))
    return {
        "app_time_iso": now_dt.isoformat(),
        "app_time_text": now_dt.strftime("%d.%m.%Y %H:%M:%S"),
        "app_uptime_seconds": app_uptime_seconds,
        "server_uptime_seconds": _read_linux_uptime_seconds(),
        "cpu_percent": cpu_percent,
        "cpu_count": int(os.cpu_count() or 1),
        "memory_total_bytes": mem.get("total"),
        "memory_used_bytes": mem.get("used"),
        "memory_available_bytes": mem.get("available"),
        "network_rx_bps": rx_bps,
        "network_tx_bps": tx_bps,
        "platform": platform.platform(),
    }


def ensure_auto_schedule_table() -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS parser_auto_schedule (
                    id SMALLINT PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    run_weekday SMALLINT NOT NULL DEFAULT 0 CHECK (run_weekday BETWEEN 0 AND 6),
                    run_time TIME NOT NULL DEFAULT '03:00:00',
                    last_triggered_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
            cur.execute("""
                INSERT INTO parser_auto_schedule(id, enabled, run_weekday, run_time)
                VALUES (1, FALSE, 0, '03:00:00')
                ON CONFLICT (id) DO NOTHING
            """)


def get_auto_schedule() -> dict:
    ensure_auto_schedule_table()
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT enabled, run_weekday, run_time, last_triggered_at FROM parser_auto_schedule WHERE id = 1")
            row = cur.fetchone()
    return {"enabled": bool(row[0]), "run_weekday": int(row[1]), "run_time": row[2], "last_triggered_at": row[3]}


def update_auto_schedule(enabled: bool, run_weekday: int, run_time_text: str) -> None:
    run_time = datetime.strptime(run_time_text, "%H:%M").time()
    ensure_auto_schedule_table()
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE parser_auto_schedule SET enabled=%s, run_weekday=%s, run_time=%s, updated_at=now() WHERE id=1", (enabled, run_weekday, run_time))


def clear_parser_storage() -> dict:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM parsed_schedule_entries")
            a = cur.rowcount
            cur.execute("DELETE FROM parser_lessons")
            b = cur.rowcount
            cur.execute("DELETE FROM parser_days")
            c = cur.rowcount
    return {"parsed_schedule_entries": a, "parser_lessons": b, "parser_days": c}


def deduplicate_storage() -> dict:
    return {"deleted_parser_days_duplicates": 0, "deleted_teachers_duplicates": 0, "deleted_subjects_duplicates": 0}


def ensure_runtime_state_table() -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS parser_runtime_state (
                    state_key VARCHAR(32) PRIMARY KEY,
                    summary TEXT NOT NULL DEFAULT '',
                    log_lines JSONB NOT NULL DEFAULT '[]'::jsonb,
                    failed_groups JSONB NOT NULL DEFAULT '[]'::jsonb,
                    is_running BOOLEAN NOT NULL DEFAULT FALSE,
                    stop_requested BOOLEAN NOT NULL DEFAULT FALSE,
                    failed_files INTEGER NOT NULL DEFAULT 0,
                    scanned_files INTEGER NOT NULL DEFAULT 0,
                    last_run_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            for state_key, defaults in RUNTIME_STATE_DEFAULTS.items():
                cur.execute(
                    """
                    INSERT INTO parser_runtime_state(
                        state_key, summary, log_lines, failed_groups, is_running,
                        stop_requested, failed_files, scanned_files, last_run_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (state_key) DO NOTHING
                    """,
                    (
                        state_key,
                        defaults["summary"],
                        Json(defaults["log_lines"]),
                        Json(defaults["failed_groups"]),
                        defaults["is_running"],
                        defaults["stop_requested"],
                        defaults["failed_files"],
                        defaults["scanned_files"],
                        defaults["last_run_at"],
                    ),
                )


def _normalize_runtime_state_row(row, state_key: str) -> dict:
    defaults = RUNTIME_STATE_DEFAULTS[state_key]
    if not row:
        return {
            "summary": defaults["summary"],
            "log_lines": list(defaults["log_lines"]),
            "failed_groups": list(defaults["failed_groups"]),
            "is_running": defaults["is_running"],
            "stop_requested": defaults["stop_requested"],
            "failed_files": defaults["failed_files"],
            "scanned_files": defaults["scanned_files"],
            "last_run_at": defaults["last_run_at"],
        }
    return {
        "summary": row[0] or defaults["summary"],
        "log_lines": list(row[1] or []),
        "failed_groups": list(row[2] or []),
        "is_running": bool(row[3]),
        "stop_requested": bool(row[4]),
        "failed_files": int(row[5] or 0),
        "scanned_files": int(row[6] or 0),
        "last_run_at": row[7],
    }


def get_runtime_state(state_key: str) -> dict:
    ensure_runtime_state_table()
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT summary, log_lines, failed_groups, is_running, stop_requested,
                       failed_files, scanned_files, last_run_at
                FROM parser_runtime_state
                WHERE state_key = %s
                """,
                (state_key,),
            )
            row = cur.fetchone()
    return _normalize_runtime_state_row(row, state_key)


def update_runtime_state(state_key: str, **updates) -> dict:
    ensure_runtime_state_table()
    with RUNTIME_STATE_LOCKS[state_key]:
        with get_main_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT summary, log_lines, failed_groups, is_running, stop_requested,
                           failed_files, scanned_files, last_run_at
                    FROM parser_runtime_state
                    WHERE state_key = %s
                    FOR UPDATE
                    """,
                    (state_key,),
                )
                state = _normalize_runtime_state_row(cur.fetchone(), state_key)

                line = updates.pop("append_line", None)
                if line is not None:
                    state["log_lines"].append(str(line))
                    state["log_lines"] = state["log_lines"][-400:]

                for key, value in updates.items():
                    if key in state:
                        state[key] = value

                cur.execute(
                    """
                    UPDATE parser_runtime_state
                    SET summary = %s,
                        log_lines = %s,
                        failed_groups = %s,
                        is_running = %s,
                        stop_requested = %s,
                        failed_files = %s,
                        scanned_files = %s,
                        last_run_at = %s,
                        updated_at = now()
                    WHERE state_key = %s
                    """,
                    (
                        state["summary"],
                        Json(state["log_lines"]),
                        Json(state["failed_groups"]),
                        state["is_running"],
                        state["stop_requested"],
                        state["failed_files"],
                        state["scanned_files"],
                        state["last_run_at"],
                        state_key,
                    ),
                )
    return state


def try_start_runtime_state(state_key: str, **updates) -> bool:
    ensure_runtime_state_table()
    with RUNTIME_STATE_LOCKS[state_key]:
        with get_main_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT is_running
                    FROM parser_runtime_state
                    WHERE state_key = %s
                    FOR UPDATE
                    """,
                    (state_key,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    return False
                state = _normalize_runtime_state_row(None, state_key)
                for key, value in updates.items():
                    if key in state:
                        state[key] = value
                cur.execute(
                    """
                    UPDATE parser_runtime_state
                    SET summary = %s,
                        log_lines = %s,
                        failed_groups = %s,
                        is_running = %s,
                        stop_requested = %s,
                        failed_files = %s,
                        scanned_files = %s,
                        last_run_at = %s,
                        updated_at = now()
                    WHERE state_key = %s
                    """,
                    (
                        state["summary"],
                        Json(state["log_lines"]),
                        Json(state["failed_groups"]),
                        state["is_running"],
                        state["stop_requested"],
                        state["failed_files"],
                        state["scanned_files"],
                        state["last_run_at"],
                        state_key,
                    ),
                )
    return True


def get_parser_state() -> dict:
    return get_runtime_state("parser")


def set_parser_state(**updates) -> None:
    update_runtime_state("parser", **updates)


def get_planshetka_state() -> dict:
    return get_runtime_state("planshetka")


def set_planshetka_state(**updates) -> None:
    update_runtime_state("planshetka", **updates)


def reset_runtime_state_flags() -> None:
    ensure_runtime_state_table()
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE parser_runtime_state
                SET is_running = FALSE,
                    stop_requested = FALSE,
                    updated_at = now()
                WHERE is_running = TRUE OR stop_requested = TRUE
                """
            )


def repair_runtime_state_encoding() -> None:
    ensure_runtime_state_table()
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT state_key, summary FROM parser_runtime_state")
            for state_key, summary in cur.fetchall():
                default_summary = RUNTIME_STATE_DEFAULTS.get(state_key, {}).get("summary")
                if not default_summary:
                    continue
                cleaned_summary = (summary or "").strip()
                # Heal rows that were previously written with broken console encoding.
                if not cleaned_summary or "???" in cleaned_summary:
                    cur.execute(
                        """
                        UPDATE parser_runtime_state
                        SET summary = %s,
                            updated_at = now()
                        WHERE state_key = %s
                        """,
                        (default_summary, state_key),
                    )

def start_parser_job(group_names: list[str], mode_label: str) -> bool:
    if not try_start_runtime_state(
        "parser",
        is_running=True,
        stop_requested=False,
        failed_groups=[],
        failed_files=0,
        scanned_files=0,
        summary=f"Запущен: {mode_label}",
        log_lines=[f"[START] {mode_label}"],
        last_run_at=None,
    ):
        return False

    def worker():
        failed, done = [], 0
        try:
            for group_name in group_names:
                if get_parser_state()["stop_requested"]:
                    set_parser_state(append_line=f"[STOP] Остановка перед группой {group_name}")
                    break
                try:
                    set_parser_state(append_line=f"[GROUP] {group_name}")
                    imported = run_parser_group(group_name, clear_parser_group=True, replace_main_group=True)
                    done += 1
                    set_parser_state(append_line=f"[OK] {group_name}: imported={imported}")
                except Exception as exc:
                    failed.append(group_name)
                    set_parser_state(append_line=f"[FAIL] {group_name}: {exc}")
        finally:
            try:
                detect_and_record_schedule_changes()
            except Exception as exc:
                set_parser_state(append_line=f"[WARN] Не удалось обновить ленту изменений: {exc}")
            set_parser_state(is_running=False, stop_requested=False, failed_groups=failed, last_run_at=datetime.now(), summary=f"Завершено: успешно {done}, ошибок {len(failed)}")

    Thread(target=worker, daemon=True).start()
    return True


def start_planshetka_job(folder_url: str, recent_files_limit: int, mode_label: str) -> bool:
    if not try_start_runtime_state(
        "planshetka",
        is_running=True,
        stop_requested=False,
        failed_groups=[],
        failed_files=0,
        scanned_files=0,
        summary=f"Запущен: {mode_label}",
        log_lines=[f"[START] {mode_label}"],
        last_run_at=None,
    ):
        return False

    def worker():
        def log(message: str):
            set_planshetka_state(append_line=message)
        try:
            imported, failed_files, scanned_files = run_planshetka_sync(folder_url, replace_group=True, recent_files_limit=recent_files_limit, log=log)
            try:
                detect_and_record_schedule_changes()
            except Exception as exc:
                set_planshetka_state(append_line=f"[WARN] Не удалось обновить ленту изменений: {exc}")
            set_planshetka_state(summary=f"Завершено: импортировано {imported}, ошибок файлов {failed_files}", failed_files=failed_files, scanned_files=scanned_files, last_run_at=datetime.now(), is_running=False)
        except Exception as exc:
            set_planshetka_state(summary=f"Ошибка Planshetka: {exc}", last_run_at=datetime.now(), is_running=False, append_line=f"[FAIL] {exc}")

    Thread(target=worker, daemon=True).start()
    return True


@app.route("/")
def index():
    return redirect(url_for("me" if "user_id" in session else "login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect(url_for("me"))

    if request.method == "POST":
        action = (request.form.get("action") or "login").strip().lower()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if action == "register":
            full_name = request.form.get("full_name", "").strip()
            password_confirm = request.form.get("password_confirm", "")

            if not username or not password:
                flash("Заполните логин и пароль для регистрации.", "error")
                return render_template("login.html", title="Вход")
            if len(username) < 3:
                flash("Логин должен содержать минимум 3 символа.", "error")
                return render_template("login.html", title="Вход")
            if len(password) < 6:
                flash("Пароль должен содержать минимум 6 символов.", "error")
                return render_template("login.html", title="Вход")
            if password != password_confirm:
                flash("Пароли не совпадают.", "error")
                return render_template("login.html", title="Вход")

            with get_main_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM site_users WHERE lower(username) = lower(%s) LIMIT 1", (username,))
                    if cur.fetchone():
                        flash("Пользователь с таким логином уже существует.", "error")
                        return render_template("login.html", title="Вход")

                    cur.execute(
                        "INSERT INTO site_users(username, password_hash, full_name) VALUES (%s, %s, %s) RETURNING id",
                        (username, generate_password_hash(password), full_name or None),
                    )
                    new_user_id = cur.fetchone()[0]

                    cur.execute("""
                        SELECT u.id, u.full_name,
                               EXISTS(SELECT 1 FROM site_admins a WHERE a.user_id = u.id) AS is_admin
                        FROM site_users u
                        WHERE u.id = %s AND u.is_active = TRUE
                    """, (new_user_id,))
                    row = cur.fetchone()

            if not row:
                flash("Не удалось завершить регистрацию. Попробуйте снова.", "error")
                return render_template("login.html", title="Вход")

            session.update({"user_id": row[0], "full_name": row[1], "is_admin": row[2]})
            flash("Регистрация успешна. Добро пожаловать!", "success")
            return redirect(url_for("me"))

        with get_main_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT u.id, u.password_hash, u.full_name,
                           EXISTS(SELECT 1 FROM site_admins a WHERE a.user_id = u.id) AS is_admin
                    FROM site_users u
                    WHERE lower(u.username) = lower(%s) AND u.is_active = TRUE
                """, (username,))
                row = cur.fetchone()
        if not row or not check_password_hash(row[1], password):
            flash("Неверный логин или пароль.", "error")
            return render_template("login.html", title="Вход")
        session.update({"user_id": row[0], "full_name": row[2], "is_admin": row[3]})
        return redirect(url_for("me"))
    return render_template("login.html", title="Вход")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/cookie-policy")
def cookie_policy():
    return render_template("cookie_policy.html", title="Соглашение о cookies")


@app.route("/me/settings")
@login_required
def me_layout_settings():
    return render_template(
        "user_dashboard_settings.html",
        title="Настройка кабинета",
        dashboard_widgets=DASHBOARD_WIDGETS,
        user_id=session["user_id"],
    )


@app.route("/ai")
@login_required
def ai_assistant_page():
    user = load_user(session["user_id"])
    if not user:
        session.clear()
        return redirect(url_for("login"))

    llm_ready, llm_error = ensure_llm_ready()
    ai_messages = []
    ai_settings = {"allow_note_creation": False}
    pending_note = None
    study_subjects: list[str] = []
    if llm_ready:
        chat_session_id = get_or_create_chat_session(user["id"])
        ai_messages = fetch_chat_messages(chat_session_id, limit=80)
        ai_settings = get_ai_user_settings(user["id"])
        pending_note = get_pending_note(user["id"])
        study_subjects = fetch_group_subject_options(user["id"])
    else:
        flash("Сервис ИИ временно недоступен. Проверьте подключение к LLM_DB.", "error")

    return render_template(
        "ai_chat.html",
        title="ИИ",
        user=user,
        ai_messages=ai_messages,
        ai_settings=ai_settings,
        pending_note=pending_note,
        study_subjects=study_subjects,
        llm_ready=llm_ready,
        llm_error=llm_error,
        ollama_model=(os.getenv("OLLAMA_MODEL") or "qwen2.5:7b-instruct").strip(),
    )


@app.route("/api/ai/settings", methods=["POST"])
@login_required
def api_ai_settings():
    llm_ready, llm_error = ensure_llm_ready()
    if not llm_ready:
        return jsonify({"ok": False, "error": f"LLM storage unavailable: {llm_error}"}), 503
    payload = request.get_json(silent=True) or {}
    raw_value = payload.get("allow_note_creation")
    allow_note_creation = str(raw_value).strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(raw_value, bool):
        allow_note_creation = raw_value
    settings = update_ai_user_settings(session["user_id"], allow_note_creation)
    return jsonify({"ok": True, "settings": settings})


@app.route("/api/ai/chat", methods=["POST"])
@login_required
def api_ai_chat():
    llm_ready, llm_error = ensure_llm_ready()
    if not llm_ready:
        return jsonify({"ok": False, "error": f"LLM storage unavailable: {llm_error}"}), 503
    payload = request.get_json(silent=True) or {}
    message_text = (payload.get("message") or "").strip()
    if not message_text:
        return jsonify({"ok": False, "error": "Введите сообщение для ИИ."}), 400

    user_id = int(session["user_id"])
    chat_session_id = get_or_create_chat_session(user_id)
    save_chat_message(chat_session_id, "user", message_text)
    ai_settings = get_ai_user_settings(user_id)
    recent_messages = fetch_chat_messages(chat_session_id, limit=40)

    def create_notes_batch(note_payloads: list[dict[str, str]], batch_source: str):
        unique_payloads: list[dict[str, str]] = []
        seen_keys: set[tuple[str, str, str]] = set()
        for note in note_payloads:
            key = (
                (note.get("title") or "").strip().lower(),
                (note.get("note_text") or "").strip().lower(),
                (note.get("due_date") or "").strip(),
            )
            if not key[0] or key in seen_keys:
                continue
            seen_keys.add(key)
            unique_payloads.append(note)

        created = 0
        failed = 0
        for note in unique_payloads:
            try:
                create_user_note(user_id, note.get("title", ""), note.get("note_text", ""), note.get("due_date", ""))
                created += 1
            except Exception:
                failed += 1

        clear_pending_note(user_id)
        if created:
            reply = f"Готово. Добавил заметок в личный кабинет: {created}."
            if failed:
                reply += f" Не удалось добавить: {failed}."
            meta = {"note_batch_created": created, "note_batch_failed": failed, "note_batch_source": batch_source}
        else:
            reply = "Не удалось добавить заметки. Проверьте формат /note и доступность ЛК."
            meta = {"note_batch_created": 0, "note_batch_failed": failed, "note_batch_source": batch_source, "note_error": "batch_failed"}

        save_chat_message(chat_session_id, "assistant", reply, meta)
        return jsonify({"ok": True, "reply": reply, "meta": meta})

    quick_command = extract_quick_command(message_text)
    if quick_command:
        reply, meta = build_quick_reply(user_id, quick_command)
        save_chat_message(chat_session_id, "assistant", reply, meta)
        return jsonify({"ok": True, "reply": reply, "meta": meta})

    study_subject = extract_study_subject(message_text)
    if study_subject is not None:
        reply, meta = build_study_plan(user_id, study_subject)
        save_chat_message(chat_session_id, "assistant", reply, meta)
        return jsonify({"ok": True, "reply": reply, "meta": meta})

    direct_note_commands = extract_note_commands(message_text, limit=40)
    if direct_note_commands:
        if not ai_settings.get("allow_note_creation"):
            reply = "Создание заметок выключено. Включите разрешение на этой странице и повторите запрос."
            save_chat_message(chat_session_id, "assistant", reply, {"note_created": False, "note_denied": True})
            return jsonify({"ok": True, "reply": reply, "meta": {"note_created": False, "note_denied": True}})
        if len(direct_note_commands) == 1 and message_text.strip().lower().startswith("/note "):
            pending_note = upsert_pending_note(user_id, chat_session_id, direct_note_commands[0])
            reply = (
                f"Проверьте черновик заметки: «{pending_note['title']}». "
                "Если всё верно, нажмите «Подтвердить заметку», иначе «Отмена»."
            )
            meta = {"note_created": False, "note_pending_confirmation": True, "pending_note": pending_note}
            save_chat_message(chat_session_id, "assistant", reply, meta)
            return jsonify({"ok": True, "reply": reply, "meta": meta})
        return create_notes_batch(direct_note_commands, "direct_note_commands")

    if extract_add_notes_request(message_text):
        if not ai_settings.get("allow_note_creation"):
            reply = "Создание заметок выключено. Включите разрешение на этой странице и повторите запрос."
            save_chat_message(chat_session_id, "assistant", reply, {"note_created": False, "note_denied": True})
            return jsonify({"ok": True, "reply": reply, "meta": {"note_created": False, "note_denied": True}})

        recent_note_commands = extract_recent_assistant_note_commands(recent_messages, limit=40)
        if not recent_note_commands:
            reply = "Не нашёл в последних ответах команд вида /note ... Сначала попросите ИИ сформировать заметки или отправьте команды /note вручную."
            meta = {"note_batch_created": 0, "note_error": "no_recent_note_commands"}
            save_chat_message(chat_session_id, "assistant", reply, meta)
            return jsonify({"ok": True, "reply": reply, "meta": meta})
        return create_notes_batch(recent_note_commands, "recent_assistant_note_commands")

    note_payload = extract_note_payload(message_text)
    if note_payload is not None:
        if not ai_settings.get("allow_note_creation"):
            reply = "Создание заметок выключено. Включите разрешение на этой странице и повторите запрос."
            save_chat_message(chat_session_id, "assistant", reply, {"note_created": False, "note_denied": True})
            return jsonify({"ok": True, "reply": reply, "meta": {"note_created": False, "note_denied": True}})
        if not note_payload.get("title"):
            reply = "Не удалось распознать заголовок. Используйте формат: /note Заголовок | Текст | YYYY-MM-DD"
            save_chat_message(chat_session_id, "assistant", reply, {"note_created": False, "note_error": "title_missing"})
            return jsonify({"ok": True, "reply": reply, "meta": {"note_created": False, "note_error": "title_missing"}})
        pending_note = upsert_pending_note(user_id, chat_session_id, note_payload)
        reply = (
            f"Проверьте черновик заметки: «{pending_note['title']}». "
            "Если всё верно, нажмите «Подтвердить заметку», иначе «Отмена»."
        )
        meta = {"note_created": False, "note_pending_confirmation": True, "pending_note": pending_note}
        save_chat_message(chat_session_id, "assistant", reply, meta)
        return jsonify({"ok": True, "reply": reply, "meta": meta})

    search_query = extract_search_query(message_text)
    schedule_context = build_default_schedule_context(user_id)
    search_context = None
    if search_query:
        today = date.today()
        search_context = fetch_schedule_context(user_id, today - timedelta(days=7), today + timedelta(days=45), limit=120, keyword=search_query)

    ollama_messages = build_ollama_messages(
        message_text,
        recent_messages,
        schedule_context=schedule_context,
        search_context=search_context,
        can_create_note=bool(ai_settings.get("allow_note_creation")),
    )

    try:
        reply = call_ollama_chat(ollama_messages)
        meta = {"search_query": search_query or "", "model": (os.getenv("OLLAMA_MODEL") or "").strip()}
    except Exception as exc:
        reply = f"Не удалось получить ответ от модели. Проверьте OLLAMA_URL/OLLAMA_MODEL и доступность сервера. Детали: {exc}"
        meta = {"search_query": search_query or "", "model_error": str(exc)}

    save_chat_message(chat_session_id, "assistant", reply, meta)
    return jsonify({"ok": True, "reply": reply, "meta": meta})


@app.route("/api/ai/clear", methods=["POST"])
@login_required
def api_ai_clear():
    llm_ready, llm_error = ensure_llm_ready()
    if not llm_ready:
        return jsonify({"ok": False, "error": f"LLM storage unavailable: {llm_error}"}), 503

    user_id = int(session["user_id"])
    chat_session_id = get_or_create_chat_session(user_id)
    deleted_messages = clear_chat_messages(chat_session_id)
    clear_pending_note(user_id)
    return jsonify({"ok": True, "meta": {"chat_cleared": True, "deleted_messages": deleted_messages}})


@app.route("/api/ai/note-action", methods=["POST"])
@login_required
def api_ai_note_action():
    llm_ready, llm_error = ensure_llm_ready()
    if not llm_ready:
        return jsonify({"ok": False, "error": f"LLM storage unavailable: {llm_error}"}), 503

    payload = request.get_json(silent=True) or {}
    action = (payload.get("action") or "").strip().lower()
    if action not in {"confirm", "cancel"}:
        return jsonify({"ok": False, "error": "Некорректное действие. Используйте confirm или cancel."}), 400

    user_id = int(session["user_id"])
    chat_session_id = get_or_create_chat_session(user_id)
    pending_note = get_pending_note(user_id)
    if not pending_note:
        return jsonify({"ok": False, "error": "Нет заметки, ожидающей подтверждения."}), 400

    if action == "cancel":
        clear_pending_note(user_id)
        reply = "Создание заметки отменено."
        meta = {"note_confirmed": False, "note_cancelled": True}
        save_chat_message(chat_session_id, "assistant", reply, meta)
        return jsonify({"ok": True, "reply": reply, "meta": meta})

    ai_settings = get_ai_user_settings(user_id)
    if not ai_settings.get("allow_note_creation"):
        reply = "Создание заметок выключено. Включите разрешение и отправьте заметку заново."
        meta = {"note_confirmed": False, "note_denied": True}
        save_chat_message(chat_session_id, "assistant", reply, meta)
        return jsonify({"ok": True, "reply": reply, "meta": meta})

    try:
        create_user_note(user_id, pending_note.get("title", ""), pending_note.get("note_text", ""), pending_note.get("due_date", ""))
        clear_pending_note(user_id)
        due_date = pending_note.get("due_date", "")
        reply = f"Готово. Заметка «{pending_note.get('title', '')}» добавлена в личный кабинет."
        if due_date:
            reply += f" Срок: {due_date}."
        meta = {"note_confirmed": True, "note_created": True}
    except Exception as exc:
        reply = f"Не удалось создать заметку: {exc}"
        meta = {"note_confirmed": False, "note_error": "create_failed"}

    save_chat_message(chat_session_id, "assistant", reply, meta)
    return jsonify({"ok": True, "reply": reply, "meta": meta})


@app.route("/me", methods=["GET", "POST"])
@login_required
def me():
    ensure_schedule_room_columns()
    ensure_personalization_tables()
    user = load_user(session["user_id"])
    if not user:
        session.clear()
        return redirect(url_for("login"))

    def redirect_me_after_post():
        return redirect(
            url_for(
                "me",
                date=request.form.get("date"),
                period=request.form.get("period"),
                q=request.form.get("q"),
                source=request.form.get("source") or "rksi",
                teacher_id=request.form.get("teacher_id"),
                all_groups=1 if request.form.get("all_groups") == "1" else 0,
            )
        )

    if request.method == "POST":
        action = request.form.get("profile_action") or "apply"
        if action in {"apply", "reset_search"}:
            preferred_group_id = (request.form.get("preferred_group_id") or "").strip() or None
            if action == "reset_search":
                preferred_group_id = None
            with get_main_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE site_users SET preferred_group_id = %s, updated_at = now() WHERE id = %s", (preferred_group_id, user["id"]))
            if action == "reset_search":
                return redirect(url_for("me", date=request.form.get("date"), period=request.form.get("period"), source=request.form.get("source") or "rksi"))
            return redirect_me_after_post()
        if action == "toggle_favorite_teacher":
            teacher_id = (request.form.get("favorite_teacher_id") or "").strip()
            if teacher_id.isdigit():
                is_favorite = toggle_favorite_teacher(user["id"], int(teacher_id))
                flash("Преподаватель добавлен в избранное." if is_favorite else "Преподаватель удален из избранного.", "success")
            else:
                flash("Некорректный преподаватель для избранного.", "error")
            return redirect_me_after_post()
        if action == "create_note":
            title = (request.form.get("note_title") or "").strip()
            if not title:
                flash("Укажите заголовок заметки.", "error")
            else:
                try:
                    create_user_note(user["id"], title, request.form.get("note_text", ""), request.form.get("note_due_date"))
                    flash("Заметка добавлена.", "success")
                except Exception as exc:
                    flash(f"Не удалось сохранить заметку: {exc}", "error")
            return redirect_me_after_post()
        if action == "toggle_note_done":
            note_id = (request.form.get("note_id") or "").strip()
            if note_id.isdigit() and toggle_user_note(user["id"], int(note_id)):
                flash("Статус заметки обновлен.", "success")
            else:
                flash("Заметка не найдена.", "error")
            return redirect_me_after_post()
        if action == "delete_note":
            note_id = (request.form.get("note_id") or "").strip()
            if note_id.isdigit() and delete_user_note(user["id"], int(note_id)):
                flash("Заметка удалена.", "success")
            else:
                flash("Заметка не найдена.", "error")
            return redirect_me_after_post()
        if action == "generate_telegram_code":
            code = generate_telegram_link_code(user["id"])
            flash(f"Код для Telegram создан: {code}. Он действует 30 минут.", "success")
            return redirect_me_after_post()
        if action == "unlink_telegram":
            unlink_telegram_account(user["id"])
            flash("Telegram-аккаунт отвязан.", "success")
            return redirect_me_after_post()
        if action == "save_telegram_notifications":
            changes_enabled = request.form.get("telegram_notifications_enabled") == "1"
            lesson_enabled = request.form.get("telegram_lesson_notifications_enabled") == "1"
            with get_main_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE site_users
                        SET telegram_notifications_enabled = %s,
                            telegram_lesson_notifications_enabled = %s,
                            updated_at = now()
                        WHERE id = %s
                        """,
                        (changes_enabled, lesson_enabled, user["id"]),
                    )
            flash("Настройки Telegram-уведомлений сохранены.", "success")
            return redirect_me_after_post()
        flash("Неизвестное действие личного кабинета.", "error")
        return redirect_me_after_post()

    selected_date = request.args.get("date")
    try:
        day = datetime.strptime(selected_date, "%Y-%m-%d").date() if selected_date else date.today()
    except ValueError:
        day = date.today()
    period = request.args.get("period", "week_current")
    if period not in PERIOD_OPTIONS:
        period = "week_current"
    keyword = (request.args.get("q") or "").strip()
    teacher_filter = (request.args.get("teacher_id") or "").strip()
    teacher_filter_id = int(teacher_filter) if teacher_filter.isdigit() else None
    all_groups = request.args.get("all_groups") in {"1", "true", "on"}
    source_mode = (request.args.get("source") or "rksi").strip().lower()
    if source_mode not in {"rksi", "planshetka"}:
        source_mode = "rksi"
    start_day, end_day = resolve_period_range(day, period)

    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, group_name FROM study_groups ORDER BY group_name")
            groups = cur.fetchall()
            cur.execute("""
                SELECT MIN(id) AS id, full_name
                FROM teachers
                WHERE full_name IS NOT NULL
                  AND btrim(full_name) <> ''
                  AND regexp_replace(full_name, '[ _.-]', '', 'g') <> ''
                GROUP BY full_name
                ORDER BY full_name
            """)
            teachers = [row for row in cur.fetchall() if not is_composite_teacher_name(row[1])]
            teacher_filter_name = None
            selected_teacher_id = teacher_filter
            if teacher_filter_id is not None:
                cur.execute("SELECT full_name FROM teachers WHERE id = %s", (teacher_filter_id,))
                row = cur.fetchone()
                teacher_filter_name = row[0] if row else None
                if teacher_filter_name:
                    cur.execute(
                        """
                        SELECT MIN(id)
                        FROM teachers
                        WHERE full_name = %s
                        """,
                        (teacher_filter_name,),
                    )
                    canonical_row = cur.fetchone()
                    if canonical_row and canonical_row[0]:
                        selected_teacher_id = str(canonical_row[0])
            group_name = None
            if user["preferred_group_id"]:
                cur.execute("SELECT group_name FROM study_groups WHERE id = %s", (user["preferred_group_id"],))
                row = cur.fetchone()
                group_name = row[0] if row else None
            use_group_filter = bool(user["preferred_group_id"]) and not all_groups
            show_schedule = use_group_filter or bool(teacher_filter_name) or bool(keyword)
            schedule_rows = []
            if show_schedule:
                if source_mode == "rksi":
                    sql = """
                        SELECT p.id, p.lesson_date, p.start_time, p.end_time,
                               subj.subject_name, COALESCE(t.full_name, p.raw_teacher_name, '') AS teacher_name, COALESCE(p.room, t.room) AS room,
                               'rksi' AS source, p.teacher_id, g.group_name, p.group_id
                        FROM parsed_schedule_entries p
                        JOIN subjects subj ON subj.id = p.subject_id
                        LEFT JOIN teachers t ON t.id = p.teacher_id
                        JOIN study_groups g ON g.id = p.group_id
                        WHERE p.lesson_date BETWEEN %s AND %s
                    """
                else:
                    sql = """
                        SELECT x.id, x.lesson_date, x.start_time, x.end_time, x.subject_name, x.teacher_name, x.room, x.source, x.teacher_id, x.group_name, x.group_id
                        FROM (
                          SELECT s.id, s.lesson_date, s.start_time, s.end_time, subj.subject_name,
                                 COALESCE(t.full_name, '') AS teacher_name, COALESCE(s.room, t.room) AS room, 'manual' AS source,
                                 s.teacher_id, g.group_name, s.group_id
                          FROM schedule_entries s JOIN subjects subj ON subj.id = s.subject_id LEFT JOIN teachers t ON t.id = s.teacher_id JOIN study_groups g ON g.id = s.group_id
                          UNION ALL
                          SELECT p.id, p.lesson_date, p.start_time, p.end_time, subj.subject_name,
                                 COALESCE(t.full_name, p.raw_teacher_name, '') AS teacher_name, COALESCE(p.room, t.room) AS room, 'planshetka' AS source,
                                 p.teacher_id, g.group_name, p.group_id
                          FROM parsed_tabletka_schedule_entries p JOIN subjects subj ON subj.id = p.subject_id LEFT JOIN teachers t ON t.id = p.teacher_id JOIN study_groups g ON g.id = p.group_id
                        ) x WHERE x.lesson_date BETWEEN %s AND %s
                    """
                params = [start_day, end_day]
                if use_group_filter:
                    sql += " AND group_id = %s"
                    params.append(user["preferred_group_id"])
                if teacher_filter_name:
                    if source_mode == "rksi":
                        sql += " AND COALESCE(t.full_name, p.raw_teacher_name, '') = %s"
                    else:
                        sql += " AND COALESCE(x.teacher_name, '') = %s"
                    params.append(teacher_filter_name)
                if keyword:
                    if source_mode == "rksi":
                        sql += " AND (subj.subject_name ILIKE %s OR COALESCE(p.room, t.room, '') ILIKE %s)"
                    else:
                        sql += " AND (x.subject_name ILIKE %s OR COALESCE(x.room, '') ILIKE %s)"
                    like = f"%{keyword}%"
                    params.extend([like, like])
                sql += " ORDER BY lesson_date, start_time NULLS LAST, end_time NULLS LAST, subject_name"
                cur.execute(sql, tuple(params))
                schedule_rows = cur.fetchall()

    days = build_days(start_day, end_day)
    grouped = {d: [] for d in days}
    for row in schedule_rows:
        grouped.setdefault(row[1], []).append(row)
    schedule_days = [{"date": d, "header": format_day_header(d), "rows": grouped.get(d, [])} for d in days]
    favorite_teachers = fetch_favorite_teachers(user["id"])
    favorite_teacher_ids = {item["id"] for item in favorite_teachers}
    notes = fetch_user_notes(user["id"])
    announcements = fetch_announcements_for_user(user["id"])
    change_events = fetch_recent_change_events_for_user(user["id"])
    today_summary = build_today_summary(user["preferred_group_id"])
    source_conflicts = fetch_source_conflicts(user["preferred_group_id"], start_day, end_day)
    telegram_settings = load_telegram_settings()
    telegram_bot_url = f"https://t.me/{telegram_settings['bot_username']}" if telegram_settings.get("bot_username") else ""
    telegram_link_code_active = bool(user.get("telegram_link_code") and user.get("telegram_link_code_created_at") and user["telegram_link_code_created_at"] >= datetime.now(user["telegram_link_code_created_at"].tzinfo) - timedelta(minutes=30))
    return render_template(
        "user_dashboard.html",
        title="Личный кабинет",
        user=user,
        dashboard_widgets=DASHBOARD_WIDGETS,
        groups=groups,
        teachers=teachers,
        schedule_days=schedule_days,
        selected_date=day.isoformat(),
        selected_period=period,
        period_options=PERIOD_OPTIONS,
        search_query=keyword,
        group_name=group_name,
        selected_teacher_id=selected_teacher_id,
        all_groups=all_groups,
        show_schedule=show_schedule,
        source_mode=source_mode,
        planshetka_folder_url=get_planshetka_folder_url(),
        rksi_schedule_url=RKSI_SCHEDULE_URL,
        rksi_mobile_schedule_url=RKSI_MOBILE_SCHEDULE_URL,
        favorite_teachers=favorite_teachers,
        favorite_teacher_ids=favorite_teacher_ids,
        notes=notes,
        announcements=announcements,
        change_events=change_events,
        today_summary=today_summary,
        source_conflicts=source_conflicts,
        telegram_settings=telegram_settings,
        telegram_bot_url=telegram_bot_url,
        telegram_link_code_active=telegram_link_code_active,
    )

@app.route("/admin", methods=["GET", "POST"])
@admin_required
def admin_dashboard():
    ensure_schedule_room_columns()
    ensure_planshetka_tables()
    ensure_personalization_tables()
    action = request.form.get("action")
    admin_section = (request.values.get("section") or "overview").strip().lower()
    if admin_section not in {"overview", "dictionaries"}:
        admin_section = "overview"
    parser_actions = {"run_parser_all", "retry_failed_parser", "stop_parser", "save_auto_schedule", "clear_parser_storage", "deduplicate_storage", "save_planshetka_source", "run_planshetka_parser_all", "clear_planshetka_storage"}
    if request.method == "POST" and action in parser_actions:
        if action == "run_parser_all":
            try:
                names = sorted(fetch_group_map().keys())
                if not names:
                    raise RuntimeError("Список групп пустой.")
                if start_parser_job(names, "Полная загрузка всех групп"):
                    flash("Запуск парсера начат. Лог обновляется в реальном времени.", "success")
                else:
                    flash("Парсер уже выполняется. Дождитесь завершения текущего запуска.", "error")
            except Exception as exc:
                flash(f"Ошибка запуска парсера: {exc}", "error")
        elif action == "retry_failed_parser":
            failed = get_parser_state()["failed_groups"]
            if not failed:
                flash("Нет групп с ошибками для повторной загрузки.", "success")
            elif start_parser_job(failed, f"Повтор только неуспешных групп ({len(failed)})"):
                flash("Повторная загрузка ошибок запущена. Лог обновляется в реальном времени.", "success")
            else:
                flash("Парсер уже выполняется. Дождитесь завершения текущего запуска.", "error")
        elif action == "stop_parser":
            if not get_parser_state()["is_running"]:
                flash("Парсер сейчас не запущен.", "success")
            else:
                set_parser_state(stop_requested=True, append_line="[STOP] Пользователь запросил остановку.")
                flash("Остановка запрошена. Парсер завершится после текущей группы.", "success")
        elif action == "save_auto_schedule":
            try:
                update_auto_schedule(request.form.get("auto_enabled") == "1", int(request.form.get("run_weekday", "0")), request.form.get("run_time", "03:00"))
                flash("Настройки автозапуска сохранены.", "success")
            except Exception:
                flash("Не удалось сохранить автозапуск. Проверьте день недели и время.", "error")
        elif action == "clear_parser_storage":
            if get_parser_state()["is_running"]:
                flash("Нельзя очистить данные во время работы парсера.", "error")
            else:
                stats = clear_parser_storage()
                set_parser_state(summary="Данные парсера очищены вручную.", failed_groups=[], append_line=f"[MANUAL CLEAN] parsed_schedule_entries={stats['parsed_schedule_entries']}, parser_lessons={stats['parser_lessons']}, parser_days={stats['parser_days']}")
                flash("Данные парсера удалены из БД.", "success")
        elif action == "deduplicate_storage":
            stats = deduplicate_storage()
            set_parser_state(append_line=f"[DEDUP] parser_days_deleted={stats['deleted_parser_days_duplicates']}, teachers_deleted={stats['deleted_teachers_duplicates']}, subjects_deleted={stats['deleted_subjects_duplicates']}")
            flash("Удаление дублей завершено.", "success")
        elif action == "save_planshetka_source":
            try:
                update_planshetka_folder_url((request.form.get("planshetka_folder_url") or "").strip())
                update_planshetka_recent_files_limit(max(1, int((request.form.get("planshetka_recent_files_limit") or "2").strip())))
                flash("Настройки Planshetka сохранены.", "success")
            except Exception:
                flash("Не удалось сохранить настройки Planshetka.", "error")
        elif action == "run_planshetka_parser_all":
            folder_url = get_planshetka_folder_url()
            limit = get_planshetka_recent_files_limit()
            if start_planshetka_job(folder_url, limit, f"Полный импорт Planshetka ({limit} files)"):
                flash("Парсер Planshetka запущен. Лог обновляется в реальном времени.", "success")
            else:
                flash("Парсер Planshetka уже выполняется. Дождитесь завершения текущего запуска.", "error")
        elif action == "clear_planshetka_storage":
            if get_planshetka_state()["is_running"]:
                flash("Нельзя очищать данные Planshetka во время работы парсера.", "error")
            else:
                deleted = clear_planshetka_data()
                set_planshetka_state(summary=f"Данные Planshetka очищены: {deleted} строк.", append_line=f"[MANUAL CLEAN] deleted_rows={deleted}", failed_files=0, scanned_files=0)
                flash(f"Данные Planshetka очищены: {deleted} строк.", "success")
        return redirect(url_for("admin_dashboard", section=admin_section))

    users_role = (request.values.get("users_role") or "all").strip().lower()
    if users_role not in {"all", "admin", "user"}:
        users_role = "all"

    users_group_id_filter = (request.values.get("users_group_id") or "all").strip().lower()
    if users_group_id_filter != "all" and users_group_id_filter != "none" and not users_group_id_filter.isdigit():
        users_group_id_filter = "all"

    users_group_by = (request.values.get("users_group_by") or "none").strip().lower()
    if users_group_by not in {"none", "role", "group"}:
        users_group_by = "none"

    with get_main_conn() as conn:
        with conn.cursor() as cur:
            if request.method == "POST":
                if action == "create_group":
                    name = request.form.get("group_name", "").strip()
                    if name:
                        cur.execute("INSERT INTO study_groups(group_name) VALUES (%s) ON CONFLICT (group_name) DO NOTHING", (name,))
                        flash("Группа добавлена.", "success")
                elif action == "update_group":
                    gid, name = (request.form.get("group_id") or "").strip(), request.form.get("group_name", "").strip()
                    if gid.isdigit() and name:
                        cur.execute("UPDATE study_groups SET group_name = %s WHERE id = %s", (name, gid))
                        flash("Группа обновлена." if cur.rowcount else "Группа с таким ID не найдена.", "success" if cur.rowcount else "error")
                    else:
                        flash("Укажите корректные ID группы и название.", "error")
                elif action == "delete_group":
                    gid = (request.form.get("group_id") or "").strip()
                    if gid.isdigit():
                        try:
                            cur.execute("DELETE FROM study_groups WHERE id = %s", (gid,))
                            flash("Группа удалена." if cur.rowcount else "Группа с таким ID не найдена.", "success" if cur.rowcount else "error")
                        except Exception:
                            flash("Нельзя удалить группу: есть связанные записи расписания/пользователей.", "error")
                elif action == "delete_groups_bulk":
                    selected_ids = parse_int_ids(request.form.getlist("group_ids"))
                    if not selected_ids:
                        flash("Выберите хотя бы одну группу для удаления.", "error")
                    else:
                        deleted, missing, failed = delete_rows_with_savepoints(cur, "study_groups", selected_ids)
                        if deleted:
                            flash(f"Удалено групп: {deleted}.", "success")
                        if missing:
                            flash(f"Группы не найдены: {', '.join(str(v) for v in missing)}.", "error")
                        if failed:
                            flash(f"Не удалось удалить группы (есть связанные записи): {', '.join(str(v) for v in failed)}.", "error")
                elif action == "create_subject":
                    name = request.form.get("subject_name", "").strip()
                    if name:
                        cur.execute("SELECT id FROM subjects WHERE lower(regexp_replace(trim(subject_name), '\\s+', ' ', 'g')) = lower(regexp_replace(trim(%s), '\\s+', ' ', 'g')) LIMIT 1", (name,))
                        if cur.fetchone():
                            flash("Предмет уже существует.", "success")
                        else:
                            cur.execute("INSERT INTO subjects(subject_name) VALUES (%s)", (name,))
                            flash("Предмет добавлен.", "success")
                elif action == "update_subject":
                    sid, name = (request.form.get("subject_id") or "").strip(), request.form.get("subject_name", "").strip()
                    if sid.isdigit() and name:
                        cur.execute("UPDATE subjects SET subject_name = %s WHERE id = %s", (name, sid))
                        flash("Предмет обновлен." if cur.rowcount else "Предмет с таким ID не найден.", "success" if cur.rowcount else "error")
                    else:
                        flash("Укажите корректные ID предмета и название.", "error")
                elif action == "delete_subject":
                    sid = (request.form.get("subject_id") or "").strip()
                    if sid.isdigit():
                        try:
                            cur.execute("DELETE FROM subjects WHERE id = %s", (sid,))
                            flash("Предмет удален." if cur.rowcount else "Предмет с таким ID не найден.", "success" if cur.rowcount else "error")
                        except Exception:
                            flash("Нельзя удалить предмет: он используется в расписании.", "error")
                elif action == "delete_subjects_bulk":
                    selected_ids = parse_int_ids(request.form.getlist("subject_ids"))
                    if not selected_ids:
                        flash("Выберите хотя бы один предмет для удаления.", "error")
                    else:
                        deleted, missing, failed = delete_rows_with_savepoints(cur, "subjects", selected_ids)
                        if deleted:
                            flash(f"Удалено предметов: {deleted}.", "success")
                        if missing:
                            flash(f"Предметы не найдены: {', '.join(str(v) for v in missing)}.", "error")
                        if failed:
                            flash(f"Не удалось удалить предметы (есть связанные записи): {', '.join(str(v) for v in failed)}.", "error")
                elif action == "create_teacher":
                    full_name = normalize_teacher_name(request.form.get("teacher_name", ""))
                    room = request.form.get("teacher_room", "").strip() or None
                    if full_name:
                        cur.execute("SELECT id, room FROM teachers WHERE lower(regexp_replace(trim(full_name), '\\s+', ' ', 'g')) = lower(regexp_replace(trim(%s), '\\s+', ' ', 'g')) ORDER BY id LIMIT 1", (full_name,))
                        row = cur.fetchone()
                        if row:
                            if room and not (row[1] or '').strip():
                                cur.execute("UPDATE teachers SET room = %s WHERE id = %s", (room, row[0]))
                            flash("Преподаватель уже существует.", "success")
                        else:
                            cur.execute("INSERT INTO teachers(full_name, room) VALUES (%s, %s)", (full_name, room))
                            flash("Преподаватель добавлен.", "success")
                    else:
                        flash("Укажите корректное имя преподавателя.", "error")
                elif action == "update_teacher":
                    tid = (request.form.get("teacher_id") or "").strip()
                    full_name = normalize_teacher_name(request.form.get("teacher_name", ""))
                    room = request.form.get("teacher_room", "").strip() or None
                    if tid.isdigit() and full_name:
                        cur.execute("UPDATE teachers SET full_name = %s, room = %s WHERE id = %s", (full_name, room, tid))
                        flash("Преподаватель обновлен." if cur.rowcount else "Преподаватель с таким ID не найден.", "success" if cur.rowcount else "error")
                    else:
                        flash("Укажите корректные ID и имя преподавателя.", "error")
                elif action == "delete_teacher":
                    tid = (request.form.get("teacher_id") or "").strip()
                    if tid.isdigit():
                        cur.execute("DELETE FROM teachers WHERE id = %s", (tid,))
                        flash("Преподаватель удален." if cur.rowcount else "Преподаватель с таким ID не найден.", "success" if cur.rowcount else "error")
                    else:
                        flash("Укажите корректный ID преподавателя.", "error")
                elif action == "delete_teachers_bulk":
                    selected_ids = parse_int_ids(request.form.getlist("teacher_ids"))
                    if not selected_ids:
                        flash("Выберите хотя бы одного преподавателя для удаления.", "error")
                    else:
                        deleted, missing, failed = delete_rows_with_savepoints(cur, "teachers", selected_ids)
                        if deleted:
                            flash(f"Удалено преподавателей: {deleted}.", "success")
                        if missing:
                            flash(f"Преподаватели не найдены: {', '.join(str(v) for v in missing)}.", "error")
                        if failed:
                            flash(f"Не удалось удалить преподавателей: {', '.join(str(v) for v in failed)}.", "error")
                elif action == "create_user":

                    username, full_name, password = request.form.get("username", "").strip(), request.form.get("full_name", "").strip(), request.form.get("password", "")
                    preferred_group_id, is_admin = request.form.get("preferred_group_id") or None, request.form.get("is_admin") == "on"
                    if username and password:
                        cur.execute("SELECT 1 FROM site_users WHERE lower(username) = lower(%s) LIMIT 1", (username,))
                        if cur.fetchone():
                            flash("Пользователь с таким логином уже существует.", "error")
                        else:
                            cur.execute("INSERT INTO site_users(username, password_hash, full_name, preferred_group_id) VALUES (%s, %s, %s, %s) RETURNING id", (username, generate_password_hash(password), full_name or None, preferred_group_id))
                            new_user_id = cur.fetchone()[0]
                            if is_admin:
                                cur.execute("INSERT INTO site_admins(user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (new_user_id,))
                            flash("Пользователь создан.", "success")
                    else:
                        flash("Укажите логин и пароль.", "error")

                elif action == "toggle_admin":
                    uid, make_admin = request.form.get("target_user_id"), request.form.get("make_admin") == "1"
                    if uid:
                        if make_admin:
                            cur.execute("INSERT INTO site_admins(user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (uid,))
                        else:
                            cur.execute("DELETE FROM site_admins WHERE user_id = %s", (uid,))
                        flash("Права админа выданы." if make_admin else "Права админа сняты.", "success")

                elif action == "delete_user":
                    uid = (request.form.get("target_user_id") or "").strip()
                    if uid.isdigit():
                        if int(uid) == int(session["user_id"]):
                            flash("Нельзя удалить текущего авторизованного пользователя.", "error")
                        else:
                            cur.execute("DELETE FROM site_users WHERE id = %s", (uid,))
                            flash("Пользователь удален." if cur.rowcount else "Пользователь не найден.", "success" if cur.rowcount else "error")
                    else:
                        flash("Укажите корректный ID пользователя.", "error")
                elif action == "delete_users_bulk":
                    selected_ids = parse_int_ids(request.form.getlist("user_ids"))
                    if not selected_ids:
                        flash("Выберите хотя бы одного пользователя для удаления.", "error")
                    else:
                        current_user_id = int(session["user_id"])
                        ids_to_delete = [uid for uid in selected_ids if uid != current_user_id]
                        skipped_current_user = len(selected_ids) - len(ids_to_delete)
                        if not ids_to_delete:
                            flash("Нельзя удалить текущего авторизованного пользователя.", "error")
                        else:
                            deleted, missing, failed = delete_rows_with_savepoints(cur, "site_users", ids_to_delete)
                            if deleted:
                                flash(f"Удалено пользователей: {deleted}.", "success")
                            if missing:
                                flash(f"Пользователи не найдены: {', '.join(str(v) for v in missing)}.", "error")
                            if failed:
                                flash(f"Не удалось удалить пользователей: {', '.join(str(v) for v in failed)}.", "error")
                        if skipped_current_user:
                            flash("Текущий авторизованный пользователь не был удален.", "error")
                elif action == "create_schedule":


                    vals = [(request.form.get(k) or "").strip() for k in ("lesson_date", "start_time", "end_time", "subject_id", "teacher_id", "group_id")]
                    if all(vals):
                        try:
                            cur.execute("INSERT INTO schedule_entries(lesson_date, start_time, end_time, subject_id, teacher_id, group_id, created_by_user_id) VALUES (%s, %s, %s, %s, %s, %s, %s)", tuple(vals) + (session["user_id"],))
                            conn.commit()
                            flash("Занятие добавлено в ручное расписание.", "success")
                            try:
                                detect_and_record_schedule_changes([int(vals[5])])
                            except Exception as exc:
                                flash(f"Занятие сохранено, но не удалось обновить ленту изменений: {exc}", "error")
                        except Exception:
                            flash("Не удалось добавить занятие (проверьте корректность и существующие связи).", "error")
                    else:
                        flash("Заполните все поля для добавления занятия.", "error")
                elif action in {"delete_schedule", "delete_schedule_by_id"}:
                    entry_id = (request.form.get("entry_id") or "").strip()
                    if entry_id.isdigit():
                        cur.execute("SELECT group_id FROM schedule_entries WHERE id = %s", (entry_id,))
                        group_row = cur.fetchone()
                        cur.execute("DELETE FROM schedule_entries WHERE id = %s", (entry_id,))
                        if cur.rowcount and group_row:
                            conn.commit()
                            try:
                                detect_and_record_schedule_changes([int(group_row[0])])
                            except Exception as exc:
                                flash(f"Запись удалена, но не удалось обновить ленту изменений: {exc}", "error")
                        flash("Запись занятия удалена." if cur.rowcount else "Запись с таким ID не найдена.", "success" if cur.rowcount else "error")
                    else:
                        flash("Укажите корректный числовой ID занятия.", "error")

                elif action == "save_telegram_settings":
                    try:
                        save_telegram_settings(
                            request.form.get("telegram_bot_token", ""),
                            request.form.get("telegram_bot_username", ""),
                            request.form.get("site_base_url", ""),
                            request.form.get("telegram_polling_enabled") == "1",
                            request.form.get("telegram_notifications_enabled") == "1",
                        )
                        flash("Настройки Telegram-бота сохранены.", "success")
                    except Exception as exc:
                        flash(f"Не удалось сохранить настройки Telegram: {exc}", "error")
                elif action == "create_announcement":
                    title = (request.form.get("announcement_title") or "").strip()
                    body = (request.form.get("announcement_body") or "").strip()
                    group_id_raw = (request.form.get("announcement_group_id") or "").strip()
                    if not title or not body:
                        flash("Заполните заголовок и текст объявления.", "error")
                    else:
                        create_announcement(int(group_id_raw) if group_id_raw.isdigit() else None, title, body, int(session["user_id"]))
                        flash("Объявление опубликовано.", "success")
                elif action == "toggle_announcement":
                    announcement_id = (request.form.get("announcement_id") or "").strip()
                    if announcement_id.isdigit() and toggle_announcement(int(announcement_id)):
                        flash("Статус объявления изменен.", "success")
                    else:
                        flash("Объявление не найдено.", "error")
                elif action == "delete_announcement":
                    announcement_id = (request.form.get("announcement_id") or "").strip()
                    if announcement_id.isdigit() and delete_announcement(int(announcement_id)):
                        flash("Объявление удалено.", "success")
                    else:
                        flash("Объявление не найдено.", "error")
                return redirect(url_for(
                    "admin_dashboard",
                    section=admin_section,
                    users_role=(request.form.get("users_role") or None),
                    users_group_id=(request.form.get("users_group_id") or None),
                    users_group_by=(request.form.get("users_group_by") or None),
                ))

            cur.execute("SELECT id, group_name FROM study_groups ORDER BY group_name")
            groups = cur.fetchall()
            cur.execute("SELECT id, subject_name FROM subjects ORDER BY subject_name")
            subjects = cur.fetchall()
            cur.execute("SELECT id, full_name, COALESCE(room, '') FROM teachers ORDER BY full_name")
            teachers = [row for row in cur.fetchall() if not is_composite_teacher_name(row[1])]
            cur.execute("""
                SELECT x.id, x.lesson_date, x.start_time, x.end_time, x.subject_name, x.teacher_name, x.group_name, x.source
                FROM (
                  SELECT s.id, s.lesson_date, s.start_time, s.end_time, subj.subject_name, COALESCE(t.full_name, '') AS teacher_name, g.group_name, 'manual' AS source
                  FROM schedule_entries s JOIN subjects subj ON subj.id = s.subject_id LEFT JOIN teachers t ON t.id = s.teacher_id JOIN study_groups g ON g.id = s.group_id
                  UNION ALL
                  SELECT p.id, p.lesson_date, p.start_time, p.end_time, subj.subject_name, COALESCE(t.full_name, p.raw_teacher_name, '') AS teacher_name, g.group_name, 'parsed' AS source
                  FROM parsed_schedule_entries p JOIN subjects subj ON subj.id = p.subject_id LEFT JOIN teachers t ON t.id = p.teacher_id JOIN study_groups g ON g.id = p.group_id
                  UNION ALL
                  SELECT pt.id, pt.lesson_date, pt.start_time, pt.end_time, subj.subject_name, COALESCE(t.full_name, pt.raw_teacher_name, '') AS teacher_name, g.group_name, 'planshetka' AS source
                  FROM parsed_tabletka_schedule_entries pt JOIN subjects subj ON subj.id = pt.subject_id LEFT JOIN teachers t ON t.id = pt.teacher_id JOIN study_groups g ON g.id = pt.group_id
                ) x ORDER BY x.lesson_date DESC, x.start_time NULLS LAST LIMIT 200
            """)
            schedule_rows = cur.fetchall()

            users_sql = """
                SELECT
                  u.id,
                  u.username,
                  COALESCE(u.full_name, ''),
                  COALESCE(g.group_name, ''),
                  EXISTS(SELECT 1 FROM site_admins a WHERE a.user_id = u.id) AS is_admin
                FROM site_users u
                LEFT JOIN study_groups g ON g.id = u.preferred_group_id
            """
            users_params: list = []
            users_where: list[str] = []

            if users_role == "admin":
                users_where.append("EXISTS(SELECT 1 FROM site_admins a WHERE a.user_id = u.id)")
            elif users_role == "user":
                users_where.append("NOT EXISTS(SELECT 1 FROM site_admins a WHERE a.user_id = u.id)")

            if users_group_id_filter == "none":
                users_where.append("u.preferred_group_id IS NULL")
            elif users_group_id_filter.isdigit():
                users_where.append("u.preferred_group_id = %s")
                users_params.append(int(users_group_id_filter))

            if users_where:
                users_sql += " WHERE " + " AND ".join(users_where)

            if users_group_by == "role":
                users_sql += " ORDER BY is_admin DESC, COALESCE(g.group_name, ''), u.id DESC"
            elif users_group_by == "group":
                users_sql += " ORDER BY COALESCE(g.group_name, ''), is_admin DESC, u.id DESC"
            else:
                users_sql += " ORDER BY u.id DESC"

            users_sql += " LIMIT 500"
            cur.execute(users_sql, tuple(users_params))
            users = cur.fetchall()


    parser_state = get_parser_state()
    planshetka_state = get_planshetka_state()
    auto_cfg = get_auto_schedule()
    telegram_settings = load_telegram_settings()
    announcements = fetch_admin_announcements()
    return render_template(
        "admin_dashboard.html",
        title="Админ-панель",
        groups=groups,
        subjects=subjects,
        teachers=teachers,
        schedule_rows=schedule_rows,
        users=users,
        parser_summary=parser_state["summary"],
        parser_failed_groups=parser_state["failed_groups"],
        parser_log_lines=parser_state["log_lines"],
        parser_last_run_at=parser_state["last_run_at"],
        parser_is_running=parser_state["is_running"],
        parser_stop_requested=parser_state["stop_requested"],
        auto_enabled=auto_cfg["enabled"],
        auto_weekday=auto_cfg["run_weekday"],
        auto_run_time=auto_cfg["run_time"].strftime("%H:%M"),
        weekday_options=WEEKDAY_OPTIONS,
        auto_scheduler_enabled=AUTO_SCHEDULER_ENABLED,
        planshetka_folder_url=get_planshetka_folder_url(),
        planshetka_recent_files_limit=get_planshetka_recent_files_limit(),
        admin_section=admin_section,
        planshetka_summary=planshetka_state["summary"],
        planshetka_log_lines=planshetka_state["log_lines"],
        planshetka_last_run_at=planshetka_state["last_run_at"],
        planshetka_is_running=planshetka_state["is_running"],
        planshetka_failed_files=planshetka_state["failed_files"],
        planshetka_scanned_files=planshetka_state["scanned_files"],
        users_role=users_role,
        users_group_id_filter=users_group_id_filter,
        users_group_by=users_group_by,
        telegram_settings=telegram_settings,
        announcements=announcements,
    )


@app.route("/admin/monitor")
@admin_required
def admin_monitor():
    metrics = collect_monitor_payload()
    online_users = 0
    online_error = ""
    try:
        online_users = count_online_users()
    except Exception as exc:
        online_error = str(exc)
    return render_template(
        "admin_monitor.html",
        title="Мониторинг",
        metrics=metrics,
        online_users=online_users,
        online_window_minutes=ONLINE_USERS_WINDOW_MINUTES,
        online_error=online_error,
    )


@app.route("/admin/monitor/status")
@admin_required
def admin_monitor_status():
    metrics = collect_monitor_payload()
    online_users = 0
    online_error = ""
    try:
        online_users = count_online_users()
    except Exception as exc:
        online_error = str(exc)
    return jsonify(
        {
            "ok": True,
            "metrics": metrics,
            "online_users": online_users,
            "online_window_minutes": ONLINE_USERS_WINDOW_MINUTES,
            "online_error": online_error,
        }
    )


@app.route("/admin/parser-status")
@admin_required
def admin_parser_status():
    state = get_parser_state()
    return jsonify({"summary": state["summary"], "failed_groups": state["failed_groups"], "log_lines": state["log_lines"], "is_running": state["is_running"], "stop_requested": state["stop_requested"], "last_run_at": state["last_run_at"].strftime("%Y-%m-%d %H:%M:%S") if state["last_run_at"] else ""})


@app.route("/admin/planshetka-status")
@admin_required
def admin_planshetka_status():
    state = get_planshetka_state()
    return jsonify({"summary": state["summary"], "log_lines": state["log_lines"], "is_running": state["is_running"], "failed_files": state["failed_files"], "scanned_files": state["scanned_files"], "last_run_at": state["last_run_at"].strftime("%Y-%m-%d %H:%M:%S") if state["last_run_at"] else ""})


def initialize_app_runtime() -> None:
    init_steps = [
        ("auto schedule", ensure_auto_schedule_table),
        ("planshetka tables", ensure_planshetka_tables),
        ("runtime state", ensure_runtime_state_table),
        ("personalization", ensure_personalization_tables),
        ("bot tables", ensure_bot_tables),
        ("monitoring tables", ensure_monitoring_tables),
        ("runtime state encoding", repair_runtime_state_encoding),
        ("runtime state flags", reset_runtime_state_flags),
        ("change feed bootstrap", detect_and_record_schedule_changes),
    ]
    for label, fn in init_steps:
        try:
            fn()
        except Exception as exc:
            print(f"[WARN] init '{label}' failed: {exc}")

    try:
        llm_ok, llm_err = ensure_llm_ready()
        if not llm_ok:
            print(f"[WARN] AI storage init failed: {llm_err}")
    except Exception as exc:
        print(f"[WARN] AI storage init failed: {exc}")

    if should_start_background_threads():
        Thread(target=telegram_polling_worker, daemon=True).start()
        Thread(target=telegram_notification_worker, daemon=True).start()


initialize_app_runtime()


if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes"}
    app_host = os.getenv("APP_HOST", "0.0.0.0")
    app_port = int(os.getenv("APP_PORT", "5000"))
    app.run(host=app_host, port=app_port, debug=debug_mode)





