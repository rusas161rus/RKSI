import os
import re
from datetime import date, datetime, timedelta
from functools import wraps
from threading import Lock, Thread

from dotenv import load_dotenv
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

from db import ensure_schedule_room_columns, get_main_conn
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

PARSER_SYNC_STATE = {"last_run_at": None, "summary": "Парсер ещё не запускался из админки.", "log_lines": [], "failed_groups": [], "is_running": False, "stop_requested": False}
PLANSHETKA_SYNC_STATE = {"last_run_at": None, "summary": "Парсер Planshetka ещё не запускался.", "log_lines": [], "is_running": False, "failed_files": 0, "scanned_files": 0}
PARSER_SYNC_LOCK = Lock()
PLANSHETKA_SYNC_LOCK = Lock()

INVALID_TEACHER_NAME_RE = re.compile(r"^[\s_.-]+$")


def normalize_teacher_name(raw_name: str | None) -> str | None:
    normalized = re.sub(r"\\s+", " ", (raw_name or "").strip())
    if not normalized:
        return None
    if INVALID_TEACHER_NAME_RE.fullmatch(normalized):
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


def load_user(user_id: int):
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.username, u.full_name, u.preferred_group_id,
                       EXISTS(SELECT 1 FROM site_admins a WHERE a.user_id = u.id) AS is_admin
                FROM site_users u
                WHERE u.id = %s AND u.is_active = TRUE
            """, (user_id,))
            row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "username": row[1], "full_name": row[2], "preferred_group_id": row[3], "is_admin": row[4]}


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


def get_parser_state() -> dict:
    with PARSER_SYNC_LOCK:
        return {k: (list(v) if isinstance(v, list) else v) for k, v in PARSER_SYNC_STATE.items()}


def set_parser_state(**updates) -> None:
    with PARSER_SYNC_LOCK:
        line = updates.pop("append_line", None)
        if line:
            PARSER_SYNC_STATE["log_lines"].append(str(line))
            PARSER_SYNC_STATE["log_lines"] = PARSER_SYNC_STATE["log_lines"][-400:]
        PARSER_SYNC_STATE.update({k: v for k, v in updates.items() if k in PARSER_SYNC_STATE})


def get_planshetka_state() -> dict:
    with PLANSHETKA_SYNC_LOCK:
        return {k: (list(v) if isinstance(v, list) else v) for k, v in PLANSHETKA_SYNC_STATE.items()}


def set_planshetka_state(**updates) -> None:
    with PLANSHETKA_SYNC_LOCK:
        line = updates.pop("append_line", None)
        if line:
            PLANSHETKA_SYNC_STATE["log_lines"].append(str(line))
            PLANSHETKA_SYNC_STATE["log_lines"] = PLANSHETKA_SYNC_STATE["log_lines"][-400:]
        PLANSHETKA_SYNC_STATE.update({k: v for k, v in updates.items() if k in PLANSHETKA_SYNC_STATE})

def start_parser_job(group_names: list[str], mode_label: str) -> bool:
    with PARSER_SYNC_LOCK:
        if PARSER_SYNC_STATE["is_running"]:
            return False
        PARSER_SYNC_STATE.update({"is_running": True, "stop_requested": False, "failed_groups": [], "summary": f"Запущен: {mode_label}", "log_lines": [f"[START] {mode_label}"]})

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
            set_parser_state(is_running=False, stop_requested=False, failed_groups=failed, last_run_at=datetime.now(), summary=f"Завершено: успешно {done}, ошибок {len(failed)}")

    Thread(target=worker, daemon=True).start()
    return True


def start_planshetka_job(folder_url: str, recent_files_limit: int, mode_label: str) -> bool:
    with PLANSHETKA_SYNC_LOCK:
        if PLANSHETKA_SYNC_STATE["is_running"]:
            return False
        PLANSHETKA_SYNC_STATE.update({"is_running": True, "summary": f"Запущен: {mode_label}", "log_lines": [f"[START] {mode_label}"], "failed_files": 0, "scanned_files": 0})

    def worker():
        def log(message: str):
            set_planshetka_state(append_line=message)
        try:
            imported, failed_files, scanned_files = run_planshetka_sync(folder_url, replace_group=True, recent_files_limit=recent_files_limit, log=log)
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


@app.route("/me", methods=["GET", "POST"])
@login_required
def me():
    ensure_schedule_room_columns()
    user = load_user(session["user_id"])
    if not user:
        session.clear()
        return redirect(url_for("login"))
    if request.method == "POST":
        action = request.form.get("profile_action") or "apply"
        preferred_group_id = (request.form.get("preferred_group_id") or "").strip() or None
        with get_main_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE site_users SET preferred_group_id = %s, updated_at = now() WHERE id = %s", (preferred_group_id, user["id"]))
        if action == "reset_search":
            return redirect(url_for("me", date=request.form.get("date"), period=request.form.get("period"), source=request.form.get("source") or "rksi"))
        return redirect(url_for("me", date=request.form.get("date"), period=request.form.get("period"), q=request.form.get("q"), source=request.form.get("source") or "rksi", teacher_id=request.form.get("teacher_id"), all_groups=1 if request.form.get("all_groups") == "1" else 0))

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
            teachers = cur.fetchall()
            teacher_filter_name = None
            if teacher_filter_id is not None:
                cur.execute("SELECT full_name FROM teachers WHERE id = %s", (teacher_filter_id,))
                row = cur.fetchone()
                teacher_filter_name = row[0] if row else None
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
                               subj.subject_name, t.full_name AS teacher_name, COALESCE(p.room, t.room) AS room,
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
                                 t.full_name AS teacher_name, COALESCE(s.room, t.room) AS room, 'manual' AS source,
                                 s.teacher_id, g.group_name, s.group_id
                          FROM schedule_entries s JOIN subjects subj ON subj.id = s.subject_id LEFT JOIN teachers t ON t.id = s.teacher_id JOIN study_groups g ON g.id = s.group_id
                          UNION ALL
                          SELECT p.id, p.lesson_date, p.start_time, p.end_time, subj.subject_name,
                                 t.full_name AS teacher_name, COALESCE(p.room, t.room) AS room, 'planshetka' AS source,
                                 p.teacher_id, g.group_name, p.group_id
                          FROM parsed_tabletka_schedule_entries p JOIN subjects subj ON subj.id = p.subject_id LEFT JOIN teachers t ON t.id = p.teacher_id JOIN study_groups g ON g.id = p.group_id
                        ) x WHERE x.lesson_date BETWEEN %s AND %s
                    """
                params = [start_day, end_day]
                if use_group_filter:
                    sql += " AND group_id = %s"
                    params.append(user["preferred_group_id"])
                if teacher_filter_name:
                    sql += " AND COALESCE(teacher_name, '') = %s"
                    params.append(teacher_filter_name)
                if keyword:
                    sql += " AND (subject_name ILIKE %s OR COALESCE(room, '') ILIKE %s)"
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
    return render_template(
        "user_dashboard.html",
        title="Личный кабинет",
        user=user,
        groups=groups,
        teachers=teachers,
        schedule_days=schedule_days,
        selected_date=day.isoformat(),
        selected_period=period,
        period_options=PERIOD_OPTIONS,
        search_query=keyword,
        group_name=group_name,
        selected_teacher_id=teacher_filter,
        all_groups=all_groups,
        show_schedule=show_schedule,
        source_mode=source_mode,
        planshetka_folder_url=get_planshetka_folder_url(),
        rksi_schedule_url=RKSI_SCHEDULE_URL,
        rksi_mobile_schedule_url=RKSI_MOBILE_SCHEDULE_URL,
    )

@app.route("/admin", methods=["GET", "POST"])
@admin_required
def admin_dashboard():
    ensure_schedule_room_columns()
    ensure_planshetka_tables()
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
                        cur.execute("INSERT INTO site_users(username, password_hash, full_name, preferred_group_id) VALUES (%s, %s, %s, %s) RETURNING id", (username, generate_password_hash(password), full_name or None, preferred_group_id))
                        new_user_id = cur.fetchone()[0]
                        if is_admin:
                            cur.execute("INSERT INTO site_admins(user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (new_user_id,))
                        flash("Пользователь создан.", "success")
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
                            flash("Занятие добавлено в ручное расписание.", "success")
                        except Exception:
                            flash("Не удалось добавить занятие (проверьте корректность и существующие связи).", "error")
                    else:
                        flash("Заполните все поля для добавления занятия.", "error")
                elif action in {"delete_schedule", "delete_schedule_by_id"}:
                    entry_id = (request.form.get("entry_id") or "").strip()
                    if entry_id.isdigit():
                        cur.execute("DELETE FROM schedule_entries WHERE id = %s", (entry_id,))
                        flash("Запись занятия удалена." if cur.rowcount else "Запись с таким ID не найдена.", "success" if cur.rowcount else "error")
                    else:
                        flash("Укажите корректный числовой ID занятия.", "error")
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
            teachers = cur.fetchall()
            cur.execute("""
                SELECT x.id, x.lesson_date, x.start_time, x.end_time, x.subject_name, x.teacher_name, x.group_name, x.source
                FROM (
                  SELECT s.id, s.lesson_date, s.start_time, s.end_time, subj.subject_name, COALESCE(t.full_name, '') AS teacher_name, g.group_name, 'manual' AS source
                  FROM schedule_entries s JOIN subjects subj ON subj.id = s.subject_id LEFT JOIN teachers t ON t.id = s.teacher_id JOIN study_groups g ON g.id = s.group_id
                  UNION ALL
                  SELECT p.id, p.lesson_date, p.start_time, p.end_time, subj.subject_name, COALESCE(t.full_name, '') AS teacher_name, g.group_name, 'parsed' AS source
                  FROM parsed_schedule_entries p JOIN subjects subj ON subj.id = p.subject_id LEFT JOIN teachers t ON t.id = p.teacher_id JOIN study_groups g ON g.id = p.group_id
                  UNION ALL
                  SELECT pt.id, pt.lesson_date, pt.start_time, pt.end_time, subj.subject_name, COALESCE(t.full_name, '') AS teacher_name, g.group_name, 'planshetka' AS source
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


ensure_auto_schedule_table()
ensure_planshetka_tables()


if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes"}
    app_host = os.getenv("APP_HOST", "0.0.0.0")
    app_port = int(os.getenv("APP_PORT", "5000"))
    app.run(host=app_host, port=app_port, debug=debug_mode)





