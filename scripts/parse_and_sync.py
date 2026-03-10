from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db import ensure_schedule_room_columns, get_main_conn

SOURCE_URL = "https://www.rksi.ru/mobileschedule"
GROUPS_URL = f"{SOURCE_URL}/groups"

MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}

DAY_HEADER_RE = re.compile(r"^(\d{1,2})\s+([а-яё]+),\s*([а-яё]+)$", re.IGNORECASE)
TIME_RANGE_RE = re.compile(r"^(\d{2}:\d{2})\s*[—-]\s*(\d{2}:\d{2})$")
ROOM_RE = re.compile(r"ауд\.?\s*([\w/.-]+)", re.IGNORECASE)
GROUP_LINK_RE = re.compile(r"/mobileschedule/groups/(\d+)(?:/)?(?:\?.*)?$")
INVALID_TEACHER_NAME_RE = re.compile(r"^[\s_.-]+$")


@dataclass
class Lesson:
    day_label: str
    lesson_date: Optional[date]
    start_time: Optional[str]
    end_time: Optional[str]
    subject_name: str
    teacher_name: Optional[str]
    room: Optional[str]
    raw_text: str


def normalize_line(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_group_name(name: str) -> str:
    return normalize_line(name).upper().replace("Ё", "Е")


def normalize_subject_name(name: str) -> str:
    cleaned = normalize_line(name)
    return cleaned if cleaned else "Неизвестный предмет"


def normalize_teacher_name(name: str | None) -> Optional[str]:
    cleaned = normalize_line(name or "")
    if not cleaned:
        return None
    if INVALID_TEACHER_NAME_RE.fullmatch(cleaned):
        return None
    return cleaned


def parse_day_label(text: str) -> tuple[str, Optional[date]]:
    cleaned = normalize_line(text.lower())
    m = DAY_HEADER_RE.match(cleaned)
    if not m:
        return normalize_line(text), None

    day_num = int(m.group(1))
    month = MONTHS.get(m.group(2))
    if not month:
        return normalize_line(text), None

    year = datetime.now().year
    return normalize_line(text), date(year, month, day_num)


def split_teacher_and_room(text: str) -> tuple[Optional[str], Optional[str]]:
    clean = normalize_line(text)
    if not clean:
        return None, None

    room_match = ROOM_RE.search(clean)
    room = room_match.group(1) if room_match else None
    teacher = ROOM_RE.sub("", clean).strip(" ,") if room_match else clean
    return normalize_teacher_name(teacher), room


def fetch_group_map() -> dict[str, str]:
    response = requests.get(GROUPS_URL, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    group_map: dict[str, str] = {}
    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        text = normalize_line(a.get_text(" ", strip=True))
        if not text:
            continue
        if text.lower().startswith("вернуться") or "обычному расписанию" in text.lower():
            continue

        m = GROUP_LINK_RE.search(href)
        if not m:
            continue

        group_map[normalize_group_name(text)] = f"https://www.rksi.ru{href}"

    if not group_map:
        raise RuntimeError("Не удалось получить список групп с /mobileschedule/groups")

    return group_map


def resolve_group_url(group_name: str) -> str:
    group_map = fetch_group_map()
    target = normalize_group_name(group_name)

    if target in group_map:
        return group_map[target]

    for name, url in group_map.items():
        if target in name or name in target:
            return url

    examples = ", ".join(list(group_map.keys())[:12])
    raise RuntimeError(f"Группа '{group_name}' не найдена на сайте. Примеры доступных: {examples}")


def fetch_group_schedule(group_name: str) -> str:
    group_url = resolve_group_url(group_name)
    response = requests.get(group_url, timeout=30)
    response.raise_for_status()
    return response.text


def extract_schedule_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    raw_lines = [normalize_line(line) for line in text.split("\n") if normalize_line(line)]

    lines: list[str] = []
    for line in raw_lines:
        low = line.lower()
        if "расписание ркси" in low:
            continue
        if low.startswith("вернуться к выбору типа расписания"):
            continue
        if low == "планшетка.":
            continue
        if "к обычному расписанию" in low:
            continue
        if line == "***" or line == "* * *":
            lines.append("* * *")
            continue
        lines.append(line)

    return lines


def parse_schedule_html(html: str) -> list[Lesson]:
    lines = extract_schedule_lines(html)

    lessons: list[Lesson] = []
    current_day_label = "Без даты"
    current_lesson_date: Optional[date] = None

    i = 0
    while i < len(lines):
        line = lines[i]

        if line == "* * *":
            i += 1
            continue

        if DAY_HEADER_RE.match(line.lower()):
            current_day_label, current_lesson_date = parse_day_label(line)
            i += 1
            continue

        time_match = TIME_RANGE_RE.match(line)
        if time_match:
            start_time, end_time = time_match.group(1), time_match.group(2)

            subject = "Неизвестный предмет"
            teacher_name = None
            room = None
            raw_parts = [line]

            if i + 1 < len(lines):
                next_line = lines[i + 1]
                if next_line != "* * *" and not DAY_HEADER_RE.match(next_line.lower()) and not TIME_RANGE_RE.match(next_line):
                    subject = next_line
                    raw_parts.append(next_line)
                    i += 1

            if i + 1 < len(lines):
                detail_line = lines[i + 1]
                if detail_line != "* * *" and not DAY_HEADER_RE.match(detail_line.lower()) and not TIME_RANGE_RE.match(detail_line):
                    teacher_name, room = split_teacher_and_room(detail_line)
                    raw_parts.append(detail_line)
                    i += 1

            lessons.append(
                Lesson(
                    day_label=current_day_label,
                    lesson_date=current_lesson_date,
                    start_time=start_time,
                    end_time=end_time,
                    subject_name=subject,
                    teacher_name=teacher_name,
                    room=room,
                    raw_text="\n".join(raw_parts),
                )
            )

        i += 1

    return lessons


def build_hash(group_name: str, lesson: Lesson) -> str:
    payload = "|".join(
        [
            normalize_group_name(group_name),
            lesson.day_label,
            lesson.lesson_date.isoformat() if lesson.lesson_date else "",
            lesson.start_time or "",
            lesson.end_time or "",
            lesson.subject_name,
            lesson.teacher_name or "",
            lesson.room or "",
            lesson.raw_text,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def store_in_parser_db(group_name: str, lessons: list[Lesson], clear_group: bool = False) -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            norm_group = normalize_group_name(group_name)
            if clear_group:
                cur.execute("DELETE FROM parser_days WHERE group_name = %s", (norm_group,))

            day_ids: dict[tuple[str, Optional[date]], int] = {}
            for lesson in lessons:
                day_key = (lesson.day_label, lesson.lesson_date)
                if day_key not in day_ids:
                    cur.execute(
                        """
                        SELECT id
                        FROM parser_days
                        WHERE group_name = %s
                          AND day_label = %s
                          AND (
                            (lesson_date IS NULL AND %s IS NULL)
                            OR lesson_date = %s
                          )
                        ORDER BY id
                        LIMIT 1
                        """,
                        (norm_group, lesson.day_label, lesson.lesson_date, lesson.lesson_date),
                    )
                    existing_day = cur.fetchone()
                    if existing_day:
                        day_ids[day_key] = existing_day[0]
                    else:
                        cur.execute(
                            """
                            INSERT INTO parser_days(source_url, group_name, lesson_date, day_label)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                            """,
                            (SOURCE_URL, norm_group, lesson.lesson_date, lesson.day_label),
                        )
                        day_ids[day_key] = cur.fetchone()[0]

                source_hash = build_hash(norm_group, lesson)
                cur.execute(
                    """
                    INSERT INTO parser_lessons(
                      day_id, start_time, end_time, subject_name, teacher_name, room, raw_text, source_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_hash) DO NOTHING
                    """,
                    (
                        day_ids[day_key],
                        lesson.start_time,
                        lesson.end_time,
                        lesson.subject_name,
                        lesson.teacher_name,
                        lesson.room,
                        lesson.raw_text,
                        source_hash,
                    ),
                )


def sync_from_parser_to_main(group_name: str, replace_group: bool = False) -> int:
    norm_group = normalize_group_name(group_name)
    ensure_schedule_room_columns()

    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.lesson_date, l.start_time, l.end_time, l.subject_name, l.teacher_name, l.room, l.source_hash
                FROM parser_lessons l
                JOIN parser_days d ON d.id = l.day_id
                WHERE d.group_name = %s
                ORDER BY d.lesson_date NULLS LAST, l.start_time NULLS LAST
                """,
                (norm_group,),
            )
            rows = cur.fetchall()

    imported_count = 0
    with get_main_conn() as main_conn:
        with main_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO study_groups(group_name)
                VALUES (%s)
                ON CONFLICT (group_name) DO UPDATE SET group_name = EXCLUDED.group_name
                RETURNING id
                """,
                (norm_group,),
            )
            group_id = cur.fetchone()[0]

            if replace_group:
                cur.execute("DELETE FROM parsed_schedule_entries WHERE group_id = %s", (group_id,))

            for lesson_date, start_time, end_time, subject_name, teacher_name, room, source_hash in rows:
                if not lesson_date:
                    continue

                cur.execute(
                    """
                    INSERT INTO subjects(subject_name)
                    VALUES (%s)
                    ON CONFLICT (subject_name) DO UPDATE SET subject_name = EXCLUDED.subject_name
                    RETURNING id
                    """,
                    (normalize_subject_name(subject_name),),
                )
                subject_id = cur.fetchone()[0]

                teacher_id = None
                teacher_name = normalize_teacher_name(teacher_name)
                if teacher_name:
                    cur.execute(
                        """
                        SELECT id, room
                        FROM teachers
                        WHERE lower(regexp_replace(trim(full_name), '\\s+', ' ', 'g')) =
                              lower(regexp_replace(trim(%s), '\\s+', ' ', 'g'))
                        ORDER BY id
                        LIMIT 1
                        """,
                        (teacher_name,),
                    )
                    existing_teacher = cur.fetchone()
                    if existing_teacher:
                        teacher_id = existing_teacher[0]
                    else:
                        cur.execute(
                            """
                            INSERT INTO teachers(full_name, room)
                            VALUES (%s, NULL)
                            RETURNING id
                            """,
                            (teacher_name,),
                        )
                        teacher_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO parsed_schedule_entries(
                      lesson_date, start_time, end_time, subject_id, teacher_id, group_id, room, source_hash, source_group_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_hash) DO NOTHING
                    RETURNING id
                    """,
                    (lesson_date, start_time, end_time, subject_id, teacher_id, group_id, room, source_hash, norm_group),
                )
                created = cur.fetchone()
                if created:
                    imported_count += 1

    return imported_count


def run(group_name: str, clear_parser_group: bool = False, replace_main_group: bool = False) -> int:
    html = fetch_group_schedule(group_name)
    lessons = parse_schedule_html(html)
    if not lessons:
        raise RuntimeError("Парсер вернул 0 занятий. Проверьте структуру страницы источника.")

    store_in_parser_db(group_name, lessons, clear_group=clear_parser_group)
    return sync_from_parser_to_main(group_name, replace_group=replace_main_group)


def clear_all_schedule_data() -> None:
    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE parser_lessons RESTART IDENTITY CASCADE")
            cur.execute("TRUNCATE TABLE parser_days RESTART IDENTITY CASCADE")
            cur.execute("TRUNCATE TABLE parsed_schedule_entries RESTART IDENTITY CASCADE")


def run_all_groups(replace_main_group: bool = True, clear_existing_before_all: bool = False) -> tuple[int, int, int]:
    group_map = fetch_group_map()
    groups = sorted(group_map.keys())
    if not groups:
        raise RuntimeError("Список групп пустой, парсинг невозможен.")

    if clear_existing_before_all:
        clear_all_schedule_data()

    success_groups = 0
    failed_groups = 0
    total_imported = 0

    for group_name in groups:
        try:
            imported = run(group_name=group_name, clear_parser_group=True, replace_main_group=replace_main_group)
            success_groups += 1
            total_imported += imported
            print(f"[OK] {group_name}: imported_new_rows={imported}")
        except Exception as exc:
            failed_groups += 1
            print(f"[FAIL] {group_name}: {exc}")

    return success_groups, failed_groups, total_imported


def main() -> None:
    parser = argparse.ArgumentParser(description="Парсинг расписания RKSI (/mobileschedule) и синхронизация")
    parser.add_argument("--group", default=None, help="Название группы, например ИС-31")
    parser.add_argument(
        "--all-groups",
        action="store_true",
        help="Обойти все группы с /mobileschedule/groups и загрузить всё в БД",
    )
    parser.add_argument(
        "--clear-parser-group",
        action="store_true",
        help="Перед загрузкой удалить старые сырые строки этой группы",
    )
    parser.add_argument(
        "--replace-main-group",
        action="store_true",
        help="Перед импортом удалить старое parsed-расписание этой группы",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Полная перезагрузка: очистить parser-таблицы и parsed_schedule_entries перед массовым импортом",
    )
    args = parser.parse_args()

    if args.all_groups or not args.group:
        ok, fail, imported_total = run_all_groups(
            replace_main_group=True,
            clear_existing_before_all=args.full_refresh,
        )
        print(f"ALL_GROUPS done: success={ok}, failed={fail}, imported_new_rows_total={imported_total}")
        return

    inserted = run(
        group_name=args.group,
        clear_parser_group=args.clear_parser_group,
        replace_main_group=args.replace_main_group,
    )
    print(f"Group={args.group}; imported_new_rows={inserted}")


if __name__ == "__main__":
    main()






