# RKSI Schedule (Flask + PostgreSQL)

Веб-приложение для работы с расписанием РКСИ: пользовательский кабинет, админ-панель, парсинг расписания из источников RKSI/Planshetka, персонализация и ИИ-чат (Ollama).

## Краткий анализ проекта

Актуально на 11.03.2026.

- Стек: `Flask`, `PostgreSQL`, `Jinja2`, `Gunicorn`, `Docker Compose`.
- Архитектура: монолитное Flask-приложение с SQL-скриптами миграций (`sql/*.sql`) и фоновой обработкой для Telegram/парсеров.
- Основные зоны ответственности:
  - `app.py` - маршруты, авторизация, админка, API мониторинга.
  - `scripts/parse_and_sync.py`, `scripts/parse_tabletka_sync.py` - синхронизация расписания.
  - `llm_assistant.py` - логика ИИ-чата и пользовательских настроек ИИ.
  - `personalization.py` - заметки, избранное, Telegram-интеграция, объявления.
- Что уже хорошо:
  - разделение по доменным модулям;
  - SQL-инициализация по шагам;
  - готовый `Dockerfile` и `docker-compose.yml`;
  - админские инструменты мониторинга и управления пользователями.
- Что рекомендуется поддерживать в приоритете:
  - регламент обновления юридических документов;
  - единые требования к настройке `.env` в проде;
  - регулярная проверка доступности внешних источников парсинга.

## Правовые документы

- Страница по cookies/кэшу: `/cookie-policy`
- Лицензионное соглашение о бесплатном пользовании: `/free-use-license`

Шаблоны:

- `templates/cookie_policy.html`
- `templates/free_use_license.html`

## Что изменено в этой редакции

- обновлено соглашение о применении cookies, localStorage, sessionStorage и кэша браузера;
- добавлено лицензионное соглашение о безвозмездном пользовании сайтом (в формате делового документа с нумерованной структурой и ссылкой на ГОСТ Р 7.0.97-2016);
- добавлены ссылки на правовые документы в баннер cookies и в нижний футер;
- обновлен `README` с кратким техническим анализом проекта и разделом про юридические документы.

## Быстрый деплой на Ubuntu (Docker)

1. Подготовить каталог:

```bash
sudo mkdir -p /opt/docker-compose/rksi
sudo chown -R $USER:$USER /opt/docker-compose/rksi
cd /opt/docker-compose/rksi
```

2. Клонировать репозиторий:

```bash
git clone <YOUR_REPO_URL> .
```

3. Создать `.env`:

```bash
cp .env.example .env
nano .env
```

Минимально заполнить:

- `FLASK_SECRET_KEY`
- `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD`
- `LLM_DB_HOST/LLM_DB_PORT/LLM_DB_NAME/LLM_DB_USER/LLM_DB_PASSWORD`
- `OLLAMA_URL`
- `OLLAMA_MODEL`

4. Поднять контейнер:

```bash
docker compose up -d --build
```

5. Проверка:

```bash
docker compose ps
docker compose logs -f --tail=200 web
curl -I http://127.0.0.1:5000/login
```

## Настройка Nginx Proxy Manager

Рекомендуемые параметры `Proxy Host`:

- Domain Names: `rksi.bastion-local.ru`
- Scheme: `http`
- Forward Hostname/IP: `127.0.0.1`
- Forward Port: `5000`
- Websockets Support: `ON`
- Block Common Exploits: `ON`
- SSL: Let's Encrypt + `Force SSL`

## Инициализация БД

Основная БД:

```bash
export PGPASSWORD='<DB_PASSWORD>'
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/000_reset_main.sql
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/001_main_schema.sql
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/002_parser_schema.sql
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/004_personalization.sql
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/008_ai_access_schema.sql
```

БД Telegram-бота:

```bash
export PGPASSWORD='<BOT_DB_PASSWORD>'
psql -h <BOT_DB_HOST> -p <BOT_DB_PORT> -U <BOT_DB_USER> -d <BOT_DB_NAME> -f sql/005_bot_schema.sql
```

БД ИИ-чата:

```bash
export PGPASSWORD='<LLM_DB_PASSWORD>'
psql -h <LLM_DB_HOST> -p <LLM_DB_PORT> -U <LLM_DB_USER> -d <LLM_DB_NAME> -f sql/006_llm_schema.sql
```

Создать первого администратора:

```bash
docker compose exec web python scripts/create_user.py --username admin --password admin123 --admin
```

## Локальный запуск без Docker (Ubuntu)

```bash
bash scripts/bootstrap_linux.sh
source .venv/bin/activate
python app.py
```

## Полезные команды эксплуатации

```bash
# Перезапуск
cd /opt/docker-compose/rksi && docker compose up -d --build

# Остановка
cd /opt/docker-compose/rksi && docker compose down

# Логи
cd /opt/docker-compose/rksi && docker compose logs -f web
```
