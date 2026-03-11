# RKSI Schedule (Flask + PostgreSQL) - Ubuntu Deploy

Проект: Flask-приложение с пользовательской и админ-панелью, парсером расписания RKSI и импортом planshetka.

## Что изменено для Linux/Ubuntu

- добавлен production запуск через `gunicorn`;
- добавлен reverse-proxy trust (`TRUST_PROXY=1`) для Nginx Proxy Manager;
- добавлен `Dockerfile` и `docker-compose.yml`;
- добавлен Linux bootstrap-скрипт `scripts/bootstrap_linux.sh`;
- обновлён `.env.example` под Linux/production.

## Быстрый деплой на сервер Ubuntu

Цель: публикация через Nginx Proxy Manager на домене `rksi.bastion-local.ru`.

1. Подключиться к серверу `192.168.88.214` и создать папку:

```bash
sudo mkdir -p /opt/docker-compose/rksi
sudo chown -R $USER:$USER /opt/docker-compose/rksi
cd /opt/docker-compose/rksi
```

2. Получить код (через git):

```bash
git clone <YOUR_REPO_URL> .
```

3. Подготовить `.env`:

```bash
cp .env.example .env
nano .env
```

Обязательно заполнить:
- `FLASK_SECRET_KEY` (длинный случайный ключ)
- `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD`

4. Поднять контейнер:

```bash
docker compose up -d --build
```

5. Проверка статуса:

```bash
docker compose ps
docker compose logs -f --tail=200 web
curl -I http://127.0.0.1:5000/login
```

## Настройка Nginx Proxy Manager

В NPM создайте `Proxy Host`:
- Domain Names: `rksi.bastion-local.ru`
- Scheme: `http`
- Forward Hostname/IP: `127.0.0.1`
- Forward Port: `5000`
- Websockets Support: `ON`
- Block Common Exploits: `ON`
- SSL: выпустить Let's Encrypt сертификат для `rksi.bastion-local.ru` и включить Force SSL.

## Миграции/инициализация БД

Если нужно развернуть схему с нуля:

```bash
export PGPASSWORD='<DB_PASSWORD>'
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/000_reset_main.sql
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/001_main_schema.sql
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/002_parser_schema.sql
psql -h <DB_HOST> -p <DB_PORT> -U <DB_USER> -d <DB_NAME> -f sql/004_personalization.sql
```

Для отдельной БД Telegram-бота:

```bash
export PGPASSWORD='<BOT_DB_PASSWORD>'
psql -h <BOT_DB_HOST> -p <BOT_DB_PORT> -U <BOT_DB_USER> -d <BOT_DB_NAME> -f sql/005_bot_schema.sql
```

Создать первого админа:

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
# перезапуск
cd /opt/docker-compose/rksi && docker compose up -d --build

# остановка
cd /opt/docker-compose/rksi && docker compose down

# логи
cd /opt/docker-compose/rksi && docker compose logs -f web
```
