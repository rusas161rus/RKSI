#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"

DB_HOST="${DB_HOST:-192.168.88.227}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-apprksi}"
DB_USER="${DB_USER:-apprksi}"
DB_PASSWORD="${DB_PASSWORD:-change_me}"

cd "$PROJECT_ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found. Install Python 3.12+ and rerun." >&2
  exit 1
fi

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install -r requirements.txt

cat > .env <<EOF
FLASK_SECRET_KEY=replace_with_long_random_secret

DB_HOST=${DB_HOST}
DB_PORT=${DB_PORT}
DB_NAME=${DB_NAME}
DB_USER=${DB_USER}
DB_PASSWORD=${DB_PASSWORD}
APP_HOST=0.0.0.0
APP_PORT=5000
TRUST_PROXY=1
FLASK_DEBUG=0
EOF

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found. Install postgresql-client and apply SQL manually from sql/*.sql" >&2
  exit 1
fi

export PGPASSWORD="$DB_PASSWORD"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$PROJECT_ROOT/sql/000_reset_main.sql"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$PROJECT_ROOT/sql/001_main_schema.sql"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$PROJECT_ROOT/sql/002_parser_schema.sql"

echo "Done."
echo "Create admin: ./.venv/bin/python scripts/create_user.py --username admin --password admin123 --admin"
echo "Run app: ./.venv/bin/python app.py"
