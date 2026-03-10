param(
    [string]$ProjectRoot = "E:\codex",
    [string]$PgHost = "192.168.88.227",
    [int]$PgPort = 5432,
    [string]$MainDb = "apprksi",
    [string]$DbUser = "apprksi",
    [string]$DbPassword = "OtOZnC88_lUg"
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$msg) {
    Write-Host "`n==> $msg" -ForegroundColor Cyan
}

Write-Step "Проверка Python"
if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python не найден. Установите Python 3.12 и повторите."
}

Write-Step "Создание виртуального окружения"
Set-Location $ProjectRoot
if (-not (Test-Path ".venv")) {
    py -3.12 -m venv .venv
}

Write-Step "Установка зависимостей"
& "$ProjectRoot\.venv\Scripts\python.exe" -m pip install --upgrade pip
& "$ProjectRoot\.venv\Scripts\python.exe" -m pip install -r "$ProjectRoot\requirements.txt"

Write-Step "Обновление .env"
@"
FLASK_SECRET_KEY=replace_with_long_random_secret

DB_HOST=$PgHost
DB_PORT=$PgPort
DB_NAME=$MainDb
DB_USER=$DbUser
DB_PASSWORD=$DbPassword
"@ | Set-Content -Encoding UTF8 "$ProjectRoot\.env"

Write-Step "Применение SQL-схем (одна БД)"
$env:PGPASSWORD = $DbPassword
psql -h $PgHost -p $PgPort -U $DbUser -d $MainDb -f "$ProjectRoot\sql\000_reset_main.sql"
psql -h $PgHost -p $PgPort -U $DbUser -d $MainDb -f "$ProjectRoot\sql\001_main_schema.sql"
psql -h $PgHost -p $PgPort -U $DbUser -d $MainDb -f "$ProjectRoot\sql\002_parser_schema.sql"

Write-Step "Готово"
Write-Host "Создайте админа:" -ForegroundColor Green
Write-Host "  .\.venv\Scripts\python.exe scripts\create_user.py --username admin --password admin123 --admin" -ForegroundColor Green
Write-Host "Запуск сайта:" -ForegroundColor Green
Write-Host "  .\.venv\Scripts\python.exe app.py" -ForegroundColor Green
Write-Host "Запуск парсера:" -ForegroundColor Green
Write-Host "  .\.venv\Scripts\python.exe scripts\parse_and_sync.py --group '2-ИС-З' --clear-parser-group --replace-main-group" -ForegroundColor Green
