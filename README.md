# LUCH Bot

Основной Telegram-бот и публичный API бронирований бара `Луч`.

## Что входит в этот репозиторий

- Telegram webhook и admin-flow: `tg_handlers.py`
- HTTP API и Flask entrypoint: `flask_app.py`
- Бизнес-логика броней: `booking_service.py`
- Интеграция с CRM: `crm_sync.py`
- Уведомления официантам: `waiter_notify.py`
- Telegram API helper: `telegram_api.py`

Связанные сервисы:
- CRM: отдельный репозиторий `LUCH_crm`
- Relay: отдельный репозиторий `luchbarbot-relay`

## Topology

```text
Telegram / Mini App / Tilda
          |
          v
      LUCH Bot
      /data/luchbar.db
          |
          +--> LUCH CRM sync
          |
          +--> Waiter Telegram group notifications
```

Если используется relay, тогда Telegram webhook и Tilda webhook сначала приходят в `luchbarbot-relay`, а уже потом в этот сервис.

## Основные сценарии

- создание брони из Telegram;
- создание брони из mini app;
- приём брони из Tilda;
- админские действия по броням и столам;
- синхронизация событий в CRM;
- уведомления в группу официантов по броням со `столом + депозитом`.

## ENV

Критичные переменные:
- `BOT_TOKEN`
- `TG_CHAT_ID`
- `WAITER_CHAT_ID`
- `CRM_API_URL`
- `CRM_API_KEY`
- `CRM_SYNC_SHARED_SECRET`
- `CRM_OUTBOX_INTERVAL_SEC`
- `CRM_OUTBOX_BATCH_LIMIT`
- `CRM_OUTBOX_MAX_ATTEMPTS`
- `CRM_OUTBOX_EMBEDDED_WORKER=0` если используется отдельный `crm_worker`
- `DB_PATH` или путь к `/data/luchbar.db`
- `TG_WEBHOOK_SECRET`
- `TILDA_SECRET`
- `DASHBOARD_SECRET`
- `MINIAPP_URL`
- `MINIAPP_MIN_LEAD_MINUTES`
- `TELEGRAM_INIT_DATA_MAX_AGE_SEC`
- `DASHBOARD_CORS_ORIGINS`
- `PUBLIC_CORS_ORIGINS`
- `VK_GROUP_ID`
- `VK_ACCESS_TOKEN`
- `VK_CALLBACK_SECRET`
- `VK_CONFIRMATION_TOKEN`
- `VK_API_VERSION`

Security-critical переменные (`BOT_TOKEN`, `TG_WEBHOOK_SECRET`, `TILDA_SECRET`, `DASHBOARD_SECRET`, `MINIAPP_URL`, `CRM_SYNC_SHARED_SECRET`) обязательны для запуска. Для локальной диагностики без них можно явно поставить `ALLOW_INSECURE_DEFAULTS=1`, но в production это использовать нельзя.

Старый Telegram auth-flow для CRM удалён. Вход в CRM выполняется по логину и паролю.

## Локальный запуск

```bash
cd luchbarbot
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python init_db.py
python flask_app.py
```

Проверка:

```bash
curl -s http://localhost:5000/health
```

## Логи

Проект использует два уровня диагностики:
- runtime-логи в stdout/stderr Railway;
- бизнес-историю в SQLite: `booking_events`, `table_events`, `guest_events`.

Правила локальных логов:
- писать через `local_log.py`;
- использовать формат `[PREFIX] key=value key=value`;
- не тащить в строку лишние персональные данные, если хватает `booking_id`, `table`, `action`, `chat_id`;
- для исключений использовать `log_exception(...)`.

Основные префиксы:
- `[TG-WEBHOOK]` — входящие Telegram update, дубли, ошибки callback/webhook.
- `[MINIAPP]` — создание брони из mini app.
- `[CRM_SYNC]` — отправка и приём sync-событий между ботом и CRM.
- `[WAITER-NOTIFY]` — уведомления в группу официантов.

Что искать в Railway Logs:
- `WAITER-NOTIFY` — почему уведомление ушло или было пропущено.
- `TG-WEBHOOK` — пришёл ли `callback_query`, не был ли update дублем.
- `CRM_SYNC` — ушло ли событие в CRM.

## Официанты

Логика уведомлений:
- если есть `deposit_amount`, но нет `assigned_table_number` — уведомление официантам не отправляется;
- админ получает напоминание назначить стол;
- когда у брони есть и `стол`, и `депозит`, бот отправляет карточку в waiter chat.

Waiter chat изолирован:
- входящие команды и callback-логика там игнорируются;
- чат используется только как канал служебных уведомлений.

## Deploy notes

- Railway service: `BOT_LUCH`
- DB volume: обычно `/data/luchbar.db`
- если включён relay, webhook Telegram должен смотреть на relay, а не на этот сервис напрямую
- VK Callback URL задаётся доменом текущего deploy: `https://<bot-domain>/vk/callback`
- outbox sync в CRM выполняет process `crm_worker`; embedded worker в web-процессе выключен по умолчанию

## История изменений

Краткая история последних рабочих правок вынесена в `CHANGELOG.md`.
