# Prod Clean Cutover

## Target

Перенести проект на новую продовую БД без исторических броней.

Переносим только:

- контакты;
- telegram/vk пользовательские связки;
- staff peers;
- новую пустую CRM-модель для будущих броней.

Не переносим:

- `bookings`
- `booking_events`
- `venue_tables`
- `table_events`
- `guest_visits`
- `guest_events`
- `pending_replies`
- `processed_tg_updates`
- исторические сообщения/архивные брони

## New DB contents

После cutover новая БД должна содержать:

- legacy-таблицы, если они еще нужны текущему runtime;
- новое CRM-ядро;
- новый integration-слой;
- новый слой `contacts/users`;
- пустые доменные таблицы броней.

## Commands

### 1. Backup old DB

```bash
cp /data/luchbar.db /data/luchbar.backup.$(date +%Y%m%d_%H%M%S).db
```

### 2. Build new DB with contacts/users only

```bash
python3 migrate_clean_prod.py \
  --source-db /data/luchbar.db \
  --target-db /data/luchbar_prod_clean.db \
  --apply
```

### 3. Switch app to new DB

Обновить `DB_PATH` на:

```bash
/data/luchbar_prod_clean.db
```

Рекомендуемые env на первом этапе:

```bash
CORE_ONLY_MODE=1
GUEST_COMM_ENABLED=0
```

Важно:

- `CORE_ONLY_MODE=1` не должен отключать синхронизацию с действующим `LUCH_crm`;
- на переходном этапе он нужен только для отключения лишних guest/staff сценариев, но не для ломания CRM ingest/pull контракта.

## Verification

Проверить, что в новой БД появились:

- `contacts`
- `contact_channels`
- `bot_peers`
- `bot_inbound_events`
- `bot_outbox`
- `bot_message_links`
- `reservations`
- `tables_core`

И что при этом:

- `bookings` пустая или содержит только новые брони после cutover;
- новые входящие брони создаются успешно;
- staff notifications продолжают работать через current runtime;
- bot actions продолжают доходить до приложения.

## Rollback

Если что-то идет не так:

1. вернуть старый `DB_PATH`;
2. перезапустить приложение;
3. продолжить разбор на копии новой БД, не трогая старую.
