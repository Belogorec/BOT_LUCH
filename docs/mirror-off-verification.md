# Mirror-Off Verification

## Goal

Подготовить `BOT_LUCH` к режиму `LEGACY_MIRROR_ENABLED=0` и заранее увидеть, какие сценарии еще зависят от legacy mirror (`bookings`, `venue_tables`, `booking_events`, `table_events`).

## Run

Используется тот же `DB_PATH`, что и у приложения.

```bash
cd repos/BOT_LUCH
python3 verify_mirror_off.py
python3 verify_mirror_off.py --strict
python3 verify_mirror_off.py --db-path /path/to/prod-copy.db --strict
```

Если runtime `DB_PATH` в локальном env указывает на недоступный путь вроде `/data/...`, для pre-deploy dry run лучше использовать снятую копию рабочей БД через `--db-path`.

## What The Script Checks

- `legacy_bookings_without_reservation`
- `legacy_reservations_without_booking`
- `legacy_reservations_without_events`
- `legacy_booking_assignments_missing_core`
- `core_assignments_missing_legacy`
- `legacy_restrictions_missing_core`
- `core_restrictions_missing_legacy`
- `legacy_booking_tokens_without_core_reservation`

Интерпретация:

- `0` означает, что конкретный mirror gap не найден.
- Любое ненулевое значение означает, что mirror-off пока рискован для соответствующего сценария.

## Remaining Legacy Read Paths

На текущем этапе `LEGACY_MIRROR_ENABLED=0` можно использовать только после проверки DB consistency и понимания, что эти reader-ветки еще не переведены полностью:

- [channel_binding_service.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/channel_binding_service.py>) уже canonical-first для `public_reservation_tokens`, `contacts`, `contact_channels`, но все еще держит legacy fallback на `bookings.reservation_token` до проверки покрытия canonical token mapping на реальной БД.

Уже переведены на canonical helper/read-model и не являются основными blocker'ами для текущего mirror-off этапа:

- [booking_render.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/booking_render.py>)
- [crm_sync.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/crm_sync.py>)
- [booking_dialog.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/booking_dialog.py>)
- [dashboard_api.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/dashboard_api.py>)
- [flask_app.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/flask_app.py>) CRM-side read/check ветки
- [waiter_notify.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/waiter_notify.py>)
- guest prefs / channel resolution в [notification_dispatcher.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/notification_dispatcher.py>)
- значимая часть hot-path веток в [tg_handlers.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/tg_handlers.py>) и [vk_staff_flow.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/vk_staff_flow.py>)

## Recommended Dry Run

1. Прогнать `python3 verify_mirror_off.py --strict` на копии рабочей БД.
2. Если проверки чистые, поднять приложение на копии БД с `LEGACY_MIRROR_ENABLED=0`.
3. Проверить вручную:
   - создание брони из Tilda;
   - создание брони из Telegram Mini App;
   - confirm/cancel/reschedule/update guests;
   - assign/clear/restrict table;
   - waiter notify;
   - admin card refresh;
   - CRM sync ingest/pull.
4. Если любой сценарий читает устаревший legacy state, вернуть `LEGACY_MIRROR_ENABLED=1` и перевести соответствующий reader на canonical tables.
