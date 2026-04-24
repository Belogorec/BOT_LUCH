# Mirror-Off Verification

## Goal

Подготовить `BOT_LUCH` к режиму `LEGACY_MIRROR_ENABLED=0` и заранее увидеть, какие сценарии еще зависят от legacy mirror (`bookings`, `venue_tables`, `booking_events`, `table_events`).

## Run

Используется тот же `DB_PATH`, что и у приложения.

```bash
cd repos/BOT_LUCH
python3 verify_mirror_off.py
python3 verify_mirror_off.py --strict
python3 verify_mirror_off.py --json
python3 verify_mirror_off.py --db-path /path/to/prod-copy.db --strict
python3 verify_mirror_off.py --db-path /path/to/prod-copy.db --json
python3 backfill_reservation_events.py --db-path /path/to/prod-copy.db --dry-run
python3 backfill_reservation_events.py --db-path /path/to/prod-copy.db
```

Если runtime `DB_PATH` в локальном env указывает на недоступный путь вроде `/data/...`, для pre-deploy dry run лучше использовать снятую копию рабочей БД через `--db-path`.

На текущем production-copy был обнаружен исторический хвост: 5 legacy-sourced `reservations` без `reservation_events`. После прогона `backfill_reservation_events.py` на копии БД `verify_mirror_off.py --db-path ... --strict` проходит полностью зелёным.

## Report Shape

Скрипт теперь строит cutover-grade отчет с тремя bucket'ами:

- `critical`
- `warning`
- `ignored_historical`

`--strict` падает, если найден хотя бы один `critical` или `warning`.
`ignored_historical` остается в отчете, но не блокирует strict-pass, если это только заранее принятый исторический хвост.

Новые брони, созданные уже в core-only/mirror-off режиме, не обязаны иметь строку в `bookings`. Для таких записей BOT использует self-owned mapping: `reservations.source='legacy_booking'` и `reservations.external_ref = reservations.id`. Verifier не считает их `warning`, потому что это нормальная post-cutover форма данных.

## What The Script Checks

`critical`:

- `legacy_bookings_without_canonical_reservation`
- `active_legacy_booking_token_without_canonical_token`
- `legacy_booking_assignments_missing_core`
- `core_assignments_missing_legacy`
- `legacy_restrictions_missing_core`
- `core_restrictions_missing_legacy`
- `pending_replies_still_used`
- `legacy_guest_channel_rows_without_canonical_mapping`
- `guest_binding_tokens_still_used`
- `legacy_vk_staff_peers_without_canonical_mapping`

`warning`:

- `canonical_reservations_without_legacy_booking`
- `legacy_reservations_without_events`
- `historical_legacy_booking_token_without_canonical_token`
- `legacy_guest_bindings_still_present`
- `legacy_vk_staff_peers_still_present`

`ignored_historical`:

- `historical_pending_replies`
- `historical_guest_binding_tokens`
- `historical_guest_channel_bindings`

Интерпретация:

- `critical = 0` обязательно для cutover readiness.
- `warning` должны быть пустыми или заранее приняты как non-runtime historical tail.
- `ignored_historical` допустимы только если это действительно архивный след, не участвующий в runtime.
- `canonical_reservations_without_legacy_booking` относится только к старым mirrored reservations, где `external_ref` указывает на другой legacy booking id, но такой строки в `bookings` уже нет. Self-owned core-only rows (`external_ref = id`) в этот warning не попадают.

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

1. Прогнать `python3 verify_mirror_off.py --db-path /path/to/prod-copy.db --json` и сохранить отчет.
2. Прогнать `python3 verify_mirror_off.py --db-path /path/to/prod-copy.db --strict`.
3. Если strict зеленый, поднять приложение на копии БД с `CORE_ONLY_MODE=1` и `LEGACY_MIRROR_ENABLED=0`.
4. Проверить вручную:
   - создание брони из Tilda;
   - создание брони из Telegram Mini App;
   - confirm/cancel/reschedule/update guests;
   - assign/clear/restrict table;
   - waiter notify;
   - admin card refresh;
   - CRM sync ingest/pull;
   - staff flow без `pending_replies`;
   - guest binding / token resolution без fallback на legacy.
5. Повторить `python3 verify_mirror_off.py --db-path /path/to/prod-copy.db --strict` после smoke-сценариев. Новые self-owned core-only reservations не должны создавать warning.
6. Если любой сценарий читает устаревший legacy state, вернуть `LEGACY_MIRROR_ENABLED=1` и перевести соответствующий reader на canonical tables.
