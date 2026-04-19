# Legacy Write Map (Phase 6 Kickoff)

## Scope

This map lists active write paths to legacy tables in `BOT_LUCH` and the canonical replacement target.

Priority tags:

- `P0`: must move first (domain state / critical runtime writes)
- `P1`: transport/session writes to migrate after `P0`
- `P2`: compatibility/migration writes that can be retired last

## Legacy Domain Writes (`P0`)

### `bookings`

Active writes found in:

- `application/miniapp_booking.py`
- `application/tilda_booking.py`
- `flask_app.py`
- `tg_handlers.py`
- `vk_staff_flow.py`
- `booking_service.py`
- `channel_binding_service.py`
- `hostess_card_delivery.py` (telegram message metadata fields in legacy row)

Canonical target:

- `reservations` for booking state (`status`, `reservation_at`, `party_size`, `comment`, deposit)
- `reservation_events` for status/action history
- `public_reservation_tokens` for guest token lifecycle
- `bot_message_links` for telegram/vk message linkage (instead of `telegram_chat_id` / `telegram_message_id` in `bookings`)

Progress:

- `done`: runtime write path for `bookings.telegram_chat_id / telegram_message_id` removed; hostess card send/edit and refresh now rely on `bot_message_links`.
- `done`: Telegram miniapp webhook handler no longer writes `INSERT INTO bookings` directly; it delegates to `application.execute_telegram_miniapp_booking`.
- `done`: direct status mutation SQL removed from inbound handler modules (`flask_app.py`, `tg_handlers.py`, `vk_staff_flow.py`) and centralized in `booking_service.set_booking_status(...)`.
- `done`: CRM sync `reschedule` / `update_guests` no longer mutate `bookings` directly in route; writes moved to `booking_service` command functions.
- `done`: CRM manual booking create path moved from `flask_app` SQL block to `booking_service.create_manual_booking(...)`.
- `done`: Tilda and Telegram Mini App application use-cases no longer execute direct `bookings` SQL; create/update writes were moved to `booking_service` command functions.
- `done`: lifecycle booking commands (`status/cancel/reschedule/guests/deposit`) now perform canonical-first writes to `reservations` + `reservation_events`, then mirror to legacy `bookings`/`booking_events`.
- `done`: table commands (`assign/clear/restrict`) now perform canonical-first writes to `reservation_tables` / `table_blocks`, then mirror to legacy `bookings` / `venue_tables` / `table_events`.
- `done`: event writes are centralized in `booking_service.log_booking_event` / `log_table_event`; `reservation_events` is now the primary write target and legacy event tables are compatibility mirror only.
- `done`: dashboard analytics created-volume reader switched from `booking_events` to `reservation_events`.
- `done`: explicit `LEGACY_MIRROR_ENABLED` switch added; event mirrors and canonical-first lifecycle/table legacy mirrors in `booking_service` can now be disabled without changing call sites.
- `done`: mirror-off verification helper added: [verify_mirror_off.py](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/verify_mirror_off.py>) with runbook in [mirror-off-verification.md](</Users/maks/Documents/LUCHBAR BOT/repos/BOT_LUCH/docs/mirror-off-verification.md>).
- `done`: canonical read helpers added in `booking_service`; `booking_render`, `crm_sync`, VK staff booking render/state checks, and several Telegram admin callback branches no longer read `bookings` / `venue_tables` directly.
- `done`: Telegram Mini App duplicate check no longer performs direct `bookings` lookup before dispatch; `channel_binding_service` partially switched to canonical read helper for reservation payload/name/phone.
- `done`: public guest-access tokens are now synced into `public_reservation_tokens` during Tilda / Mini App booking create-update paths; `channel_binding_service` resolves reservation token canonical-first and only falls back to legacy `bookings.reservation_token` when canonical mapping is missing.
- `done`: guest communication preferences moved to canonical `contacts`, and guest binding now upserts canonical `contact_channels`; `notification_dispatcher` resolves preferred/service-enabled state from contacts instead of legacy booking flags.
- `done`: `booking_dialog` no longer reads returning-user history from `bookings`; bootstrap now resolves user phone/name via `tg_bot_users`, `contact_channels` / `contacts`, and canonical `reservations`.
- `done`: `dashboard_api` reserved metrics now read canonical `reservations`, and `waiter_notify` no longer queries `bookings` directly, using core-first read-model paths instead.
- `done`: `flask_app` CRM sync endpoints no longer read `bookings` directly for existence/assigned-table checks, and recent CRM pull is now sourced from canonical `reservations`.
- `remaining`: legacy mirror writes to `bookings` / `venue_tables` / legacy event tables are still executed for compatibility.
- `remaining`: full mirror-off is still blocked by legacy-only booking fields and readers, primarily `reservation_token`, guest communication preferences, dialog history bootstrap, and several CRM/dashboard/fallback read branches.

Action:

- Keep `bookings` as compatibility bridge only for the remaining legacy-only fields/readers, then retire it from runtime paths in this order:
  1. remove remaining token fallback after `public_reservation_tokens` coverage verification
  2. then disable legacy mirrors in production dry run

### `venue_tables`

Active writes found in:

- `booking_service.py` (table label/restriction updates and upserts)

Canonical target:

- `tables_core` (table identity/metadata)
- `reservation_tables` (active table assignment)
- `table_blocks` (restrictions)

Action:

- Move table restriction/label mutations to canonical table services; keep legacy sync only as transitional mirror.

### `booking_events` / `table_events`

Active writes found in:

- `booking_service.py`
- `channel_binding_service.py`

Canonical target:

- `reservation_events`

Action:

- Replace legacy event writes with canonical reservation events.

## Legacy Transport / Session Writes (`P1`)

### `pending_replies`

Active writes found in:

- `tg_handlers.py`
- `vk_staff_flow.py`
- `booking_dialog.py`

Canonical target:

- `bot_inbound_events` (incoming stateful steps)
- `bot_outbox` (next-step prompts/messages)

Action:

- Replace stateful prompt flow persistence from `pending_replies` to integration event/state model.

### `vk_staff_peers`

Active writes found in:

- `vk_staff_notify.py`
- `db.py` (normalization/migration helpers)

Canonical target:

- `bot_peers`

Action:

- Replace peer upsert/read paths with `bot_peers`; keep fallback reader only during transition window.

### `guest_channel_bindings` / `guest_binding_tokens`

Active writes found in:

- `channel_binding_service.py`

Canonical target:

- `contact_channels`
- `public_reservation_tokens`

Action:

- Migrate guest binding/token lifecycle to canonical contacts + public tokens; retire guest legacy binding tables.

## Compatibility / Migration Writes (`P2`)

### `processed_tg_updates`, `tg_outbox`, legacy migration helpers

Status:

- legacy infrastructure; not part of canonical runtime target.

Action:

- Freeze for compatibility only; remove after parity and monitoring sign-off.

## Suggested Execution Order

1. Remove the remaining legacy token fallback after verifying canonical token coverage on a production-like DB copy
2. Run mirror-off dry run and verify `LEGACY_MIRROR_ENABLED=0` on a production-like DB copy
3. Remove the last token fallback from `channel_binding_service` if DB verification confirms canonical token coverage
4. Migrate `pending_replies` flow
5. Migrate `vk_staff_peers` and guest binding/token storage

## Exit Criteria For Phase 6

- No runtime `INSERT/UPDATE/DELETE` to `bookings`, `venue_tables`, `booking_events`, `table_events`
- No runtime dependency on `bookings.reservation_token`, `preferred_channel`, `service_notifications_enabled`, `marketing_notifications_enabled`
- No runtime writes to `pending_replies`, `vk_staff_peers`, `guest_channel_bindings`, `guest_binding_tokens`
- Legacy writes remain only in explicit migration/compat scripts
