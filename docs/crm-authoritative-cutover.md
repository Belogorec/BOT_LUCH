# CRM Authoritative Cutover

## Decision

CRM becomes the source of truth for reservations, tables, table assignments, deposits, restrictions, and domain events.

`BOT_LUCH` stops being a domain owner. It remains only a transport/integration service for staff channels:

- Telegram staff groups;
- VK staff groups;
- webhook transport/parsing while the incoming channel still terminates at the bot service;
- delivery state: peers, inbound events, outbox, and message links.

Client-facing Telegram chatbot behavior is deprecated and must be removed from the runtime path. The Telegram bot is not a customer cabinet, not a booking-history bot, and not a client dialog engine anymore.

## Target Ownership

### CRM Owns

- `reservations`
- `tables_core`
- `reservation_tables`
- `table_blocks`
- `table_sessions_core`
- `reservation_events`
- conflict checks
- idempotent domain commands
- CRM UI state

Only CRM code should decide whether a booking can be created, updated, assigned to a table, restricted, completed, cancelled, or marked with a deposit.

### Bot/Integration Owns

- `bot_peers`
- `bot_message_links`
- `bot_inbound_events`
- `bot_outbox`
- staff Telegram/VK message rendering and delivery
- staff Telegram/VK callbacks and pending prompts
- relay-facing webhook adapter code during migration

The bot may store channel state and delivery metadata, but it must not be the authority for reservation state.

### Legacy Becomes Archive

These tables must leave the primary runtime path:

- `bookings`
- `venue_tables`
- `booking_events`
- `table_events`
- `pending_replies`
- `tg_outbox`
- `processed_tg_updates`
- `guest_channel_bindings`
- `guest_binding_tokens`
- client-facing fields in `tg_bot_users`

During migration they may remain as compatibility/read-only data, but no new domain behavior should depend on them.

## Product Scope Change: Telegram Bot

The Telegram bot is reduced to staff operations only.

Keep:

- staff group booking cards;
- staff group action buttons;
- staff prompts for assigning table/deposit/restriction when needed;
- waiter notifications;
- hostess notifications;
- peer registration for staff groups;
- message links for updating staff cards.

Remove or archive:

- client chat dialog/history;
- `/start` client booking flow;
- returning-client history bootstrap;
- client phone-sharing flow as a domain dependency;
- customer cabinet behavior in Telegram;
- guest binding tokens for Telegram client chat;
- client marketing/service notification preferences inside the bot;
- any “guest communication” workflow where BOT is the customer-facing product.

If a client-facing cabinet is needed later, it should be a CRM-owned/public product with a dedicated API, not hidden inside the staff bot.

## Current Split-Brain To Eliminate

Today CRM can write core state and then ask BOT to repeat the action via `BOT_SYNC_API_URL`. BOT can reject the same action because it still has its own command handlers and compatibility state.

This creates two decision makers:

- CRM core path;
- BOT booking service path.

The target state has one decision maker:

- CRM command API accepts or rejects the command;
- BOT only renders and delivers the result.

## Target Flow

### Incoming Booking From Tilda

1. Relay or BOT receives webhook.
2. Adapter parses and normalizes payload.
3. Adapter calls CRM command API to create or update a reservation.
4. CRM writes `reservations` and `reservation_events`.
5. CRM emits staff notification tasks.
6. BOT sends cards/messages to staff groups.

### Incoming Booking From Telegram Mini App

Preferred long-term direction: move client booking creation out of the Telegram bot runtime.

Transition option:

1. BOT validates Telegram transport/auth data.
2. BOT calls CRM command API to create the reservation.
3. BOT stores only transport metadata needed for staff notifications.
4. CRM remains authoritative.

Exit criterion: the Telegram bot no longer owns client booking state or client history.

### Staff Action From Telegram/VK

1. Staff callback or reply arrives in BOT.
2. BOT stores `bot_inbound_events`.
3. BOT calls CRM command API.
4. CRM validates, checks conflicts, writes state, and returns accepted/rejected.
5. BOT replies to staff and updates staff cards.

BOT must not call local domain functions like `assign_table_to_booking`, `set_booking_status`, `set_booking_deposit`, or `set_table_label` in CRM-authoritative mode.

### CRM Manual Action

1. CRM UI calls CRM service directly.
2. CRM writes canonical core state.
3. CRM enqueues or emits notification events.
4. BOT delivers notifications.

Failed notification delivery must be visible, but it must not make the domain command ambiguous.

## Required CRM Command API

Add explicit command endpoints, separate from passive ingest:

- `POST /api/commands/reservations`
- `POST /api/commands/reservations/<reservation_id>/status`
- `POST /api/commands/reservations/<reservation_id>/reschedule`
- `POST /api/commands/reservations/<reservation_id>/guests`
- `POST /api/commands/reservations/<reservation_id>/assign-table`
- `POST /api/commands/reservations/<reservation_id>/clear-table`
- `POST /api/commands/reservations/<reservation_id>/restriction`
- `DELETE /api/commands/reservations/<reservation_id>/restriction`
- `POST /api/commands/reservations/<reservation_id>/deposit`
- `DELETE /api/commands/reservations/<reservation_id>/deposit`
- `POST /api/commands/tables/<table_number>/restriction`
- `DELETE /api/commands/tables/<table_number>/restriction`
- `POST /api/commands/tables/<table_number>/session`
- `DELETE /api/commands/tables/<table_number>/session`

Every command must support:

- shared auth key;
- `event_id` / idempotency key;
- actor id/name;
- canonical `reservation_id`;
- compatibility `booking_id` only as an alias during transition;
- structured error codes such as `table_time_conflict`, `booking_not_found`, `state_conflict`.

## Required Bot Changes

### New CRM Client

Add a small CRM command client in `BOT_LUCH`, for example:

- `crm_commands.py`

Responsibilities:

- call CRM command endpoints;
- include auth headers;
- pass idempotency keys;
- normalize accepted/rejected responses;
- never print secrets.

### Staff Handlers

Switch staff action paths to CRM commands:

- `tg_handlers.py`
- `vk_staff_flow.py`
- `telegram_pending_prompt.py`

In `CRM_AUTHORITATIVE=1` mode these paths call CRM and do not mutate `reservations`, `reservation_tables`, `table_blocks`, or legacy tables locally.

### Client Chat Removal

Remove or quarantine runtime paths:

- `booking_dialog.py`
- `pending_reply_service.py`
- client `/start` branches in `tg_handlers.py`
- guest binding and customer notification paths in `channel_binding_service.py` and `notification_dispatcher.py`
- `GUEST_COMM_ENABLED` behavior
- Telegram client booking-history flows

If some data must be kept for audit, keep it read-only and do not expose it as live product behavior.

### Legacy Domain Calls To Stop Using

In CRM-authoritative mode, BOT staff flows must not call:

- `assign_table_to_booking`
- `clear_table_assignment`
- `set_booking_status`
- `mark_booking_cancelled`
- `set_booking_deposit`
- `clear_booking_deposit`
- `set_table_label`

Those functions can remain temporarily for rollback and migration scripts, but not for the primary runtime path.

## Required CRM Changes

### Command Boundary

Move all domain decisions behind explicit command services. Existing core functions are a good base:

- `update_booking_action_core`
- `update_table_action_core`
- `create_standalone_manual_booking`

The command API should call these or their extracted service equivalents.

### Notification Outbox

CRM should emit notification jobs after successful domain changes:

- reservation created;
- status changed;
- table assigned/cleared;
- deposit set/cleared;
- table restricted/cleared;
- manual table session opened/closed.

BOT consumes/delivers these jobs. A delivery failure is a notification problem, not a domain-state conflict.

### Remove BOT As Domain Sync Peer

Deprecate current CRM-to-BOT sync as a domain write mechanism:

- `_sync_booking_to_bot`
- `_sync_table_to_bot`
- `_create_manual_booking_in_bot`
- `BOT_SYNC_API_URL`

During transition, these may remain behind a flag, but the final state does not ask BOT to re-approve CRM decisions.

## Cutover Phases

### Phase 0. Full Project Re-Analysis And Plan Validation

No implementation starts before a fresh full-project review confirms this plan still matches the codebase and production shape.

- Re-scan all three services: `LUCH_crm`, `BOT_LUCH`, and `luchbarbot-relay`.
- Re-map all routes, background workers, webhook flows, outbox flows, and staff action handlers.
- Re-map all writes and critical reads for `reservations`, `reservation_tables`, `table_blocks`, `table_sessions_core`, legacy booking/table tables, and bot integration tables.
- Re-check Telegram/VK/Tilda/relay/CRM sync links and env variables.
- Re-check tests and docs for stale assumptions about BOT as domain owner.
- Update this document before coding if any mismatch is found.
- Produce a short implementation checklist with exact files and rollback flags.

Exit criterion: the plan has been validated against the current tree and no critical flow is undocumented.

#### Phase 0 Validation Snapshot, 2026-04-27

The current tree still matches the architectural problem described above. The split-brain is active and concrete:

- CRM writes canonical/core state through `LUCH_crm/service.py`, then calls BOT sync functions from `LUCH_crm/flask_app.py`.
- BOT receives those sync calls in `BOT_LUCH/flask_app.py` and mutates its own local booking/table state.
- BOT staff callbacks in `BOT_LUCH/tg_handlers.py` and `BOT_LUCH/vk_staff_flow.py` still call local domain functions directly.
- Incoming Tilda and Telegram Mini App booking creation still starts in BOT local storage before syncing to CRM.
- Relay only forwards to BOT and has no CRM-aware command destination.

#### Current Route And Worker Map

##### BOT_LUCH

Runtime routes:

- `POST /webhook/tilda` -> `tilda_api.tilda_webhook_impl()` -> `application/tilda_booking.py`
- `POST /tg/webhook` -> `tg_handlers.tg_webhook_impl()`
- `POST /vk/callback` -> `vk_staff_flow.py` handlers from `flask_app.py`
- `POST /api/booking` -> Telegram Mini App booking API
- `GET /public/api/guest` -> public guest reservation page data
- `POST /admin/api/crm-sync/booking/<booking_id>` -> CRM-to-BOT domain sync
- `POST /admin/api/crm-sync/manual-booking` -> CRM asks BOT to create a legacy booking
- `POST /admin/api/crm-sync/table` -> CRM-to-BOT table sync
- dashboard helper routes: `/admin/api/segments`, `/admin/api/load`

Workers:

- `crm_worker: python crm_outbox_worker.py` from `Procfile`
- optional embedded CRM outbox worker from `embedded_crm_outbox_worker.py`, gated by `CRM_OUTBOX_EMBEDDED_WORKER`

Important current env flags:

- Existing: `CORE_ONLY_MODE`, `LEGACY_MIRROR_ENABLED`, `GUEST_COMM_ENABLED`
- Missing for this cutover: `CRM_AUTHORITATIVE`

##### LUCH_crm

Runtime routes:

- CRM UI: `/crm/dashboard`, `/crm/bookings`, `/crm/today`, `/crm/tables`, `/crm/restrictions`, `/crm/archive`, `/crm/guests`
- CRM manual writes: `POST /crm/bookings/new`, `POST /crm/bookings/<booking_id>/action`, `POST /crm/tables/action`
- Passive BOT ingest: `POST /api/events`
- Health/pulse: `/health`, `/api/crm/pulse`

Workers:

- `restriction-alerts: python restriction_alert_worker.py --loop`

Important current env flags:

- Existing: `CRM_USE_CORE_DOMAIN`
- Existing BOT sync dependency: `BOT_SYNC_API_URL`, `BOT_SYNC_SHARED_SECRET`
- Missing for this cutover: explicit command API auth/config separate from passive ingest.

##### luchbarbot-relay

Runtime routes:

- `POST /webhook/tilda` stores payload in `relay_queue` and forwards to `MAIN_BOT_URL/webhook/tilda`
- `POST /tg/webhook` tries synchronous forwarding to `MAIN_BOT_URL/tg/webhook`, then queues fallback
- `/relay/flush`, `/relay/status`, `/relay/retry-dead` are protected maintenance endpoints

Worker:

- background queue flush thread started in `relay_app.py`
- `Procfile` intentionally keeps `--workers 1 --threads 4`

Cutover implication: relay can remain BOT-facing during early phases, but Phase 5 needs either a CRM-facing Tilda command adapter or a BOT adapter that immediately calls CRM commands without local domain writes.

#### Current Domain Write Map

##### BOT_LUCH Domain Writes To Remove From Primary Runtime

`BOT_LUCH/flask_app.py`

- CRM sync routes call `set_booking_status`, `assign_table_to_booking`, `set_booking_deposit`, `set_table_label`, and manual booking creation.
- These endpoints are the current mechanism that lets CRM ask BOT to re-approve or repeat a domain change.

`BOT_LUCH/tg_handlers.py`

- Staff booking callbacks call `set_booking_status` and `mark_booking_cancelled`.
- Staff table callbacks/text prompts call `assign_table_to_booking`, `clear_table_assignment`, and `set_table_label`.
- Deposit prompts call `set_booking_deposit`.
- Client-facing `/start`, contact sharing, Mini App submission, guest binding, and service notification paths are still live.

`BOT_LUCH/vk_staff_flow.py`

- VK staff callbacks call `set_booking_status`, `mark_booking_cancelled`, `clear_table_assignment`, and `clear_booking_deposit`.
- VK staff pending text calls `assign_table_to_booking`, `set_booking_deposit`, and `set_table_label`.

`BOT_LUCH/application/tilda_booking.py`

- Creates or updates local BOT booking via `upsert_tilda_booking_record`.
- Mirrors to core via `sync_booking_to_core`.
- Emits CRM sync via `send_booking_event`.
- Renders/delivers staff cards from BOT.

`BOT_LUCH/application/miniapp_booking.py`

- Creates local Telegram Mini App booking via `create_telegram_miniapp_booking_record`.
- Mirrors to core via `sync_booking_to_core`.
- Emits CRM sync via `send_booking_event`.
- Renders/delivers staff cards from BOT.

`BOT_LUCH/booking_service.py`

- Still contains the rollback-compatible local domain functions:
  - `assign_table_to_booking`
  - `clear_table_assignment`
  - `set_booking_status`
  - `mark_booking_cancelled`
  - `set_booking_deposit`
  - `clear_booking_deposit`
  - `set_table_label`
- These functions write canonical tables and, depending on flags, legacy tables too.

##### BOT_LUCH Integration Writes To Keep

`BOT_LUCH/integration_service.py`

- `bot_peers`
- `bot_message_links`
- `bot_inbound_events`
- `bot_outbox`

`BOT_LUCH/telegram_pending_prompt.py` and `BOT_LUCH/vk_staff_flow.py`

- Prompt state is now stored in `bot_inbound_events`, which fits the target integration ownership.
- Legacy `pending_replies` still exists for old flows and cleanup, but should not be the long-term prompt store.

##### LUCH_crm Domain Writes To Keep And Expose Through Commands

`LUCH_crm/service.py`

- `update_booking_action_core`
- `update_table_action_core`
- `create_standalone_manual_booking`
- reservation/table conflict checks
- `reservation_events`

`LUCH_crm/crm/application/manual_actions.py`

- Current orchestration wraps CRM domain writes and then calls BOT sync functions.
- For booking actions, BOT sync rejection can roll back the CRM transaction.
- For manual booking/manual table session, CRM can still depend on BOT returning a legacy `booking_id` to rewrite `reservations.source/external_ref`.

`LUCH_crm/repositories/reservations_repo.py`

- Contains smaller repository methods for core reservation/status/assignment/deposit writes, but the active UI path still primarily goes through `service.py`.

##### LUCH_crm Notification/Client Writes To Revisit

`LUCH_crm/flask_app.py`

- `crm_booking_action()` can still send Telegram client notifications after status actions.
- This conflicts with the target product decision unless replaced by a CRM-owned public/client product deliberately.

`LUCH_crm/waiter_notify.py` and `LUCH_crm/hostess_notify.py`

- CRM can already send some waiter/hostess notifications directly.
- Phase 6 should decide which notification jobs CRM emits and which delivery jobs BOT owns.

#### Current Test And Documentation Gaps

Existing useful tests:

- BOT mirror-off and cleanup: `tests/test_mirror_off_smoke.py`, `tests/test_verify_mirror_off.py`, `tests/test_backfill_mirror_off_prereqs.py`
- BOT staff prompt state: `tests/test_telegram_pending_prompt.py`, `tests/test_vk_staff_flow.py`
- CRM core domain actions: `LUCH_crm/tests/test_core_cutover_phase0.py`, `tests/test_crm_core_smoke.py`, `tests/test_reconcile_legacy_core.py`
- CRM/BOT sync security: `BOT_LUCH/tests/test_security_boundaries.py`

Gaps to fill before enabling `CRM_AUTHORITATIVE=1`:

- CRM command endpoint tests for accepted/rejected/idempotent responses.
- BOT CRM command client tests with CRM rejection and network failure.
- Telegram staff callback tests proving no local domain mutation under `CRM_AUTHORITATIVE=1`.
- VK staff callback/prompt tests proving no local domain mutation under `CRM_AUTHORITATIVE=1`.
- Tilda duplicate delivery test through the new CRM create/update command.
- Explicit tests that client `/start`, guest binding, and service notification paths are unreachable or quarantined when the client Telegram product is disabled.

#### Phase 0 Implementation Checklist

##### 1. Add Runtime Guards And Config

BOT files:

- `config.py`: add `CRM_AUTHORITATIVE`, command API URL/key/timeout config, and security validation.
- `.env.example`: document `CRM_AUTHORITATIVE=0`, command API URL/key, and timeout.
- `flask_app.py`: when `CRM_AUTHORITATIVE=1`, reject or no-op old `/admin/api/crm-sync/*` domain write endpoints with a clear compatibility response.

CRM files:

- `config.py`: add command API auth key, separate from passive `CRM_INGEST_API_KEY`.
- `.env.example`: document command API key.

Rollback flag:

- Disable `CRM_AUTHORITATIVE` to restore current BOT-local staff behavior during the observation window.

##### 2. Add CRM Command Boundary

CRM files:

- Add command routes in `flask_app.py` or a small imported `command_routes.py`.
- Add command service functions around `update_booking_action_core`, `update_table_action_core`, and `create_standalone_manual_booking`.
- Add idempotency using `event_id` before domain mutation. Prefer a command/inbound event table if existing `crm_inbound_events` is not suitable.
- Return structured responses:
  - `{"ok": true, "reservation_id": ..., "state": ...}`
  - `{"ok": false, "error": "table_time_conflict", "message": ...}`

Tests:

- Add CRM tests for status, assign/clear table, deposit set/clear, restriction set/clear, manual session open/close, duplicate `event_id`, and conflict rejection.

##### 3. Add BOT CRM Command Client

BOT files:

- New `crm_commands.py`.
- Normalize command responses into accepted/rejected/transport-error outcomes.
- Include auth headers and idempotency key.
- Never log command auth values.

Tests:

- Add unit tests for success, structured rejection, 403/auth failure, timeout/network failure, and duplicate accepted response.

##### 4. Switch Staff Actions Under `CRM_AUTHORITATIVE`

BOT files:

- `tg_handlers.py`: route confirm/cancel/assign/clear/deposit/restriction staff actions to `crm_commands.py` when `CRM_AUTHORITATIVE=1`.
- `vk_staff_flow.py`: same for callback and pending text flows.
- `telegram_pending_prompt.py`: keep prompt storage in `bot_inbound_events`; pass prompt completion data to CRM command calls.
- `booking_render.py` / `hostess_card_delivery.py`: keep rendering/delivery, but refresh cards from CRM command results or canonical read model, not from a BOT-local mutation.

Tests:

- Add/extend Telegram and VK staff tests so CRM rejection leaves local domain tables unchanged and sends a staff-facing rejection message.

##### 5. Reverse Incoming Booking Creation

BOT files:

- `application/tilda_booking.py`: in `CRM_AUTHORITATIVE=1`, parse payload and call CRM create/update command before any local booking write.
- `application/miniapp_booking.py`: in `CRM_AUTHORITATIVE=1`, validate Telegram transport data and call CRM create command.
- Keep only transport metadata, message links, inbound events, and outbox jobs in BOT.

CRM files:

- Implement create/update reservation command with Tilda idempotency based on `tranid`/normalized payload id.
- Implement Telegram Mini App create command if that path remains in BOT during transition.

Tests:

- Duplicate Tilda delivery must not duplicate reservations.
- Telegram Mini App duplicate `reservation_token` must not duplicate reservations.

##### 6. Stop CRM-To-BOT Domain Sync

CRM files:

- `crm/application/manual_actions.py`: remove rollback-on-BOT-sync from the authoritative path.
- `flask_app.py`: deprecate `_sync_booking_to_bot`, `_sync_table_to_bot`, `_create_manual_booking_in_bot`, and `_pull_recent_bookings_from_bot` behind a compatibility flag.
- Manual booking/manual session should not require BOT to return a legacy `booking_id`.

BOT files:

- Keep old `/admin/api/crm-sync/*` endpoints only for rollback while `CRM_AUTHORITATIVE=0`.

##### 7. Quarantine Client Telegram Product

BOT files:

- `tg_handlers.py`: disable client `/start` booking/contact flow when the cutover flag is enabled.
- `booking_dialog.py`, `pending_reply_service.py`, `channel_binding_service.py`, `notification_dispatcher.py`: keep audit/cleanup helpers, but remove live product reachability.
- Keep `/myid`, `/chatid`, staff utilities, lineup commands, and staff group operations as needed.

CRM files:

- Remove or explicitly gate client Telegram notification from `crm_booking_action()`.

##### 8. Verification Before First Staging Enablement

Commands to run on a staging/prod-copy DB before enabling `CRM_AUTHORITATIVE=1`:

- BOT: `python verify_mirror_off.py --strict --output reports/bot-mirror-off-pre-authoritative.json`
- BOT: `python backfill_mirror_off_prereqs.py --dry-run`
- CRM: `python reconcile_legacy_core.py --strict --output reports/crm-reconcile-pre-authoritative.json`
- Unit tests for touched BOT and CRM modules.

Operational flags for first staging run:

- BOT: `CRM_AUTHORITATIVE=1`
- BOT: keep `LEGACY_MIRROR_ENABLED` unchanged until smoke tests pass.
- CRM: keep `CRM_USE_CORE_DOMAIN=1`
- Relay: keep single worker; do not route relay directly to CRM until the Tilda command adapter is deployed.

### Phase 1. Production DB Audit And Cleanup

Before code cutover, inspect the real database or a production copy and remove/archive obsolete data deliberately.

- Run schema inventory for core, integration, and legacy tables.
- Count active rows in deprecated tables: `bookings`, `venue_tables`, `booking_events`, `table_events`, `pending_replies`, `processed_tg_updates`, `tg_outbox`, `guest_channel_bindings`, `guest_binding_tokens`.
- Find live dependencies on client Telegram/chatbot artifacts.
- Reconcile active reservations, assignments, restrictions, sessions, deposits, and events between legacy and core.
- Verify public tokens and message links point to canonical `reservations.id`.
- Archive or delete only data that is confirmed obsolete; keep backups before destructive cleanup.
- Remove expired/superseded pending prompts and stale client binding tokens.
- Keep staff peer/message-link data needed for Telegram/VK groups.
- Run mirror-off and reconciliation checks after cleanup.

Exit criterion: the DB is clean enough that legacy/client-chat data cannot silently influence the cutover.

#### Phase 1 Audit Snapshot, 2026-04-27

Input used:

- local production-copy payload from `секреты личное/luchbar_prod_copy.b64`;
- decoded into a temporary SQLite DB under `/tmp`;
- `PRAGMA integrity_check` returned `ok`;
- only aggregate counts were inspected. No secret values or guest records were copied into this document.

Aggregate inventory:

- tables: 31
- legacy bookings: 7 total, 7 active
- core reservations: 7 total, 7 active
- core reservation source split: `legacy_booking` = 7
- legacy/core active mapping gaps:
  - active legacy bookings without core reservation: 0
  - active core legacy reservations without legacy booking: 0
  - active legacy table assignments without core assignment: 0
- legacy table state:
  - `venue_tables`: 1
  - restricted legacy tables: 1
  - `booking_events`: 14
  - `table_events`: 1
- client/chat legacy tails:
  - `pending_replies`: 0 total, 0 active
  - `guest_channel_bindings`: 0 total, 0 active
  - `guest_binding_tokens`: 0 total, 0 active
  - `processed_tg_updates`: 21
- core live table state:
  - active `reservation_tables`: 0
  - active `table_blocks`: 0
  - active `table_sessions_core`: 0
  - reservations with deposit: 0
  - `reservation_events`: 8
- bot integration state:
  - `bot_peers`: 0 total, 0 active
  - `bot_message_links`: 7
  - `bot_inbound_events`: 5 total, 0 pending
  - `bot_outbox`: 10 total, 7 not yet sent

Status breakdowns:

- legacy booking statuses: `CONFIRMED` = 4, `WAITING` = 3
- core reservation statuses: `confirmed` = 6, `pending` = 1
- `bot_outbox`: `sent` = 3, `new` = 7
- `bot_inbound_events`: `new` = 5

Automated verification:

- `verify_mirror_off.py` summary: critical = 1 row, warning = 0 rows.
- Critical code: `missing_required_canonical_tables`, missing table: `channel_binding_tokens`.
- `reconcile_legacy_core.py` could not complete against this DB copy because the DB schema lacks `table_sessions_core.closed_reason`.

Phase 1 blockers before code cutover:

- Run current schema migrations on production or the production copy before authoritative cutover:
  - BOT `run_integration_schema_migrations()` must create `channel_binding_tokens`.
  - CRM `run_core_schema_migrations()` must add `table_sessions_core.closed_reason` and other current core columns.
- Re-run:
  - `python verify_mirror_off.py --strict --db-path <prod-copy-db>`
  - `python reconcile_legacy_core.py --strict`
- Investigate the 7 `bot_outbox` rows in `new` state before treating notification delivery as healthy.
- Do not delete or archive legacy tables from this copy yet. The current data is small and mapped, but the schema is behind the code.

Temporary migrated-copy check:

- Current BOT/CRM schema migrations were applied only to a temporary copy.
- `PRAGMA integrity_check` still returned `ok`.
- Schema blockers were resolved:
  - `channel_binding_tokens` exists;
  - `table_sessions_core.closed_reason` exists.
- After schema catch-up, real data blockers appeared:
  - `active_legacy_booking_token_without_canonical_token`: 5 critical rows;
  - `legacy_vk_staff_peers_without_canonical_mapping`: 1 critical row;
  - `legacy_reservations_without_events`: 5 warning rows;
  - CRM reconciliation: 2 critical `legacy_core_field_mismatch` rows, both status mismatches where legacy is pending and core is confirmed.

Phase 1 cleanup/backfill scope:

- Run `backfill_mirror_off_prereqs.py` on a prod copy after schema migrations, with dry-run first.
- Backfill canonical public reservation tokens for active legacy booking tokens.
- Backfill or deactivate legacy VK staff peer rows only after confirming canonical `bot_peers` coverage.
- Backfill missing `reservation_events` for legacy-sourced reservations.
- Reconcile the two status mismatches deliberately; do not let either BOT or CRM silently win without operator approval.
- Re-run both mirror-off and CRM reconciliation checks after cleanup.

Temporary cleanup rehearsal:

- On the temporary migrated copy, `backfill_mirror_off_prereqs.py --active-only` applied:
  - public token backfill: 5 rows;
  - canonical bot peer backfill: 1 row.
- On the same temporary copy, `backfill_reservation_events.py` applied:
  - missing reservation event backfill: 5 rows.
- On the same temporary copy, `backfill_mirror_off_prereqs.py --active-only --deactivate-mirrored-vk-staff` applied:
  - legacy VK staff peer deactivation: 1 row.
- After those rehearsal steps, `verify_mirror_off.py` returned 0 critical and 0 warning rows.
- CRM reconciliation still returned 2 critical status mismatches. These require explicit operator decision before production cleanup.

### Phase 2. Inventory And Guards

- Add `CRM_AUTHORITATIVE=0/1` in BOT.
- Keep `CRM_USE_CORE_DOMAIN=1` in CRM staging.
- Keep `LEGACY_MIRROR_ENABLED` unchanged until smoke tests pass.
- Add logs for every path that still calls BOT local domain functions.
- Add idempotency keys to all new command calls.

#### Phase 2 Progress, 2026-04-27

Implemented in BOT:

- `config.py`: added `CRM_AUTHORITATIVE`, `CRM_COMMAND_API_URL`, `CRM_COMMAND_API_KEY`, and `CRM_COMMAND_TIMEOUT`.
- `.env.example`: documented the new BOT variables.
- `config.py`: when `CRM_AUTHORITATIVE=1`, startup validation now requires command API URL/key.
- `booking_service.py`: added `CRM-AUTHORITATIVE-GUARD` warning logs when local domain functions are called while authoritative mode is enabled.

Currently guarded local domain functions:

- `assign_table_to_booking`
- `clear_table_assignment`
- `set_booking_status`
- `mark_booking_cancelled`
- `set_booking_deposit`
- `clear_booking_deposit`
- `set_table_label`

Verification:

- `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile config.py booking_service.py`

### Phase 3. CRM Command API

- Implement command endpoints.
- Cover status, assign/clear table, deposit, restriction, manual session.
- Add tests for accepted/rejected command responses.
- Ensure `assign_table` checks only canonical CRM state in authoritative mode.

#### Phase 3 Progress, 2026-04-27

Implemented in CRM:

- `config.py`: added `CRM_COMMAND_API_KEY`.
- `.env.example`: documented `CRM_COMMAND_API_KEY`.
- `integration_schema.py`: added `crm_command_events` idempotency ledger.
- `flask_app.py`: added command auth via `X-CRM-Command-Key`.
- `flask_app.py`: added idempotent command execution wrapper with structured accepted/rejected responses.

Command endpoints added:

- `POST /api/commands/reservations`
- `POST /api/commands/reservations/<reservation_id>/status`
- `POST /api/commands/reservations/<reservation_id>/reschedule`
- `POST /api/commands/reservations/<reservation_id>/guests`
- `POST /api/commands/reservations/<reservation_id>/assign-table`
- `POST /api/commands/reservations/<reservation_id>/clear-table`
- `POST /api/commands/reservations/<reservation_id>/restriction`
- `DELETE /api/commands/reservations/<reservation_id>/restriction`
- `POST /api/commands/reservations/<reservation_id>/deposit`
- `DELETE /api/commands/reservations/<reservation_id>/deposit`
- `POST /api/commands/tables/<table_number>/restriction`
- `DELETE /api/commands/tables/<table_number>/restriction`
- `POST /api/commands/tables/<table_number>/session`
- `DELETE /api/commands/tables/<table_number>/session`

Implemented in BOT:

- `crm_commands.py`: small CRM command client using `CRM_COMMAND_API_URL`, `CRM_COMMAND_API_KEY`, `CRM_COMMAND_TIMEOUT`.
- Client normalizes command success, structured rejection, duplicate response, and transport failure.

Tests added:

- CRM: `tests/test_crm_command_api.py`
- BOT: `tests/test_crm_commands.py`

Verification:

- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile config.py booking_service.py crm_commands.py tests/test_crm_commands.py`
- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m unittest tests.test_crm_commands`
- CRM: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile config.py integration_schema.py flask_app.py tests/test_crm_command_api.py`

Local limitation:

- CRM command API tests could not be executed with system `python3` because Flask is not installed in the current Python environment. They are compile-checked and ready to run in the CRM virtualenv.

#### Phase 3 Progress, 2026-04-28

Implemented in CRM:

- `flask_app.py`: added booking-bound restriction command endpoints:
  - `POST /api/commands/reservations/<reservation_id>/restriction`;
  - `DELETE /api/commands/reservations/<reservation_id>/restriction`.
- `service.py`: booking-bound `restrict_table` now honors explicit `force_override` from the command payload, matching the staff override flow.

Implemented in BOT:

- `crm_commands.py`: added booking-bound restriction client methods:
  - `restrict_reservation_table()`;
  - `clear_reservation_table_restriction()`.

Tests added/extended:

- CRM: `tests/test_crm_command_api.py` covers force-override booking-bound restriction.
- BOT: `tests/test_crm_commands.py` checks that booking-bound restriction uses the reservation command endpoint.

Verification:

- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m unittest tests.test_tg_authoritative_flow tests.test_crm_commands tests.test_vk_staff_flow`
- CRM: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile flask_app.py service.py tests/test_crm_command_api.py`

### Phase 4. Staff Actions Through CRM

- Switch VK staff callbacks/text prompts to CRM commands.
- Switch Telegram staff callbacks/text prompts to CRM commands.
- Keep card rendering and notification delivery in BOT.
- Add tests where CRM rejects a table conflict and BOT shows rejection without local mutation.

#### Phase 4 Progress, 2026-04-27

Implemented in BOT:

- `vk_staff_flow.py`: under `CRM_AUTHORITATIVE=1`, VK staff callbacks now call CRM commands for:
  - confirm;
  - cancel;
  - clear table;
  - clear deposit.
- `vk_staff_flow.py`: under `CRM_AUTHORITATIVE=1`, VK pending text now calls CRM commands for:
  - assign table;
  - set deposit;
  - restrict table.
- `tg_handlers.py`: under `CRM_AUTHORITATIVE=1`, Telegram staff callbacks now call CRM commands for:
  - confirm;
  - cancel;
  - assign table override;
  - clear table.
- `tg_handlers.py`: under `CRM_AUTHORITATIVE=1`, Telegram pending text now calls CRM commands for:
  - assign table;
  - set deposit;
  - restrict table;
  - clear table restriction.

#### Phase 4 Progress, 2026-04-28

Implemented in BOT:

- `vk_staff_flow.py`: booking-bound table restriction now calls `restrict_reservation_table()` when a booking id is present; generic table restriction still calls the table endpoint.
- `tg_handlers.py`: Telegram restriction prompts now call CRM commands under `CRM_AUTHORITATIVE=1`:
  - booking-bound restriction uses `/api/commands/reservations/<reservation_id>/restriction`;
  - generic restriction uses `/api/commands/tables/<table_number>/restriction`;
  - generic clear restriction uses the table clear endpoint.
- `booking_render.py`: added `render_booking_card_from_reservation()` for CRM command snapshots.
- `tg_handlers.py`: `_sync_admin_booking_card()` now prefers a CRM command snapshot from the accepted command response and falls back to the legacy local read model only when no snapshot is available.

Implemented in CRM:

- `service.py`: added `get_reservation_command_snapshot()` with canonical reservation, assignment, deposit, and active restriction fields.
- `flask_app.py`: accepted reservation commands now include a `reservation` snapshot in the command response.

Tests added/extended:

- `tests/test_tg_authoritative_flow.py`: authoritative confirm callback sends CRM command and leaves local booking status unchanged.
- `tests/test_tg_authoritative_flow.py`: authoritative assign-table rejection leaves local assignment empty and keeps the prompt pending.
- `tests/test_tg_authoritative_flow.py`: authoritative booking-bound restriction sends CRM command, completes the prompt, and does not create local `table_blocks`.
- `tests/test_tg_authoritative_flow.py`: staff card refresh prefers CRM command snapshot and leaves local legacy booking status unchanged.
- `tests/test_crm_command_api.py`: command responses include the canonical reservation snapshot.
- `tests/test_vk_staff_flow.py`: authoritative confirm and assign rejection remain covered.

Verification:

- `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile booking_render.py tg_handlers.py crm_commands.py tests/test_tg_authoritative_flow.py tests/test_crm_commands.py`
- `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m unittest tests.test_tg_authoritative_flow tests.test_crm_commands tests.test_vk_staff_flow`
- `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile flask_app.py service.py tests/test_crm_command_api.py`

Phase 4 status:

- Staff actions that were in scope now route through CRM under `CRM_AUTHORITATIVE=1`.
- Telegram staff card refresh can render from the CRM command response without waiting for local legacy state to mirror.

### Phase 5. Incoming Booking Creation Through CRM

- Tilda parser calls CRM create/update command.
- Telegram Mini App path either moves out of BOT or calls CRM create command after transport validation.
- BOT stores only transport metadata and staff delivery tasks.
- Verify duplicate Tilda delivery does not duplicate reservations.

#### Phase 5 Progress, 2026-04-28

Implemented in CRM:

- `service.py`: added `create_incoming_reservation_command()` for external booking sources.
- `flask_app.py`: `POST /api/commands/reservations` now routes non-`crm_manual` sources through the incoming reservation command.
- Incoming create commands are idempotent by `(source, external_ref)` and keep external bookings in `pending` status by default.

Implemented in BOT:

- `crm_commands.py`: added `create_reservation()`.
- `application/tilda_booking.py`: under `CRM_AUTHORITATIVE=1`, Tilda webhook handling calls CRM create command and does not write local `bookings`.
- `application/miniapp_booking.py`: under `CRM_AUTHORITATIVE=1`, Telegram Mini App booking handling calls CRM create command and does not write local `bookings`.
- Legacy local creation remains behind `CRM_AUTHORITATIVE=0`.

Tests added/extended:

- `tests/test_authoritative_incoming_booking.py`: Tilda authoritative create calls CRM and leaves local `bookings` empty.
- `tests/test_authoritative_incoming_booking.py`: Telegram Mini App authoritative create calls CRM and leaves local `bookings` empty.
- CRM `tests/test_crm_command_api.py`: incoming create is pending and idempotent by external ref.

Verification:

- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile application/tilda_booking.py application/miniapp_booking.py crm_commands.py tests/test_authoritative_incoming_booking.py`
- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m unittest tests.test_authoritative_incoming_booking tests.test_tg_authoritative_flow tests.test_crm_commands tests.test_vk_staff_flow`
- CRM: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile flask_app.py service.py tests/test_crm_command_api.py`

Remaining Phase 5/6 boundary:

- Under `CRM_AUTHORITATIVE=1`, BOT no longer sends staff cards directly from Tilda/Mini App creation because local `bot_message_links` still depend on local `reservations` FK. Staff delivery should move to the CRM notification outbox in Phase 6.

### Phase 6. CRM Notification Outbox

- CRM emits staff notification tasks after successful commands.
- BOT delivers staff notifications and updates message links.
- Failed delivery is visible in logs/UI, but domain state remains canonical in CRM.

#### Phase 6 Progress, 2026-04-28

Implemented in CRM:

- `crm_notification_outbox.py`: added staff reservation notification enqueue, claim, and complete helpers.
- `flask_app.py`: successful reservation command responses enqueue `reservation_card_upsert` jobs into CRM-owned `bot_outbox`.
- `flask_app.py`: added BOT-facing delivery endpoints:
  - `POST /api/notification-outbox/claim`;
  - `POST /api/notification-outbox/<job_id>/complete`.
- `complete` records successful `reservation_card` message links in CRM-owned `bot_message_links`, so later CRM jobs can include `message_id` for card edits.

Implemented in BOT:

- `crm_notification_worker.py`: claims CRM notification jobs, renders reservation cards from CRM snapshots, sends/edits Telegram cards, sends VK staff cards, and completes jobs back to CRM.
- The worker does not mutate reservation/table/deposit state locally.
- `embedded_crm_notification_worker.py`: optional embedded worker with env flag, file lock, interval, batch limit, and max-attempt settings.
- `crm_notification_worker_loop.py`: standalone loop entrypoint for running notification delivery as a separate process.
- `flask_app.py`: starts the embedded notification worker only when `CRM_NOTIFICATION_EMBEDDED_WORKER=1`.
- `.env.example`: documented `CRM_NOTIFICATION_*` runtime settings.

Tests added/extended:

- BOT `tests/test_crm_notification_worker.py`: worker renders a CRM reservation snapshot and ack-s success with provider message id.
- BOT `tests/test_embedded_crm_notification_worker.py`: embedded worker is disabled by default and starts only once when explicitly enabled.
- CRM `tests/test_crm_command_api.py`: incoming create enqueues notification outbox jobs.
- CRM `tests/test_crm_command_api.py`: claim/complete marks job sent and records message link.

Verification:

- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile crm_notification_worker.py tests/test_crm_notification_worker.py application/tilda_booking.py application/miniapp_booking.py`
- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile embedded_crm_notification_worker.py crm_notification_worker_loop.py flask_app.py tests/test_embedded_crm_notification_worker.py`
- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m unittest tests.test_embedded_crm_notification_worker tests.test_crm_notification_worker tests.test_authoritative_incoming_booking tests.test_tg_authoritative_flow tests.test_crm_commands tests.test_vk_staff_flow`
- CRM: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile crm_notification_outbox.py flask_app.py service.py tests/test_crm_command_api.py`

Phase 6 runtime status:

- Notification delivery can now run either embedded via `CRM_NOTIFICATION_EMBEDDED_WORKER=1` or as a standalone `python crm_notification_worker_loop.py` process.
- Run CRM command API tests inside the CRM virtualenv where Flask is installed.

### Phase 7. Legacy Freeze

- Run reconciliation on production-like DB.
- Run BOT mirror-off verification on production-like DB.
- Enable `LEGACY_MIRROR_ENABLED=0` in staging.
- Stop new writes to `bookings`, `venue_tables`, `booking_events`, `table_events`.
- Keep rollback flag until observation window is clean.

#### Phase 7 Progress, 2026-04-28

Implemented in BOT:

- `flask_app.py`: old `/admin/api/crm-sync/*` compatibility endpoints now return `crm_authoritative_mode` with HTTP 409 when `CRM_AUTHORITATIVE=1`.
- This closes the old CRM-to-BOT domain write path during authoritative operation.

Tests added/extended:

- `tests/test_security_boundaries.py`: authoritative mode disables legacy CRM sync endpoints.

Verification:

- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile flask_app.py tg_handlers.py tests/test_security_boundaries.py tests/test_tg_authoritative_flow.py`
- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m unittest tests.test_notification_dispatcher tests.test_channel_binding_service tests.test_tg_authoritative_flow tests.test_embedded_crm_notification_worker tests.test_crm_notification_worker tests.test_authoritative_incoming_booking tests.test_crm_commands tests.test_vk_staff_flow tests.test_security_boundaries`

Remaining Phase 7 runtime work:

- Run `verify_mirror_off.py` and CRM reconciliation on the current staging/prod-copy database before setting `LEGACY_MIRROR_ENABLED=0` outside local tests.
- Observe staging with `CRM_AUTHORITATIVE=1` and `LEGACY_MIRROR_ENABLED=0` before removing rollback flags.

### Phase 8. Remove Client Telegram Bot

- Remove client dialog routes/branches.
- Remove customer history/bootstrap code.
- Remove guest binding token runtime.
- Remove `GUEST_COMM_ENABLED` product behavior.
- Keep only staff group bot behavior.

#### Phase 8 Progress, 2026-04-28

Implemented in BOT:

- `tg_handlers.py`: under `CRM_AUTHORITATIVE=1`, client `/start` no longer registers users, asks for phone sharing, opens the client menu, or enters guest binding runtime.
- `tg_handlers.py`: under `CRM_AUTHORITATIVE=1`, Telegram contact sharing is acknowledged as archived and does not write `tg_bot_users` or `guests`.
- `channel_binding_service.py`: `create_binding_token()` and `consume_binding_token_once()` reject runtime use under `CRM_AUTHORITATIVE=1`.
- `notification_dispatcher.py`: guest service notifications reject runtime use under `CRM_AUTHORITATIVE=1`.
- Staff utilities such as `/myid`, `/chatid`, staff callbacks, and staff pending prompts remain available.

Tests added/extended:

- `tests/test_tg_authoritative_flow.py`: authoritative `/start` does not register a client user.
- `tests/test_tg_authoritative_flow.py`: authoritative contact sharing does not store phone/user rows.
- `tests/test_channel_binding_service.py`: authoritative mode rejects guest binding token create/consume runtime.
- `tests/test_notification_dispatcher.py`: authoritative mode rejects guest service notifications before creating outbox work.

Verification:

- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m py_compile channel_binding_service.py notification_dispatcher.py tests/test_channel_binding_service.py tests/test_notification_dispatcher.py`
- BOT: `PYTHONPYCACHEPREFIX=/tmp/luchbar_pycache python3 -m unittest tests.test_notification_dispatcher tests.test_channel_binding_service tests.test_tg_authoritative_flow tests.test_embedded_crm_notification_worker tests.test_crm_notification_worker tests.test_authoritative_incoming_booking tests.test_crm_commands tests.test_vk_staff_flow tests.test_security_boundaries`

Remaining Phase 8 cleanup:

- Delete or archive unreachable client dialog modules only after the observation window confirms no rollback is needed.

#### Authoritative Tail Fixes, 2026-04-28

Implemented in BOT:

- Telegram staff callbacks no longer require a local `bookings` row under `CRM_AUTHORITATIVE=1`; callback `b:<id>:...` can be handled as a CRM `reservation_id`.
- Telegram staff callbacks edit the clicked card directly from the CRM command snapshot when CRM returns `reservation`.
- Telegram pending assign-table flow skips local BOT conflict checks and local booking existence checks under `CRM_AUTHORITATIVE=1`; CRM is the only accept/reject authority.
- VK staff confirm/cancel/clear/deposit flows can operate with only the CRM reservation id.
- VK pending assign-table flow skips local BOT conflict checks and local booking existence checks under `CRM_AUTHORITATIVE=1`.
- BOT no longer calls local waiter notification helpers from authoritative assign/deposit paths; delivery must come from CRM notification/outbox behavior.

Implemented in CRM:

- `assign-table` command accepts a minimal BOT payload with only `table_number`; CRM fills guest name, phone, and party size from its own reservation row.
- Added `BOT_SYNC_LEGACY_ENABLED=0` for authoritative cutover.
- CRM manual booking/action orchestration skips old CRM-to-BOT sync when `BOT_SYNC_LEGACY_ENABLED=0` and does not roll back domain state because BOT rejected legacy sync.

Required production env for full authoritative cutover:

BOT:

```bash
CRM_AUTHORITATIVE=1
CRM_COMMAND_API_URL=https://<crm-host>
CRM_COMMAND_API_KEY=<shared-command-key>
CRM_NOTIFICATION_EMBEDDED_WORKER=1
GUEST_COMM_ENABLED=0
CORE_ONLY_MODE=1
LEGACY_MIRROR_ENABLED=0
```

CRM:

```bash
CRM_COMMAND_API_KEY=<shared-command-key>
BOT_SYNC_LEGACY_ENABLED=0
HOSTESS_CHAT_IDS=<telegram-staff-chat-ids>
VK_HOSTESS_PEER_IDS=<vk-staff-peer-ids-if-used>
```

`BOT_SYNC_API_URL` / `BOT_SYNC_SHARED_SECRET` may stay configured only for rollback compatibility, but `BOT_SYNC_LEGACY_ENABLED` must be `0` during authoritative cutover.

Verification added:

- BOT: no-local-booking Telegram confirm callback calls CRM and edits the card from CRM snapshot.
- BOT: no-local-booking Telegram pending assign-table calls CRM with the CRM reservation id.
- BOT: no-local-booking VK confirm and pending assign-table call CRM with the CRM reservation id.
- CRM: assign-table command accepts minimal BOT payload.
- CRM: manual CRM action does not call or rollback on legacy BOT sync when `BOT_SYNC_LEGACY_ENABLED=0`.

Latest local verification:

- BOT: `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'` → 90 tests OK.
- CRM: `.venv/bin/python -m unittest discover -s tests -p 'test_crm_command_api.py'` → 8 tests OK.
- BOT/CRM: `git diff --check` clean.

### Phase 9. Delete Or Archive Legacy

- Move old tables to explicit archive/migration ownership.
- Remove fallback reads from primary UI and staff flows.
- Remove old CRM-to-BOT domain sync endpoints after production observation.

## Verification Matrix

Run before production cutover:

- Tilda create booking.
- Tilda duplicate/update booking.
- Telegram Mini App create booking or confirmed replacement path.
- CRM create manual booking.
- CRM confirm/cancel/no-show/complete.
- CRM reschedule and update guests.
- CRM assign table.
- CRM table conflict rejection.
- CRM clear table.
- CRM set/clear deposit.
- CRM restrict/clear/extend table restriction.
- CRM open/clear manual table session.
- VK staff confirm/cancel/assign/deposit/restrict.
- Telegram staff confirm/cancel/assign/deposit/restrict.
- waiter notification for table + deposit.
- hostess notification/card update.
- CRM pages: today, bookings, booking detail, tables, restrictions, archive, dashboard, guests.
- relay retry when BOT is unavailable.
- idempotency retry for the same event id.

## Rollback

Rollback must be flag-based until the observation window is clean:

- disable `CRM_AUTHORITATIVE`;
- restore old BOT local domain handlers;
- keep `CRM_USE_CORE_DOMAIN=1` only if core state stayed consistent;
- re-enable legacy mirror if mirror-off introduced stale reads;
- do not delete legacy data until rollback is no longer needed.

## Exit Criteria

- CRM is the only writer of domain state.
- BOT staff actions call CRM commands and do not mutate reservation/table state locally.
- Telegram bot has no live customer-chat/customer-history behavior.
- Staff Telegram/VK groups still receive and operate booking cards.
- `bookings`, `venue_tables`, `booking_events`, and `table_events` are not used in the primary runtime path.
- Failed notifications are visible and retryable without creating domain divergence.
