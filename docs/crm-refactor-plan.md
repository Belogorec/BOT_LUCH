# CRM Refactor Plan

## Goal

Привести систему к простому и устойчивому ядру CRM, в котором остаются только:

- брони;
- столы;
- назначения столов на бронь;
- блокировки столов;
- история изменений.

Все остальные сценарии должны быть либо вынесены из ядра, либо отключаемы как внешние модули.

## Current Problems

### Data model

- `bookings` совмещает доменные данные брони, интеграционные поля, токены, депозит, канал связи и сырой payload.
- гостевая модель раздвоена между `guests` и `guest_visits`, причем агрегаты пересчитываются кодом;
- ряд связей завязан на `phone_e164`, а не на стабильные внутренние идентификаторы;
- номер стола в логике трактуется как `TEXT`, а в реальной SQLite-схеме еще хранится как `INTEGER`;
- часть схемы уже живет в коде, но еще не применена к фактической базе.

### Code structure

- HTTP, Telegram, VK, Tilda, CRM sync и waiter-уведомления связаны напрямую через `flask_app.py`;
- бизнес-логика и интеграции перемешаны;
- `db.py` одновременно хранит схему, миграции, seed-логику и guest/distribution модули.

## Target Architecture

### Core modules

- `core_schema.py` или аналогичный модуль со схемой нового ядра;
- `booking_service.py` как доменная логика броней и столов;
- `flask_app.py` как тонкий слой маршрутов;
- отдельные адаптеры интеграций, не влияющие на модель ядра.

### Target entities

#### `reservations`

Хранит только бронь:

- `id`
- `source`
- `external_ref`
- `guest_name`
- `guest_phone`
- `reservation_at`
- `party_size`
- `comment`
- `status`
- `created_at`
- `updated_at`

#### `tables`

Справочник столов:

- `id`
- `code`
- `title`
- `capacity`
- `zone`
- `is_active`
- `created_at`
- `updated_at`

`code` хранится как `TEXT`.

#### `reservation_tables`

Связь брони и стола:

- `id`
- `reservation_id`
- `table_id`
- `assigned_at`
- `assigned_by`
- `released_at`

#### `table_blocks`

Блокировки и ограничения стола:

- `id`
- `table_id`
- `starts_at`
- `ends_at`
- `reason`
- `block_type`
- `reservation_id`
- `created_by`
- `created_at`

#### `reservation_events`

Аудит доменных событий:

- `id`
- `reservation_id`
- `event_type`
- `actor`
- `payload_json`
- `created_at`

## Legacy Scope

### Keep in legacy or move out of core

- `guests`
- `guest_visits`
- `guest_notes`
- `guest_events`
- `pending_replies`
- `discount_codes`
- `tg_bot_users`
- `lineup_posters`
- `processed_tg_updates`
- `vk_staff_peers`
- `guest_channel_bindings`
- `guest_binding_tokens`
- `notification_delivery_log`

Эти сущности не должны участвовать в принятии решений ядром CRM.

## Refactor Stages

### Stage 1. Freeze the target model

- зафиксировать целевую ER-модель;
- описать, какие таблицы становятся `legacy_*`;
- перестать добавлять новую бизнес-логику в текущую схему.

### Stage 2. Introduce the new core schema

- добавить новый модуль схемы;
- создать новые таблицы рядом с текущими, без удаления старых;
- добавить явные `FOREIGN KEY` и индексы;
- подготовить функции переноса данных.

### Stage 3. Migrate data

- перенести брони из `bookings` в `reservations`;
- перенести столы из `venue_tables` в `tables`;
- перенести назначения столов в `reservation_tables`;
- преобразовать текущие ограничения в `table_blocks`;
- перенести доменную историю в `reservation_events`.

### Stage 4. Switch backend to core tables

- переписать чтение и запись доменных операций на новые таблицы;
- ограничить API тремя базовыми сценариями:
  - создание/редактирование брони;
  - назначение/снятие стола;
  - блокировка/разблокировка стола;
- убрать из критического пути CRM sync, guest communication, VK/TG service modules.

### Stage 5. Archive legacy

- перевести старые таблицы в read-only режим;
- переименовать в `legacy_*` либо оставить как архив без участия в runtime;
- удалить старые зависимости из основного приложения.

### Stage 6. Rebuild CRM and frontend

- собрать чистый backend API;
- построить новый CRM UI поверх стабильной модели;
- затем отдельно вернуть нужные интеграции, если они действительно нужны.

## Safety Rules

- не удалять старые таблицы до завершения миграции и сверки;
- сначала запускать dual-write или scripted migration, потом переключать чтение;
- хранить legacy-данные до полного завершения приемки;
- не смешивать новый core runtime с guest/marketing/staff модулями.

## Immediate Work Plan

1. Добавить новый модуль схемы ядра.
2. Подготовить idempotent-инициализацию новых core-таблиц.
3. Подготовить отдельный модуль миграции `bookings -> reservations`.
4. После этого переключить доменную логику броней и столов на новый слой данных.
