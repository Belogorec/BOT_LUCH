# DB Boundaries

## Principle

Одна физическая БД допустима, но с двумя строго разделенными зонами:

- CRM core
- bot/integration

CRM core является источником истины для брони и столов.
Боты и интеграции не владеют доменной моделью, а только доставляют события и получают задания на отправку.

## CRM Tables

Эти таблицы относятся к CRM и описывают бизнес-состояние:

- `reservations`
- `tables_core`
- `reservation_tables`
- `table_blocks`
- `reservation_events`

### Ownership

- только CRM-слой меняет статус брони;
- только CRM-слой назначает и снимает стол;
- только CRM-слой создает доменные события;
- интеграции не должны напрямую обновлять поля внутри этих таблиц вне доменных сервисов.

## Bot/Integration Tables

Эти таблицы относятся к транспорту, каналам и служебному обмену:

- `bot_peers`
- `bot_message_links`
- `bot_inbound_events`
- `bot_outbox`
- `public_reservation_tokens`

### Purpose

- `bot_peers` хранит известных peer/user получателей в Telegram/VK;
- `bot_message_links` связывает доменную бронь с отправленным сообщением в канале;
- `bot_inbound_events` хранит входящие события от ботов и вебхуков;
- `bot_outbox` хранит исходящие задания на отправку;
- `public_reservation_tokens` хранит публичные токены доступа к брони.

## Allowed Relations

Допустимы только такие направления связей:

- `bot_message_links.reservation_id -> reservations.id`
- `bot_inbound_events.reservation_id -> reservations.id`
- `bot_outbox.reservation_id -> reservations.id`
- `public_reservation_tokens.reservation_id -> reservations.id`
- `bot_message_links.peer_id -> bot_peers.id`
- `bot_outbox.target_peer_id -> bot_peers.id`

Это значит, что integration-слой может ссылаться на CRM-ядро, но не наоборот.

## Forbidden Relations

Нужно запрещать следующие зависимости:

- CRM-таблицы не должны хранить `telegram_chat_id`, `telegram_message_id`, `vk_peer_id`, webhook ids, callback ids;
- CRM-таблицы не должны хранить raw webhook payload как обязательную часть модели;
- CRM-таблицы не должны зависеть от `bot_peers`, `bot_outbox`, `bot_inbound_events`;
- логика брони не должна читать `processed_tg_updates`, `pending_replies`, `vk_staff_peers` как источник доменного состояния.

Проще говоря:

- bot/integration может ссылаться на бронь;
- бронь не должна знать, как именно устроен бот.

## Data Flow

### Incoming bookings from bots/forms

1. Вебхук или бот пишет запись в `bot_inbound_events`.
2. Сервисный слой валидирует payload.
3. Сервисный слой создает или обновляет `reservations`.
4. Если нужно уведомление персоналу, создается запись в `bot_outbox`.

### Outgoing booking cards to bots

1. CRM меняет бронь или стол.
2. Доменный слой пишет `reservation_events`.
3. Интеграционный слой ставит задания в `bot_outbox`.
4. Worker отправляет сообщение.
5. После успешной отправки создается или обновляется `bot_message_links`.

### Incoming actions from staff bots

1. Callback или reply приходит в `bot_inbound_events`.
2. Обработчик находит `reservation_id`.
3. Вызывает доменный сервис CRM.
4. CRM меняет `reservations` или `reservation_tables`.
5. Новое состояние снова уходит в `bot_outbox`.

## Migration Notes

Legacy-таблицы, которые должны уйти из ядра:

- поля `telegram_chat_id`, `telegram_message_id`, `raw_payload_json`, `reservation_token` из `bookings`;
- `processed_tg_updates`
- `pending_replies`
- `tg_outbox`
- `vk_staff_peers`

На переходном этапе они могут жить как legacy, но новая логика должна писать уже в `bot_*` и `public_reservation_tokens`.
