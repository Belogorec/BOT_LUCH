# CRM Refactor Plan

## Цель

Собрать устойчивое CRM-ядро, где критический путь ограничен только доменными сценариями бронирования:

- бронь;
- стол;
- назначение стола на бронь;
- блокировка стола;
- история доменных событий.

Интеграции (Telegram, VK, Tilda, CRM sync, waiter notify, guest-модули) остаются адаптерами вокруг ядра и не определяют доменную модель.

## Что считаем ядром

### Доменные сущности

- `reservations`
- `tables`
- `reservation_tables`
- `table_blocks`
- `reservation_events`

### Вне ядра (legacy / adapters)

- `guests`, `guest_visits`, `guest_notes`, `guest_events`
- `pending_replies`, `discount_codes`, `tg_bot_users`
- `lineup_posters`, `processed_tg_updates`, `vk_staff_peers`
- `guest_channel_bindings`, `guest_binding_tokens`, `notification_delivery_log`

## Архитектурные правила

- Route layer: только HTTP/transport, валидация входа, маппинг request/response.
- Application layer: use-cases входящих потоков (`telegram_miniapp`, `tilda`, `vk`, `crm`).
- Domain/service layer: бизнес-правила, инварианты, аудит.
- Data layer: работа с core-таблицами и миграциями.
- Любая новая inbound-логика идёт по шаблону: `route -> parser -> application use-case -> domain service`.

## Фазы

### Phase 1. Domain command boundary

Сделано:
- добавлены доменные команды/DTO/ошибки для валидации операций ядра.

Критерий готовности:
- сервисные операции проходят через единый набор команд и ограничений.

### Phase 2. Core schema bootstrap

Сделано:
- включена инициализация core-схемы и idempotent-механика миграций рядом с legacy.

Критерий готовности:
- core-таблицы поднимаются без удаления legacy-структур.

### Phase 3. Core sync + dual-write readiness

Сделано:
- подключён синк состояния брони/столов в core-слой.

Критерий готовности:
- операции в runtime отражаются в core без потери совместимости.

### Phase 4. Application-layer унификация inbound

Сделано:
- `telegram miniapp booking` вынесен в `application/miniapp_booking.py`.
- `tilda booking creation` вынесен в `application/tilda_booking.py`.
- `tilda_api.py` оставлен как transport/parser-адаптер.

Критерий готовности:
- inbound-потоки не содержат бизнес-логики в route-обработчиках.

### Phase 5. Backend switch to core-first behavior

Цель:
- перевести доменные операции (бронь/стол/блокировки) на core-таблицы как источник истины.

Задачи:
- переключить чтение доменных карточек и списков на core-модель;
- переключить write-path критических операций на core;
- оставить compatibility-read/bridge до завершения приёмки;
- изолировать внешние sync/notify от принятия доменных решений.

Критерий готовности:
- доменные решения не зависят от legacy-таблиц.

### Phase 5.5. Parser split for inbound adapters

(Добавлено после Phase 5.)

Цель:
- довести inbound до полностью единообразной схемы.

Задачи:
- вынести parsing/normalization Tilda payload в отдельный parser-модуль;
- применить ту же схему к VK/CRM inbound, где ещё смешаны parsing и orchestration;
- зафиксировать единый контракт input DTO для application use-cases.

Критерий готовности:
- все inbound каналы имеют структуру `route -> parser -> application`, без дублирования правил парсинга.

### Phase 6. Legacy archive

Цель:
- убрать legacy из runtime-критического пути.

Задачи:
- пометить legacy-таблицы read-only;
- переименовать в `legacy_*` или явно задокументировать архивный статус;
- удалить зависимости на legacy из primary-path кода.

Критерий готовности:
- runtime не пишет в legacy-структуры, кроме контролируемого архива.

### Phase 7. CRM/API rebuild

Цель:
- собрать чистый API/CRM UI поверх stable core-модели.

Задачи:
- новый backend API для базовых доменных операций;
- новый CRM UI на core endpoints;
- возврат интеграций только как подключаемых адаптеров.

Критерий готовности:
- UI и API работают на core без скрытых legacy-зависимостей.

## Safety / Rollout

- Не удалять legacy-данные до финальной сверки.
- Любое переключение делать через dual-write/compat-read этап.
- На каждый switch иметь rollback-флаг или обратимый migration script.
- Для интеграционных изменений проверять связки:
  - `BOT_LUCH -> LUCH_crm` (`CRM_API_URL`, `CRM_API_KEY`, `BOT_SYNC_API_URL`, `CRM_INGEST_API_KEY`)
  - `relay -> BOT_LUCH` (`MAIN_BOT_URL`, `MAIN_BOT_TILDA_SECRET`, `MAIN_BOT_TG_SECRET`)
  - waiter notify (`WAITER_CHAT_ID`, VK waiter creds)

## Definition of Done (per phase)

- Код: изменения в нужных слоях без разрастания `flask_app.py`.
- Данные: миграции idempotent и обратимы процедурно.
- Наблюдаемость: события ошибок логируются в доменной истории/логах.
- Совместимость: существующие входящие каналы не ломаются.

## Ближайший practical next step

1. Начать Phase 5 с выбора одного сценария-пилота: `assign/clear table` как first core-first write-path.
2. Добавить compatibility-read adapter для карточки брони, чтобы UI не ломался при переключении.
3. После стабилизации — аналогично перевести `set/clear deposit` и status transitions.
