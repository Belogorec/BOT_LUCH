# VK Bot README

## Что это

Документ по подключению и дальнейшей работе с VK Bot для `LUCH Bot`.

## Текущая схема

- основной сервис: `BOT_LUCH`
- публичный callback URL: `https://<bot-domain>/vk/callback`
- backend endpoint в коде: `flask_app.py`
- env-конфиг: `config.py`
- на одном callback URL теперь можно держать несколько VK-сообществ, роутинг идёт по `group_id`

## Уже реализовано

- маршрут `POST /vk/callback`
- обработка `confirmation`
- проверка `group_id`
- проверка `secret key`
- возврат plain text `ok` для следующих событий
- MVP-ответ на входящий `message_new`
- helper для исходящих сообщений через `messages.send`
- авто-регистрация staff-диалогов в базе
- отправка новых броней в VK из старых webhook-источников
- кнопки действий по брони прямо в VK
- пошаговый ввод для стола, депозита и ограничения

Это минимальный стартовый слой, чтобы:
- подтвердить адрес сервера в VK
- безопасно принимать callback-события
- сразу проверить полный цикл входящего сообщения и тестового ответа
- сразу начать использовать VK как рабочий чат хостес
- потом отдельно добавить кнопки и сценарий управления бронями

## Текущий рабочий режим

Этот VK-бот сейчас рассматривается как внутренний рабочий канал.

Что это значит:
- гостевой сценарий здесь не нужен
- новые брони должны приходить в диалоги staff
- staff-пользователь пишет боту хотя бы один раз и автоматически регистрируется
- после этого новые брони из старых webhook-источников приходят в его VK-диалог

Текущие источники уведомлений в VK:
- `tilda`
- `telegram_miniapp_api`

Текущие действия в VK:
- `Подтвердить`
- `Отменить`
- `Назначить стол`
- `Снять стол`
- `Поставить депозит`
- `Снять депозит`
- `Ограничить стол`

## Нужные ENV

- `VK_GROUP_ID`
- `VK_ACCESS_TOKEN`
- `VK_CALLBACK_SECRET`
- `VK_CONFIRMATION_TOKEN`
- `VK_WAITER_GROUP_ID`
- `VK_WAITER_ACCESS_TOKEN`
- `VK_WAITER_CALLBACK_SECRET`
- `VK_WAITER_CONFIRMATION_TOKEN`
- `VK_API_VERSION`

## Текущие рабочие значения для настройки VK

- `URL`: `https://<bot-domain>/vk/callback`
- `API version`: `5.199`

Секреты и токены не хранить в этом README. Они должны лежать только:
- в локальном `.env`
- в Railway Variables
- в личном хранилище доступов

Обратная совместимость:
- старые переменные `VK_*` продолжают обслуживать текущий hostess-бот
- для waiter-сообщества используются отдельные `VK_WAITER_*`

## Что заполнять в VK Callback API

- `URL`: `https://<bot-domain>/vk/callback`
- `Secret key`: значение из `VK_CALLBACK_SECRET`
- `Confirmation code`: значение из `VK_CONFIRMATION_TOKEN`
- `Group ID`: значение из `VK_GROUP_ID`
- `API version`: `5.199`

## Какие события включить на старте

- `message_new`
- `message_reply`
- `message_event`
- `message_allow`
- `message_deny`

## Как проходит подтверждение

VK отправляет `POST` на callback URL с телом вида:

```json
{ "type": "confirmation", "group_id": 237565078 }
```

Сервер должен вернуть plain text строку подтверждения из `VK_CONFIRMATION_TOKEN`.

Для второго waiter-сообщества сервер так же принимает:

```json
{ "type": "confirmation", "group_id": 237584508 }
```

и возвращает plain text строку из `VK_WAITER_CONFIRMATION_TOKEN`.

После этого обычные события должны завершаться ответом:

```text
ok
```

## Файлы в проекте

- [config.py](/Users/maks/Documents/code%20vs/luchbarbot/config.py)
- [flask_app.py](/Users/maks/Documents/code%20vs/luchbarbot/flask_app.py)
- [.env.example](/Users/maks/Documents/code%20vs/luchbarbot/.env.example)
- [README.md](/Users/maks/Documents/code%20vs/luchbarbot/README.md)

## Следующий этап

После подтверждения callback URL:

1. вынести логику VK в отдельный `vk_handlers.py`
2. сделать стартовый сценарий меню и приветствия
3. связать VK-диалог с логикой броней
4. добавить кнопки, state-машину и обработку `message_event`
