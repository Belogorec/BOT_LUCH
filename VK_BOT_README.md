# VK Bot README

## Что это

Документ по подключению и дальнейшей работе с VK Bot для `LUCH Bot`.

## Текущая схема

- основной сервис: `BOT_LUCH`
- публичный callback URL: `https://botluch-production.up.railway.app/vk/callback`
- backend endpoint в коде: `flask_app.py`
- env-конфиг: `config.py`

## Уже реализовано

- маршрут `POST /vk/callback`
- обработка `confirmation`
- проверка `group_id`
- проверка `secret key`
- возврат plain text `ok` для следующих событий
- MVP-ответ на входящий `message_new`
- helper для исходящих сообщений через `messages.send`

Это минимальный стартовый слой, чтобы:
- подтвердить адрес сервера в VK
- безопасно принимать callback-события
- сразу проверить полный цикл входящего сообщения и тестового ответа
- потом отдельно добавить кнопки и сценарий бронирования

## Нужные ENV

- `VK_GROUP_ID`
- `VK_ACCESS_TOKEN`
- `VK_CALLBACK_SECRET`
- `VK_CONFIRMATION_TOKEN`
- `VK_API_VERSION`

## Текущие рабочие значения для настройки VK

- `URL`: `https://botluch-production.up.railway.app/vk/callback`
- `API version`: `5.199`

Секреты и токены не хранить в этом README. Они должны лежать только:
- в локальном `.env`
- в Railway Variables
- в личном хранилище доступов

## Что заполнять в VK Callback API

- `URL`: `https://botluch-production.up.railway.app/vk/callback`
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
