# Changelog

## 2026-04-04

### `2ae29d2` Remind admins to assign table after deposit
- После установки депозита без стола бот напоминает администратору назначить стол.
- Это закрывает “тихий” сценарий, когда депозит уже есть, а официанты ещё не уведомлены.

## 2026-04-03

### `f541250` Standardize bot local waiter logs
- Локальные waiter-логи переведены на единый формат `[PREFIX] key=value`.
- Добавлен helper [local_log.py](/Users/maks/Documents/code%20vs/luchbarbot/local_log.py).

### `c3d1cfe` Harden waiter notification flow logging
- Добавлены явные `skip/send/sent` логи по waiter-уведомлениям.
- `message is not modified` больше не валит поток рядом с Telegram-обновлением.

### `b54ec86` Lock down waiter chat interactions
- Waiter group переведена в режим “только получение уведомлений”.
- Команды и входящие действия из этого чата игнорируются.

### `b405c01` Add waiter group notifications for deposit bookings
- Добавлены уведомления в группу официантов при состоянии `стол + депозит`.
- В сообщении поддержаны дата, время, бронь, депозит, комментарии и количество гостей.

## Более ранние опорные изменения

### `cfc33b7` Support clearing booking deposits from CRM
- Бот поддерживает снятие депозита через CRM sync.

### `5561bd0` Move table restrictions when reassigning bookings
- Логика ограничений по столам обновлена под перенос брони между столами.
