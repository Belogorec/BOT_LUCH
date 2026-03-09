# Инструкции по развертыванию на PythonAnywhere

## Файлы для загрузки на PythonAnywhere

На сервер нужно загрузить по следующим путям:

```
/home/Barluch/luchbarbot/
├── booking_dialog.py        ← НОВЫЙ файл (скопировать)
├── tg_handlers.py           ← ОБНОВИТЬ (скопировать `tg_handlers.py`)
├── booking_render.py        ← без изменений
├── booking_service.py       ← без изменений
├── telegram_api.py          ← без изменений
├── db.py                    ← без изменений
├── config.py                ← без изменений
└── ...остальные файлы
```

## Пошаговая инструкция

### 1. Загрузить новый файл `booking_dialog.py`
```bash
# На локальной машине
scp /Users/maks/Documents/code\ vs/luchbarbot/booking_dialog.py \
    Barluch@ssh.pythonanywhere.com:/home/Barluch/luchbarbot/
```

### 2. Обновить `tg_handlers.py`
```bash
# На локальной машине
scp /Users/maks/Documents/code\ vs/luchbarbot/tg_handlers.py \
    Barluch@ssh.pythonanywhere.com:/home/Barluch/luchbarbot/
```

### 3. Перезагрузить приложение Flask
- Зайти на https://www.pythonanywhere.com
- Перейти на вкладку "Web"
- Нажать "Reload" для приложения luchbarbot

### 4. Проверить логи
```bash
# SSH в pythonanywhere
ssh Barluch@ssh.pythonanywhere.com

# Проверить логи ошибок
tail -f /var/log/Barluch.pythonanywhere.com.error.log

# Проверить логи доступа
tail -f /var/log/Barluch.pythonanywhere.com.access.log
```

## Если возникают ошибки

### Ошибка: "ModuleNotFoundError: No module named 'booking_dialog'"
- Убедитесь что файл `booking_dialog.py` загружен в правильную директорию
- Проверьте права доступа: `chmod 644 /home/Barluch/luchbarbot/booking_dialog.py`

### Ошибка: "ImportError" в логах
- Перезагрузите Flask приложение в PythonAnywhere
- Дождитесь завершения reload (может занять 30 секунд)

### Диалог не работает в Telegram
1. Проверьте что Flask приложение перезагружено
2. Отправьте `/book` в Telegram и проверьте ответ бота
3. Если ошибка - посмотрите логи в PythonAnywhere

## Важно!

**Всегда загружайте следующие файлы:**
- `booking_dialog.py` — сам модуль диалога
- `tg_handlers.py` — обновленные обработчики (новая версия заменяет старую tg_handlers.py)

**Локально для разработки:**
- `tg_handlers.py` — остаютсяр версионированные копии
- `booking_dialog.py` — основной модуль (уже существует)

## Проверка после развертывания

### В Telegram боте:
1. Напишите `/book`
2. Должно появиться сообщение с просьбой указать имя
3. Ответьте "Иван" (или любое имя)
4. Должно попросить телефон в формате +7XXXXXXXXXX

### В БД на PythonAnywhere:
```sql
-- Проверить новые брони
SELECT * FROM bookings ORDER BY id DESC LIMIT 3;

-- Проверить события
SELECT * FROM booking_events ORDER BY id DESC LIMIT 5;
```

## Откаты если что-то сломалось

Если нужно откатить изменения:

```bash
# На PythonAnywhere удалить новый файл
rm /home/Barluch/luchbarbot/booking_dialog.py

# Восстановить старую версию обработчика (из backup если есть)
# Или временно отключить диалог в tg_handlers.py
```

## Локальные версии для разработки

На локальной машине есть:
- `tg_handlers.py` — версионированная копия для разработки
- `tg_handlers.py` — production версия для PythonAnywhere

**После изменений на локальной машине:**
1. Отредактируйте `tg_handlers.py`
2. Скопируйте в `tg_handlers.py` для production
3. Загрузите `tg_handlers.py` на PythonAnywhere
