"""
Диалоги для сбора контактных данных и приема бронирований от пользователя.
"""
import json
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple

# Состояния диалога
STATE_AWAITING_NAME = "dialog:awaiting_name"
STATE_AWAITING_PHONE = "dialog:awaiting_phone"
STATE_AWAITING_DATE = "dialog:awaiting_date"
STATE_AWAITING_TIME = "dialog:awaiting_time"
STATE_AWAITING_GUESTS_COUNT = "dialog:awaiting_guests_count"
STATE_AWAITING_CONFIRMATION = "dialog:awaiting_confirmation"

# Состояния для запроса контактов
STATE_AWAITING_CONTACT = "dialog:awaiting_contact"

# Состояние для вопросов
STATE_AWAITING_QUESTION = "dialog:awaiting_question"


def normalize_phone(phone_raw: str) -> Optional[str]:
    """Нормализует телефон в формат E.164 (+7XXXXXXXXXX)."""
    phone = re.sub(r'[^\d+]', '', phone_raw or '').strip()

    # Обработка русских номеров
    if phone.startswith('8'):
        phone = '7' + phone[1:]
    elif phone.startswith('9'):
        phone = '7' + phone
    elif not phone.startswith('+'):
        if phone.startswith('7'):
            phone = '+' + phone
        else:
            return None

    if not phone.startswith('+'):
        phone = '+' + phone

    # Проверка что это русский номер (7 страна код, 10 цифр после +)
    if re.match(r'^\+7\d{10}$', phone):
        return phone

    return None


def validate_date(date_str: str) -> Optional[str]:
    """
    Валидирует дату. Принимает форматы: DD.MM, DD.MM.YYYY, YYYY-MM-DD.
    Возвращает дату в формате YYYY-MM-DD или None.
    """
    date_str = (date_str or '').strip()

    # Попытка парсинга разных форматов
    formats = ['%d.%m', '%d.%m.%Y', '%Y-%m-%d', '%d/%m', '%d/%m/%Y']

    for fmt in formats:
        try:
            if fmt not in ['%d.%m', '%d/%m']:  # Форматы без года
                dt = datetime.strptime(date_str, fmt)
                result_date = dt.date()
            else:  # Если нет года, предполагаем текущий или следующий год
                dt = datetime.strptime(date_str, fmt)
                today = datetime.now().date()
                candidate_date = dt.date().replace(year=today.year)
                # Если эта дата в прошлом, используем следующий год
                if candidate_date < today:
                    candidate_date = candidate_date.replace(year=today.year + 1)
                result_date = candidate_date

            # Проверяем что дата не в прошлом и не слишком далеко в будущем
            today = datetime.now().date()
            if result_date >= today and (result_date - today).days <= 365:
                return result_date.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None


def validate_time(time_str: str) -> Optional[str]:
    """
    Валидирует время. Принимает форматы: HH:MM, H:MM.
    Возвращает время в формате HH:MM или None.
    """
    time_str = (time_str or '').strip()

    # Проверяем основной формат
    match = re.match(r'^(\d{1,2}):(\d{2})$', time_str)
    if not match:
        return None

    hour, minute = int(match.group(1)), int(match.group(2))

    # Проверяем корректность
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        # Примерное время работы ресторана (можно настроить)
        if 11 <= hour <= 23:  # Работают примерно с 11:00 до 23:00
            return f"{hour:02d}:{minute:02d}"

    return None


def validate_guests_count(count_str: str) -> Optional[int]:
    """Валидирует количество гостей."""
    try:
        count = int((count_str or '').strip())
        if 1 <= count <= 20:  # От 1 до 20 гостей
            return count
    except ValueError:
        pass
    return None


def extract_phone_from_contact(contact: dict) -> Optional[str]:
    """Извлекает телефон из Telegram контакта."""
    if not contact:
        return None

    phone_raw = contact.get("phone_number", "")
    if not phone_raw:
        return None

    return normalize_phone(phone_raw)


def extract_name_from_contact(contact: dict) -> str:
    """Извлекает имя из Telegram контакта."""
    if not contact:
        return ""

    first_name = (contact.get("first_name") or "").strip()
    last_name = (contact.get("last_name") or "").strip()

    if first_name and last_name:
        return f"{first_name} {last_name}"
    return first_name or last_name or ""


def ask_for_contact(chat_id: str, user_id: str) -> Tuple[str, dict]:
    """Просит пользователя отправить контакт без нижней клавиатуры."""
    text = (
        "👋 Добрый день! Для бронирования нам нужны ваши контактные данные.\n\n"
        "Отправьте номер телефона в ответном сообщении в формате +7XXXXXXXXXX."
    )
    return text, {"force_reply": True}


def ask_name(chat_id: str, user_id: str) -> Tuple[str, dict]:
    """Запрашивает имя гостя (резервный вариант если контакт не поделился)."""
    text = "Как вас зовут?"
    return text, {"force_reply": True}


def ask_phone(chat_id: str, user_id: str) -> Tuple[str, dict]:
    """Запрашивает телефон гостя (резервный вариант)."""
    text = "📱 Укажите ваш телефон (+7XXXXXXXXXX или 8XXXXXXXXXX):"
    return text, {"force_reply": True}


def get_existing_user_data(conn, user_id: str) -> Optional[Tuple[str, str]]:
    """Проверяет есть ли у пользователя предыдущие бронирования и возвращает имя и телефон."""
    row = conn.execute(
        """
        SELECT name, phone_e164
        FROM bookings
        WHERE user_chat_id=? OR telegram_chat_id=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user_id, user_id),
    ).fetchone()
    
    if row and row["phone_e164"]:
        return row["name"] or "", row["phone_e164"]
    
    return None


def start_booking_dialog(conn, chat_id: str, user_id: str, user_name: str) -> Tuple[str, dict]:
    """Начинает диалог приема бронирования с запроса контактов."""
    # Проверяем есть ли у пользователя предыдущие бронирования
    existing_data = get_existing_user_data(conn, user_id)
    
    if existing_data:
        # Пользователь уже бронировал раньше - пропускаем запрос контактов
        name, phone = existing_data
        greeting_name = name.split()[0] if name else "снова"
        text = (
            f"👋 Привет, {greeting_name}!\n\n"
            "Рад видеть вас снова. Давайте забронируем стол.\n\n"
            "Когда вы хотите забронировать стол? 📅\n\n"
            "Пожалуйста, укажите дату (например: 25.03 или 25.03.2026)"
        )
        return text, {"force_reply": True}
    
    # Новый пользователь - сразу просим поделиться контактом через встроенную кнопку Telegram
    return ask_for_contact(chat_id, user_id)


def ask_date(chat_id: str, user_id: str) -> Tuple[str, dict]:
    """Запрашивает дату бронирования."""
    text = "Когда вы хотите забронировать стол? 📅\n\nПожалуйста, укажите дату (например: 25.03 или 25.03.2026)"
    return text, {"force_reply": True}


def ask_time(chat_id: str, user_id: str) -> Tuple[str, dict]:
    """Запрашивает время бронирования."""
    text = "В какое время? ⏰\n\nУкажите время в формате ЧЧ:МММ (например: 19:30)"
    return text, {"force_reply": True}


def ask_guests_count(chat_id: str, user_id: str) -> Tuple[str, dict]:
    """Запрашивает количество гостей."""
    text = "Сколько гостей? 👥\n\nУкажите количество (от 1 до 20)"
    return text, {"force_reply": True}


def confirm_booking(
    name: str,
    phone: str,
    date: str,
    time: str,
    guests_count: int
) -> Tuple[str, dict]:
    """Выводит подтверждение бронирования перед финальным подтверждением."""
    text = (
        "✅ <b>Проверьте данные бронирования:</b>\n\n"
        f"<b>Имя:</b> {name}\n"
        f"<b>Телефон:</b> {phone}\n"
        f"<b>Дата:</b> {date}\n"
        f"<b>Время:</b> {time}\n"
        f"<b>Гостей:</b> {guests_count}\n\n"
        "Подтвердить бронирование?"
    )

    kb = {
        "inline_keyboard": [
            [
                {"text": "✅ Да, подтвердить", "callback_data": "booking:confirm_yes"},
                {"text": "❌ Отмена", "callback_data": "booking:confirm_no"}
            ]
        ]
    }

    return text, kb


def booking_confirmed_user_message(booking_id: int) -> Tuple[str, dict]:
    """Сообщение пользователю об успешном получении брони (до подтверждения админом)."""
    text = (
        "📋 <b>Бронирование получено!</b>\n\n"
        "Статус: ⏳ Ожидание подтверждения\n\n"
        "Наша команда проверит вашу бронь и отправит финальное подтверждение.\n\n"
        "Если у вас есть вопросы - нажмите кнопку ниже:"
    )

    kb = {
        "inline_keyboard": [
            [
                {"text": "❓ Задать вопрос", "callback_data": f"booking:{booking_id}:question"}
            ]
        ]
    }

    return text, kb


def booking_confirmed_final_message(name: str, date: str, time: str, guests_count: int, booking_id: int = 0) -> Tuple[str, dict]:
    """Финальное подтверждение пользователю после подтверждения админом."""
    text = (
        "🎉 <b>Ваша бронь подтверждена!</b>\n\n"
        f"<b>Имя:</b> {name}\n"
        f"<b>Дата:</b> {date}\n"
        f"<b>Время:</b> {time}\n"
        f"<b>Гостей:</b> {guests_count}\n\n"
        "Спасибо! Мы ждём вас в LUCHBAR! 🍸"
    )
    
    kb_rows = []
    if booking_id > 0:
        kb_rows.append([{"text": "❓ Задать вопрос", "callback_data": f"booking:{booking_id}:question"}])
    
    kb = {"inline_keyboard": kb_rows} if kb_rows else {}
    return text, kb


def save_dialog_state(
    conn,
    chat_id: str,
    user_id: str,
    state: str,
    data: dict,
    prompt_message_id: str
):
    """Сохраняет состояние диалога."""
    expires = (datetime.utcnow() + timedelta(hours=1)).isoformat(timespec="seconds")

    # Сохраняем всё данные в phone_e164 как JSON (переиспользуем колонку для хранилища)
    dialog_json = json.dumps({"state": state, **data}, ensure_ascii=False)

    conn.execute(
        """
        INSERT INTO pending_replies (kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at)
        VALUES ('booking_dialog', 0, ?, ?, ?, ?, ?)
        """,
        (dialog_json, chat_id, user_id, prompt_message_id, expires),
    )


def get_dialog_state(
    conn,
    chat_id: str,
    user_id: str,
    prompt_message_id: Optional[str] = None
) -> Optional[Tuple[str, dict]]:
    """Получает сохраненное состояние диалога."""
    where = "chat_id=? AND actor_tg_id=? AND kind='booking_dialog'"
    params = [chat_id, user_id]

    if prompt_message_id:
        where += " AND prompt_message_id=?"
        params.append(prompt_message_id)

    row = conn.execute(
        f"SELECT phone_e164, expires_at FROM pending_replies WHERE {where} ORDER BY id DESC LIMIT 1",
        params
    ).fetchone()

    if not row:
        return None

    try:
        exp = datetime.fromisoformat(str(row["expires_at"]))
        if datetime.utcnow() > exp:
            return None

        # phone_e164 содержит JSON с данными диалога
        data = json.loads(row["phone_e164"] or "{}")
        state = data.pop("state", None)
        if not state:
            return None
        return state, data
    except (json.JSONDecodeError, ValueError):
        return None


def clear_dialog_state(conn, chat_id: str, user_id: str):
    """Очищает состояние диалога."""
    conn.execute(
        "DELETE FROM pending_replies WHERE chat_id=? AND actor_tg_id=? AND kind='booking_dialog'",
        (chat_id, user_id),
    )


def ask_question(chat_id: str, user_id: str) -> Tuple[str, dict]:
    """Запрашивает вопрос о бронировании."""
    text = (
        "❓ <b>Задайте ваш вопрос</b>\n\n"
        "Напишите ваш вопрос - он будет отправлен нашей команде и вы получите ответ через этого бота."
    )
    return text, {"force_reply": True}


def save_booking_question(conn, booking_id: int, phone_e164: str, question: str, chat_id: str, user_id: str):
    """Сохраняет вопрос о бронировании."""
    conn.execute(
        """
        INSERT INTO pending_replies (kind, booking_id, phone_e164, chat_id, actor_tg_id, expires_at)
        VALUES ('booking_question', ?, ?, ?, ?, datetime('now', '+7 day'))
        """,
        (booking_id, question, chat_id, user_id),
    )
