import html
import json
from datetime import datetime, timedelta

from flask import request, abort

from config import TG_WEBHOOK_SECRET, PROMO_ADMIN_IDS, TG_CHAT_ID
from telegram_api import tg_send_message, tg_edit_message, tg_answer_callback
from booking_service import (
    compute_segment,
    upsert_guest_if_missing,
    log_booking_event,
    log_guest_event,
    add_guest_note,
    toggle_guest_tag,
    ensure_visit_from_confirmed_booking,
    mark_booking_cancelled,
)
from booking_render import render_booking_card
from db import connect, init_schema


def _h(s: str) -> str:
    return html.escape(s or "", quote=False)


def ensure_db():
    conn = connect()
    init_schema(conn)
    conn.commit()
    return conn


def tg_webhook_impl():
    if TG_WEBHOOK_SECRET:
        hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if hdr != TG_WEBHOOK_SECRET:
            abort(403)

    update = request.get_json(silent=True) or {}

    conn = ensure_db()
    try:
        if "callback_query" in update:
            cq = update["callback_query"] or {}
            cq_id = str(cq.get("id") or "")
            data = str(cq.get("data") or "")
            actor = cq.get("from") or {}
            actor_id = str(actor.get("id") or "")
            actor_name = (actor.get("username") or actor.get("first_name") or "").strip()

            msg = cq.get("message") or {}
            chat = msg.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            message_id = str(msg.get("message_id") or "")

            if data.startswith("promo:redeem:"):
                code = data.replace("promo:redeem:", "", 1).strip()

                if actor_id not in PROMO_ADMIN_IDS:
                    tg_answer_callback(cq_id, "Нет доступа")
                    return {"ok": True}

                row = conn.execute(
                    "SELECT code, status FROM discount_codes WHERE code=?",
                    (code,),
                ).fetchone()

                if not row:
                    tg_answer_callback(cq_id, "Карта не найдена")
                    return {"ok": True}

                if row["status"] == "USED":
                    tg_answer_callback(cq_id, "Карта уже использована")
                    tg_send_message(chat_id, f"❌ Карта <b>{_h(code)}</b> уже была использована ранее.")
                    return {"ok": True}

                conn.execute(
                    """
                    UPDATE discount_codes
                    SET status='USED',
                        redeemed_at=datetime('now'),
                        redeemed_by_tg_id=?
                    WHERE code=? AND status='ACTIVE'
                    """,
                    (actor_id, code),
                )

                tg_answer_callback(cq_id, "Скидка проведена")
                tg_send_message(chat_id, f"✅ Скидка по карте <b>{_h(code)}</b> проведена.")
                return {"ok": True}

            parts = data.split(":")
            if len(parts) >= 3 and parts[0] == "b":
                booking_id = int(parts[1])

                b = conn.execute("SELECT id, phone_e164 FROM bookings WHERE id=?", (booking_id,)).fetchone()
                if not b:
                    tg_answer_callback(cq_id, "Бронь не найдена")
                    return {"ok": True}

                phone = b["phone_e164"] or ""
                if phone:
                    upsert_guest_if_missing(conn, phone, "")

                if parts[2] == "tag" and len(parts) >= 4:
                    tag = parts[3].strip().upper()
                    if not phone:
                        tg_answer_callback(cq_id, "Нет телефона у брони")
                        return {"ok": True}

                    tags2, action = toggle_guest_tag(conn, phone, tag)
                    log_guest_event(conn, phone, action, actor_id, actor_name, {"tag": tag})

                    g_row = conn.execute("SELECT visits_count FROM guests WHERE phone_e164=?", (phone,)).fetchone()
                    visits_count = int(g_row["visits_count"] or 0) if g_row else 0
                    seg = compute_segment(visits_count, tags2)
                    conn.execute("UPDATE bookings SET guest_segment=?, updated_at=datetime('now') WHERE id=?", (seg, booking_id))

                    text, kb = render_booking_card(conn, booking_id)
                    tg_edit_message(chat_id, message_id, text, kb)
                    tg_answer_callback(cq_id, "Готово")
                    return {"ok": True}

                if parts[2] == "booking" and len(parts) >= 4:
                    action = parts[3].strip().lower()

                    if action == "confirm":
                        conn.execute("UPDATE bookings SET status='CONFIRMED', updated_at=datetime('now') WHERE id=?", (booking_id,))
                        log_booking_event(conn, booking_id, "CONFIRMED", actor_id, actor_name, {})
                        ensure_visit_from_confirmed_booking(conn, booking_id, actor_id, actor_name)

                        text, kb = render_booking_card(conn, booking_id)
                        tg_edit_message(chat_id, message_id, text, kb)
                        tg_answer_callback(cq_id, "Подтверждено")
                        return {"ok": True}

                    if action == "cancel":
                        mark_booking_cancelled(conn, booking_id, actor_id, actor_name)
                        text, kb = render_booking_card(conn, booking_id)
                        tg_edit_message(chat_id, message_id, text, kb)
                        tg_answer_callback(cq_id, "Отменено")
                        return {"ok": True}

                if parts[2] == "note":
                    if not phone:
                        tg_answer_callback(cq_id, "Нет телефона у брони")
                        return {"ok": True}

                    prompt_text = (
                        "<b>Комментарий к гостю</b>\n"
                        f"Бронь #{booking_id}\n"
                        f"Телефон: <a href=\"tel:{_h(phone)}\">{_h(phone)}</a>\n\n"
                        "Напишите следующим сообщением текст комментария. Можно без reply, в течение 10 минут."
                    )

                    prompt_markup = {"force_reply": True, "selective": True}
                    prompt_msg_id = tg_send_message(chat_id, prompt_text, reply_markup=prompt_markup)

                    expires = (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds")
                    conn.execute(
                        """
                        INSERT INTO pending_replies (kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at)
                        VALUES ('guest_note', ?, ?, ?, ?, ?, ?)
                        """,
                        (booking_id, phone, chat_id, actor_id, str(prompt_msg_id), expires),
                    )

                    tg_answer_callback(cq_id, "Ожидаю текст")
                    return {"ok": True}

            tg_answer_callback(cq_id)
            return {"ok": True}

        if "message" in update:
            m = update["message"] or {}
            chat = m.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            from_ = m.get("from") or {}
            actor_id = str(from_.get("id") or "")
            actor_name = (from_.get("username") or from_.get("first_name") or "").strip()

            web_app_data = (m.get("web_app_data") or {}).get("data")
            if web_app_data:
                try:
                    payload = json.loads(str(web_app_data))
                except Exception:
                    tg_send_message(chat_id, "Не удалось прочитать данные формы. Попробуйте ещё раз.")
                    return {"ok": True}

                date_value = str(payload.get("date") or "").strip()
                time_value = str(payload.get("time") or "").strip()
                guests_value = str(payload.get("guests") or "").strip()
                comment_value = str(payload.get("comment") or "").strip()

                if not date_value or not time_value or not guests_value:
                    tg_send_message(chat_id, "Форма заполнена не полностью. Откройте её ещё раз.")
                    return {"ok": True}

                user_text = (
                    "✅ <b>Запрос на бронь принят</b>\n\n"
                    f"Дата: <b>{_h(date_value)}</b>\n"
                    f"Время: <b>{_h(time_value)}</b>\n"
                    f"Гостей: <b>{_h(guests_value)}</b>\n"
                )
                if comment_value:
                    user_text += f"Комментарий: {_h(comment_value)}\n"
                user_text += "\nСкоро с вами свяжемся."

                admin_text = (
                    "🆕 <b>Новая заявка из Telegram Mini App</b>\n\n"
                    f"Пользователь: <b>{_h(actor_name or 'Без имени')}</b>\n"
                    f"TG ID: <code>{_h(actor_id)}</code>\n"
                    f"Дата: <b>{_h(date_value)}</b>\n"
                    f"Время: <b>{_h(time_value)}</b>\n"
                    f"Гостей: <b>{_h(guests_value)}</b>\n"
                )
                if comment_value:
                    admin_text += f"Комментарий: {_h(comment_value)}\n"

                tg_send_message(chat_id, user_text)
                tg_send_message(str(TG_CHAT_ID), admin_text)
                return {"ok": True}

            text = (m.get("text") or "").strip()

            if text.startswith("/start"):
                parts = text.split()

                if len(parts) > 1 and parts[1].startswith("promo_"):
                    code = parts[1].replace("promo_", "").strip()

                    row = conn.execute(
                        "SELECT code, status FROM discount_codes WHERE code=?",
                        (code,),
                    ).fetchone()

                    if not row:
                        tg_send_message(chat_id, "❌ Эта подарочная карта не найдена.")
                        return {"ok": True}

                    status = row["status"]

                    if status == "USED":
                        tg_send_message(chat_id, "❌ Эта подарочная карта уже была использована.")
                        return {"ok": True}

                    text_msg = (
                        "🎁 <b>Подарочная карта LUCHBAR</b>\n\n"
                        "Ваша карта активна.\n\n"
                        "Скидка: <b>15%</b>\n"
                        "Действует до: <b>31 мая</b>\n\n"
                        "Вы можете воспользоваться скидкой "
                        "один раз, предъявив открытку с QR-кодом официанту.\n\n"
                        "Будем рады видеть вас в LUCHBAR."
                    )

                    inline_rows = [
                        [
                            {
                                "text": "🍸 Забронировать стол",
                                "url": "https://barluch.ru/reserve"
                            }
                        ]
                    ]

                    if actor_id in PROMO_ADMIN_IDS:
                        inline_rows.append(
                            [
                                {
                                    "text": "✅ Провести скидку",
                                    "callback_data": f"promo:redeem:{code}"
                                }
                            ]
                        )

                    kb = {"inline_keyboard": inline_rows}
                    tg_send_message(chat_id, text_msg, kb)
                    return {"ok": True}

                start_text = (
                    "🍸 <b>LUCHBAR</b>\n\n"
                    "Откройте форму бронирования прямо внутри Telegram.\n"
                    "Там можно выбрать дату, время, количество гостей и комментарий."
                )
                start_kb = {
                    "keyboard": [
                        [
                            {
                                "text": "Открыть форму брони",
                                "web_app": {
                                    "url": "https://botluch-production.up.railway.app/miniapp/reserve"
                                }
                            }
                        ]
                    ],
                    "resize_keyboard": True,
                    "one_time_keyboard": True
                }
                tg_send_message(chat_id, start_text, start_kb)
                return {"ok": True}

            if text == "/myid":
                is_admin = actor_id in PROMO_ADMIN_IDS
                admins_preview = ", ".join(PROMO_ADMIN_IDS[:10]) if PROMO_ADMIN_IDS else "(пусто)"
                tg_send_message(
                    chat_id,
                    "\n".join([
                        "<b>Профиль Telegram</b>",
                        f"ID: <code>{_h(actor_id)}</code>",
                        f"username: @{_h(actor_name) if actor_name else '—'}",
                        f"PROMO_ADMIN_IDS (загружено): <code>{_h(admins_preview)}</code>",
                        f"admin доступ QR: <b>{'ДА' if is_admin else 'НЕТ'}</b>",
                    ])
                )
                return {"ok": True}

            if text == "/stat":
                if actor_id not in PROMO_ADMIN_IDS:
                    tg_send_message(chat_id, "Нет доступа.")
                    return {"ok": True}

                total_users_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM tg_bot_users"
                ).fetchone()
                total_users = int(total_users_row["c"] or 0) if total_users_row else 0

                promo_users_row = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM tg_bot_users
                    WHERE last_start_param LIKE 'promo_%'
                    """
                ).fetchone()
                promo_users = int(promo_users_row["c"] or 0) if promo_users_row else 0

                used_qr_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM discount_codes WHERE status='USED'"
                ).fetchone()
                used_qr = int(used_qr_row["c"] or 0) if used_qr_row else 0

                recent_redeems = conn.execute(
                    """
                    SELECT code, redeemed_at, redeemed_by_tg_id
                    FROM discount_codes
                    WHERE status='USED'
                    ORDER BY datetime(redeemed_at) DESC, id DESC
                    LIMIT 15
                    """
                ).fetchall()

                lines = [
                    "<b>Статистика бота</b>",
                    "",
                    f"Пользователей, нажавших /start: <b>{total_users}</b>",
                    f"Из них открывали promo QR: <b>{promo_users}</b>",
                    f"Погашено QR: <b>{used_qr}</b>",
                ]

                if recent_redeems:
                    lines.extend(["", "<b>Последние погашения:</b>"])
                    for r in recent_redeems:
                        code = _h(r["code"] or "")
                        redeemed_at = _h(r["redeemed_at"] or "—")
                        admin_id = _h(r["redeemed_by_tg_id"] or "—")
                        lines.append(f"• {code} — {redeemed_at} — админ {admin_id}")

                tg_send_message(chat_id, "\n".join(lines))
                return {"ok": True}

            if not text:
                return {"ok": True}

            reply_to = m.get("reply_to_message")
            prompt_mid = str(reply_to.get("message_id") or "") if reply_to else ""

            row = None

            if prompt_mid:
                row = conn.execute(
                    """
                    SELECT id, booking_id, phone_e164, expires_at
                    FROM pending_replies
                    WHERE chat_id=? AND prompt_message_id=? AND actor_tg_id=? AND kind='guest_note'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (chat_id, prompt_mid, actor_id),
                ).fetchone()

            if row is None:
                row = conn.execute(
                    """
                    SELECT id, booking_id, phone_e164, expires_at
                    FROM pending_replies
                    WHERE chat_id=? AND actor_tg_id=? AND kind='guest_note'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (chat_id, actor_id),
                ).fetchone()

            if not row:
                return {"ok": True}

            try:
                exp = datetime.fromisoformat(str(row["expires_at"]))
                if datetime.utcnow() > exp:
                    conn.execute("DELETE FROM pending_replies WHERE id=?", (row["id"],))
                    return {"ok": True}
            except Exception:
                pass

            booking_id = int(row["booking_id"])
            phone = str(row["phone_e164"] or "")

            add_guest_note(conn, phone, text, actor_id, actor_name)
            conn.execute("DELETE FROM pending_replies WHERE id=?", (row["id"],))

            b = conn.execute(
                "SELECT telegram_chat_id, telegram_message_id FROM bookings WHERE id=?",
                (booking_id,),
            ).fetchone()
            if b and b["telegram_chat_id"] and b["telegram_message_id"]:
                card_text, kb = render_booking_card(conn, booking_id)
                tg_edit_message(str(b["telegram_chat_id"]), str(b["telegram_message_id"]), card_text, kb)

            tg_send_message(chat_id, "Сохранён в базе.")
            return {"ok": True}

        return {"ok": True}
    finally:
        conn.commit()
        conn.close()


def tg_webhook_impl_OLD_BACKUP():
    if TG_WEBHOOK_SECRET:
        hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if hdr != TG_WEBHOOK_SECRET:
            abort(403)

    update = request.get_json(silent=True) or {}

    conn = ensure_db()
    try:
        if "callback_query" in update:
            cq = update["callback_query"] or {}
            cq_id = str(cq.get("id") or "")
            data = str(cq.get("data") or "")
            actor = cq.get("from") or {}
            actor_id = str(actor.get("id") or "")
            actor_name = (actor.get("username") or actor.get("first_name") or "").strip()

            msg = cq.get("message") or {}
            chat = msg.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            message_id = str(msg.get("message_id") or "")

            # ===== Обработка меню кнопок =====
            if data.startswith("menu:"):
                action = data.replace("menu:", "", 1).strip()

                if action == "book":
                    # Начинаем диалог бронирования
                    msg_text, markup = start_booking_dialog(conn, chat_id, actor_id, actor_name)
                    prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup=markup)
                    
                    # Проверяем есть ли у пользователя данные из предыдущих броней
                    existing_data = get_existing_user_data(conn, actor_id)
                    
                    if existing_data:
                        # Возвращающийся пользователь - сохраняем его данные и начинаем с даты
                        name, phone = existing_data
                        save_dialog_state(
                            conn,
                            chat_id,
                            actor_id,
                            STATE_AWAITING_DATE,
                            {"name": name, "phone_e164": phone},
                            prompt_msg_id or "0"
                        )
                    else:
                        # Новый пользователь - начинаем с запроса контактов
                        save_dialog_state(
                            conn,
                            chat_id,
                            actor_id,
                            STATE_AWAITING_CONTACT,
                            {},
                            prompt_msg_id or "0"
                        )
                    
                    tg_answer_callback(cq_id, "Открыт диалог бронирования")

                elif action == "events":
                    tg_answer_callback(cq_id, "")
                    tg_send_message(
                        chat_id,
                        "📅 <b>События LUCHBAR</b>\n\n"
                        "Актуальная информация о мероприятиях:\n"
                        "👉 <a href=\"http://barluch.ru/\">barluch.ru</a>"
                    )

                elif action == "pdf":
                    tg_answer_callback(cq_id, "")
                    tg_send_message(
                        chat_id,
                        "📋 <b>Меню ресторана</b>\n\n"
                        "Посмотреть меню:\n"
                        "👉 <a href=\"http://barluch.ru/qr-menu\">barluch.ru/qr-menu</a>"
                    )

                return {"ok": True}

            if data.startswith("promo:redeem:"):
                code = data.replace("promo:redeem:", "", 1).strip().upper()

                if actor_id not in PROMO_ADMIN_IDS:
                    tg_answer_callback(cq_id, "Нет доступа")
                    return {"ok": True}

                row = conn.execute(
                    "SELECT code, status FROM discount_codes WHERE code=?",
                    (code,),
                ).fetchone()

                if not row:
                    seed_discount_codes_from_csv(conn)
                    row = conn.execute(
                        "SELECT code, status FROM discount_codes WHERE code=?",
                        (code,),
                    ).fetchone()

                if not row:
                    tg_answer_callback(cq_id, "Карта не найдена")
                    return {"ok": True}

                if row["status"] == "USED":
                    tg_answer_callback(cq_id, "Карта уже использована")
                    tg_send_message(chat_id, f"❌ Карта <b>{_h(code)}</b> уже была использована ранее.")
                    return {"ok": True}

                conn.execute(
                    """
                    UPDATE discount_codes
                    SET status='USED',
                        redeemed_at=datetime('now'),
                        redeemed_by_tg_id=?
                    WHERE code=? AND status='ACTIVE'
                    """,
                    (actor_id, code),
                )

                tg_answer_callback(cq_id, "Скидка проведена")
                tg_send_message(chat_id, f"✅ Скидка по карте <b>{_h(code)}</b> проведена.")
                return {"ok": True}

            # Кнопка вопроса у пользователя: booking:{id}:question
            if data.startswith("booking:") and not data.startswith("booking:confirm_"):
                parts = data.split(":")
                if len(parts) >= 3 and parts[2] == "question":
                    booking_id = int(parts[1])
                    save_dialog_state(
                        conn,
                        chat_id,
                        actor_id,
                        STATE_AWAITING_QUESTION,
                        {"booking_id": booking_id},
                        "0"
                    )
                    msg_text, markup = ask_question(chat_id, actor_id)
                    tg_send_message(chat_id, msg_text, markup)
                    tg_answer_callback(cq_id, "Введите ваш вопрос")
                    return {"ok": True}

            parts = data.split(":")
            if len(parts) >= 3 and parts[0] == "b":
                booking_id = int(parts[1])

                b = conn.execute("SELECT id, phone_e164 FROM bookings WHERE id=?", (booking_id,)).fetchone()
                if not b:
                    tg_answer_callback(cq_id, "Бронь не найдена")
                    return {"ok": True}

                phone = b["phone_e164"] or ""
                if phone:
                    upsert_guest_if_missing(conn, phone, "")

                if parts[2] == "tag" and len(parts) >= 4:
                    tag = parts[3].strip().upper()
                    if not phone:
                        tg_answer_callback(cq_id, "Нет телефона у брони")
                        return {"ok": True}

                    tags2, action = toggle_guest_tag(conn, phone, tag)
                    log_guest_event(conn, phone, action, actor_id, actor_name, {"tag": tag})

                    g_row = conn.execute("SELECT visits_count FROM guests WHERE phone_e164=?", (phone,)).fetchone()
                    visits_count = int(g_row["visits_count"] or 0) if g_row else 0
                    seg = compute_segment(visits_count, tags2)
                    conn.execute(
                        "UPDATE bookings SET guest_segment=?, updated_at=datetime('now') WHERE id=?",
                        (seg, booking_id),
                    )

                    text, kb = render_booking_card(conn, booking_id)
                    tg_edit_message(chat_id, message_id, text, kb)
                    tg_answer_callback(cq_id, "Готово")
                    return {"ok": True}

                if parts[2] == "booking" and len(parts) >= 4:
                    action = parts[3].strip().lower()

                    if action == "confirm":
                        conn.execute(
                            "UPDATE bookings SET status='CONFIRMED', updated_at=datetime('now') WHERE id=?",
                            (booking_id,),
                        )
                        log_booking_event(conn, booking_id, "CONFIRMED", actor_id, actor_name, {})
                        ensure_visit_from_confirmed_booking(conn, booking_id, actor_id, actor_name)

                        # Получаем данные брони
                        b = conn.execute(
                            "SELECT name, phone_e164, reservation_date, reservation_time, guests_count, user_chat_id, telegram_chat_id FROM bookings WHERE id=?",
                            (booking_id,)
                        ).fetchone()

                        # Отправляем финальное подтверждение пользователю
                        user_chat_id = ""
                        if b:
                            user_chat_id = str((b["user_chat_id"] or b["telegram_chat_id"] or "")).strip()
                        if user_chat_id:
                            final_msg, final_kb = booking_confirmed_final_message(
                                b["name"] or "Гость",
                                b["reservation_date"] or "—",
                                b["reservation_time"] or "—",
                                b["guests_count"] or 0,
                                booking_id
                            )
                            tg_send_message(user_chat_id, final_msg, reply_markup=final_kb if final_kb else None)

                        text, kb = render_booking_card(conn, booking_id)
                        tg_edit_message(chat_id, message_id, text, kb)
                        tg_answer_callback(cq_id, "Подтверждено и уведомлено")
                        return {"ok": True}

                    if action == "cancel":
                        mark_booking_cancelled(conn, booking_id, actor_id, actor_name)
                        text, kb = render_booking_card(conn, booking_id)
                        tg_edit_message(chat_id, message_id, text, kb)
                        tg_answer_callback(cq_id, "Отменено")
                        return {"ok": True}

                if parts[2] == "question":
                    # Пользователь хочет задать вопрос по бронирванию
                    # Переводим диалог в режим ввода вопроса
                    save_dialog_state(
                        conn,
                        chat_id,
                        actor_id,
                        STATE_AWAITING_QUESTION,
                        {"booking_id": booking_id},
                        "0"
                    )
                    msg_text, markup = ask_question(chat_id, actor_id)
                    tg_send_message(chat_id, msg_text, markup)
                    tg_answer_callback(cq_id, "Введите ваш вопрос")
                    return {"ok": True}

                if parts[2] == "answer_question":
                    # Админ нажал кнопку ответить на вопрос - сохраняем кнопку для последующей обработки
                    prompt_text = (
                        f"<b>Ответ на вопрос по бронированию #{booking_id}</b>\n\n"
                        "Напишите ответ для пользователя:"
                    )
                    prompt_markup = {"force_reply": True, "selective": True}
                    prompt_msg_id = tg_send_message(chat_id, prompt_text, reply_markup=prompt_markup)

                    # Сохраняем в pending_replies что ждем ответа админа
                    expires = (datetime.utcnow() + timedelta(hours=1)).isoformat(timespec="seconds")
                    conn.execute(
                        """
                        INSERT INTO pending_replies (kind, booking_id, chat_id, actor_tg_id, prompt_message_id, expires_at)
                        VALUES ('admin_question_answer', ?, ?, ?, ?, ?)
                        """,
                        (booking_id, chat_id, actor_id, str(prompt_msg_id), expires),
                    )

                    tg_answer_callback(cq_id, "Ожидаю ответ")
                    return {"ok": True}

                if parts[2] == "note":
                    if not phone:
                        tg_answer_callback(cq_id, "Нет телефона у брони")
                        return {"ok": True}

                    prompt_text = (
                        "<b>Комментарий к гостю</b>\n"
                        f"Бронь #{booking_id}\n"
                        f"Телефон: <a href=\"tel:{_h(phone)}\">{_h(phone)}</a>\n\n"
                        "Напишите следующим сообщением текст комментария. Можно без reply, в течение 10 минут."
                    )

                    prompt_markup = {"force_reply": True, "selective": True}
                    prompt_msg_id = tg_send_message(chat_id, prompt_text, reply_markup=prompt_markup)

                    expires = (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds")
                    conn.execute(
                        """
                        INSERT INTO pending_replies (kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at)
                        VALUES ('guest_note', ?, ?, ?, ?, ?, ?)
                        """,
                        (booking_id, phone, chat_id, actor_id, str(prompt_msg_id), expires),
                    )

                    tg_answer_callback(cq_id, "Ожидаю текст")
                    return {"ok": True}

            # ===== Обработка подтверждения/отмены бронирования =====
            if data.startswith("booking:confirm_"):
                action = data.replace("booking:confirm_", "", 1).strip()

                # Получаем данные диалога
                dialog_state_data = get_dialog_state(conn, chat_id, actor_id)

                if not dialog_state_data or dialog_state_data[0] != STATE_AWAITING_CONFIRMATION:
                    tg_answer_callback(cq_id, "❌ Диалог истек")
                    return {"ok": True}

                state, booking_data = dialog_state_data

                if action == "yes":
                    # Создаем бронирование в БД
                    phone = booking_data.get('phone_e164', '')
                    name = booking_data.get('name', '')
                    date = booking_data.get('date', '')
                    time = booking_data.get('time', '')
                    guests_count = booking_data.get('guests_count', 0)

                    from datetime import datetime as dt
                    reservation_dt = f"{date}T{time}"

                    # Вставляем бронирование
                    cursor = conn.execute(
                        """
                        INSERT INTO bookings
                          (name, phone_e164, reservation_date, reservation_time, reservation_dt,
                                    guests_count, status, telegram_chat_id, user_chat_id, created_at, updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, 'WAITING', ?, ?, datetime('now'), datetime('now'))
                        """,
                                (name, phone, date, time, reservation_dt, guests_count, chat_id, chat_id)
                    )
                    booking_id = cursor.lastrowid

                    # Логируем событие
                    log_booking_event(conn, booking_id, "CREATED_FROM_BOT", actor_id, name, {
                        "guests_count": guests_count,
                        "reservation_dt": reservation_dt
                    })

                    # Очищаем диалог
                    clear_dialog_state(conn, chat_id, actor_id)

                    # Отправляем временное сообщение с кнопкой вопроса (финальное будет после подтверждения админом)
                    msg_text, markup = booking_confirmed_user_message(booking_id)
                    tg_send_message(chat_id, msg_text, markup)

                    # Отправляем карточку бронирования в админский чат
                    try:
                        admin_text, admin_kb = render_booking_card(conn, booking_id)
                        admin_msg_id = tg_send_message(TG_CHAT_ID, admin_text, admin_kb)
                        if admin_msg_id:
                            conn.execute(
                                "UPDATE bookings SET telegram_chat_id=?, telegram_message_id=?, updated_at=datetime('now') WHERE id=?",
                                (str(TG_CHAT_ID), str(admin_msg_id), booking_id),
                            )
                            log_booking_event(conn, booking_id, "ADMIN_NOTIFIED", "system", "system", {})
                    except Exception as e:
                        # Если не удалось отправить админу, логируем ошибку но не прерываем процесс
                        log_booking_event(conn, booking_id, "ADMIN_NOTIFY_FAILED", "system", "system", {"error": str(e)})

                    tg_answer_callback(cq_id, "✅ Бронирование отправлено на проверку!")

                elif action == "no":
                    # Отмена диалога
                    clear_dialog_state(conn, chat_id, actor_id)
                    tg_send_message(chat_id, "❌ Бронирование отменено. Если хотите забронировать ещё раз, напишите /book")
                    tg_answer_callback(cq_id, "❌ Отменено")

                return {"ok": True}

            tg_answer_callback(cq_id)
            return {"ok": True}

        if "message" in update:
            m = update["message"] or {}
            chat = m.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            from_ = m.get("from") or {}
            actor_id = str(from_.get("id") or "")
            actor_name = (from_.get("username") or from_.get("first_name") or "").strip()
            first_name = str(from_.get("first_name") or "").strip()
            last_name = str(from_.get("last_name") or "").strip()
            username = str(from_.get("username") or "").strip()

            # ===== Обработка контакта (поделились контактом) =====
            contact = m.get("contact")
            if contact:
                phone = extract_phone_from_contact(contact)
                name = extract_name_from_contact(contact)

                if phone:
                    # Обновляем или создаем гостя
                    upsert_guest_if_missing(conn, phone, name)

                    # Проверяем есть ли активный диалог в state AWAITING_CONTACT
                    dialog_result = get_dialog_state(conn, chat_id, actor_id)

                    if dialog_result and dialog_result[0] == STATE_AWAITING_CONTACT:
                        # Продолжаем диалог - переходим на запрос даты
                        booking_data = {
                            'phone_e164': phone,
                            'name': name or "Гость"
                        }
                        msg_text, markup = ask_date(chat_id, actor_id)
                        prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup=markup)
                        clear_dialog_state(conn, chat_id, actor_id)
                        save_dialog_state(
                            conn,
                            chat_id,
                            actor_id,
                            STATE_AWAITING_DATE,
                            booking_data,
                            prompt_msg_id or "0"
                        )
                    else:
                        # Контакт поделился просто так, сохраняем
                        tg_send_message(chat_id, f"✅ Спасибо! Ваш контакт сохранен.\n\nТелефон: {phone}\nИмя: {name or 'не указано'}")

                conn.commit()
                return {"ok": True}

            text = (m.get("text") or "").strip()

            if text == "/myid":
                is_admin = actor_id in PROMO_ADMIN_IDS
                admins_preview = ", ".join(PROMO_ADMIN_IDS[:10]) if PROMO_ADMIN_IDS else "(пусто)"
                tg_send_message(
                    chat_id,
                    "\n".join([
                        "<b>Профиль Telegram</b>",
                        f"ID: <code>{_h(actor_id)}</code>",
                        f"username: @{_h(username) if username else '—'}",
                        f"PROMO_ADMIN_IDS (загружено): <code>{_h(admins_preview)}</code>",
                        f"admin доступ QR: <b>{'ДА' if is_admin else 'НЕТ'}</b>",
                    ])
                )
                return {"ok": True}

            if text.startswith("/start"):
                parts = text.split()
                start_param = parts[1].strip() if len(parts) > 1 else ""

                conn.execute(
                    """
                    INSERT INTO tg_bot_users
                      (tg_user_id, username, first_name, last_name, first_started_at, last_started_at, start_count, last_start_param)
                    VALUES
                      (?, ?, ?, ?, datetime('now'), datetime('now'), 1, ?)
                    ON CONFLICT(tg_user_id) DO UPDATE SET
                      username=excluded.username,
                      first_name=excluded.first_name,
                      last_name=excluded.last_name,
                      last_started_at=datetime('now'),
                      start_count=tg_bot_users.start_count + 1,
                      last_start_param=CASE
                        WHEN excluded.last_start_param LIKE 'promo_%' THEN excluded.last_start_param
                        WHEN tg_bot_users.last_start_param LIKE 'promo_%' THEN tg_bot_users.last_start_param
                        ELSE excluded.last_start_param
                      END
                    """,
                    (actor_id, username, first_name, last_name, start_param),
                )

                if start_param.startswith("promo_"):
                    code = start_param.replace("promo_", "", 1).strip().upper()

                    row = conn.execute(
                        "SELECT code, status FROM discount_codes WHERE code=?",
                        (code,),
                    ).fetchone()

                    if not row:
                        seed_discount_codes_from_csv(conn)
                        row = conn.execute(
                            "SELECT code, status FROM discount_codes WHERE code=?",
                            (code,),
                        ).fetchone()

                    if not row:
                        tg_send_message(chat_id, "❌ Эта подарочная карта не найдена.")
                        return {"ok": True}

                    status = row["status"]

                    if status == "USED":
                        tg_send_message(chat_id, "❌ Эта подарочная карта уже была использована.")
                        return {"ok": True}

                    text_msg = (
                        "🎁 <b>Подарочная карта LUCHBAR</b>\n\n"
                        "Ваша карта активна.\n\n"
                        "Скидка: <b>15%</b>\n"
                        "Действует до: <b>31 мая</b>\n\n"
                        "Вы можете воспользоваться скидкой "
                        "один раз, предъявив открытку с QR-кодом официанту.\n\n"
                        "Будем рады видеть вас в LUCHBAR."
                    )

                    inline_rows = [
                        [
                            {
                                "text": "🍸 Забронировать стол",
                                "callback_data": "menu:book"
                            }
                        ]
                    ]

                    if actor_id in PROMO_ADMIN_IDS:
                        inline_rows.append(
                            [
                                {
                                    "text": "✅ Провести скидку",
                                    "callback_data": f"promo:redeem:{code}"
                                }
                            ]
                        )

                    kb = {"inline_keyboard": inline_rows}
                    tg_send_message(chat_id, text_msg, kb)
                    return {"ok": True}

                tg_send_message(
                    chat_id,
                    "👋 <b>Добро пожаловать в LUCHBAR!</b>\n\n"
                    "Через этого бота вы можете:",
                    {
                        "inline_keyboard": [
                            [{"text": "🍸 Забронировать стол", "callback_data": "menu:book"}],
                            [{"text": "📅 Ближайшие события", "callback_data": "menu:events"}],
                            [{"text": "📋 Меню ресторана", "callback_data": "menu:pdf"}],
                        ]
                    }
                )
                return {"ok": True}

            if text == "/stat":
                if actor_id not in PROMO_ADMIN_IDS:
                    tg_send_message(chat_id, "Нет доступа.")
                    return {"ok": True}

                total_users_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM tg_bot_users"
                ).fetchone()
                total_users = int(total_users_row["c"] or 0) if total_users_row else 0

                promo_users_row = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM tg_bot_users
                    WHERE last_start_param LIKE 'promo_%'
                    """
                ).fetchone()
                promo_users = int(promo_users_row["c"] or 0) if promo_users_row else 0

                used_qr_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM discount_codes WHERE status='USED'"
                ).fetchone()
                used_qr = int(used_qr_row["c"] or 0) if used_qr_row else 0

                recent_redeems = conn.execute(
                    """
                    SELECT code, redeemed_at, redeemed_by_tg_id
                    FROM discount_codes
                    WHERE status='USED'
                    ORDER BY datetime(redeemed_at) DESC, id DESC
                    LIMIT 15
                    """
                ).fetchall()

                lines = [
                    "<b>Статистика бота</b>",
                    "",
                    f"Пользователей, нажавших /start: <b>{total_users}</b>",
                    f"Из них открывали promo QR: <b>{promo_users}</b>",
                    f"Погашено QR: <b>{used_qr}</b>",
                ]

                if recent_redeems:
                    lines.extend(["", "<b>Последние погашения:</b>"])
                    for r in recent_redeems:
                        code = _h(r["code"] or "")
                        redeemed_at = _h(r["redeemed_at"] or "—")
                        admin_id = _h(r["redeemed_by_tg_id"] or "—")
                        lines.append(f"• {code} — {redeemed_at} — админ {admin_id}")

                tg_send_message(chat_id, "\n".join(lines))
                return {"ok": True}

            if text.startswith("/book"):
                # Начинаем диалог бронирования - сначала просим контакт
                msg_text, markup = start_booking_dialog(conn, chat_id, actor_id, first_name)
                prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup=markup)

                # Сохраняем начальное состояние диалога (ожидаем контакт)
                save_dialog_state(
                    conn,
                    chat_id,
                    actor_id,
                    STATE_AWAITING_CONTACT,
                    {},
                    prompt_msg_id or "0"
                )
                return {"ok": True}

            if not text:
                return {"ok": True}

            # ===== Обработка диалога бронирования =====
            # Сначала проверяем есть ли активный диалог
            dialog_result = get_dialog_state(conn, chat_id, actor_id)

            if dialog_result:
                state, data = dialog_result
                next_state = None
                next_msg = None
                next_markup = None
                # Копируем существующие данные для дальнейшего обновления
                booking_data = {k: v for k, v in data.items()}

                if state == STATE_AWAITING_CONTACT:
                    # Пользователь ввел текст вместо контакта - предлагаем альтернативы
                    msg_text = (
                        "📱 Пожалуйста, используйте кнопку выше для поделки контактом.\n\n"
                        "Или введите свое имя чтобы продолжить:"
                    )
                    prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup={"force_reply": True})
                    # Переходим на ввод имени если хочет ручной ввод
                    save_dialog_state(
                        conn,
                        chat_id,
                        actor_id,
                        STATE_AWAITING_NAME,
                        booking_data,
                        prompt_msg_id or "0"
                    )
                    return {"ok": True}

                if state == STATE_AWAITING_NAME:
                    name = text.strip()
                    if len(name) < 2:
                        msg_text = "❌ Пожалуйста, укажите корректное имя (минимум 2 символа)"
                        prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup={"force_reply": True})
                        return {"ok": True}

                    booking_data['name'] = name
                    next_state = STATE_AWAITING_PHONE
                    next_msg, next_markup = ask_phone(chat_id, actor_id)

                elif state == STATE_AWAITING_PHONE:
                    phone = normalize_phone(text)
                    if not phone:
                        msg_text = "❌ Некорректный формат телефона. Попробуйте еще раз: +7XXXXXXXXXX или 8XXXXXXXXXX"
                        prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup={"force_reply": True})
                        return {"ok": True}

                    booking_data['phone_e164'] = phone
                    # Обновляем или создаем гостя
                    upsert_guest_if_missing(conn, phone, booking_data.get('name', ''))

                    next_state = STATE_AWAITING_DATE
                    next_msg, next_markup = ask_date(chat_id, actor_id)

                elif state == STATE_AWAITING_DATE:
                    date_val = validate_date(text)
                    if not date_val:
                        msg_text = "❌ Некорректная дата. Попробуйте в формате: 25.03 или 25.03.2026"
                        prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup={"force_reply": True})
                        return {"ok": True}

                    booking_data['date'] = date_val
                    next_state = STATE_AWAITING_TIME
                    next_msg, next_markup = ask_time(chat_id, actor_id)

                elif state == STATE_AWAITING_TIME:
                    time_val = validate_time(text)
                    if not time_val:
                        msg_text = "❌ Некорректное время. Укажите в формате ЧЧ:МММ (например 19:30)"
                        prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup={"force_reply": True})
                        return {"ok": True}

                    booking_data['time'] = time_val
                    next_state = STATE_AWAITING_GUESTS_COUNT
                    next_msg, next_markup = ask_guests_count(chat_id, actor_id)

                elif state == STATE_AWAITING_GUESTS_COUNT:
                    guests_count = validate_guests_count(text)
                    if not guests_count:
                        msg_text = "❌ Укажите количество гостей от 1 до 20"
                        prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup={"force_reply": True})
                        return {"ok": True}

                    booking_data['guests_count'] = guests_count
                    next_state = STATE_AWAITING_CONFIRMATION
                    next_msg, next_markup = confirm_booking(
                        booking_data['name'],
                        booking_data['phone_e164'],
                        booking_data['date'],
                        booking_data['time'],
                        guests_count
                    )

                elif state == STATE_AWAITING_QUESTION:
                    # Пользователь задает вопрос
                    question = text.strip()
                    if not question or len(question) < 3:
                        msg_text = "❌ Вопрос слишком короткий. Пожалуйста, опишите подробнее."
                        prompt_msg_id = tg_send_message(chat_id, msg_text, reply_markup={"force_reply": True})
                        return {"ok": True}

                    booking_id = booking_data.get('booking_id', 0)

                    # Сохраняем вопрос
                    save_booking_question(conn, booking_id, "", question, chat_id, actor_id)

                    # Отправляем вопрос в админский чат
                    if not TG_CHAT_ID:
                        tg_send_message(chat_id, "❌ Ошибка конфигурации: админский чат не настроен")
                        return {"ok": True}

                    try:
                        admin_question_text = (
                            f"❓ <b>Вопрос по бронированию #{booking_id}</b>\n\n"
                            f"<b>От пользователя:</b> {actor_name or 'Неизвестный'} (ID: {actor_id})\n"
                            f"<b>Чат:</b> {chat_id}\n\n"
                            f"<b>Вопрос:</b>\n{_h(question)}"
                        )
                        admin_kb = {
                            "inline_keyboard": [
                                [
                                    {"text": "↩️ Ответить", "callback_data": f"b:{booking_id}:answer_question"}
                                ]
                            ]
                        }
                        tg_send_message(TG_CHAT_ID, admin_question_text, admin_kb)
                    except Exception as e:
                        tg_send_message(chat_id, f"❌ Ошибка при отправке вопроса: {str(e)}")
                        return {"ok": True}

                    # Очищаем диалог и показываем спасибо
                    clear_dialog_state(conn, chat_id, actor_id)
                    tg_send_message(chat_id, "✅ Спасибо за ваш вопрос! Наша команда ответит вам в ближайшее время.")
                    return {"ok": True}

                # Обновляем состояние
                if next_state:
                    prompt_msg_id = tg_send_message(chat_id, next_msg, reply_markup=next_markup)
                    clear_dialog_state(conn, chat_id, actor_id)  # Очищаем старое состояние
                    save_dialog_state(
                        conn,
                        chat_id,
                        actor_id,
                        next_state,
                        booking_data,
                        prompt_msg_id or "0"
                    )

                return {"ok": True}

            reply_to = m.get("reply_to_message")
            prompt_mid = str(reply_to.get("message_id") or "") if reply_to else ""

            row = None
            answer_row = None

            # ===== Обработка ответа админа на вопрос пользователя =====
            if prompt_mid:
                answer_row = conn.execute(
                    """
                    SELECT id, booking_id, chat_id, actor_tg_id, expires_at
                    FROM pending_replies
                    WHERE chat_id=? AND prompt_message_id=? AND actor_tg_id=? AND kind='admin_question_answer'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (chat_id, prompt_mid, actor_id),
                ).fetchone()

            if answer_row is not None:
                # Админ ответил на вопрос - отправляем ответ пользователю
                try:
                    exp = datetime.fromisoformat(str(answer_row["expires_at"]))
                    if datetime.utcnow() > exp:
                        conn.execute("DELETE FROM pending_replies WHERE id=?", (answer_row["id"],))
                        return {"ok": True}
                except Exception:
                    pass

                booking_id = int(answer_row["booking_id"])
                
                # Получаем данные брони (где chat_id пользователя)
                b = conn.execute(
                    "SELECT user_chat_id, telegram_chat_id FROM bookings WHERE id=?",
                    (booking_id,),
                ).fetchone()

                user_chat_id = ""
                if b:
                    user_chat_id = str((b["user_chat_id"] or b["telegram_chat_id"] or "")).strip()

                if user_chat_id:
                    admin_answer_text = (
                        f"💬 <b>Ответ на ваш вопрос по бронированию #{booking_id}</b>\n\n"
                        f"{_h(text)}\n\n"
                        "Спасибо за ваше внимание!"
                    )
                    tg_send_message(user_chat_id, admin_answer_text)
                    tg_send_message(chat_id, "✅ Ответ отправлен пользователю.")
                else:
                    tg_send_message(chat_id, "⚠️ Не удалось найти контакт пользователя.")

                conn.execute("DELETE FROM pending_replies WHERE id=?", (answer_row["id"],))
                conn.commit()
                return {"ok": True}

            # ===== Обработка примечания к гостю =====
            if prompt_mid:
                row = conn.execute(
                    """
                    SELECT id, booking_id, phone_e164, expires_at
                    FROM pending_replies
                    WHERE chat_id=? AND prompt_message_id=? AND actor_tg_id=? AND kind='guest_note'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (chat_id, prompt_mid, actor_id),
                ).fetchone()

            if row is None:
                row = conn.execute(
                    """
                    SELECT id, booking_id, phone_e164, expires_at
                    FROM pending_replies
                    WHERE chat_id=? AND actor_tg_id=? AND kind='guest_note'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (chat_id, actor_id),
                ).fetchone()

            if not row:
                return {"ok": True}

            try:
                exp = datetime.fromisoformat(str(row["expires_at"]))
                if datetime.utcnow() > exp:
                    conn.execute("DELETE FROM pending_replies WHERE id=?", (row["id"],))
                    return {"ok": True}
            except Exception:
                pass

            booking_id = int(row["booking_id"])
            phone = str(row["phone_e164"] or "")

            add_guest_note(conn, phone, text, actor_id, actor_name)
            conn.execute("DELETE FROM pending_replies WHERE id=?", (row["id"],))

            b = conn.execute(
                "SELECT telegram_chat_id, telegram_message_id FROM bookings WHERE id=?",
                (booking_id,),
            ).fetchone()
            if b and b["telegram_chat_id"] and b["telegram_message_id"]:
                card_text, kb = render_booking_card(conn, booking_id)
                tg_edit_message(str(b["telegram_chat_id"]), str(b["telegram_message_id"]), card_text, kb)

            tg_send_message(chat_id, "Сохранён в базе.")
            return {"ok": True}

        return {"ok": True}
    finally:
        conn.commit()
        conn.close()