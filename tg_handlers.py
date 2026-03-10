import html
import json
from datetime import datetime, timedelta

from flask import request, abort

from config import TG_WEBHOOK_SECRET, PROMO_ADMIN_IDS, TG_CHAT_ID
from telegram_api import tg_send_message, tg_edit_message, tg_answer_callback, tg_send_photo
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

MINIAPP_URL = "https://botluch-production.up.railway.app/miniapp/reserve"


def _h(s: str) -> str:
    return html.escape(s or "", quote=False)


def ensure_db():
    conn = connect()
    init_schema(conn)
    conn.commit()
    return conn


def build_luch_main_menu():
    return {
        "inline_keyboard": [
            [
                {
                    "text": "🍸 Забронировать",
                    "web_app": {"url": MINIAPP_URL}
                },
                {
                    "text": "📖 Меню",
                    "url": "https://barluch.ru/osnovnoe-menu"
                }
            ],
            [
                {
                    "text": "🎧 Line-up",
                    "callback_data": "lineup"
                },
                {
                    "text": "✨ О Луче",
                    "callback_data": "about_luch"
                }
            ],
            [
                {
                    "text": "📍 Контакты",
                    "callback_data": "contacts_luch"
                },
                {
                    "text": "🥂 Банкеты",
                    "callback_data": "banquets_luch"
                }
            ]
        ]
    }


def get_luch_info_text(section: str) -> str:
    section = (section or "").strip().lower()

    if section == "about_luch":
        return (
            "<b>О Луче</b>\n\n"
            "LUCH — бар и ресторан в пространстве бывшего завода на Большой Пироговской.\n"
            "Это проект с акцентом на атмосферу, барную культуру, вечерние события, "
            "ужины, встречи и выходные с DJ-программой.\n\n"
            "Для бронирования используйте кнопку «Забронировать», "
            "а с актуальным меню можно ознакомиться по кнопке «Меню»."
        )

    if section == "contacts_luch":
        return (
            "<b>Контакты</b>\n\n"
            "Адрес: Москва, Большая Пироговская, 27/1\n"
            "Телефон: +7 (495) 287-00-22\n\n"
            "Для брони удобнее всего использовать кнопку «Забронировать» в меню бота."
        )

    if section == "banquets_luch":
        return (
            "<b>Банкеты и мероприятия</b>\n\n"
            "В LUCH можно обсудить проведение банкетов, закрытых мероприятий, "
            "ужинов и специальных событий.\n\n"
            "Для быстрого контакта оставьте бронь через мини-апп или свяжитесь с площадкой по телефону."
        )

    return (
        "<b>LUCH</b>\n\n"
        "Используйте кнопки ниже: бронь, меню, line-up, контакты и информация о проекте."
    )


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

            if data == "about_luch":
                tg_answer_callback(cq_id)
                tg_send_message(chat_id, get_luch_info_text("about_luch"), reply_markup=build_luch_main_menu())
                return {"ok": True}

            if data == "contacts_luch":
                tg_answer_callback(cq_id)
                tg_send_message(chat_id, get_luch_info_text("contacts_luch"), reply_markup=build_luch_main_menu())
                return {"ok": True}

            if data == "banquets_luch":
                tg_answer_callback(cq_id)
                tg_send_message(chat_id, get_luch_info_text("banquets_luch"), reply_markup=build_luch_main_menu())
                return {"ok": True}

            if data == "lineup":
                tg_answer_callback(cq_id)

                lineup_row = conn.execute(
                    "SELECT file_id, caption FROM lineup_posters ORDER BY id DESC LIMIT 1"
                ).fetchone()

                if not lineup_row:
                    tg_send_message(chat_id, "🎵 DJ line-up скоро появится!", build_luch_main_menu())
                    return {"ok": True}

                file_id = lineup_row["file_id"]
                caption = lineup_row["caption"] or "🎵 <b>DJ line-up LUCH</b>\n\nПятница / Суббота"
                tg_send_photo(chat_id, file_id, caption)
                return {"ok": True}

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

                b = conn.execute(
                    "SELECT id, phone_e164, raw_payload_json FROM bookings WHERE id=?",
                    (booking_id,),
                ).fetchone()
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

                    g_row = conn.execute(
                        "SELECT visits_count FROM guests WHERE phone_e164=?",
                        (phone,),
                    ).fetchone()
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

                        # уведомление пользователю только после подтверждения админом
                        try:
                            payload = json.loads(b["raw_payload_json"] or "{}")
                        except Exception:
                            payload = {}

                        requester_chat_id = str(payload.get("requester_chat_id") or "").strip()
                        if requester_chat_id:
                            notify_text = (
                                "✅ <b>Бронь подтверждена</b>\n\n"
                                "Ваш стол подтвержден.\n"
                                "Ждём вас в LUCHBAR."
                            )
                            tg_send_message(requester_chat_id, notify_text)

                        text, kb = render_booking_card(conn, booking_id)
                        tg_edit_message(chat_id, message_id, text, kb)
                        tg_answer_callback(cq_id, "Подтверждено")
                        return {"ok": True}

                    if action == "cancel":
                        mark_booking_cancelled(conn, booking_id, actor_id, actor_name)

                        # по желанию можно уведомлять и об отмене
                        try:
                            payload = json.loads(b["raw_payload_json"] or "{}")
                        except Exception:
                            payload = {}

                        requester_chat_id = str(payload.get("requester_chat_id") or "").strip()
                        if requester_chat_id:
                            notify_text = (
                                "❌ <b>Бронь не подтверждена</b>\n\n"
                                "Пожалуйста, свяжитесь с нами или отправьте новую заявку."
                            )
                            tg_send_message(requester_chat_id, notify_text)

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
                        f"Телефон: <code>{_h(phone)}</code>\n\n"
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
            first_name = str(from_.get("first_name") or "").strip()

            # ===== Обработка контакта (поделились контактом) =====
            contact = m.get("contact")
            if contact:
                from booking_dialog import extract_phone_from_contact, extract_name_from_contact
                
                phone = extract_phone_from_contact(contact)
                name = extract_name_from_contact(contact)

                if phone:
                    # Сохраняем контакт в базу
                    upsert_guest_if_missing(conn, phone, name)
                    
                    # Сохраняем в tg_bot_users что у этого юзера есть телефон
                    conn.execute(
                        """
                        INSERT INTO tg_bot_users (tg_user_id, username, first_name, has_shared_phone, phone_e164, first_started_at, last_started_at, start_count)
                        VALUES (?, ?, ?, 1, ?, datetime('now'), datetime('now'), 0)
                        ON CONFLICT(tg_user_id) DO UPDATE SET
                            has_shared_phone=1,
                            phone_e164=excluded.phone_e164
                        """,
                        (actor_id, actor_name, name or first_name, phone),
                    )
                    
                    # Теперь показываем кнопку с Mini App
                    start_text = (
                        "✅ <b>Спасибо за контакт!</b>\n\n"
                        "Главное меню LUCH готово.\n"
                        "Выберите нужный раздел кнопками ниже."
                    )
                    tg_send_message(chat_id, start_text, build_luch_main_menu())

                conn.commit()
                return {"ok": True}

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

                guests_count = 0
                try:
                    guests_count = int(guests_value)
                except Exception:
                    guests_count = 0

                if guests_count <= 0:
                    tg_send_message(chat_id, "Не удалось определить количество гостей. Попробуйте ещё раз.")
                    return {"ok": True}

                # Получаем телефон пользователя из базы
                user_row = conn.execute(
                    "SELECT phone_e164, first_name FROM tg_bot_users WHERE tg_user_id=? AND has_shared_phone=1",
                    (actor_id,),
                ).fetchone()
                
                phone_e164 = user_row["phone_e164"] if user_row else None
                saved_name = user_row["first_name"] if user_row else None

                # Mini App теперь создает обычную бронь в БД
                raw_payload = {
                    "source": "telegram_miniapp",
                    "requester_chat_id": chat_id,
                    "requester_tg_user_id": actor_id,
                    "requester_name": saved_name or actor_name,
                    "date": date_value,
                    "time": time_value,
                    "guests": guests_count,
                    "comment": comment_value,
                }

                cur = conn.execute(
                    """
                    INSERT INTO bookings
                    (
                        tranid,
                        formname,
                        name,
                        phone_e164,
                        phone_raw,
                        reservation_date,
                        reservation_time,
                        reservation_dt,
                        guests_count,
                        comment,
                        utm_source,
                        utm_medium,
                        utm_campaign,
                        utm_content,
                        utm_term,
                        status,
                        guest_segment,
                        raw_payload_json
                    )
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WAITING', ?, ?)
                    """,
                    (
                        None,
                        "telegram_miniapp",
                        saved_name or actor_name or "Telegram",
                        phone_e164,
                        phone_e164,
                        date_value,
                        time_value,
                        f"{date_value} {time_value}:00",
                        guests_count,
                        comment_value,
                        "telegram",
                        "miniapp",
                        None,
                        None,
                        None,
                        "NEW" if phone_e164 else "NEW",
                        json.dumps(raw_payload, ensure_ascii=False),
                    ),
                )
                booking_id = int(cur.lastrowid)

                log_booking_event(
                    conn,
                    booking_id,
                    "CREATED",
                    actor_id,
                    actor_name,
                    {"source": "telegram_miniapp"},
                )

                text, kb = render_booking_card(conn, booking_id)
                msg_id = tg_send_message(str(TG_CHAT_ID), text, kb)

                conn.execute(
                    """
                    UPDATE bookings
                    SET telegram_chat_id=?, telegram_message_id=?, updated_at=datetime('now')
                    WHERE id=?
                    """,
                    (str(TG_CHAT_ID), str(msg_id), booking_id),
                )

                log_booking_event(
                    conn,
                    booking_id,
                    "TG_SYNC_OK",
                    "system",
                    "system",
                    {"status": "sent", "source": "telegram_miniapp"},
                )

                # гостю пока не подтверждаем бронь — только сообщаем, что заявка принята в работу
                waiting_text = (
                    "🕓 <b>Заявка отправлена</b>\n\n"
                    "Мы передали её администратору.\n"
                    "Сообщение о подтверждении придёт сюда после проверки."
                )
                tg_send_message(chat_id, waiting_text)
                return {"ok": True}

            text = (m.get("text") or "").strip()
            text_lc = text.lower()
            cmd = ""
            if text.startswith("/"):
                # Support commands in groups like /lineup@my_bot
                cmd = text.split()[0].split("@", 1)[0].lower()

            if cmd == "/start":
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

                # Проверяем есть ли у пользователя телефон
                user_row = conn.execute(
                    "SELECT has_shared_phone FROM tg_bot_users WHERE tg_user_id=?",
                    (actor_id,),
                ).fetchone()

                has_phone = user_row and user_row["has_shared_phone"]

                if not has_phone:
                    # Просим поделиться контактом
                    contact_text = (
                        "👋 <b>Добро пожаловать в LUCHBAR!</b>\n\n"
                        "Для бронирования нам нужен ваш номер телефона.\n"
                        "Пожалуйста, нажмите кнопку ниже:"
                    )
                    contact_kb = {
                        "keyboard": [
                            [{"text": "📱 Поделиться контактом", "request_contact": True}]
                        ],
                        "one_time_keyboard": True,
                        "resize_keyboard": True
                    }
                    tg_send_message(chat_id, contact_text, contact_kb)
                else:
                    # Показываем кнопку с Mini App
                    start_text = (
                        "🍸 <b>LUCHBAR</b>\n\n"
                        "Выберите нужный раздел в меню ниже: бронь, меню, line-up, "
                        "контакты или информация о проекте."
                    )
                    tg_send_message(chat_id, start_text, build_luch_main_menu())
                
                return {"ok": True}

            if cmd == "/myid":
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

            if cmd == "/stat":
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

            if cmd == "/set_lineup":
                if actor_id not in PROMO_ADMIN_IDS:
                    tg_send_message(chat_id, "Нет доступа.")
                    return {"ok": True}

                # Создаём запись ожидания загрузки афиши
                prompt_msg_id = tg_send_message(
                    chat_id,
                    "📸 <b>Загрузка афиши DJ</b>\n\nОтправьте картинку с афишей на неделю."
                )

                expires = (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds")
                conn.execute(
                    """
                    INSERT INTO pending_replies (kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at)
                    VALUES ('lineup_upload', 0, '', ?, ?, ?, ?)
                    """,
                    (chat_id, actor_id, str(prompt_msg_id), expires),
                )
                conn.commit()
                return {"ok": True}

            if cmd == "/lineup" or text_lc in ("line-up", "lineup", "🎵 line-up", "🎵 lineup"):
                # Получаем последнюю афишу
                lineup_row = conn.execute(
                    "SELECT file_id, caption FROM lineup_posters ORDER BY id DESC LIMIT 1"
                ).fetchone()

                if not lineup_row:
                    tg_send_message(chat_id, "🎵 DJ line-up скоро появится!")
                    return {"ok": True}

                file_id = lineup_row["file_id"]
                caption = lineup_row["caption"] or "🎵 <b>DJ line-up LUCH</b>\n\nПятница / Суббота"

                tg_send_photo(chat_id, file_id, caption)
                return {"ok": True}

            # Обработка фото (для загрузки афиши)
            photo = m.get("photo")
            if photo:
                # Проверяем есть ли pending для lineup_upload
                pending_row = conn.execute(
                    """
                    SELECT id, expires_at
                    FROM pending_replies
                    WHERE actor_tg_id=? AND kind='lineup_upload'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (actor_id,),
                ).fetchone()

                if pending_row:
                    try:
                        exp = datetime.fromisoformat(str(pending_row["expires_at"]))
                        if datetime.utcnow() > exp:
                            conn.execute("DELETE FROM pending_replies WHERE id=?", (pending_row["id"],))
                            conn.commit()
                            return {"ok": True}
                    except Exception:
                        pass

                    # Берём лучшее качество (последний элемент массива)
                    file_id = photo[-1].get("file_id")

                    if not file_id:
                        tg_send_message(chat_id, "❌ Не удалось получить file_id изображения.")
                        return {"ok": True}

                    # Удаляем старые афиши
                    conn.execute("DELETE FROM lineup_posters")

                    # Сохраняем новую афишу
                    caption = "🎵 <b>DJ line-up LUCH</b>\n\nПятница / Суббота"
                    conn.execute(
                        """
                        INSERT INTO lineup_posters (file_id, caption, uploaded_by)
                        VALUES (?, ?, ?)
                        """,
                        (file_id, caption, actor_id),
                    )

                    # Удаляем pending
                    conn.execute("DELETE FROM pending_replies WHERE id=?", (pending_row["id"],))

                    tg_send_message(chat_id, "✅ Афиша сохранена!")
                    conn.commit()
                    return {"ok": True}

            if not photo and actor_id in PROMO_ADMIN_IDS:
                pending_lineup = conn.execute(
                    """
                    SELECT id
                    FROM pending_replies
                    WHERE actor_tg_id=? AND kind='lineup_upload'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (actor_id,),
                ).fetchone()
                if pending_lineup and cmd != "/set_lineup":
                    tg_send_message(chat_id, "Пожалуйста, отправьте изображение афиши (как фото).")
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
