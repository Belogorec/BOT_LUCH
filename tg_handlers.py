import html
import hashlib
import json
import os
import traceback
from datetime import datetime, timedelta
import requests

from flask import request, abort

from config import (
    TG_WEBHOOK_SECRET,
    PROMO_ADMIN_IDS,
    TG_CHAT_ID,
    WAITER_CHAT_ID,
    BOT_TOKEN,
    CRM_AUTH_CONFIRM_URL,
    CRM_AUTH_TIMEOUT_SEC,
)
from telegram_api import (
    tg_send_message,
    tg_edit_message,
    tg_answer_callback,
    tg_send_photo,
)
from booking_service import (
    compute_segment,
    upsert_guest_if_missing,
    log_booking_event,
    log_guest_event,
    add_guest_note,
    toggle_guest_tag,
    ensure_visit_from_confirmed_booking,
    mark_booking_cancelled,
    normalize_table_number,
    parse_restriction_until,
    assign_table_to_booking,
    clear_table_assignment,
    set_booking_deposit,
    set_table_label,
    get_table_assignment_conflicts,
    get_active_table_restrictions,
)
from booking_render import (
    render_booking_card,
    render_guest_visits_message,
)
from crm_sync import send_booking_event, send_table_event
from db import connect
from waiter_notify import notify_waiters_about_deposit_booking

MINIAPP_URL = os.environ.get(
    "MINIAPP_URL",
    "https://botluch-production.up.railway.app/miniapp/reserve",
).strip()


def _h(s: str) -> str:
    return html.escape(s or "", quote=False)


def _normalize_auth_code(raw: str) -> str:
    code = (raw or "").strip().upper()
    if not code:
        return ""
    if not code.startswith("AUTH-"):
        code = f"AUTH-{code}"
    return code


def _confirm_crm_auth(code: str, telegram_id: str) -> tuple[bool, str]:
    if not CRM_AUTH_CONFIRM_URL:
        return False, "CRM auth URL не настроен"
    if not BOT_TOKEN:
        return False, "BOT_TOKEN не настроен"

    try:
        resp = requests.post(
            CRM_AUTH_CONFIRM_URL,
            json={
                "code": code,
                "telegram_id": int(telegram_id),
                "bot_token": BOT_TOKEN,
            },
            timeout=max(3, int(CRM_AUTH_TIMEOUT_SEC)),
        )
    except Exception as exc:
        return False, f"ошибка соединения с CRM: {exc}"

    if not resp.ok:
        return False, f"CRM вернула HTTP {resp.status_code}"

    try:
        payload = resp.json()
    except Exception:
        payload = {}

    if payload.get("ok") is True:
        return True, "ok"

    return False, str(payload.get("error") or "неподтвержденный или истекший код")


def ensure_db():
    return connect()


def safe_answer_callback(callback_query_id: str, text: str = ""):
    try:
        tg_answer_callback(callback_query_id, text)
    except Exception as e:
        print(f"[TG-WEBHOOK] answerCallbackQuery failed: id={callback_query_id} error={e}", flush=True)


def _is_backoffice_context(chat_id: str, actor_id: str) -> bool:
    return (str(chat_id or "").strip() == str(TG_CHAT_ID or "").strip()) or (actor_id in PROMO_ADMIN_IDS)


def _is_waiter_chat(chat_id: str) -> bool:
    return bool(WAITER_CHAT_ID) and str(chat_id or "").strip() == str(WAITER_CHAT_ID).strip()


def _sync_admin_booking_card(conn, booking_id: int) -> None:
    try:
        booking_row = conn.execute(
            """
            SELECT telegram_chat_id, telegram_message_id
            FROM bookings
            WHERE id=?
            """,
            (booking_id,),
        ).fetchone()
        if booking_row and booking_row["telegram_chat_id"] and booking_row["telegram_message_id"]:
            text_card, kb_card = render_booking_card(conn, booking_id)
            tg_edit_message(
                str(booking_row["telegram_chat_id"]),
                str(booking_row["telegram_message_id"]),
                text_card,
                kb_card,
            )
    except Exception:
        traceback.print_exc()


def _create_pending_reply(conn, kind: str, booking_id: int, chat_id: str, actor_id: str, payload: dict, prompt_text: str) -> None:
    prompt_message_id = tg_send_message(
        chat_id,
        prompt_text,
        reply_markup={"force_reply": True, "selective": True},
    )
    expires = (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO pending_replies (kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            kind,
            int(booking_id or 0),
            json.dumps(payload or {}, ensure_ascii=False),
            chat_id,
            actor_id,
            str(prompt_message_id),
            expires,
        ),
    )


def _load_pending_reply_payload(row) -> dict:
    if not row:
        return {}
    try:
        payload = json.loads(row["phone_e164"] or "{}")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _format_table_conflict_message(conflicts: dict, table_number: int) -> str:
    lines = [f"⚠️ Стол <b>#{table_number}</b> уже занят или ограничен."]
    restricted = conflicts.get("restricted")
    if restricted:
        until_raw = str(restricted.get("restricted_until") or "—")
        until = _h(until_raw[11:16] if len(until_raw) >= 16 else until_raw)
        lines.append(f"Ограничение действует до: <code>{until}</code>")
    for item in conflicts.get("booking_conflicts") or []:
        lines.append(
            f"Бронь #{item.get('id')} · {item.get('reservation_dt') or '—'} · {item.get('status') or 'WAITING'}"
        )
    lines.append("Подтвердите override отдельной кнопкой или выберите другой стол.")
    return "\n".join(lines)


def _display_restriction_time(value: str) -> str:
    raw = str(value or "").strip()
    if len(raw) >= 16:
        return raw[11:16]
    return raw or "—"


def build_luch_main_menu():
    return {
        "inline_keyboard": [
            [
                {
                    "text": "🍸 Забронировать",
                    "web_app": {"url": MINIAPP_URL},
                },
                {
                    "text": "📖 Меню",
                    "url": "https://barluch.ru/osnovnoe-menu",
                },
            ],
            [
                {
                    "text": "🎧 Line-up",
                    "callback_data": "lineup",
                },
                {
                    "text": "✨ О Луче",
                    "callback_data": "about_luch",
                },
            ],
            [
                {
                    "text": "📍 Контакты",
                    "callback_data": "contacts_luch",
                },
                {
                    "text": "🥂 Банкеты",
                    "url": "https://barluch.ru/banket",
                },
            ],
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

    update_id = update.get("update_id")
    update_type = "unknown"
    chat_id_dbg = ""
    message_id_dbg = ""
    callback_query_id_dbg = ""
    if "callback_query" in update:
        update_type = "callback_query"
        cq_dbg = update.get("callback_query") or {}
        callback_query_id_dbg = str(cq_dbg.get("id") or "")
        msg_dbg = cq_dbg.get("message") or {}
        message_id_dbg = str(msg_dbg.get("message_id") or "")
        chat_id_dbg = str((msg_dbg.get("chat") or {}).get("id") or "")
    elif "message" in update:
        update_type = "message"
        m_dbg = update.get("message") or {}
        message_id_dbg = str(m_dbg.get("message_id") or "")
        chat_id_dbg = str((m_dbg.get("chat") or {}).get("id") or "")

    print(
        "[TG-WEBHOOK] "
        f"update_id={update_id} "
        f"type={update_type} "
        f"chat_id={chat_id_dbg} "
        f"message_id={message_id_dbg} "
        f"callback_query_id={callback_query_id_dbg}",
        flush=True,
    )

    conn = ensure_db()
    try:
        if isinstance(update_id, int):
            conn.execute(
                """
                INSERT OR IGNORE INTO processed_tg_updates
                (update_id, update_type, chat_id, message_id, callback_query_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (update_id, update_type, chat_id_dbg, message_id_dbg, callback_query_id_dbg),
            )
            inserted = conn.execute("SELECT changes() AS c").fetchone()
            if inserted and int(inserted["c"] or 0) == 0:
                recent_rows = conn.execute(
                    """
                    SELECT update_id
                    FROM processed_tg_updates
                    ORDER BY created_at DESC, update_id DESC
                    LIMIT 20
                    """
                ).fetchall()
                recent_ids = [str(r["update_id"]) for r in recent_rows]
                print(
                    "[TG-WEBHOOK] duplicate update ignored "
                    f"update_id={update_id} recent={','.join(recent_ids)}",
                    flush=True,
                )
                return {"ok": True}

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

            # Acknowledge quickly so Telegram doesn't keep spinner on cold starts.
            safe_answer_callback(cq_id)

            if _is_waiter_chat(chat_id):
                return {"ok": True}

            if data == "about_luch":
                tg_send_message(chat_id, get_luch_info_text("about_luch"), reply_markup=build_luch_main_menu())
                return {"ok": True}

            if data == "contacts_luch":
                tg_send_message(chat_id, get_luch_info_text("contacts_luch"), reply_markup=build_luch_main_menu())
                return {"ok": True}

            if data == "lineup":
                lineup_row = conn.execute(
                    "SELECT file_id, caption FROM lineup_posters ORDER BY id DESC LIMIT 1"
                ).fetchone()

                if not lineup_row:
                    tg_send_message(chat_id, "🎵 DJ line-up скоро появится!", build_luch_main_menu())
                    return {"ok": True}

                file_id = lineup_row["file_id"]
                tg_send_photo(chat_id, file_id)
                return {"ok": True}

            if data.startswith("promo:redeem:"):
                code = data.replace("promo:redeem:", "", 1).strip()

                if actor_id not in PROMO_ADMIN_IDS:
                    safe_answer_callback(cq_id, "Нет доступа")
                    return {"ok": True}

                row = conn.execute(
                    "SELECT code, status FROM discount_codes WHERE code=?",
                    (code,),
                ).fetchone()

                if not row:
                    safe_answer_callback(cq_id, "Карта не найдена")
                    return {"ok": True}

                if row["status"] == "USED":
                    safe_answer_callback(cq_id, "Карта уже использована")
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

                safe_answer_callback(cq_id, "Скидка проведена")
                tg_send_message(chat_id, f"✅ Скидка по карте <b>{_h(code)}</b> проведена.")
                return {"ok": True}

            parts = data.split(":")
            if len(parts) >= 3 and parts[0] == "b":
                booking_id = int(parts[1])

                b = conn.execute(
                    """
                    SELECT id, phone_e164, raw_payload_json, assigned_table_number,
                           reservation_date, reservation_time, reservation_dt, status
                    FROM bookings
                    WHERE id=?
                    """,
                    (booking_id,),
                ).fetchone()
                if not b:
                    safe_answer_callback(cq_id, "Бронь не найдена")
                    return {"ok": True}

                phone = b["phone_e164"] or ""
                if phone:
                    upsert_guest_if_missing(conn, phone, "")

                if parts[2] == "visits":
                    if not phone:
                        safe_answer_callback(cq_id, "Нет телефона у брони")
                        return {"ok": True}

                    visits_msg = render_guest_visits_message(conn, phone)
                    tg_send_message(chat_id, visits_msg)
                    safe_answer_callback(cq_id, "История отправлена")
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
                            notify_kb = {
                                "inline_keyboard": [
                                    [{"text": "❌ Отменить бронь", "callback_data": f"b:{booking_id}:booking:cancel_guest"}]
                                ]
                            }
                            tg_send_message(requester_chat_id, notify_text, notify_kb)

                        text, kb = render_booking_card(conn, booking_id)
                        tg_edit_message(chat_id, message_id, text, kb)
                        try:
                            send_booking_event(
                                conn,
                                booking_id,
                                "BOOKING_STATUS_CONFIRMED",
                                {
                                    "actor_tg_id": actor_id,
                                    "actor_name": actor_name,
                                    "payload": {"status": "CONFIRMED"},
                                },
                            )
                        except Exception:
                            pass
                        safe_answer_callback(cq_id, "Подтверждено")
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
                        try:
                            send_booking_event(
                                conn,
                                booking_id,
                                "BOOKING_STATUS_CANCELLED",
                                {
                                    "actor_tg_id": actor_id,
                                    "actor_name": actor_name,
                                    "payload": {"status": "CANCELLED"},
                                },
                            )
                        except Exception:
                            pass
                        safe_answer_callback(cq_id, "Отменено")
                        return {"ok": True}

                    if action == "cancel_guest":
                        mark_booking_cancelled(conn, booking_id, actor_id, actor_name)

                        # убираем кнопку у гостя
                        tg_edit_message(
                            chat_id,
                            message_id,
                            "❌ <b>Бронь отменена</b>\n\nЕсли хотите забронировать снова — используйте кнопку ниже.",
                            build_luch_main_menu(),
                        )

                        # уведомляем администраторов отдельным сообщением
                        if TG_CHAT_ID:
                            try:
                                brow = conn.execute(
                                    "SELECT name, phone_e164, reservation_date, reservation_time, guests_count FROM bookings WHERE id=?",
                                    (booking_id,),
                                ).fetchone()
                                guest_name = _h(str(brow["name"] or "—")) if brow else "—"
                                guest_phone = _h(str(brow["phone_e164"] or "—")) if brow else "—"
                                res_date = _h(str(brow["reservation_date"] or "—")) if brow else "—"
                                res_time = _h(str(brow["reservation_time"] or "—")) if brow else "—"
                                guests_cnt = _h(str(brow["guests_count"] or "—")) if brow else "—"
                                admin_notify = (
                                    f"❌ <b>Гость отменил бронь #{booking_id}</b>\n\n"
                                    f"Гость: {guest_name}\n"
                                    f"Телефон: {guest_phone}\n"
                                    f"Дата: {res_date} {res_time}\n"
                                    f"Гостей: {guests_cnt}"
                                )
                                tg_send_message(str(TG_CHAT_ID), admin_notify)
                            except Exception:
                                pass

                        try:
                            send_booking_event(
                                conn,
                                booking_id,
                                "BOOKING_STATUS_CANCELLED",
                                {
                                    "actor_tg_id": actor_id,
                                    "actor_name": actor_name,
                                    "payload": {"status": "CANCELLED", "source": "guest"},
                                },
                            )
                        except Exception:
                            pass
                        safe_answer_callback(cq_id, "Бронь отменена")
                        return {"ok": True}

                if parts[2] == "table" and len(parts) >= 4:
                    action = parts[3].strip().lower()

                    if action == "assign":
                        _create_pending_reply(
                            conn,
                            "table_flow",
                            booking_id,
                            chat_id,
                            actor_id,
                            {"mode": "assign_table", "booking_id": booking_id},
                            (
                                f"<b>Назначить стол</b>\nБронь #{booking_id}\n\n"
                                "Напишите номер стола одним сообщением."
                            ),
                        )
                        safe_answer_callback(cq_id, "Жду номер стола")
                        return {"ok": True}

                    if action == "assign_override" and len(parts) >= 5:
                        table_number = normalize_table_number(parts[4])
                        if not table_number:
                            safe_answer_callback(cq_id, "Некорректный стол")
                            return {"ok": True}
                        try:
                            result = assign_table_to_booking(conn, booking_id, table_number, actor_id, actor_name, force_override=True)
                            _sync_admin_booking_card(conn, booking_id)
                            try:
                                send_booking_event(
                                    conn,
                                    booking_id,
                                    "BOOKING_TABLE_UPDATED",
                                    {
                                        "actor_tg_id": actor_id,
                                        "actor_name": actor_name,
                                        "payload": {
                                            "action": "assign_table",
                                            "table_number": result["table_number"],
                                            "force_override": True,
                                        },
                                        "table_number": result["table_number"],
                                    },
                                )
                            except Exception:
                                traceback.print_exc()
                            try:
                                notify_waiters_about_deposit_booking(conn, booking_id)
                            except Exception:
                                traceback.print_exc()
                            safe_answer_callback(cq_id, f"Стол #{table_number} назначен")
                        except Exception:
                            traceback.print_exc()
                            safe_answer_callback(cq_id, "Не удалось назначить стол")
                        return {"ok": True}

                    if action == "clear":
                        try:
                            result = clear_table_assignment(conn, booking_id, actor_id, actor_name)
                            _sync_admin_booking_card(conn, booking_id)
                            send_booking_event(
                                conn,
                                booking_id,
                                "BOOKING_TABLE_UPDATED",
                                {
                                    "actor_tg_id": actor_id,
                                    "actor_name": actor_name,
                                    "payload": {
                                        "action": "clear_table",
                                        "old_table_number": result["previous_table_number"],
                                    },
                                },
                            )
                            safe_answer_callback(cq_id, "Стол снят")
                        except Exception:
                            traceback.print_exc()
                            safe_answer_callback(cq_id, "Не удалось снять стол")
                        return {"ok": True}

                    if action == "restrict":
                        assigned_table = normalize_table_number(b["assigned_table_number"])
                        if assigned_table:
                            _create_pending_reply(
                                conn,
                                "table_flow",
                                booking_id,
                                chat_id,
                                actor_id,
                                {
                                    "mode": "restrict_until",
                                    "booking_id": booking_id,
                                    "table_number": assigned_table,
                                },
                                (
                                    f"<b>Ограничить стол</b>\nБронь #{booking_id}\n"
                                    f"Стол #{assigned_table}\n\n"
                                    "Напишите, на сколько часов поставить ограничение.\n"
                                    "Пример: <code>2</code> или <code>5</code>"
                                ),
                            )
                            safe_answer_callback(cq_id, "Жду часы ограничения")
                        else:
                            _create_pending_reply(
                                conn,
                                "table_flow",
                                booking_id,
                                chat_id,
                                actor_id,
                                {"mode": "restrict_number", "booking_id": booking_id},
                                (
                                    f"<b>Ограничить стол</b>\nБронь #{booking_id}\n\n"
                                    "Сначала напишите номер стола."
                                ),
                            )
                            safe_answer_callback(cq_id, "Жду номер стола")
                        return {"ok": True}

                    if action == "restrict_override" and len(parts) >= 5:
                        table_number = normalize_table_number(parts[4])
                        if not table_number:
                            safe_answer_callback(cq_id, "Некорректный стол")
                            return {"ok": True}
                        _create_pending_reply(
                            conn,
                            "table_flow",
                            booking_id,
                            chat_id,
                            actor_id,
                            {
                                "mode": "restrict_until",
                                "booking_id": booking_id,
                                "table_number": table_number,
                                "force_override": True,
                            },
                            (
                                f"<b>Ограничить стол с override</b>\nБронь #{booking_id}\n"
                                f"Стол #{table_number}\n\n"
                                "Напишите, на сколько часов поставить ограничение."
                            ),
                        )
                        safe_answer_callback(cq_id, "Жду часы ограничения")
                        return {"ok": True}

                    if action == "show_restrictions":
                        rows = get_active_table_restrictions(conn)
                        if not rows:
                            tg_send_message(chat_id, "Сейчас активных ограничений по столам нет.")
                        else:
                            lines = ["<b>Активные ограничения столов</b>"]
                            for row in rows:
                                lines.append(
                                    f"• Стол #{row['table_number']} до <code>{_h(_display_restriction_time(row['restricted_until']))}</code>"
                                )
                            tg_send_message(chat_id, "\n".join(lines))
                        safe_answer_callback(cq_id, "Список отправлен")
                        return {"ok": True}

                if parts[2] == "deposit" and len(parts) >= 4:
                    action = parts[3].strip().lower()
                    if action == "set":
                        _create_pending_reply(
                            conn,
                            "table_flow",
                            booking_id,
                            chat_id,
                            actor_id,
                            {"mode": "set_deposit", "booking_id": booking_id},
                            (
                                f"<b>Установить депозит</b>\nБронь #{booking_id}\n\n"
                                "Напишите сумму депозита целым числом."
                            ),
                        )
                        safe_answer_callback(cq_id, "Жду сумму депозита")
                        return {"ok": True}

                if parts[2] == "note":
                    if not phone:
                        safe_answer_callback(cq_id, "Нет телефона у брони")
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

                    safe_answer_callback(cq_id, "Ожидаю текст")
                    return {"ok": True}

                safe_answer_callback(cq_id)
            return {"ok": True}

        if "message" in update:
            m = update["message"] or {}
            chat = m.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            from_ = m.get("from") or {}
            actor_id = str(from_.get("id") or "")
            tg_username = str(from_.get("username") or "").strip()
            actor_name = (tg_username or from_.get("first_name") or "").strip()
            first_name = str(from_.get("first_name") or "").strip()

            if _is_waiter_chat(chat_id):
                return {"ok": True}

            # ===== Обработка контакта (поделились контактом) =====
            contact = m.get("contact")
            if contact:
                from booking_dialog import extract_phone_from_contact, extract_name_from_contact
                
                phone = extract_phone_from_contact(contact)
                name = extract_name_from_contact(contact)

                if phone:
                    # Сохраняем контакт в базу
                    upsert_guest_if_missing(conn, phone, name, overwrite_name=True)
                    
                    # Сохраняем в tg_bot_users что у этого юзера есть телефон
                    conn.execute(
                        """
                        INSERT INTO tg_bot_users (tg_user_id, username, first_name, has_shared_phone, phone_e164, first_started_at, last_started_at, start_count)
                        VALUES (?, ?, ?, 1, ?, datetime('now'), datetime('now'), 0)
                        ON CONFLICT(tg_user_id) DO UPDATE SET
                            username=CASE
                                WHEN excluded.username IS NOT NULL AND trim(excluded.username) <> '' THEN excluded.username
                                ELSE tg_bot_users.username
                            END,
                            first_name=CASE
                                WHEN excluded.first_name IS NOT NULL AND trim(excluded.first_name) <> '' THEN excluded.first_name
                                ELSE tg_bot_users.first_name
                            END,
                            has_shared_phone=1,
                            phone_e164=excluded.phone_e164
                        """,
                        (actor_id, tg_username, name or first_name, phone),
                    )
                    
                    tg_send_message(
                        chat_id,
                        "✅ <b>Спасибо!</b> Контакт сохранён.",
                        {"remove_keyboard": True},
                    )
                    tg_send_message(
                        chat_id,
                        "<b>LUCHBAR</b>\n\nВыберите нужный раздел ниже.",
                        build_luch_main_menu(),
                    )

                conn.commit()
                return {"ok": True}

            web_app_data = (m.get("web_app_data") or {}).get("data")
            if web_app_data:
                print(
                    f"[MINIAPP] web_app_data received: user={actor_id} chat={chat_id} "
                    f"len={len(str(web_app_data))}",
                    flush=True,
                )
                try:
                    payload = json.loads(str(web_app_data))
                    print(f"[MINIAPP] payload parsed OK: keys={list(payload.keys())}", flush=True)
                except Exception as e:
                    print(f"[MINIAPP] JSON parse error: {e}", flush=True)
                    tg_send_message(chat_id, "Не удалось прочитать данные формы. Попробуйте ещё раз.")
                    return {"ok": True}

                date_value = str(payload.get("date") or "").strip()
                time_value = str(payload.get("time") or "").strip()
                guests_value = str(payload.get("guests") or "").strip()
                comment_value = str(payload.get("comment") or "").strip()
                reservation_token = str(
                    payload.get("reservation_token")
                    or payload.get("request_id")
                    or ""
                ).strip()
                if not reservation_token:
                    reservation_token = hashlib.sha256(
                        f"{actor_id}|{chat_id}|{date_value}|{time_value}|{guests_value}|{comment_value}".encode("utf-8")
                    ).hexdigest()

                existing_booking = conn.execute(
                    "SELECT id FROM bookings WHERE reservation_token=?",
                    (reservation_token,),
                ).fetchone()
                if existing_booking:
                    tg_send_message(chat_id, "Заявка уже принята в обработку.")
                    return {"ok": True}

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
                    "reservation_token": reservation_token,
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
                        user_chat_id,
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
                        reservation_token,
                        raw_payload_json
                    )
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WAITING', ?, ?, ?)
                    """,
                    (
                        None,
                        "telegram_miniapp",
                        saved_name or actor_name or "Telegram",
                        phone_e164,
                        phone_e164,
                        chat_id,
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
                        reservation_token,
                        json.dumps(raw_payload, ensure_ascii=False),
                    ),
                )
                booking_id = int(cur.lastrowid)
                print(
                    f"[MINIAPP] booking created: id={booking_id} "
                    f"date={date_value} time={time_value} guests={guests_count}",
                    flush=True,
                )

                log_booking_event(
                    conn,
                    booking_id,
                    "CREATED",
                    actor_id,
                    actor_name,
                    {"source": "telegram_miniapp"},
                )

                # Проверяем наличие TG_CHAT_ID
                if not TG_CHAT_ID:
                    print(f"[MINIAPP] TG_CHAT_ID not configured, booking #{booking_id} NOT sent to admins", flush=True)
                    log_booking_event(
                        conn,
                        booking_id,
                        "TG_SYNC_FAIL",
                        "system",
                        "system",
                        {
                            "reason": "TG_CHAT_ID_EMPTY",
                            "error": "Чат администраторов не настроен",
                            "source": "telegram_miniapp"
                        },
                    )
                    tg_send_message(
                        chat_id,
                        "❌ <b>Ошибка отправки</b>\n\n"
                        "Заявка сохранена, но чат администраторов не настроен. "
                        "Свяжитесь с поддержкой."
                    )
                    return {"ok": True}

                text, kb = render_booking_card(conn, booking_id)
                print(f"[MINIAPP] sending booking #{booking_id} to admin chat {TG_CHAT_ID}", flush=True)
                try:
                    msg_id = tg_send_message(str(TG_CHAT_ID), text, kb)
                    if not msg_id:
                        raise RuntimeError("Telegram sendMessage returned empty message_id")
                except Exception as e:
                    print(f"[MINIAPP] ERROR sending booking #{booking_id} to admin chat {TG_CHAT_ID}: {e}", flush=True)
                    log_booking_event(
                        conn,
                        booking_id,
                        "TG_SYNC_FAIL",
                        "system",
                        "system",
                        {
                            "reason": "SEND_TO_ADMIN_CHAT_FAILED",
                            "target_chat_id": str(TG_CHAT_ID),
                            "error": str(e),
                            "source": "telegram_miniapp"
                        },
                    )
                    tg_send_message(
                        chat_id,
                        "❌ <b>Ошибка отправки</b>\n\n"
                        "Заявка сохранена, но чат администраторов недоступен. "
                        "Попробуйте ещё раз позже или свяжитесь прямо по телефону."
                    )
                    return {"ok": True}

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
                    {"target_chat_id": str(TG_CHAT_ID), "status": "sent", "source": "telegram_miniapp"},
                )
                print(f"[MINIAPP] booking #{booking_id} sent OK, msg_id={msg_id}", flush=True)

                # гостю пока не подтверждаем бронь — только сообщаем, что заявка принята в работу
                waiting_text = (
                    "🕓 <b>Заявка отправлена</b>\n\n"
                    "Мы передали её администратору.\n"
                    "Сообщение о подтверждении придёт сюда после проверки."
                )
                tg_send_message(chat_id, waiting_text)
                try:
                    sync_ok = send_booking_event(
                        conn,
                        booking_id,
                        "BOOKING_UPSERT",
                        {
                            "actor_tg_id": actor_id,
                            "actor_name": actor_name,
                            "payload": {"source": "telegram_miniapp"},
                        },
                    )
                    if not sync_ok:
                        log_booking_event(
                            conn,
                            booking_id,
                            "CRM_SYNC_FAIL",
                            "system",
                            "system",
                            {"source": "telegram_miniapp", "reason": "send_booking_event_false"},
                        )
                except Exception:
                    log_booking_event(
                        conn,
                        booking_id,
                        "CRM_SYNC_FAIL",
                        "system",
                        "system",
                        {"source": "telegram_miniapp", "reason": "send_booking_event_exception"},
                    )
                return {"ok": True}

            text = (m.get("text") or "").strip()
            text_lc = text.lower()
            cmd = ""
            if text.startswith("/"):
                # Support commands in groups like /lineup@my_bot
                cmd = text.split()[0].split("@", 1)[0].lower()

            # ===== Высокий приоритет: ожидание комментария к гостю =====
            # Обрабатывается ДО команд, загрузки афиши и любых других pending-сценариев.
            if text and not cmd:
                _table_row = conn.execute(
                    """
                    SELECT id, booking_id, phone_e164, expires_at
                    FROM pending_replies
                    WHERE chat_id=? AND actor_tg_id=? AND kind='table_flow'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (chat_id, actor_id),
                ).fetchone()
                if _table_row:
                    _valid = True
                    try:
                        _exp = datetime.fromisoformat(str(_table_row["expires_at"]))
                        if datetime.utcnow() > _exp:
                            conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                            _valid = False
                    except Exception:
                        pass

                    if _valid:
                        flow = _load_pending_reply_payload(_table_row)
                        mode = str(flow.get("mode") or "").strip()
                        booking_id = int(_table_row["booking_id"] or 0)

                        if mode == "assign_table":
                            table_number = normalize_table_number(text)
                            if not table_number:
                                tg_send_message(chat_id, "Номер стола должен быть положительным целым числом.")
                                return {"ok": True}

                            booking_row = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
                            if not booking_row:
                                conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                                tg_send_message(chat_id, "Бронь не найдена.")
                                return {"ok": True}

                            conflicts = get_table_assignment_conflicts(conn, booking_row, table_number, exclude_booking_id=booking_id)
                            if conflicts["booking_conflicts"] or conflicts["restricted"]:
                                conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                                tg_send_message(
                                    chat_id,
                                    _format_table_conflict_message(conflicts, table_number),
                                    reply_markup={
                                        "inline_keyboard": [[
                                            {
                                                "text": f"⚠️ Override стол #{table_number}",
                                                "callback_data": f"b:{booking_id}:table:assign_override:{table_number}",
                                            }
                                        ]]
                                    },
                                )
                                return {"ok": True}

                            result = assign_table_to_booking(conn, booking_id, table_number, actor_id, actor_name)
                            conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                            _sync_admin_booking_card(conn, booking_id)
                            try:
                                send_booking_event(
                                    conn,
                                    booking_id,
                                    "BOOKING_TABLE_UPDATED",
                                    {
                                        "actor_tg_id": actor_id,
                                        "actor_name": actor_name,
                                        "payload": {
                                            "action": "assign_table",
                                            "table_number": result["table_number"],
                                        },
                                        "table_number": result["table_number"],
                                    },
                                )
                            except Exception:
                                pass
                            try:
                                notify_waiters_about_deposit_booking(conn, booking_id)
                            except Exception:
                                traceback.print_exc()
                            tg_send_message(chat_id, f"✅ Стол #{result['table_number']} назначен к брони #{booking_id}.")
                            return {"ok": True}

                        if mode == "set_deposit":
                            try:
                                deposit = set_booking_deposit(conn, booking_id, text, actor_id, actor_name)
                            except ValueError:
                                tg_send_message(chat_id, "Сумма депозита должна быть положительным целым числом.")
                                return {"ok": True}

                            conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                            _sync_admin_booking_card(conn, booking_id)
                            try:
                                send_booking_event(
                                    conn,
                                    booking_id,
                                    "BOOKING_DEPOSIT_SET",
                                    {
                                        "actor_tg_id": actor_id,
                                        "actor_name": actor_name,
                                        "payload": {
                                            "action": "set_deposit",
                                            "deposit_amount": deposit["deposit_amount"],
                                            "deposit_comment": deposit["deposit_comment"],
                                        },
                                    },
                                )
                            except Exception:
                                pass
                            try:
                                notify_waiters_about_deposit_booking(conn, booking_id)
                            except Exception:
                                traceback.print_exc()
                            tg_send_message(
                                chat_id,
                                f"✅ Депозит {deposit['deposit_amount']} сохранён для брони #{booking_id}.",
                            )
                            return {"ok": True}

                        if mode in {"restrict_number", "manual_restrict_number"}:
                            table_number = normalize_table_number(text)
                            if not table_number:
                                tg_send_message(chat_id, "Номер стола должен быть положительным целым числом.")
                                return {"ok": True}

                            flow["table_number"] = table_number
                            flow["mode"] = "restrict_until"
                            conn.execute(
                                "UPDATE pending_replies SET phone_e164=?, expires_at=? WHERE id=?",
                                (
                                    json.dumps(flow, ensure_ascii=False),
                                    (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds"),
                                    _table_row["id"],
                                ),
                            )
                            target = f"Бронь #{booking_id}\n" if booking_id else ""
                            tg_send_message(
                                chat_id,
                                (
                                    "<b>Ограничение стола</b>\n"
                                    f"{target}Стол #{table_number}\n\n"
                                    "Напишите, на сколько часов поставить ограничение.\n"
                                    "Пример: <code>2</code> или <code>5</code>"
                                ).strip(),
                            )
                            return {"ok": True}

                        if mode == "restrict_until":
                            table_number = normalize_table_number(flow.get("table_number"))
                            restricted_until = parse_restriction_until(text)
                            if not table_number:
                                conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                                tg_send_message(chat_id, "Не удалось определить стол. Начните заново.")
                                return {"ok": True}
                            if not restricted_until:
                                tg_send_message(chat_id, "Нужно указать положительное число часов. Пример: 3")
                                return {"ok": True}

                            try:
                                result = set_table_label(
                                    conn,
                                    table_number,
                                    "RESTRICTED",
                                    actor_id,
                                    actor_name,
                                    restricted_until=restricted_until,
                                    booking_id=booking_id or None,
                                    force_override=bool(flow.get("force_override")),
                                )
                            except ValueError as exc:
                                if str(exc) == "table_conflict":
                                    conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                                    if booking_id:
                                        tg_send_message(
                                            chat_id,
                                            f"⚠️ Для стола #{table_number} есть конфликт. Подтвердите override кнопкой ниже.",
                                            reply_markup={
                                                "inline_keyboard": [[
                                                    {
                                                        "text": f"⚠️ Override restriction #{table_number}",
                                                        "callback_data": f"b:{booking_id}:table:restrict_override:{table_number}",
                                                    }
                                                ]]
                                            },
                                        )
                                    else:
                                        tg_send_message(
                                            chat_id,
                                            "⚠️ Для этого стола есть конфликтующая бронь. Повторите команду позже или используйте CRM для ручного override.",
                                        )
                                    return {"ok": True}
                                raise

                            conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                            if booking_id:
                                _sync_admin_booking_card(conn, booking_id)
                                try:
                                    send_booking_event(
                                        conn,
                                        booking_id,
                                        "BOOKING_TABLE_RESTRICTED",
                                        {
                                            "actor_tg_id": actor_id,
                                            "actor_name": actor_name,
                                            "payload": {
                                                "action": "restrict_table",
                                                "table_number": result["table_number"],
                                                "restricted_until": result["restricted_until"],
                                            },
                                            "table_number": result["table_number"],
                                        },
                                    )
                                except Exception:
                                    pass
                            else:
                                try:
                                    send_table_event(
                                        conn,
                                        result["table_number"],
                                        "TABLE_RESTRICTED",
                                        {
                                            "actor_tg_id": actor_id,
                                            "actor_name": actor_name,
                                            "payload": {
                                                "action": "restrict_table",
                                                "table_number": result["table_number"],
                                                "restricted_until": result["restricted_until"],
                                            },
                                        },
                                    )
                                except Exception:
                                    pass
                            tg_send_message(
                                chat_id,
                                f"✅ Стол #{result['table_number']} ограничен до <code>{_h(_display_restriction_time(result['restricted_until']))}</code>.",
                            )
                            return {"ok": True}

                        if mode == "clear_restriction":
                            table_number = normalize_table_number(text)
                            if not table_number:
                                tg_send_message(chat_id, "Номер стола должен быть положительным целым числом.")
                                return {"ok": True}

                            result = set_table_label(conn, table_number, "NONE", actor_id, actor_name)
                            conn.execute("DELETE FROM pending_replies WHERE id=?", (_table_row["id"],))
                            try:
                                send_table_event(
                                    conn,
                                    result["table_number"],
                                    "TABLE_LABEL_CLEARED",
                                    {
                                        "actor_tg_id": actor_id,
                                        "actor_name": actor_name,
                                        "payload": {
                                            "action": "clear_restriction",
                                            "table_number": result["table_number"],
                                        },
                                    },
                                )
                            except Exception:
                                pass
                            tg_send_message(chat_id, f"✅ Ограничение со стола #{result['table_number']} снято.")
                            return {"ok": True}

                _note_row = conn.execute(
                    """
                    SELECT id, booking_id, phone_e164, expires_at
                    FROM pending_replies
                    WHERE chat_id=? AND actor_tg_id=? AND kind='guest_note'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (chat_id, actor_id),
                ).fetchone()
                if _note_row:
                    _valid = True
                    try:
                        _exp = datetime.fromisoformat(str(_note_row["expires_at"]))
                        if datetime.utcnow() > _exp:
                            conn.execute("DELETE FROM pending_replies WHERE id=?", (_note_row["id"],))
                            _valid = False
                    except Exception:
                        pass
                    if _valid:
                        booking_id = int(_note_row["booking_id"])
                        phone = str(_note_row["phone_e164"] or "")

                        add_guest_note(conn, phone, text, actor_id, actor_name)
                        conn.execute("DELETE FROM pending_replies WHERE id=?", (_note_row["id"],))

                        try:
                            text_card, kb_card = render_booking_card(conn, booking_id)

                            booking_row = conn.execute(
                                """
                                SELECT telegram_chat_id, telegram_message_id
                                FROM bookings
                                WHERE id=?
                                """,
                                (booking_id,),
                            ).fetchone()

                            if booking_row and booking_row["telegram_chat_id"] and booking_row["telegram_message_id"]:
                                tg_edit_message(
                                    str(booking_row["telegram_chat_id"]),
                                    str(booking_row["telegram_message_id"]),
                                    text_card,
                                    kb_card,
                                )
                        except Exception:
                            pass

                        tg_send_message(chat_id, "Комментарий к гостю сохранён.")
                        try:
                            send_booking_event(
                                conn,
                                booking_id,
                                "BOOKING_NOTE_ADDED",
                                {
                                    "actor_tg_id": actor_id,
                                    "actor_name": actor_name,
                                    "guest_note": text,
                                },
                            )
                        except Exception:
                            pass
                        return {"ok": True}

            if cmd == "/start":
                parts = text.split()

                if len(parts) > 1 and parts[1].startswith("auth_"):
                    code_raw = parts[1].replace("auth_", "", 1)
                    code = _normalize_auth_code(code_raw)
                    if not code:
                        tg_send_message(chat_id, "Код авторизации не распознан. Откройте CRM и получите новый код.")
                        return {"ok": True}

                    ok, msg = _confirm_crm_auth(code, actor_id)
                    if ok:
                        tg_send_message(
                            chat_id,
                            "✅ <b>Вход подтвержден</b>\n\n"
                            "Вернитесь в окно CRM, страница войдет автоматически.",
                        )
                    else:
                        tg_send_message(
                            chat_id,
                            "❌ Не удалось подтвердить вход.\n"
                            f"Причина: <code>{_h(msg)}</code>\n\n"
                            "Получите новый код в CRM и повторите попытку.",
                        )
                    return {"ok": True}

                if len(parts) > 1 and parts[1].startswith("promo_"):
                    code = parts[1].replace("promo_", "").strip()

                    # Сохраняем параметр /start в истории пользователя ДО проверки статуса
                    # Так пользователь учитывается даже если QR уже использован
                    conn.execute(
                        """
                        INSERT INTO tg_bot_users (tg_user_id, username, first_name, 
                                                  first_started_at, last_started_at, 
                                                  start_count, last_start_param)
                        VALUES (?, ?, ?, datetime('now'), datetime('now'), 1, ?)
                        ON CONFLICT(tg_user_id) DO UPDATE SET
                            username=CASE
                                WHEN excluded.username IS NOT NULL AND trim(excluded.username) <> '' THEN excluded.username
                                ELSE tg_bot_users.username
                            END,
                            first_name=CASE
                                WHEN excluded.first_name IS NOT NULL AND trim(excluded.first_name) <> '' THEN excluded.first_name
                                ELSE tg_bot_users.first_name
                            END,
                            last_started_at=datetime('now'),
                            start_count=start_count+1,
                            last_start_param=excluded.last_start_param
                        """,
                        (actor_id, tg_username, first_name, parts[1]),
                    )
                    conn.commit()

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
                        "Действует до: <b>31 марта</b>\n\n"
                        "Вы можете воспользоваться скидкой "
                        "один раз, предъявив открытку с QR-кодом официанту.\n\n"
                        "Будем рады видеть вас в LUCHBAR."
                    )

                    if actor_id in PROMO_ADMIN_IDS:
                        # Для админов только кнопка "Провести скидку"
                        kb = {
                            "inline_keyboard": [
                                [
                                    {
                                        "text": "✅ Провести скидку",
                                        "callback_data": f"promo:redeem:{code}"
                                    }
                                ]
                            ]
                        }
                    else:
                        # Для гостей меню с основными функциями
                        kb = build_luch_main_menu()

                    tg_send_message(chat_id, text_msg, kb)
                    return {"ok": True}

                # Проверяем есть ли у пользователя телефон
                user_row = conn.execute(
                    "SELECT has_shared_phone FROM tg_bot_users WHERE tg_user_id=?",
                    (actor_id,),
                ).fetchone()

                has_phone = user_row and user_row["has_shared_phone"]

                if not has_phone:
                    contact_text = (
                        "<b>Добро пожаловать в LUCHBAR!</b>\n\n"
                        "Для бронирования нам нужен ваш номер телефона.\n"
                        "Нажмите кнопку ниже, чтобы поделиться контактом."
                    )
                    tg_send_message(
                        chat_id,
                        contact_text,
                        {
                            "keyboard": [[{"text": "📱 Поделиться номером", "request_contact": True}]],
                            "resize_keyboard": True,
                            "one_time_keyboard": True,
                        },
                    )
                    return {"ok": True}
                else:
                    tg_send_message(
                        chat_id,
                        "<b>LUCHBAR</b>\n\n"
                        "Выберите нужный раздел ниже.",
                        build_luch_main_menu(),
                    )
                    return {"ok": True}

                # Обычный /start без параметра - учитываем пользователя
                conn.execute(
                    """
                    INSERT INTO tg_bot_users (tg_user_id, username, first_name, 
                                              first_started_at, last_started_at, 
                                              start_count)
                    VALUES (?, ?, ?, datetime('now'), datetime('now'), 1)
                    ON CONFLICT(tg_user_id) DO UPDATE SET
                        username=CASE
                            WHEN excluded.username IS NOT NULL AND trim(excluded.username) <> '' THEN excluded.username
                            ELSE tg_bot_users.username
                        END,
                        first_name=CASE
                            WHEN excluded.first_name IS NOT NULL AND trim(excluded.first_name) <> '' THEN excluded.first_name
                            ELSE tg_bot_users.first_name
                        END,
                        last_started_at=datetime('now'),
                        start_count=start_count+1
                    """,
                    (actor_id, tg_username, first_name),
                )
                conn.commit()

            if cmd == "/auth":
                parts = text.split(maxsplit=1)
                code = _normalize_auth_code(parts[1] if len(parts) > 1 else "")
                if not code:
                    tg_send_message(
                        chat_id,
                        "Используйте формат: <code>/auth AUTH-XXXXXX</code>",
                    )
                    return {"ok": True}

                ok, msg = _confirm_crm_auth(code, actor_id)
                if ok:
                    tg_send_message(
                        chat_id,
                        "✅ <b>Вход подтвержден</b>\n\n"
                        "Вернитесь в окно CRM, страница войдет автоматически.",
                    )
                else:
                    tg_send_message(
                        chat_id,
                        "❌ Не удалось подтвердить вход.\n"
                        f"Причина: <code>{_h(msg)}</code>",
                    )
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

            if cmd == "/chatid":
                chat_type = str(chat.get("type") or "").strip() or "unknown"
                chat_title = str(chat.get("title") or chat.get("username") or "").strip()
                lines = [
                    "<b>Текущий чат</b>",
                    f"chat_id: <code>{_h(chat_id)}</code>",
                    f"type: <code>{_h(chat_type)}</code>",
                ]
                if chat_title:
                    lines.append(f"title: <code>{_h(chat_title)}</code>")

                configured_waiter_chat = str(WAITER_CHAT_ID or "").strip()
                if configured_waiter_chat and actor_id in PROMO_ADMIN_IDS:
                    lines.append(
                        f"совпадает с WAITER_CHAT_ID: <b>{'ДА' if configured_waiter_chat == chat_id else 'НЕТ'}</b>"
                    )

                tg_send_message(chat_id, "\n".join(lines))
                return {"ok": True}

            if cmd == "/stat":
                print(f"[/STAT] actor_id={actor_id}, PROMO_ADMIN_IDS={PROMO_ADMIN_IDS}", flush=True)
                
                if actor_id not in PROMO_ADMIN_IDS:
                    error_msg = (
                        "❌ Доступ запрещён\n\n"
                        "Ваш ID: <code>{}</code>\n\n"
                    ).format(_h(actor_id))
                    
                    if PROMO_ADMIN_IDS:
                        error_msg += f"Админы: {', '.join(PROMO_ADMIN_IDS[:5])}"
                    else:
                        error_msg += "⚠️ PROMO_ADMIN_IDS не установлены на сервере!"
                    
                    tg_send_message(chat_id, error_msg)
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

            if cmd == "/testadminchat":
                if actor_id not in PROMO_ADMIN_IDS:
                    tg_send_message(chat_id, "Нет доступа.")
                    return {"ok": True}

                admin_chat_id = str(TG_CHAT_ID or "").strip()
                if not admin_chat_id:
                    tg_send_message(
                        chat_id,
                        "TG_CHAT_ID пустой.\nПроверь переменную окружения на сервере."
                    )
                    return {"ok": True}

                try:
                    test_message_id = tg_send_message(
                        admin_chat_id,
                        "🧪 Тестовое сообщение\n\nПроверка отправки в чат администраторов."
                    )
                    tg_send_message(
                        chat_id,
                        "Тест отправки выполнен успешно.\n\n"
                        f"TG_CHAT_ID: <code>{_h(admin_chat_id)}</code>\n"
                        f"message_id: <code>{_h(str(test_message_id))}</code>"
                    )
                except Exception as e:
                    tg_send_message(
                        chat_id,
                        "Ошибка отправки в чат администраторов.\n\n"
                        f"TG_CHAT_ID: <code>{_h(admin_chat_id)}</code>\n"
                        f"Ошибка: <code>{_h(str(e))}</code>"
                    )
                return {"ok": True}

            if cmd == "/restrict_table":
                if not _is_backoffice_context(chat_id, actor_id):
                    tg_send_message(chat_id, "Команда доступна только в рабочем контуре.")
                    return {"ok": True}

                _create_pending_reply(
                    conn,
                    "table_flow",
                    0,
                    chat_id,
                    actor_id,
                    {"mode": "manual_restrict_number"},
                    (
                        "<b>Ограничение стола</b>\n\n"
                        "Напишите номер стола, который нужно ограничить."
                    ),
                )
                return {"ok": True}

            if cmd == "/clear_table_restriction":
                if not _is_backoffice_context(chat_id, actor_id):
                    tg_send_message(chat_id, "Команда доступна только в рабочем контуре.")
                    return {"ok": True}

                _create_pending_reply(
                    conn,
                    "table_flow",
                    0,
                    chat_id,
                    actor_id,
                    {"mode": "clear_restriction"},
                    (
                        "<b>Снять ограничение</b>\n\n"
                        "Напишите номер стола, с которого нужно снять ограничение."
                    ),
                )
                return {"ok": True}

            if cmd == "/restricted_tables":
                rows = get_active_table_restrictions(conn)
                if not rows:
                    tg_send_message(chat_id, "Сейчас активных ограничений по столам нет.")
                    return {"ok": True}

                lines = ["<b>Активные ограничения столов</b>"]
                for row in rows:
                    lines.append(
                        f"• Стол #{row['table_number']} до <code>{_h(_display_restriction_time(row['restricted_until']))}</code>"
                    )
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

                tg_send_photo(chat_id, file_id)
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
                if pending_lineup and not cmd:
                    tg_send_message(chat_id, "Пожалуйста, отправьте изображение афиши (как фото).")
                    return {"ok": True}

            if not text:
                return {"ok": True}

        return {"ok": True}
    except Exception as e:
        print(
            "[TG-WEBHOOK] ERROR "
            f"update_id={update_id} "
            f"type={update_type} "
            f"chat_id={chat_id_dbg} "
            f"message_id={message_id_dbg} "
            f"callback_query_id={callback_query_id_dbg} "
            f"error={e}",
            flush=True,
        )
        print(traceback.format_exc(), flush=True)
        return {"ok": True}
    finally:
        conn.commit()
        conn.close()
