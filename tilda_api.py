import json
import re

from flask import request, abort

from config import TILDA_SECRET, TG_CHAT_ID
from telegram_api import tg_send_message, tg_edit_message
from booking_service import (
    compute_segment,
    upsert_guest_if_missing,
    log_booking_event,
)
from booking_render import render_booking_card
from crm_sync import send_booking_event
from db import connect, get_tags


def ensure_db():
    return connect()


def tilda_webhook_impl(normalize_name, normalize_phone_e164, normalize_time_hhmm):
    key = (request.args.get("key") or "").strip()
    if not TILDA_SECRET or key != TILDA_SECRET:
        abort(403)

    payload = request.get_json(silent=True)
    if payload is None:
        payload = request.form.to_dict(flat=True)
    payload = payload or {}

    def pick(*keys, default=""):
        for k in keys:
            v = payload.get(k)
            if v is not None:
                s = str(v).strip()
                if s:
                    return s
        return default

    def pick_int_from_text(*keys):
        raw = pick(*keys, default="")
        if not raw:
            return None
        m = re.search(r"\d+", raw)
        return int(m.group(0)) if m else None

    name = normalize_name(
        pick(
            "Name", "name", "NAME",
            "Имя", "имя",
            default=""
        )
    )

    phone_raw = pick(
        "Phone", "phone", "PHONE",
        "Телефон", "телефон",
        "Mobile", "mobile",
        default=""
    )
    phone_e164 = normalize_phone_e164(phone_raw, default_region="RU")

    date_raw = pick(
        "date", "Date", "DATE",
        "reservation_date", "Reservation date",
        "Дата", "дата",
        default=""
    ).strip()

    time_raw_src = pick(
        "time", "Time", "TIME",
        "reservation_time", "Reservation time",
        "Время", "время",
        default=""
    ).strip()
    time_raw = normalize_time_hhmm(time_raw_src)

    guests_count = pick_int_from_text(
        "amountofguests", "guests", "Guests", "guests_count",
        "Количество гостей", "количество гостей",
        "Гостей", "гостей"
    )

    comment = pick(
        "comment", "Comment", "Comments",
        "Комментарий", "комментарий",
        "Комментарий к бронированию", "commentary",
        default=""
    )

    tranid = pick("tranid", "Tranid", "TRANID", default="")
    formname = pick("formname", "Formname", "FORMNAME", default="Бронь стола")

    utm_source = pick("utm_source", default="")
    utm_medium = pick("utm_medium", default="")
    utm_campaign = pick("utm_campaign", default="")
    utm_content = pick("utm_content", default="")
    utm_term = pick("utm_term", default="")

    reservation_dt = f"{date_raw}T{time_raw}" if (date_raw and time_raw) else ""

    conn = ensure_db()
    booking_id = None
    tg_status = "skipped"

    try:
        if phone_e164:
            upsert_guest_if_missing(conn, phone_e164, name)

        tags = get_tags(conn, phone_e164) if phone_e164 else []
        g_row = conn.execute(
            "SELECT visits_count FROM guests WHERE phone_e164=?",
            (phone_e164,),
        ).fetchone() if phone_e164 else None
        visits_count = int(g_row["visits_count"] or 0) if g_row else 0
        guest_segment = compute_segment(visits_count, tags)

        existing = None
        if tranid:
            existing = conn.execute(
                "SELECT id, telegram_chat_id, telegram_message_id FROM bookings WHERE tranid=?",
                (tranid,),
            ).fetchone()

        if existing:
            booking_id = int(existing["id"])
            conn.execute(
                """
                UPDATE bookings
                SET name=?,
                    phone_e164=?,
                    phone_raw=?,
                    reservation_date=?,
                    reservation_time=?,
                    reservation_dt=?,
                    guests_count=?,
                    comment=?,
                    utm_source=?,
                    utm_medium=?,
                    utm_campaign=?,
                    utm_content=?,
                    utm_term=?,
                    formname=?,
                    guest_segment=?,
                    raw_payload_json=?,
                    updated_at=datetime('now')
                WHERE id=?
                """,
                (
                    name,
                    phone_e164,
                    phone_raw,
                    date_raw,
                    time_raw,
                    reservation_dt,
                    guests_count,
                    comment,
                    utm_source,
                    utm_medium,
                    utm_campaign,
                    utm_content,
                    utm_term,
                    formname,
                    guest_segment,
                    json.dumps(payload, ensure_ascii=False),
                    booking_id,
                ),
            )
            log_booking_event(
                conn,
                booking_id,
                "UPDATED",
                "system",
                "system",
                {"source": "tilda"},
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO bookings
                  (tranid, formname, name, phone_e164, phone_raw, reservation_date, reservation_time, reservation_dt,
                   guests_count, comment, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                   status, guest_segment, raw_payload_json)
                VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WAITING', ?, ?)
                """,
                (
                    tranid or None,
                    formname,
                    name,
                    phone_e164,
                    phone_raw,
                    date_raw,
                    time_raw,
                    reservation_dt,
                    guests_count,
                    comment,
                    utm_source,
                    utm_medium,
                    utm_campaign,
                    utm_content,
                    utm_term,
                    guest_segment,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
            booking_id = int(cur.lastrowid)
            log_booking_event(conn, booking_id, "CREATED", "system", "system", {"source": "tilda"})

        conn.commit()

        try:
            send_booking_event(
                conn,
                booking_id,
                "BOOKING_UPSERT",
                {
                    "actor_tg_id": "system",
                    "actor_name": "tilda",
                    "payload": {"source": "tilda", "tg_status": tg_status},
                },
            )
        except Exception:
            pass

        try:
            text, kb = render_booking_card(conn, booking_id)

            if existing and existing["telegram_chat_id"] and existing["telegram_message_id"]:
                tg_edit_message(existing["telegram_chat_id"], existing["telegram_message_id"], text, kb)
                tg_status = "edited"
            else:
                msg_id = tg_send_message(TG_CHAT_ID, text, kb)
                conn.execute(
                    "UPDATE bookings SET telegram_chat_id=?, telegram_message_id=?, updated_at=datetime('now') WHERE id=?",
                    (str(TG_CHAT_ID), str(msg_id), booking_id),
                )
                tg_status = "sent"

            log_booking_event(
                conn,
                booking_id,
                "TG_SYNC_OK",
                "system",
                "system",
                {"status": tg_status},
            )
            conn.commit()

        except Exception as e:
            log_booking_event(
                conn,
                booking_id,
                "TG_SYNC_ERROR",
                "system",
                "system",
                {
                    "error": str(e),
                    "stage": "render_or_send",
                },
            )
            conn.commit()
            tg_status = "error"

        return {"ok": True, "booking_id": booking_id, "tg_status": tg_status}

    finally:
        conn.close()