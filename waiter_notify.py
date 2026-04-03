import html
from typing import Optional

from config import WAITER_CHAT_ID
from local_log import log_event
from telegram_api import tg_send_message


def _h(value: object) -> str:
    return html.escape(str(value or ""), quote=False)


def _load_waiter_booking_row(conn, booking_id: int):
    row = conn.execute(
        """
        SELECT
            id,
            name,
            phone_e164,
            phone_raw,
            reservation_date,
            reservation_time,
            guests_count,
            comment,
            assigned_table_number,
            deposit_amount,
            deposit_comment
        FROM bookings
        WHERE id = ?
        """,
        (int(booking_id),),
    ).fetchone()
    return row


def build_waiter_booking_message(conn, booking_id: int) -> Optional[str]:
    row = _load_waiter_booking_row(conn, booking_id)
    if not row:
        return None

    table_number = row["assigned_table_number"]
    deposit_amount = row["deposit_amount"]
    if not table_number or not deposit_amount:
        return None

    dt_value = " ".join(
        part for part in [str(row["reservation_date"] or "").strip(), str(row["reservation_time"] or "").strip()] if part
    ).strip()
    guest_name = str(row["name"] or "").strip()
    guest_phone = str(row["phone_e164"] or row["phone_raw"] or "").strip()
    booking_comment = str(row["comment"] or "").strip()
    deposit_comment = str(row["deposit_comment"] or "").strip()

    lines = [
        "<b>Стол с депозитом</b>",
        f"<b>Бронь:</b> #{int(row['id'])}",
        f"<b>Стол:</b> #{int(table_number)}",
        f"<b>Депозит:</b> {_h(int(deposit_amount))} руб.",
    ]

    if dt_value:
        lines.append(f"<b>Дата/время:</b> {_h(dt_value)}")
    if row["guests_count"] is not None:
        lines.append(f"<b>Гостей:</b> {_h(int(row['guests_count']))}")
    if guest_name:
        lines.append(f"<b>Имя:</b> {_h(guest_name)}")
    if guest_phone:
        lines.append(f"<b>Телефон:</b> {_h(guest_phone)}")
    if deposit_comment:
        lines.append(f"<b>Комментарий к депозиту:</b> {_h(deposit_comment)}")
    if booking_comment:
        lines.append(f"<b>Комментарий к брони:</b> {_h(booking_comment)}")

    return "\n".join(lines)


def notify_waiters_about_deposit_booking(conn, booking_id: int) -> bool:
    target_chat_id = str(WAITER_CHAT_ID or "").strip()
    if not target_chat_id:
        log_event("WAITER-NOTIFY", status="skip", booking_id=int(booking_id), reason="missing_waiter_chat_id")
        return False

    row = _load_waiter_booking_row(conn, booking_id)
    if not row:
        log_event("WAITER-NOTIFY", status="skip", booking_id=int(booking_id), reason="booking_not_found")
        return False

    table_number = row["assigned_table_number"]
    deposit_amount = row["deposit_amount"]
    if not table_number:
        log_event(
            "WAITER-NOTIFY",
            status="skip",
            booking_id=int(booking_id),
            reason="missing_table",
            deposit=deposit_amount,
        )
        return False
    if not deposit_amount:
        log_event(
            "WAITER-NOTIFY",
            status="skip",
            booking_id=int(booking_id),
            reason="missing_deposit",
            table=table_number,
        )
        return False

    text = build_waiter_booking_message(conn, booking_id)
    if not text:
        log_event(
            "WAITER-NOTIFY",
            status="skip",
            booking_id=int(booking_id),
            reason="empty_message",
            table=table_number,
            deposit=deposit_amount,
        )
        return False

    log_event(
        "WAITER-NOTIFY",
        status="send",
        booking_id=int(booking_id),
        chat_id=target_chat_id,
        table=table_number,
        deposit=deposit_amount,
    )
    message_id = tg_send_message(target_chat_id, text)
    log_event(
        "WAITER-NOTIFY",
        status="sent",
        booking_id=int(booking_id),
        chat_id=target_chat_id,
        message_id=message_id,
    )
    return True
