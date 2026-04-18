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


def _build_waiter_message_from_row(row, *, rich_text: bool) -> Optional[str]:
    if not row:
        return None

    table_number = row["assigned_table_number"]
    deposit_amount = row["deposit_amount"]
    if not table_number or not deposit_amount:
        return None

    if rich_text:
        lines = [
            "<b>Стол с депозитом</b>",
            f"<b>Стол:</b> #{table_number}",
            f"<b>Депозит:</b> {_h(int(deposit_amount))} руб.",
        ]
    else:
        lines = [
            "Стол с депозитом",
            f"Стол: #{table_number}",
            f"Депозит: {int(deposit_amount)} руб.",
        ]

    return "\n".join(lines)


def build_waiter_booking_message(conn, booking_id: int) -> Optional[str]:
    row = _load_waiter_booking_row(conn, booking_id)
    return _build_waiter_message_from_row(row, rich_text=True)


def build_waiter_vk_booking_message(conn, booking_id: int) -> Optional[str]:
    row = _load_waiter_booking_row(conn, booking_id)
    return _build_waiter_message_from_row(row, rich_text=False)


def notify_waiters_about_deposit_booking(conn, booking_id: int) -> bool:
    target_chat_id = str(WAITER_CHAT_ID or "").strip()
    has_waiter_tg = bool(target_chat_id)

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
        log_event("WAITER-NOTIFY", status="skip", booking_id=int(booking_id), reason="missing_deposit", table=table_number)
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

    delivered = False
    if has_waiter_tg:
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
        delivered = True
    else:
        log_event("WAITER-NOTIFY", status="skip", booking_id=int(booking_id), reason="missing_waiter_chat_id")

    from vk_staff_notify import notify_vk_waiters

    waiter_vk_text = build_waiter_vk_booking_message(conn, booking_id) or text
    waiter_vk_sent = notify_vk_waiters(conn, waiter_vk_text, source="deposit_booking", booking_id=int(booking_id))
    return delivered or bool(waiter_vk_sent)
