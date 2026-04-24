import html
from typing import Optional

from booking_service import load_booking_read_model, resolve_core_reservation_id
from config import WAITER_CHAT_ID
from integration_service import create_outbox_message
from local_log import log_event
from outbox_dispatcher import dispatch_outbox_message


def _h(value: object) -> str:
    return html.escape(str(value or ""), quote=False)


def _load_waiter_booking_row_core(conn, booking_id: int):
    resolved_reservation_id = resolve_core_reservation_id(conn, int(booking_id or 0), allow_booking_sync=False)
    if not resolved_reservation_id:
        return None
    return conn.execute(
        """
        SELECT
            r.id AS reservation_id,
            r.guest_name AS name,
            r.guest_phone AS phone_e164,
            NULL AS phone_raw,
            substr(r.reservation_at, 1, 10) AS reservation_date,
            substr(r.reservation_at, 12, 5) AS reservation_time,
            r.party_size AS guests_count,
            r.comment AS comment,
            tc.code AS assigned_table_number,
            r.deposit_amount AS deposit_amount,
            r.deposit_comment AS deposit_comment
        FROM reservations r
        LEFT JOIN reservation_tables rt
          ON rt.reservation_id = r.id
         AND rt.released_at IS NULL
        LEFT JOIN tables_core tc
          ON tc.id = rt.table_id
        WHERE r.id = ?
        ORDER BY rt.id DESC
        LIMIT 1
        """,
        (int(resolved_reservation_id),),
    ).fetchone()


def _load_waiter_booking_row(conn, booking_id: int):
    row = _load_waiter_booking_row_core(conn, booking_id)
    if row:
        return row
    return load_booking_read_model(conn, int(booking_id))


def _build_waiter_message_from_row(row, *, rich_text: bool) -> Optional[str]:
    if not row:
        return None

    table_number = row["assigned_table_number"]
    deposit_amount = row["deposit_amount"]
    if not table_number or not deposit_amount:
        return None
    comment = str(row["deposit_comment"] or row["comment"] or "").strip()

    if rich_text:
        lines = [
            "<b>Стол с депозитом</b>",
            f"<b>Стол:</b> #{table_number}",
            f"<b>Депозит:</b> {_h(int(deposit_amount))} руб.",
        ]
        if comment:
            lines.append(f"<b>Комментарий:</b> {_h(comment)}")
    else:
        lines = [
            "Стол с депозитом",
            f"Стол: #{table_number}",
            f"Депозит: {int(deposit_amount)} руб.",
        ]
        if comment:
            lines.append(f"Комментарий: {comment}")

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
        core_reservation_id = resolve_core_reservation_id(conn, int(booking_id or 0), allow_booking_sync=False)
        log_event(
            "WAITER-NOTIFY",
            status="send",
            booking_id=int(booking_id),
            chat_id=target_chat_id,
            table=table_number,
            deposit=deposit_amount,
        )
        outbox_id = create_outbox_message(
            conn,
            reservation_id=core_reservation_id,
            platform="telegram",
            bot_scope="waiter",
            message_type="waiter_deposit_notification",
            payload={"text": text, "source": "deposit_booking", "booking_id": int(booking_id)},
            target_external_id=target_chat_id,
        )
        result = dispatch_outbox_message(conn, outbox_id)
        if result.get("ok"):
            log_event(
                "WAITER-NOTIFY",
                status="sent",
                booking_id=int(booking_id),
                chat_id=target_chat_id,
                message_id=result.get("provider_message_id") or "-",
            )
            delivered = True
        else:
            log_event(
                "WAITER-NOTIFY",
                status="failed",
                booking_id=int(booking_id),
                chat_id=target_chat_id,
                error=str(result.get("error") or "dispatch_failed"),
            )
    else:
        log_event("WAITER-NOTIFY", status="skip", booking_id=int(booking_id), reason="missing_waiter_chat_id")

    from vk_staff_notify import fetch_active_vk_staff_peers

    waiter_vk_text = build_waiter_vk_booking_message(conn, booking_id) or text
    peers = fetch_active_vk_staff_peers(conn, bot_key="waiter")
    vk_sent = 0
    for peer in peers:
        peer_id = str(peer.get("peer_id") or "").strip()
        if not peer_id:
            continue
        outbox_id = create_outbox_message(
            conn,
            reservation_id=None,
            platform="vk",
            bot_scope="waiter",
            message_type="waiter_deposit_notification",
            payload={"text": waiter_vk_text, "source": "deposit_booking", "booking_id": int(booking_id)},
            target_external_id=peer_id,
        )
        result = dispatch_outbox_message(conn, outbox_id)
        if result.get("ok"):
            vk_sent += 1
            log_event("VK-WAITER-NOTIFY", status="sent", booking_id=int(booking_id), peer_id=peer_id, source="deposit_booking")
        else:
            log_event(
                "VK-WAITER-NOTIFY",
                status="failed",
                booking_id=int(booking_id),
                peer_id=peer_id,
                error=str(result.get("error") or "dispatch_failed"),
            )
    return delivered or bool(vk_sent)
