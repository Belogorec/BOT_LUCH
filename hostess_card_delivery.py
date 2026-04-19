from integration_service import create_outbox_message, link_message_to_reservation
from outbox_dispatcher import dispatch_outbox_message


def get_hostess_card_link(conn, *, reservation_id: int) -> dict | None:
    row = conn.execute(
        """
        SELECT external_chat_id, external_message_id
        FROM bot_message_links
        WHERE reservation_id=?
          AND platform='telegram'
          AND bot_scope='hostess'
          AND message_kind='reservation_card'
          AND external_chat_id IS NOT NULL
          AND external_message_id IS NOT NULL
        ORDER BY datetime(updated_at) DESC, id DESC
        LIMIT 1
        """,
        (int(reservation_id),),
    ).fetchone()
    if not row:
        return None
    return {
        "chat_id": str(row["external_chat_id"] or "").strip(),
        "message_id": str(row["external_message_id"] or "").strip(),
    }


def dispatch_hostess_booking_card(
    conn,
    *,
    booking_id: int,
    reservation_id: int,
    chat_id: str,
    text: str,
    reply_markup: dict | None = None,
    message_id: str = "",
) -> dict:
    payload = {"text": str(text or "").strip()}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if str(message_id or "").strip():
        payload["message_id"] = str(message_id).strip()

    outbox_id = create_outbox_message(
        conn,
        reservation_id=int(reservation_id),
        platform="telegram",
        bot_scope="hostess",
        target_external_id=str(chat_id or "").strip(),
        message_type="reservation_card_upsert",
        payload=payload,
    )
    result = dispatch_outbox_message(conn, outbox_id)
    if not result.get("ok"):
        return result

    sent_message_id = str(result.get("provider_message_id") or "").strip()
    if sent_message_id and not str(message_id or "").strip():
        link_message_to_reservation(
            conn,
            reservation_id=int(reservation_id),
            platform="telegram",
            bot_scope="hostess",
            external_chat_id=str(chat_id or "").strip(),
            external_message_id=sent_message_id,
        )
    return result
