from typing import Any, Optional

from local_log import log_event, log_exception
from vk_api import vk_api_enabled, vk_send_message


def upsert_vk_staff_peer(conn, *, peer_id: object, from_id: object = "", message_text: str = "") -> bool:
    peer = str(peer_id or "").strip()
    sender = str(from_id or "").strip()
    if not peer:
        return False

    existing = conn.execute("SELECT peer_id FROM vk_staff_peers WHERE peer_id = ?", (peer,)).fetchone()
    conn.execute(
        """
        INSERT INTO vk_staff_peers (
            peer_id, from_id, is_active, role_hint, last_message_text, last_seen_at, created_at, updated_at
        ) VALUES (?, ?, 1, 'hostess', ?, datetime('now'), datetime('now'), datetime('now'))
        ON CONFLICT(peer_id) DO UPDATE SET
            from_id = excluded.from_id,
            is_active = 1,
            last_message_text = excluded.last_message_text,
            last_seen_at = datetime('now'),
            updated_at = datetime('now')
        """,
        (peer, sender or None, (message_text or "").strip()[:500] or None),
    )
    return existing is None


def fetch_active_vk_staff_peers(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT peer_id, from_id, is_active, role_hint, last_message_text, last_seen_at, created_at, updated_at
        FROM vk_staff_peers
        WHERE is_active = 1
        ORDER BY datetime(updated_at) DESC, peer_id DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def build_vk_staff_booking_message(conn, booking_id: int, source: str = "") -> Optional[str]:
    row = conn.execute(
        """
        SELECT
            id, status, formname, name, phone_e164, phone_raw,
            reservation_date, reservation_time, guests_count, comment,
            assigned_table_number, deposit_amount, deposit_comment
        FROM bookings
        WHERE id = ?
        """,
        (int(booking_id),),
    ).fetchone()
    if not row:
        return None

    dt_value = " ".join(
        part for part in [str(row["reservation_date"] or "").strip(), str(row["reservation_time"] or "").strip()] if part
    ).strip() or "—"
    guest_name = str(row["name"] or "").strip() or "—"
    guest_phone = str(row["phone_e164"] or row["phone_raw"] or "").strip() or "—"
    comment = str(row["comment"] or "").strip()
    table_number = str(row["assigned_table_number"] or "").strip()
    deposit_amount = row["deposit_amount"]
    deposit_comment = str(row["deposit_comment"] or "").strip()
    source_label = str(source or row["formname"] or "booking").strip()

    lines = [
        "Новая бронь",
        f"ID: #{int(row['id'])}",
        f"Статус: {str(row['status'] or 'WAITING').strip() or 'WAITING'}",
        f"Дата/время: {dt_value}",
        f"Гость: {guest_name}",
        f"Телефон: {guest_phone}",
        f"Гостей: {row['guests_count'] if row['guests_count'] is not None else '—'}",
        f"Источник: {source_label}",
    ]

    if table_number:
        lines.append(f"Стол: #{table_number}")
    if deposit_amount:
        deposit_line = f"Депозит: {int(deposit_amount)}"
        if deposit_comment:
            deposit_line += f" ({deposit_comment})"
        lines.append(deposit_line)
    if comment:
        lines.append(f"Комментарий: {comment}")

    lines.append("")
    lines.append("Рабочий чат LUCH для хостес.")
    return "\n".join(lines)


def notify_vk_staff_about_new_booking(conn, booking_id: int, *, source: str = "") -> int:
    if not vk_api_enabled():
        log_event("VK-STAFF-NOTIFY", status="skip", booking_id=int(booking_id), reason="vk_api_disabled")
        return 0

    peers = fetch_active_vk_staff_peers(conn)
    if not peers:
        log_event("VK-STAFF-NOTIFY", status="skip", booking_id=int(booking_id), reason="no_active_peers")
        return 0

    text = build_vk_staff_booking_message(conn, booking_id, source=source)
    if not text:
        log_event("VK-STAFF-NOTIFY", status="skip", booking_id=int(booking_id), reason="booking_not_found")
        return 0

    sent = 0
    for peer in peers:
        peer_id = str(peer.get("peer_id") or "").strip()
        if not peer_id:
            continue
        try:
            vk_send_message(int(peer_id), text)
            sent += 1
            log_event("VK-STAFF-NOTIFY", status="sent", booking_id=int(booking_id), peer_id=peer_id, source=source or "-")
        except Exception as exc:
            log_exception("VK-STAFF-NOTIFY", status="send_failed", booking_id=int(booking_id), peer_id=peer_id, error=exc)
    return sent
