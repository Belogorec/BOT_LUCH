from typing import Any, Optional

from config import VK_HOSTESS_PEER_IDS, VK_WAITER_PEER_IDS
from integration_service import create_outbox_message
from local_log import log_event, log_exception
from outbox_dispatcher import dispatch_outbox_message
from vk_api import vk_api_enabled, vk_send_message
from vk_staff_flow import build_vk_booking_keyboard, render_vk_booking_message


def upsert_vk_staff_peer(
    conn,
    *,
    bot_key: str,
    role_hint: str,
    peer_id: object,
    from_id: object = "",
    message_text: str = "",
) -> bool:
    peer = str(peer_id or "").strip()
    sender = str(from_id or "").strip()
    if not peer:
        return False

    storage_peer_id = f"{str(bot_key or '').strip() or 'hostess'}:{peer}"
    existing = conn.execute("SELECT peer_id FROM vk_staff_peers WHERE peer_id = ?", (storage_peer_id,)).fetchone()
    conn.execute(
        """
        INSERT INTO vk_staff_peers (
            peer_id, peer_external_id, bot_key, from_id, is_active, role_hint, last_message_text, last_seen_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 1, ?, ?, datetime('now'), datetime('now'), datetime('now'))
        ON CONFLICT(peer_id) DO UPDATE SET
            peer_external_id = excluded.peer_external_id,
            bot_key = excluded.bot_key,
            from_id = excluded.from_id,
            is_active = 1,
            role_hint = excluded.role_hint,
            last_message_text = excluded.last_message_text,
            last_seen_at = datetime('now'),
            updated_at = datetime('now')
        """,
        (
            storage_peer_id,
            peer,
            str(bot_key or "").strip() or "hostess",
            sender or None,
            str(role_hint or "").strip() or None,
            (message_text or "").strip()[:500] or None,
        ),
    )
    return existing is None


def fetch_active_vk_staff_peers(conn, *, bot_key: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            COALESCE(NULLIF(peer_external_id, ''), peer_id) AS peer_id,
            from_id,
            is_active,
            role_hint,
            bot_key,
            last_message_text,
            last_seen_at,
            created_at,
            updated_at
        FROM vk_staff_peers
        WHERE is_active = 1 AND bot_key = ?
        ORDER BY datetime(updated_at) DESC, peer_id DESC
        """,
        (str(bot_key or "").strip() or "hostess",),
    ).fetchall()

    # After the multi-bot migration we can temporarily have both legacy rows
    # ("2000000001") and namespaced rows ("hostess:2000000001") for the same
    # VK chat. Keep only the freshest row per external peer to avoid duplicates.
    deduped: list[dict[str, Any]] = []
    seen_peer_ids: set[str] = set()
    for row in rows:
        peer_id = str(row["peer_id"] or "").strip()
        if not peer_id or peer_id in seen_peer_ids:
            continue
        seen_peer_ids.add(peer_id)
        deduped.append(dict(row))

    fallback_ids = VK_HOSTESS_PEER_IDS if str(bot_key or "").strip() == "hostess" else VK_WAITER_PEER_IDS
    for peer_id in fallback_ids:
        normalized = str(peer_id or "").strip()
        if not normalized or normalized in seen_peer_ids:
            continue
        seen_peer_ids.add(normalized)
        deduped.append(
            {
                "peer_id": normalized,
                "from_id": "",
                "is_active": 1,
                "role_hint": str(bot_key or "").strip() or "hostess",
                "bot_key": str(bot_key or "").strip() or "hostess",
                "last_message_text": "",
                "last_seen_at": "",
                "created_at": "",
                "updated_at": "",
            }
        )
    return deduped


def build_vk_staff_booking_message(conn, booking_id: int, source: str = "") -> Optional[str]:
    try:
        return render_vk_booking_message(conn, booking_id)
    except Exception:
        return None


def notify_vk_staff_about_new_booking(conn, booking_id: int, *, source: str = "") -> int:
    if not vk_api_enabled("hostess"):
        log_event("VK-STAFF-NOTIFY", status="skip", booking_id=int(booking_id), reason="vk_api_disabled")
        return 0

    peers = fetch_active_vk_staff_peers(conn, bot_key="hostess")
    if not peers:
        log_event("VK-STAFF-NOTIFY", status="skip", booking_id=int(booking_id), reason="no_active_peers")
        return 0

    text = build_vk_staff_booking_message(conn, booking_id, source=source)
    if not text:
        log_event("VK-STAFF-NOTIFY", status="skip", booking_id=int(booking_id), reason="booking_not_found")
        return 0

    sent = 0
    core_row = conn.execute(
        """
        SELECT id
        FROM reservations
        WHERE source='legacy_booking' AND external_ref=?
        LIMIT 1
        """,
        (str(int(booking_id)),),
    ).fetchone()
    core_reservation_id = int(core_row["id"]) if core_row else None

    for peer in peers:
        peer_id = str(peer.get("peer_id") or "").strip()
        if not peer_id:
            continue
        try:
            outbox_id = create_outbox_message(
                conn,
                reservation_id=core_reservation_id,
                platform="vk",
                bot_scope="hostess",
                target_external_id=peer_id,
                message_type="reservation_created",
                payload={
                    "text": text,
                    "keyboard": build_vk_booking_keyboard(int(booking_id)),
                    "source": source or "",
                    "booking_id": int(booking_id),
                },
            )
            result = dispatch_outbox_message(conn, outbox_id)
            if result.get("ok"):
                sent += 1
                log_event("VK-STAFF-NOTIFY", status="sent", booking_id=int(booking_id), peer_id=peer_id, source=source or "-")
            else:
                log_event(
                    "VK-STAFF-NOTIFY",
                    status="failed",
                    booking_id=int(booking_id),
                    peer_id=peer_id,
                    source=source or "-",
                    error=str(result.get("error") or "dispatch_failed"),
                )
        except Exception as exc:
            log_exception("VK-STAFF-NOTIFY", status="send_failed", booking_id=int(booking_id), peer_id=peer_id, error=exc)
    return sent


def notify_vk_waiters(conn, text: str, *, source: str = "", booking_id: Optional[int] = None) -> int:
    message_text = str(text or "").strip()
    if not message_text:
        return 0

    if not vk_api_enabled("waiter"):
        log_event("VK-WAITER-NOTIFY", status="skip", booking_id=booking_id or "-", reason="vk_api_disabled")
        return 0

    peers = fetch_active_vk_staff_peers(conn, bot_key="waiter")
    if not peers:
        log_event("VK-WAITER-NOTIFY", status="skip", booking_id=booking_id or "-", reason="no_active_peers")
        return 0

    sent = 0
    for peer in peers:
        peer_id = str(peer.get("peer_id") or "").strip()
        if not peer_id:
            continue
        try:
            vk_send_message(int(peer_id), message_text, bot_key="waiter")
            sent += 1
            log_event("VK-WAITER-NOTIFY", status="sent", booking_id=booking_id or "-", peer_id=peer_id, source=source or "-")
        except Exception as exc:
            log_exception("VK-WAITER-NOTIFY", status="send_failed", booking_id=booking_id or "-", peer_id=peer_id, error=exc)
    return sent
