from typing import Any, Optional

from config import VK_HOSTESS_PEER_IDS, VK_WAITER_PEER_IDS
from booking_service import resolve_core_reservation_id
from integration_service import create_outbox_message
from integration_service import upsert_bot_peer
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

    scope = str(bot_key or "").strip() or "hostess"
    existing = conn.execute(
        """
        SELECT id
        FROM bot_peers
        WHERE platform = 'vk' AND bot_scope = ? AND external_peer_id = ?
        LIMIT 1
        """,
        (scope, peer),
    ).fetchone()

    upsert_bot_peer(
        conn,
        platform="vk",
        bot_scope=scope,
        external_peer_id=peer,
        external_user_id=sender or None,
        display_name=str(message_text or role_hint or "").strip()[:255],
    )
    return existing is None


def fetch_active_vk_staff_peers(conn, *, bot_key: str) -> list[dict[str, Any]]:
    canonical_rows = conn.execute(
        """
        SELECT
            external_peer_id AS peer_id,
            external_user_id AS from_id,
            is_active,
            bot_scope AS role_hint,
            bot_scope AS bot_key,
            display_name AS last_message_text,
            last_seen_at,
            created_at,
            updated_at
        FROM bot_peers
        WHERE platform = 'vk'
          AND bot_scope = ?
          AND is_active = 1
        ORDER BY datetime(updated_at) DESC, external_peer_id DESC
        """,
        (str(bot_key or "").strip() or "hostess",),
    ).fetchall()

    deduped: list[dict[str, Any]] = []
    seen_peer_ids: set[str] = set()
    for row in canonical_rows:
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
    core_reservation_id = resolve_core_reservation_id(conn, int(booking_id or 0), allow_booking_sync=False)

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
