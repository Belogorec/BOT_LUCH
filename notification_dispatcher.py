import json
from typing import Any, Optional

from config import GUEST_COMM_ENABLED, GUEST_NOTIFICATION_TEST_MODE
from local_log import log_event, log_exception
from telegram_api import tg_send_message
from vk_api import vk_send_message


def _snapshot(payload: Optional[dict[str, Any]]) -> str:
    try:
        return json.dumps(payload or {}, ensure_ascii=False)
    except Exception:
        return "{}"


def _log_delivery(
    conn,
    *,
    reservation_id: Optional[int],
    guest_phone_e164: str,
    channel_binding_id: Optional[int],
    channel_type: str,
    event_type: str,
    payload_snapshot: Optional[dict[str, Any]],
    delivery_status: str,
    provider_message_id: str = "",
    error_text: str = "",
) -> int:
    conn.execute(
        """
        INSERT INTO notification_delivery_log (
            reservation_id, guest_phone_e164, channel_binding_id, channel_type,
            event_type, payload_snapshot_json, delivery_status, provider_message_id, error_text, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            reservation_id,
            str(guest_phone_e164 or "").strip() or None,
            channel_binding_id,
            str(channel_type or "").strip(),
            str(event_type or "").strip(),
            _snapshot(payload_snapshot),
            str(delivery_status or "").strip(),
            str(provider_message_id or "").strip() or None,
            str(error_text or "").strip() or None,
        ),
    )
    row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
    return int(row["id"]) if row else 0


def _binding_for_channel(conn, guest_phone_e164: str, channel_type: str):
    return conn.execute(
        """
        SELECT id, guest_phone_e164, channel_type, external_user_id, external_username, external_display_name
        FROM guest_channel_bindings
        WHERE guest_phone_e164=? AND channel_type=? AND status='active'
        ORDER BY datetime(updated_at) DESC, id DESC
        LIMIT 1
        """,
        (str(guest_phone_e164 or "").strip(), str(channel_type or "").strip()),
    ).fetchone()


def resolve_preferred_channel(conn, *, reservation_id: Optional[int] = None, guest_phone_e164: str = "") -> dict[str, Any]:
    if not GUEST_COMM_ENABLED:
        return {"ok": False, "error": "guest_comm_disabled"}

    phone = str(guest_phone_e164 or "").strip()
    preferred = ""
    resolved_reservation_id = int(reservation_id or 0) or None
    if reservation_id:
        booking = conn.execute(
            """
            SELECT id, phone_e164, preferred_channel, service_notifications_enabled
            FROM bookings WHERE id=?
            """,
            (int(reservation_id),),
        ).fetchone()
        if not booking:
            return {"ok": False, "error": "booking_not_found"}
        phone = phone or str(booking["phone_e164"] or "").strip()
        preferred = str(booking["preferred_channel"] or "").strip().lower()
        if int(booking["service_notifications_enabled"] or 1) == 0:
            return {"ok": False, "error": "service_notifications_disabled", "guest_phone_e164": phone}

    if not phone:
        return {"ok": False, "error": "guest_phone_missing"}

    bindings = conn.execute(
        """
        SELECT id, guest_phone_e164, channel_type, external_user_id, external_username, external_display_name
        FROM guest_channel_bindings
        WHERE guest_phone_e164=? AND status='active'
        ORDER BY datetime(updated_at) DESC, id DESC
        """,
        (phone,),
    ).fetchall()
    if not bindings:
        return {"ok": False, "error": "no_active_bindings", "guest_phone_e164": phone}

    by_channel = {str(r["channel_type"]): dict(r) for r in bindings}
    chosen = None
    if preferred and preferred in by_channel:
        chosen = by_channel[preferred]
    if chosen is None:
        chosen = dict(bindings[0])
        preferred = str(chosen["channel_type"])

    return {
        "ok": True,
        "reservation_id": resolved_reservation_id,
        "guest_phone_e164": phone,
        "preferred_channel": preferred,
        "binding": chosen,
    }


def send_service_notification_to_telegram(conn, *, binding: dict[str, Any], text: str) -> dict[str, Any]:
    chat_id = str(binding.get("external_user_id") or "").strip()
    if not chat_id:
        return {"ok": False, "error": "telegram_chat_id_missing"}
    if GUEST_NOTIFICATION_TEST_MODE:
        return {"ok": True, "provider_message_id": "test-mode"}
    message_id = tg_send_message(chat_id, str(text or "").strip())
    return {"ok": True, "provider_message_id": str(message_id or "")}


def send_service_notification_to_vk(conn, *, binding: dict[str, Any], text: str) -> dict[str, Any]:
    peer_id = str(binding.get("external_user_id") or "").strip()
    if not peer_id:
        return {"ok": False, "error": "vk_peer_id_missing"}
    if GUEST_NOTIFICATION_TEST_MODE:
        return {"ok": True, "provider_message_id": "test-mode"}
    resp = vk_send_message(int(peer_id), str(text or "").strip(), bot_key="guest")
    return {"ok": True, "provider_message_id": str(resp or "")}


def send_service_notification(
    conn,
    *,
    event_type: str,
    text: str,
    reservation_id: Optional[int] = None,
    guest_phone_e164: str = "",
    force_channel: str = "",
) -> dict[str, Any]:
    if not GUEST_COMM_ENABLED:
        return {"ok": False, "error": "guest_comm_disabled"}

    resolved = resolve_preferred_channel(conn, reservation_id=reservation_id, guest_phone_e164=guest_phone_e164)
    if not resolved.get("ok"):
        _log_delivery(
            conn,
            reservation_id=reservation_id,
            guest_phone_e164=str(resolved.get("guest_phone_e164") or guest_phone_e164 or ""),
            channel_binding_id=None,
            channel_type=force_channel or str(resolved.get("preferred_channel") or ""),
            event_type=event_type,
            payload_snapshot={"text": text},
            delivery_status="SKIPPED",
            error_text=str(resolved.get("error") or "resolve_failed"),
        )
        return resolved

    binding = dict(resolved["binding"])
    channel = str(force_channel or binding.get("channel_type") or "").strip().lower()
    binding_id = int(binding.get("id") or 0) or None
    phone = str(resolved.get("guest_phone_e164") or "")

    try:
        if channel == "telegram":
            result = send_service_notification_to_telegram(conn, binding=binding, text=text)
        elif channel == "vk":
            result = send_service_notification_to_vk(conn, binding=binding, text=text)
        else:
            result = {"ok": False, "error": "unsupported_channel"}
    except Exception as exc:
        log_exception("GUEST-NOTIFY", event=event_type, channel=channel, error=exc)
        result = {"ok": False, "error": str(exc)}

    if result.get("ok"):
        _log_delivery(
            conn,
            reservation_id=reservation_id,
            guest_phone_e164=phone,
            channel_binding_id=binding_id,
            channel_type=channel,
            event_type=event_type,
            payload_snapshot={"text": text},
            delivery_status="SENT",
            provider_message_id=str(result.get("provider_message_id") or ""),
        )
        log_event("GUEST-NOTIFY", status="sent", event=event_type, channel=channel, reservation_id=reservation_id or "-")
        return {
            "ok": True,
            "channel": channel,
            "reservation_id": reservation_id,
            "channel_binding_id": binding_id,
            "provider_message_id": str(result.get("provider_message_id") or ""),
        }

    _log_delivery(
        conn,
        reservation_id=reservation_id,
        guest_phone_e164=phone,
        channel_binding_id=binding_id,
        channel_type=channel,
        event_type=event_type,
        payload_snapshot={"text": text},
        delivery_status="FAILED",
        error_text=str(result.get("error") or "send_failed"),
    )
    log_event(
        "GUEST-NOTIFY",
        status="failed",
        event=event_type,
        channel=channel,
        reservation_id=reservation_id or "-",
        error=str(result.get("error") or "send_failed"),
    )
    return {
        "ok": False,
        "error": str(result.get("error") or "send_failed"),
        "channel": channel,
        "reservation_id": reservation_id,
        "channel_binding_id": binding_id,
    }
