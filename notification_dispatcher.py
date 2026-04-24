import json
from typing import Any, Optional

from config import GUEST_COMM_ENABLED, GUEST_NOTIFICATION_TEST_MODE
from booking_service import resolve_core_reservation_id
from integration_service import create_outbox_message
from local_log import log_event, log_exception
from outbox_dispatcher import dispatch_outbox_message


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


def _resolve_core_reservation_id(conn, reservation_id: Optional[int]) -> Optional[int]:
    return resolve_core_reservation_id(conn, int(reservation_id or 0), allow_booking_sync=False)


def _load_active_contact_channels(conn, guest_phone_e164: str):
    return conn.execute(
        """
        SELECT
            cc.id,
            c.phone_e164 AS guest_phone_e164,
            c.preferred_channel,
            c.service_notifications_enabled,
            c.marketing_notifications_enabled,
            cc.platform AS channel_type,
            cc.external_user_id,
            cc.external_peer_id,
            cc.username AS external_username,
            cc.display_name AS external_display_name
        FROM contacts c
        JOIN contact_channels cc
          ON cc.contact_id = c.id
        WHERE c.phone_e164 = ?
          AND cc.status = 'active'
        ORDER BY datetime(cc.updated_at) DESC, cc.id DESC
        """,
        (str(guest_phone_e164 or "").strip(),),
    ).fetchall()


def resolve_preferred_channel(conn, *, reservation_id: Optional[int] = None, guest_phone_e164: str = "") -> dict[str, Any]:
    if not GUEST_COMM_ENABLED:
        return {"ok": False, "error": "guest_comm_disabled"}

    phone = str(guest_phone_e164 or "").strip()
    preferred = ""
    resolved_reservation_id = _resolve_core_reservation_id(conn, reservation_id)
    if reservation_id:
        reservation = conn.execute(
            """
            SELECT id, guest_phone
            FROM reservations
            WHERE id=?
            """,
            (int(resolved_reservation_id or 0),),
        ).fetchone()
        if not reservation:
            return {"ok": False, "error": "reservation_not_found"}
        phone = phone or str(reservation["guest_phone"] or "").strip()

    if not phone:
        return {"ok": False, "error": "guest_phone_missing"}

    bindings = _load_active_contact_channels(conn, phone)
    if not bindings:
        return {"ok": False, "error": "no_active_bindings", "guest_phone_e164": phone}

    first_binding = dict(bindings[0])
    if int(first_binding.get("service_notifications_enabled") or 0) == 0:
        return {"ok": False, "error": "service_notifications_disabled", "guest_phone_e164": phone}

    by_channel = {str(r["channel_type"]): dict(r) for r in bindings}
    preferred = str(first_binding.get("preferred_channel") or "").strip().lower()
    chosen = None
    if preferred and preferred in by_channel:
        chosen = by_channel[preferred]
    if chosen is None:
        chosen = first_binding
        preferred = str(chosen["channel_type"])

    return {
        "ok": True,
        "reservation_id": resolved_reservation_id,
        "guest_phone_e164": phone,
        "preferred_channel": preferred,
        "binding": chosen,
    }


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
    resolved_reservation_id = int(resolved.get("reservation_id") or 0) or reservation_id
    channel = str(force_channel or binding.get("channel_type") or "").strip().lower()
    binding_id = int(binding.get("id") or 0) or None
    phone = str(resolved.get("guest_phone_e164") or "")

    if channel == "telegram":
        target_external_id = str(binding.get("external_user_id") or "").strip()
    elif channel == "vk":
        target_external_id = str(binding.get("external_peer_id") or binding.get("external_user_id") or "").strip()
    else:
        target_external_id = ""

    if not target_external_id:
        result = {"ok": False, "error": "target_external_id_missing"}
    elif GUEST_NOTIFICATION_TEST_MODE:
        result = {"ok": True, "provider_message_id": "test-mode"}
    else:
        try:
            outbox_id = create_outbox_message(
                conn,
                reservation_id=resolved_reservation_id,
                platform=channel,
                bot_scope="guest",
                message_type="guest_service_notification",
                payload={"text": str(text or "").strip(), "event_type": str(event_type or "").strip()},
                target_external_id=target_external_id,
            )
            result = dispatch_outbox_message(conn, outbox_id)
        except Exception as exc:
            log_exception("GUEST-NOTIFY", event=event_type, channel=channel, error=exc)
            result = {"ok": False, "error": str(exc)}

    if result.get("ok"):
        _log_delivery(
            conn,
            reservation_id=resolved_reservation_id,
            guest_phone_e164=phone,
            channel_binding_id=binding_id,
            channel_type=channel,
            event_type=event_type,
            payload_snapshot={"text": text},
            delivery_status="SENT",
            provider_message_id=str(result.get("provider_message_id") or ""),
        )
        log_event("GUEST-NOTIFY", status="sent", event=event_type, channel=channel, reservation_id=resolved_reservation_id or "-")
        return {
            "ok": True,
            "channel": channel,
            "reservation_id": resolved_reservation_id,
            "channel_binding_id": binding_id,
            "provider_message_id": str(result.get("provider_message_id") or ""),
        }

    _log_delivery(
        conn,
        reservation_id=resolved_reservation_id,
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
        reservation_id=resolved_reservation_id or "-",
        error=str(result.get("error") or "send_failed"),
    )
    return {
        "ok": False,
        "error": str(result.get("error") or "send_failed"),
        "channel": channel,
        "reservation_id": resolved_reservation_id,
        "channel_binding_id": binding_id,
    }
