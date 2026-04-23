import json
import hashlib
import traceback
from typing import Any, Optional

from booking_service import load_booking_read_model, load_table_read_model
from config import CRM_API_URL
from integration_service import create_outbox_message


def crm_sync_enabled() -> bool:
    return bool(CRM_API_URL)


def _row_to_dict(row) -> dict[str, Any]:
    if not row:
        return {}
    return {k: row[k] for k in row.keys()}


def _payload_fingerprint(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _build_payload(conn, booking_id: int, event_name: str, meta: Optional[dict[str, Any]]) -> dict[str, Any]:
    booking = load_booking_read_model(conn, booking_id)
    if not booking:
        raise ValueError("booking_not_found")
    raw_payload = {}
    try:
        raw_payload = json.loads(booking.get("raw_payload_json") or "{}")
    except Exception:
        raw_payload = {}
    booking["raw_payload"] = raw_payload
    table_payload = {}

    guest = {}
    tg_user = {}

    phone = str(booking.get("phone_e164") or "").strip()
    if phone:
        g_row = conn.execute("SELECT * FROM guests WHERE phone_e164=?", (phone,)).fetchone()
        if g_row:
            guest = _row_to_dict(g_row)
            try:
                guest["tags"] = json.loads(guest.get("tags_json") or "[]")
            except Exception:
                guest["tags"] = []

        t_row = conn.execute(
            """
            SELECT *
            FROM tg_bot_users
            WHERE phone_e164=?
            ORDER BY datetime(last_started_at) DESC
            LIMIT 1
            """,
            (phone,),
        ).fetchone()
        if t_row:
            tg_user = _row_to_dict(t_row)

    target_table_number = meta.get("table_number") if isinstance(meta, dict) else None
    if target_table_number in (None, "", 0):
        target_table_number = booking.get("assigned_table_number")
    try:
        if target_table_number:
            table_payload = load_table_read_model(conn, str(target_table_number)) or {}
    except Exception:
        table_payload = {}

    payload = {
        "event": event_name,
        "source": "luchbarbot",
        "booking": booking,
        "guest": guest,
        "tg_user": tg_user,
        "meta": meta or {},
    }
    if table_payload:
        payload["table"] = table_payload
    payload["external_event_id"] = f"booking:{booking_id}:{event_name}:{_payload_fingerprint(payload)}"
    return payload


def build_booking_sync_payload(
    conn,
    booking_id: int,
    event_name: str = "BOOKING_UPSERT",
    meta: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return _build_payload(conn, booking_id, event_name, meta)


def _build_table_payload(conn, table_number: str, event_name: str, meta: Optional[dict[str, Any]]) -> dict[str, Any]:
    table_payload = load_table_read_model(conn, str(table_number))
    if not table_payload:
        raise ValueError("table_not_found")
    payload = {
        "event": event_name,
        "source": "luchbarbot",
        "table": table_payload,
        "meta": meta or {},
    }
    payload["external_event_id"] = f"table:{table_number}:{event_name}:{_payload_fingerprint(payload)}"
    return payload


def send_booking_event(conn, booking_id: int, event_name: str, meta: Optional[dict[str, Any]] = None) -> bool:
    if not CRM_API_URL:
        return False

    try:
        payload = build_booking_sync_payload(conn, booking_id, event_name, meta)
    except Exception:
        traceback.print_exc()
        return False

    create_outbox_message(
        conn,
        reservation_id=None,
        platform="http",
        bot_scope="crm_sync",
        target_external_id=CRM_API_URL,
        message_type=f"crm_booking:{event_name}",
        payload=payload,
    )
    return True


def send_table_event(conn, table_number: str, event_name: str, meta: Optional[dict[str, Any]] = None) -> bool:
    if not CRM_API_URL:
        return False

    try:
        payload = _build_table_payload(conn, table_number, event_name, meta)
    except Exception:
        traceback.print_exc()
        return False

    create_outbox_message(
        conn,
        reservation_id=None,
        platform="http",
        bot_scope="crm_sync",
        target_external_id=CRM_API_URL,
        message_type=f"crm_table:{event_name}",
        payload=payload,
    )
    return True
