import json
import time
import traceback
from typing import Any, Optional

import requests

from config import CRM_API_KEY, CRM_API_URL, CRM_SYNC_TIMEOUT

_session = requests.Session()


def crm_sync_enabled() -> bool:
    return bool(CRM_API_URL)


def _row_to_dict(row) -> dict[str, Any]:
    if not row:
        return {}
    return {k: row[k] for k in row.keys()}


def _build_payload(conn, booking_id: int, event_name: str, meta: Optional[dict[str, Any]]) -> dict[str, Any]:
    booking_row = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not booking_row:
        raise ValueError("booking_not_found")

    booking = _row_to_dict(booking_row)
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
            table_row = conn.execute(
                """
                SELECT table_number, label, restricted_until, restriction_comment, updated_by, updated_at, created_at
                FROM venue_tables
                WHERE table_number = ?
                """,
                (str(target_table_number),),
            ).fetchone()
            if table_row:
                table_payload = _row_to_dict(table_row)
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
    return payload


def build_booking_sync_payload(
    conn,
    booking_id: int,
    event_name: str = "BOOKING_UPSERT",
    meta: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return _build_payload(conn, booking_id, event_name, meta)


def _build_table_payload(conn, table_number: str, event_name: str, meta: Optional[dict[str, Any]]) -> dict[str, Any]:
    table_row = conn.execute(
        """
        SELECT table_number, label, restricted_until, restriction_comment, updated_by, updated_at, created_at
        FROM venue_tables
        WHERE table_number = ?
        """,
        (str(table_number),),
    ).fetchone()
    if not table_row:
        raise ValueError("table_not_found")

    table_payload = _row_to_dict(table_row)
    return {
        "event": event_name,
        "source": "luchbarbot",
        "table": table_payload,
        "meta": meta or {},
    }


def send_booking_event(conn, booking_id: int, event_name: str, meta: Optional[dict[str, Any]] = None) -> bool:
    if not CRM_API_URL:
        return False

    try:
        payload = build_booking_sync_payload(conn, booking_id, event_name, meta)
    except Exception:
        traceback.print_exc()
        return False

    headers = {"Content-Type": "application/json"}
    if CRM_API_KEY:
        headers["X-CRM-API-Key"] = CRM_API_KEY

    max_attempts = 3
    timeout = max(3, int(CRM_SYNC_TIMEOUT))

    for attempt in range(1, max_attempts + 1):
        try:
            response = _session.post(
                CRM_API_URL,
                json=payload,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return True
        except Exception as exc:
            print(
                f"[CRM_SYNC] booking_id={booking_id} event={event_name} "
                f"attempt={attempt}/{max_attempts} failed: {exc}",
                flush=True,
            )
            traceback.print_exc()
            if attempt < max_attempts:
                time.sleep(attempt)

    return False


def send_table_event(conn, table_number: str, event_name: str, meta: Optional[dict[str, Any]] = None) -> bool:
    if not CRM_API_URL:
        return False

    try:
        payload = _build_table_payload(conn, table_number, event_name, meta)
    except Exception:
        traceback.print_exc()
        return False

    headers = {"Content-Type": "application/json"}
    if CRM_API_KEY:
        headers["X-CRM-API-Key"] = CRM_API_KEY

    max_attempts = 3
    timeout = max(3, int(CRM_SYNC_TIMEOUT))

    for attempt in range(1, max_attempts + 1):
        try:
            response = _session.post(
                CRM_API_URL,
                json=payload,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return True
        except Exception as exc:
            print(
                f"[CRM_SYNC] table_number={table_number} event={event_name} "
                f"attempt={attempt}/{max_attempts} failed: {exc}",
                flush=True,
            )
            traceback.print_exc()
            if attempt < max_attempts:
                time.sleep(attempt)

    return False
