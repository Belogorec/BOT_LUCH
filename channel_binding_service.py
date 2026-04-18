import hashlib
import json
import secrets
from datetime import datetime, timedelta
from typing import Any, Optional

from config import (
    GUEST_BINDING_TOKEN_PEPPER,
    GUEST_BINDING_TOKEN_TTL_MIN,
    GUEST_COMM_ENABLED,
    GUEST_PUBLIC_BASE_URL,
    TG_BINDING_START_PREFIX,
    TG_BOT_USERNAME,
    VK_GUEST_GROUP_ID,
)
from local_log import log_event

SUPPORTED_CHANNELS = {"telegram", "vk"}


def _now_utc_sql() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _to_sql(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _hash_token(token: str) -> str:
    material = f"{str(token or '').strip()}::{GUEST_BINDING_TOKEN_PEPPER}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _normalize_channel(channel_type: str) -> str:
    c = str(channel_type or "").strip().lower()
    if c not in SUPPORTED_CHANNELS:
        raise ValueError("unsupported_channel")
    return c


def _load_booking_by_reservation_token(conn, reservation_token: str):
    return conn.execute(
        """
        SELECT id, reservation_token, phone_e164, name, reservation_date, reservation_time, guests_count,
               status, preferred_channel, service_notifications_enabled, marketing_notifications_enabled
        FROM bookings
        WHERE reservation_token = ?
        LIMIT 1
        """,
        (str(reservation_token or "").strip(),),
    ).fetchone()


def _ensure_guest_profile(conn, phone_e164: str, name: str = "") -> None:
    phone = str(phone_e164 or "").strip()
    if not phone:
        return
    row = conn.execute("SELECT phone_e164, name_last FROM guests WHERE phone_e164=?", (phone,)).fetchone()
    if row:
        if (not (row["name_last"] or "").strip()) and str(name or "").strip():
            conn.execute(
                "UPDATE guests SET name_last=?, updated_at=datetime('now') WHERE phone_e164=?",
                (str(name or "").strip(), phone),
            )
        return
    conn.execute(
        """
        INSERT INTO guests (phone_e164, name_last, visits_count, first_visit_dt, last_visit_dt, tags_json)
        VALUES (?, ?, 0, NULL, NULL, '[]')
        """,
        (phone, str(name or "").strip() or None),
    )


def create_binding_token(
    conn,
    *,
    reservation_id: int,
    guest_phone_e164: str,
    channel_type: str,
    ttl_minutes: Optional[int] = None,
) -> dict[str, Any]:
    if not GUEST_COMM_ENABLED:
        raise ValueError("guest_comm_disabled")

    channel = _normalize_channel(channel_type)
    ttl = int(ttl_minutes or GUEST_BINDING_TOKEN_TTL_MIN or 45)
    if ttl <= 0:
        ttl = 45

    booking = conn.execute(
        "SELECT id, reservation_token, phone_e164 FROM bookings WHERE id=?",
        (int(reservation_id),),
    ).fetchone()
    if not booking:
        raise ValueError("booking_not_found")

    phone = str(guest_phone_e164 or booking["phone_e164"] or "").strip()
    raw_token = secrets.token_urlsafe(32)
    token_hash = _hash_token(raw_token)
    expires_at = _to_sql(datetime.utcnow() + timedelta(minutes=ttl))

    conn.execute(
        """
        INSERT INTO guest_binding_tokens (
            token_hash, reservation_id, guest_phone_e164, channel_type, status, expires_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'active', ?, datetime('now'), datetime('now'))
        """,
        (token_hash, int(reservation_id), phone or None, channel, expires_at),
    )
    token_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    log_event(
        "GUEST-BIND",
        action="token_created",
        token_id=token_id,
        reservation_id=int(reservation_id),
        channel=channel,
        expires_at=expires_at,
    )
    return {
        "token": raw_token,
        "token_id": token_id,
        "expires_at": expires_at,
        "reservation_id": int(reservation_id),
        "channel_type": channel,
        "guest_phone_e164": phone,
    }


def build_channel_deep_link(channel_type: str, plain_token: str) -> str:
    channel = _normalize_channel(channel_type)
    token = str(plain_token or "").strip()
    if channel == "telegram":
        if TG_BOT_USERNAME:
            return f"https://t.me/{TG_BOT_USERNAME}?start={TG_BINDING_START_PREFIX}{token}"
        return ""
    if channel == "vk":
        gid = str(VK_GUEST_GROUP_ID or "").strip()
        if gid:
            return f"https://vk.com/public{gid}?start={token}"
        return ""
    return ""


def get_reservation_by_token(conn, reservation_token: str):
    return _load_booking_by_reservation_token(conn, reservation_token)


def get_guest_bindings(conn, guest_phone_e164: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, guest_phone_e164, channel_type, external_user_id, external_username,
               external_display_name, status, is_verified, linked_at, created_at, updated_at
        FROM guest_channel_bindings
        WHERE guest_phone_e164 = ?
        ORDER BY datetime(updated_at) DESC, id DESC
        """,
        (str(guest_phone_e164 or "").strip(),),
    ).fetchall()
    return [dict(r) for r in rows]


def get_reservation_channel_status(conn, reservation_id: int) -> dict[str, Any]:
    booking = conn.execute(
        """
        SELECT id, reservation_token, phone_e164, preferred_channel,
               service_notifications_enabled, marketing_notifications_enabled
        FROM bookings WHERE id = ?
        """,
        (int(reservation_id),),
    ).fetchone()
    if not booking:
        raise ValueError("booking_not_found")
    phone = str(booking["phone_e164"] or "").strip()
    bindings = get_guest_bindings(conn, phone) if phone else []
    active = [b for b in bindings if str(b.get("status") or "").strip().lower() == "active"]
    return {
        "booking": dict(booking),
        "bindings": bindings,
        "active_bindings": active,
        "by_channel": {b["channel_type"]: b for b in active},
    }


def list_binding_tokens_for_reservation(conn, reservation_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, reservation_id, guest_phone_e164, channel_type, status, expires_at, used_at,
               used_by_external_user_id, created_at, updated_at
        FROM guest_binding_tokens
        WHERE reservation_id = ?
        ORDER BY id DESC
        """,
        (int(reservation_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def _normalize_profile_meta(profile_meta: Optional[dict[str, Any]]) -> dict[str, str]:
    profile_meta = profile_meta or {}
    return {
        "external_username": str(profile_meta.get("external_username") or "").strip(),
        "external_display_name": str(profile_meta.get("external_display_name") or "").strip(),
    }


def _upsert_channel_binding(
    conn,
    *,
    guest_phone_e164: str,
    channel_type: str,
    external_user_id: str,
    profile_meta: Optional[dict[str, Any]] = None,
) -> int:
    meta = _normalize_profile_meta(profile_meta)
    conn.execute(
        """
        INSERT INTO guest_channel_bindings (
            guest_phone_e164, channel_type, external_user_id,
            external_username, external_display_name, status, is_verified, linked_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'active', 1, datetime('now'), datetime('now'), datetime('now'))
        ON CONFLICT(channel_type, external_user_id) DO UPDATE SET
            guest_phone_e164 = excluded.guest_phone_e164,
            external_username = CASE WHEN trim(COALESCE(excluded.external_username, '')) <> '' THEN excluded.external_username ELSE guest_channel_bindings.external_username END,
            external_display_name = CASE WHEN trim(COALESCE(excluded.external_display_name, '')) <> '' THEN excluded.external_display_name ELSE guest_channel_bindings.external_display_name END,
            status = 'active',
            is_verified = 1,
            linked_at = datetime('now'),
            updated_at = datetime('now')
        """,
        (
            str(guest_phone_e164 or "").strip(),
            str(channel_type or "").strip(),
            str(external_user_id or "").strip(),
            meta["external_username"] or None,
            meta["external_display_name"] or None,
        ),
    )
    row = conn.execute(
        """
        SELECT id
        FROM guest_channel_bindings
        WHERE channel_type = ? AND external_user_id = ?
        LIMIT 1
        """,
        (str(channel_type or "").strip(), str(external_user_id or "").strip()),
    ).fetchone()
    return int(row["id"]) if row else 0


def consume_binding_token_once(
    conn,
    *,
    token_plain: str,
    channel_type: str,
    external_user_id: str,
    profile_meta: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if not GUEST_COMM_ENABLED:
        return {"ok": False, "error": "guest_comm_disabled"}

    channel = _normalize_channel(channel_type)
    token = str(token_plain or "").strip()
    ext_uid = str(external_user_id or "").strip()
    if not token or not ext_uid:
        return {"ok": False, "error": "token_or_external_user_id_required"}

    token_hash = _hash_token(token)
    updated = conn.execute(
        """
        UPDATE guest_binding_tokens
        SET status='used',
            used_at=datetime('now'),
            used_by_external_user_id=?,
            updated_at=datetime('now')
        WHERE token_hash=?
          AND channel_type=?
          AND status='active'
          AND datetime(expires_at) > datetime('now')
        """,
        (ext_uid, token_hash, channel),
    )

    token_row = conn.execute(
        """
        SELECT id, reservation_id, guest_phone_e164, channel_type, status, expires_at, used_at, used_by_external_user_id
        FROM guest_binding_tokens
        WHERE token_hash=? AND channel_type=?
        LIMIT 1
        """,
        (token_hash, channel),
    ).fetchone()

    if not token_row:
        log_event("GUEST-BIND", action="consume_failed", channel=channel, reason="token_not_found")
        return {"ok": False, "error": "token_invalid"}

    if int(updated.rowcount or 0) == 0:
        status = str(token_row["status"] or "").strip().lower()
        if status == "used" and str(token_row["used_by_external_user_id"] or "").strip() == ext_uid:
            pass
        elif status == "active" and str(token_row["expires_at"] or "").strip():
            if str(token_row["expires_at"]) <= _now_utc_sql():
                conn.execute(
                    """
                    UPDATE guest_binding_tokens
                    SET status='expired', updated_at=datetime('now')
                    WHERE id=? AND status='active'
                    """,
                    (int(token_row["id"]),),
                )
                return {"ok": False, "error": "token_expired"}
            return {"ok": False, "error": "token_unavailable"}
        elif status == "used":
            return {"ok": False, "error": "token_used"}
        elif status == "revoked":
            return {"ok": False, "error": "token_revoked"}
        else:
            return {"ok": False, "error": "token_invalid"}

    reservation_id = int(token_row["reservation_id"])
    booking = conn.execute(
        "SELECT id, phone_e164, name FROM bookings WHERE id=?",
        (reservation_id,),
    ).fetchone()
    if not booking:
        return {"ok": False, "error": "booking_not_found"}

    guest_phone = str(token_row["guest_phone_e164"] or booking["phone_e164"] or "").strip()
    if not guest_phone:
        return {"ok": False, "error": "guest_phone_missing"}

    _ensure_guest_profile(conn, guest_phone, str(booking["name"] or "").strip())
    binding_id = _upsert_channel_binding(
        conn,
        guest_phone_e164=guest_phone,
        channel_type=channel,
        external_user_id=ext_uid,
        profile_meta=profile_meta,
    )
    conn.execute(
        """
        UPDATE bookings
        SET preferred_channel=?,
            service_notifications_enabled=1,
            updated_at=datetime('now')
        WHERE id=?
        """,
        (channel, reservation_id),
    )
    conn.execute(
        """
        INSERT INTO booking_events (booking_id, event_type, actor_tg_id, actor_name, payload_json)
        VALUES (?, 'GUEST_CHANNEL_BOUND', ?, ?, ?)
        """,
        (
            reservation_id,
            f"{channel}:{ext_uid}",
            f"{channel}:{ext_uid}",
            json.dumps(
                {
                    "channel_type": channel,
                    "channel_binding_id": binding_id,
                    "guest_phone_e164": guest_phone,
                },
                ensure_ascii=False,
            ),
        ),
    )

    log_event(
        "GUEST-BIND",
        action="consume_success",
        token_id=int(token_row["id"]),
        reservation_id=reservation_id,
        channel=channel,
        binding_id=binding_id,
    )
    return {
        "ok": True,
        "reservation_id": reservation_id,
        "guest_phone_e164": guest_phone,
        "channel_type": channel,
        "channel_binding_id": binding_id,
    }


def build_guest_page_public_url(reservation_token: str) -> str:
    token = str(reservation_token or "").strip()
    if not token:
        return ""
    base = str(GUEST_PUBLIC_BASE_URL or "").strip().rstrip("/")
    if base:
        return f"{base}/guest/reservation/{token}"
    return f"/guest/reservation/{token}"
