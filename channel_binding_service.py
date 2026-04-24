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
from booking_service import ensure_public_reservation_token, load_booking_read_model, log_booking_event, resolve_core_reservation_id
from core_sync import sync_booking_to_core
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


def _resolve_core_reservation_id(conn, reservation_id: int) -> Optional[int]:
    return resolve_core_reservation_id(conn, int(reservation_id or 0), allow_booking_sync=False)


def _ensure_core_reservation_id(conn, reservation_id: int) -> Optional[int]:
    resolved = resolve_core_reservation_id(conn, int(reservation_id or 0), allow_booking_sync=True)
    return int(resolved) if resolved else None


def _get_public_reservation_token(conn, reservation_id: int, *, token_kind: str = "guest_access") -> str:
    core_reservation_id = _resolve_core_reservation_id(conn, int(reservation_id))
    if not core_reservation_id:
        return ""

    row = conn.execute(
        """
        SELECT public_token
        FROM public_reservation_tokens
        WHERE reservation_id=?
          AND token_kind=?
          AND status='active'
        ORDER BY id DESC
        LIMIT 1
        """,
        (core_reservation_id, str(token_kind or "").strip() or "guest_access"),
    ).fetchone()
    return str(row["public_token"] or "").strip() if row else ""


def _load_contact_preferences(conn, phone_e164: str) -> dict[str, Any]:
    phone = str(phone_e164 or "").strip()
    if not phone:
        return {
            "preferred_channel": "",
            "service_notifications_enabled": 1,
            "marketing_notifications_enabled": 0,
        }

    row = conn.execute(
        """
        SELECT preferred_channel, service_notifications_enabled, marketing_notifications_enabled
        FROM contacts
        WHERE phone_e164=?
        LIMIT 1
        """,
        (phone,),
    ).fetchone()
    if not row:
        return {
            "preferred_channel": "",
            "service_notifications_enabled": 1,
            "marketing_notifications_enabled": 0,
        }
    return {
        "preferred_channel": str(row["preferred_channel"] or "").strip().lower(),
        "service_notifications_enabled": int(row["service_notifications_enabled"] if row["service_notifications_enabled"] is not None else 1),
        "marketing_notifications_enabled": int(row["marketing_notifications_enabled"] if row["marketing_notifications_enabled"] is not None else 0),
    }


def _upsert_contact_preferences(
    conn,
    *,
    phone_e164: str,
    display_name: str = "",
    preferred_channel: str = "",
    service_notifications_enabled: Optional[int] = None,
    marketing_notifications_enabled: Optional[int] = None,
) -> None:
    phone = str(phone_e164 or "").strip()
    if not phone:
        return

    existing = _load_contact_preferences(conn, phone)
    preferred = str(preferred_channel or existing["preferred_channel"] or "").strip().lower() or None
    service_enabled = (
        int(service_notifications_enabled)
        if service_notifications_enabled is not None
        else int(existing["service_notifications_enabled"])
    )
    marketing_enabled = (
        int(marketing_notifications_enabled)
        if marketing_notifications_enabled is not None
        else int(existing["marketing_notifications_enabled"])
    )
    name = str(display_name or "").strip()

    conn.execute(
        """
        INSERT INTO contacts (
            phone_e164, display_name, preferred_channel,
            service_notifications_enabled, marketing_notifications_enabled,
            tags_json, source, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, '[]', 'guest_binding', datetime('now'), datetime('now'))
        ON CONFLICT(phone_e164) DO UPDATE SET
            display_name = CASE
                WHEN trim(COALESCE(excluded.display_name, '')) <> '' THEN excluded.display_name
                ELSE contacts.display_name
            END,
            preferred_channel = COALESCE(excluded.preferred_channel, contacts.preferred_channel),
            service_notifications_enabled = excluded.service_notifications_enabled,
            marketing_notifications_enabled = excluded.marketing_notifications_enabled,
            updated_at = datetime('now')
        """,
        (phone, name or None, preferred, service_enabled, marketing_enabled),
    )


def _ensure_contact_id(conn, phone_e164: str, display_name: str = "") -> Optional[int]:
    phone = str(phone_e164 or "").strip()
    if not phone:
        return None

    _upsert_contact_preferences(conn, phone_e164=phone, display_name=display_name)
    row = conn.execute(
        "SELECT id FROM contacts WHERE phone_e164=? LIMIT 1",
        (phone,),
    ).fetchone()
    return int(row["id"]) if row else None


def _upsert_contact_channel(
    conn,
    *,
    guest_phone_e164: str,
    channel_type: str,
    external_user_id: str,
    profile_meta: Optional[dict[str, Any]] = None,
) -> int:
    contact_id = _ensure_contact_id(
        conn,
        str(guest_phone_e164 or "").strip(),
        str((profile_meta or {}).get("external_display_name") or "").strip(),
    )
    meta = _normalize_profile_meta(profile_meta)
    platform = str(channel_type or "").strip().lower()
    external_id = str(external_user_id or "").strip()
    external_peer_id = external_id if platform == "vk" else None
    conn.execute(
        """
        INSERT INTO contact_channels (
            contact_id, platform, channel_kind, external_user_id, external_peer_id,
            username, display_name, status, linked_at, created_at, updated_at
        ) VALUES (?, ?, 'user', ?, ?, ?, ?, 'active', datetime('now'), datetime('now'), datetime('now'))
        ON CONFLICT(platform, external_user_id) DO UPDATE SET
            contact_id = COALESCE(excluded.contact_id, contact_channels.contact_id),
            external_peer_id = COALESCE(excluded.external_peer_id, contact_channels.external_peer_id),
            username = CASE
                WHEN trim(COALESCE(excluded.username, '')) <> '' THEN excluded.username
                ELSE contact_channels.username
            END,
            display_name = CASE
                WHEN trim(COALESCE(excluded.display_name, '')) <> '' THEN excluded.display_name
                ELSE contact_channels.display_name
            END,
            status = 'active',
            linked_at = datetime('now'),
            updated_at = datetime('now')
        """,
        (
            contact_id,
            platform,
            external_id,
            external_peer_id,
            meta["external_username"] or None,
            meta["external_display_name"] or None,
        ),
    )
    row = conn.execute(
        """
        SELECT id
        FROM contact_channels
        WHERE platform=? AND external_user_id=?
        LIMIT 1
        """,
        (platform, external_id),
    ).fetchone()
    return int(row["id"]) if row else 0


def _build_booking_token_payload(conn, booking_id: int, reservation_token: str) -> Optional[dict[str, Any]]:
    booking = load_booking_read_model(conn, int(booking_id))
    if not booking:
        return None

    prefs = _load_contact_preferences(conn, str(booking.get("phone_e164") or "").strip())
    payload = dict(booking)
    payload.update(
        {
            "reservation_token": str(reservation_token or "").strip(),
            "preferred_channel": prefs["preferred_channel"],
            "service_notifications_enabled": prefs["service_notifications_enabled"],
            "marketing_notifications_enabled": prefs["marketing_notifications_enabled"],
        }
    )
    return payload


def _build_booking_token_payload_from_reservation(conn, reservation_id: int, reservation_token: str) -> Optional[dict[str, Any]]:
    row = conn.execute(
        """
        SELECT
            r.id AS reservation_id,
            r.external_ref,
            r.source,
            r.guest_name,
            r.guest_phone,
            r.reservation_at,
            r.party_size,
            r.comment,
            r.deposit_amount,
            r.deposit_comment,
            r.status,
            r.created_at,
            r.updated_at,
            rm.formname,
            rm.tranid,
            rm.phone_raw,
            rm.guest_segment,
            rm.raw_payload_json,
            rm.utm_source,
            rm.utm_medium,
            rm.utm_campaign,
            rm.utm_content,
            rm.utm_term,
            tc.code AS assigned_table_number
        FROM reservations r
        LEFT JOIN reservation_metadata rm
          ON rm.reservation_id = r.id
        LEFT JOIN reservation_tables rt
          ON rt.reservation_id = r.id
         AND rt.released_at IS NULL
        LEFT JOIN tables_core tc
          ON tc.id = rt.table_id
        WHERE r.id = ?
        ORDER BY rt.id DESC
        LIMIT 1
        """,
        (int(reservation_id),),
    ).fetchone()
    if not row:
        return None

    reservation_at = str(row["reservation_at"] or "").strip()
    external_ref = str(row["external_ref"] or "").strip()
    booking_like_id = int(external_ref) if external_ref.isdigit() else int(row["reservation_id"])
    payload = {
        "id": booking_like_id,
        "reservation_id": int(row["reservation_id"]),
        "name": row["guest_name"],
        "phone_e164": row["guest_phone"],
        "phone_raw": row["phone_raw"] or row["guest_phone"],
        "reservation_date": reservation_at[:10] if reservation_at else None,
        "reservation_time": reservation_at[11:16] if len(reservation_at) >= 16 else None,
        "reservation_dt": reservation_at or None,
        "guests_count": row["party_size"],
        "comment": row["comment"],
        "assigned_table_number": row["assigned_table_number"],
        "deposit_amount": row["deposit_amount"],
        "deposit_comment": row["deposit_comment"],
        "status": str(row["status"] or "").strip(),
        "formname": row["formname"] or row["source"],
        "source": row["source"],
        "reservation_token": str(reservation_token or "").strip(),
        "raw_payload_json": row["raw_payload_json"],
        "utm_source": row["utm_source"],
        "utm_medium": row["utm_medium"],
        "utm_campaign": row["utm_campaign"],
        "utm_content": row["utm_content"],
        "utm_term": row["utm_term"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "tranid": row["tranid"],
        "guest_segment": row["guest_segment"],
    }
    prefs = _load_contact_preferences(conn, str(payload.get("phone_e164") or "").strip())
    payload.update(
        {
            "preferred_channel": prefs["preferred_channel"],
            "service_notifications_enabled": prefs["service_notifications_enabled"],
            "marketing_notifications_enabled": prefs["marketing_notifications_enabled"],
        }
    )
    return payload


def _load_booking_by_reservation_token(conn, reservation_token: str):
    token = str(reservation_token or "").strip()
    if not token:
        return None

    core_row = conn.execute(
        """
        SELECT prt.reservation_id, r.external_ref
        FROM public_reservation_tokens prt
        JOIN reservations r ON r.id = prt.reservation_id
        WHERE prt.public_token = ?
          AND prt.token_kind = 'guest_access'
          AND prt.status = 'active'
        ORDER BY prt.id DESC
        LIMIT 1
        """,
        (token,),
    ).fetchone()
    if core_row:
        payload = _build_booking_token_payload_from_reservation(conn, int(core_row["reservation_id"]), token)
        if payload:
            return payload
    return None


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

    booking = load_booking_read_model(conn, int(reservation_id))
    if not booking:
        raise ValueError("booking_not_found")
    core_reservation_id = _ensure_core_reservation_id(conn, int(reservation_id))
    if not core_reservation_id:
        raise ValueError("canonical_reservation_not_found")

    phone = str(guest_phone_e164 or booking["phone_e164"] or "").strip()
    raw_token = secrets.token_urlsafe(32)
    token_hash = _hash_token(raw_token)
    expires_at = _to_sql(datetime.utcnow() + timedelta(minutes=ttl))

    conn.execute(
        """
        INSERT INTO channel_binding_tokens (
            token_hash, reservation_id, guest_phone_e164, channel_type, status, expires_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'active', ?, datetime('now'), datetime('now'))
        """,
        (token_hash, int(core_reservation_id), phone or None, channel, expires_at),
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
        "core_reservation_id": int(core_reservation_id),
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
    canonical_rows = conn.execute(
        """
        SELECT
            cc.id,
            c.phone_e164 AS guest_phone_e164,
            cc.platform AS channel_type,
            cc.external_user_id,
            cc.username AS external_username,
            cc.display_name AS external_display_name,
            cc.status,
            1 AS is_verified,
            cc.linked_at,
            cc.created_at,
            cc.updated_at
        FROM contacts c
        JOIN contact_channels cc
          ON cc.contact_id = c.id
        WHERE c.phone_e164 = ?
        ORDER BY datetime(cc.updated_at) DESC, cc.id DESC
        """,
        (str(guest_phone_e164 or "").strip(),),
    ).fetchall()
    if canonical_rows:
        return [dict(r) for r in canonical_rows]
    return []


def get_reservation_channel_status(conn, reservation_id: int) -> dict[str, Any]:
    booking = load_booking_read_model(conn, int(reservation_id))
    if not booking:
        raise ValueError("booking_not_found")
    phone = str(booking["phone_e164"] or "").strip()
    prefs = _load_contact_preferences(conn, phone)
    booking_state = {
        "id": int(booking["id"]),
        "reservation_token": _get_public_reservation_token(conn, int(reservation_id)),
        "phone_e164": phone,
        "preferred_channel": prefs["preferred_channel"],
        "service_notifications_enabled": prefs["service_notifications_enabled"],
        "marketing_notifications_enabled": prefs["marketing_notifications_enabled"],
    }
    bindings = get_guest_bindings(conn, phone) if phone else []
    active = [b for b in bindings if str(b.get("status") or "").strip().lower() == "active"]
    return {
        "booking": booking_state,
        "bindings": bindings,
        "active_bindings": active,
        "by_channel": {b["channel_type"]: b for b in active},
    }


def list_binding_tokens_for_reservation(conn, reservation_id: int) -> list[dict[str, Any]]:
    core_reservation_id = _resolve_core_reservation_id(conn, int(reservation_id))
    if core_reservation_id:
        rows = conn.execute(
            """
            SELECT id, reservation_id, guest_phone_e164, channel_type, status, expires_at, used_at,
                   used_by_external_user_id, created_at, updated_at
            FROM channel_binding_tokens
            WHERE reservation_id = ?
            ORDER BY id DESC
            """,
            (int(core_reservation_id),),
        ).fetchall()
        return [dict(r) for r in rows]
    return []


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


def _load_canonical_binding_token(conn, *, token_hash: str, channel_type: str):
    return conn.execute(
        """
        SELECT
            cbt.id,
            cbt.reservation_id,
            cbt.guest_phone_e164,
            cbt.channel_type,
            cbt.status,
            cbt.expires_at,
            cbt.used_at,
            cbt.used_by_external_user_id,
            r.external_ref
        FROM channel_binding_tokens cbt
        JOIN reservations r
          ON r.id = cbt.reservation_id
        WHERE cbt.token_hash=?
          AND cbt.channel_type=?
        LIMIT 1
        """,
        (token_hash, channel_type),
    ).fetchone()


def _resolve_booking_id_for_binding_token_row(token_row: Any) -> Optional[int]:
    if not token_row:
        return None
    external_ref = str(token_row["external_ref"] or "").strip()
    if external_ref.isdigit():
        return int(external_ref)
    return None


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
        UPDATE channel_binding_tokens
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

    token_row = _load_canonical_binding_token(conn, token_hash=token_hash, channel_type=channel)

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
                    UPDATE channel_binding_tokens
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

    reservation_id = _resolve_booking_id_for_binding_token_row(token_row)
    if not reservation_id:
        return {"ok": False, "error": "booking_not_found"}

    booking = load_booking_read_model(conn, reservation_id)
    if not booking:
        return {"ok": False, "error": "booking_not_found"}

    guest_phone = str(token_row["guest_phone_e164"] or booking["phone_e164"] or "").strip()
    if not guest_phone:
        return {"ok": False, "error": "guest_phone_missing"}

    _ensure_guest_profile(conn, guest_phone, str(booking["name"] or "").strip())
    _upsert_contact_preferences(
        conn,
        phone_e164=guest_phone,
        display_name=str(booking["name"] or "").strip(),
        preferred_channel=channel,
        service_notifications_enabled=1,
    )
    _upsert_contact_channel(
        conn,
        guest_phone_e164=guest_phone,
        channel_type=channel,
        external_user_id=ext_uid,
        profile_meta=profile_meta,
    )
    channel_row = conn.execute(
        """
        SELECT id
        FROM contact_channels
        WHERE platform=? AND external_user_id=?
        LIMIT 1
        """,
        (channel, ext_uid),
    ).fetchone()
    contact_channel_id = int(channel_row["id"]) if channel_row else 0
    log_booking_event(
        conn,
        reservation_id,
        "GUEST_CHANNEL_BOUND",
        f"{channel}:{ext_uid}",
        f"{channel}:{ext_uid}",
        {
            "channel_type": channel,
            "contact_channel_id": contact_channel_id or None,
            "guest_phone_e164": guest_phone,
        },
    )

    log_event(
        "GUEST-BIND",
        action="consume_success",
        token_id=int(token_row["id"]),
        reservation_id=reservation_id,
        channel=channel,
        contact_channel_id=contact_channel_id or 0,
        token_storage="canonical",
    )
    return {
        "ok": True,
        "reservation_id": reservation_id,
        "guest_phone_e164": guest_phone,
        "channel_type": channel,
        "contact_channel_id": contact_channel_id or 0,
    }


def build_guest_page_public_url(reservation_token: str) -> str:
    token = str(reservation_token or "").strip()
    if not token:
        return ""
    base = str(GUEST_PUBLIC_BASE_URL or "").strip().rstrip("/")
    if base:
        return f"{base}/guest/reservation/{token}"
    return f"/guest/reservation/{token}"
