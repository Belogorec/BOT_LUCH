import argparse
import json
import sqlite3
from typing import Any, Optional

from booking_service import ensure_public_reservation_token
from channel_binding_service import _upsert_contact_channel, _upsert_contact_preferences
from core_sync import sync_booking_to_core
from db import DB_PATH
from integration_service import upsert_bot_peer
from pending_reply_service import delete_expired_pending_replies, delete_superseded_pending_replies


TERMINAL_LEGACY_STATUSES = {"DECLINED", "CANCELLED", "NO_SHOW", "COMPLETED"}


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or "").strip())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def find_missing_public_tokens(conn: sqlite3.Connection, *, active_only: bool = False) -> list[sqlite3.Row]:
    status_filter = ""
    params: list[Any] = []
    if active_only:
        status_filter = """
          AND COALESCE(upper(trim(b.status)), 'WAITING') NOT IN ('DECLINED', 'CANCELLED', 'NO_SHOW', 'COMPLETED')
        """

    return conn.execute(
        f"""
        SELECT
          b.id AS booking_id,
          b.status AS legacy_status,
          b.reservation_token,
          r.id AS reservation_id
        FROM bookings b
        JOIN reservations r
          ON r.source = 'legacy_booking'
         AND r.external_ref = CAST(b.id AS TEXT)
        LEFT JOIN public_reservation_tokens prt
          ON prt.reservation_id = r.id
         AND prt.public_token = b.reservation_token
         AND prt.token_kind = 'guest_access'
         AND prt.status = 'active'
        WHERE b.reservation_token IS NOT NULL
          AND trim(b.reservation_token) <> ''
          AND prt.id IS NULL
          {status_filter}
        ORDER BY b.id ASC
        """,
        tuple(params),
    ).fetchall()


def backfill_public_tokens(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
    active_only: bool = False,
) -> dict[str, Any]:
    rows = find_missing_public_tokens(conn, active_only=active_only)
    applied = 0
    for row in rows:
        if dry_run:
            continue
        ensure_public_reservation_token(
            conn,
            reservation_id=int(row["reservation_id"]),
            public_token=str(row["reservation_token"] or "").strip(),
        )
        applied += 1
    return {
        "scope": "public_reservation_tokens",
        "found": len(rows),
        "applied": applied,
        "examples": [dict(row) for row in rows[:20]],
    }


def _ensure_core_reservation_id_for_booking(conn: sqlite3.Connection, booking_id: int) -> Optional[int]:
    bid = int(booking_id or 0)
    if bid <= 0:
        return None

    row = conn.execute(
        """
        SELECT id
        FROM reservations
        WHERE source = 'legacy_booking'
          AND external_ref = ?
        LIMIT 1
        """,
        (str(bid),),
    ).fetchone()
    if row:
        return int(row["id"])

    booking_row = conn.execute("SELECT id FROM bookings WHERE id = ? LIMIT 1", (bid,)).fetchone()
    if not booking_row:
        return None
    return int(sync_booking_to_core(conn, bid))


def _booking_exists(conn: sqlite3.Connection, booking_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM bookings WHERE id = ? LIMIT 1", (int(booking_id or 0),)).fetchone()
    return bool(row)


def find_missing_channel_binding_tokens(
    conn: sqlite3.Connection,
    *,
    active_only: bool = False,
) -> list[sqlite3.Row]:
    status_filter = ""
    if active_only:
        status_filter = """
          AND lower(trim(COALESCE(gbt.status, 'active'))) = 'active'
          AND datetime(gbt.expires_at) > datetime('now')
        """

    return conn.execute(
        f"""
        SELECT
          gbt.id AS legacy_token_id,
          gbt.reservation_id AS booking_id,
          r.id AS canonical_reservation_id,
          gbt.token_hash,
          gbt.guest_phone_e164,
          gbt.channel_type,
          gbt.status,
          gbt.expires_at,
          gbt.used_at,
          gbt.used_by_external_user_id,
          gbt.created_at,
          gbt.updated_at
        FROM guest_binding_tokens gbt
        LEFT JOIN reservations r
          ON r.source = 'legacy_booking'
         AND r.external_ref = CAST(gbt.reservation_id AS TEXT)
        LEFT JOIN channel_binding_tokens cbt
          ON cbt.token_hash = gbt.token_hash
        WHERE cbt.id IS NULL
          {status_filter}
        ORDER BY gbt.id ASC
        """
    ).fetchall()


def backfill_channel_binding_tokens(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
    active_only: bool = False,
) -> dict[str, Any]:
    rows = find_missing_channel_binding_tokens(conn, active_only=active_only)
    applied = 0
    skipped = 0
    for row in rows:
        core_reservation_id = row["canonical_reservation_id"]
        if not core_reservation_id:
            if dry_run:
                if not _booking_exists(conn, int(row["booking_id"])):
                    skipped += 1
                continue
            core_reservation_id = _ensure_core_reservation_id_for_booking(conn, int(row["booking_id"]))
        if not core_reservation_id:
            skipped += 1
            continue
        if dry_run:
            continue

        conn.execute(
            """
            INSERT INTO channel_binding_tokens (
                reservation_id,
                token_hash,
                guest_phone_e164,
                channel_type,
                status,
                expires_at,
                used_at,
                used_by_external_user_id,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(token_hash) DO UPDATE SET
                reservation_id = excluded.reservation_id,
                guest_phone_e164 = COALESCE(excluded.guest_phone_e164, channel_binding_tokens.guest_phone_e164),
                channel_type = excluded.channel_type,
                status = excluded.status,
                expires_at = excluded.expires_at,
                used_at = excluded.used_at,
                used_by_external_user_id = COALESCE(
                    excluded.used_by_external_user_id,
                    channel_binding_tokens.used_by_external_user_id
                ),
                updated_at = excluded.updated_at
            """,
            (
                int(core_reservation_id),
                str(row["token_hash"] or "").strip(),
                str(row["guest_phone_e164"] or "").strip() or None,
                str(row["channel_type"] or "").strip(),
                str(row["status"] or "").strip() or "active",
                str(row["expires_at"] or "").strip(),
                str(row["used_at"] or "").strip() or None,
                str(row["used_by_external_user_id"] or "").strip() or None,
                str(row["created_at"] or "").strip() or None,
                str(row["updated_at"] or "").strip() or None,
            ),
        )
        applied += 1
    return {
        "scope": "channel_binding_tokens_from_guest_binding_tokens",
        "found": len(rows),
        "applied": applied,
        "skipped": skipped,
        "examples": [dict(row) for row in rows[:20]],
    }


def find_missing_contact_channels_from_guest_bindings(
    conn: sqlite3.Connection,
    *,
    active_only: bool = False,
) -> list[sqlite3.Row]:
    status_filter = ""
    if active_only:
        status_filter = "AND lower(trim(COALESCE(gcb.status, 'active'))) = 'active'"

    return conn.execute(
        f"""
        SELECT
          gcb.id AS legacy_binding_id,
          gcb.guest_phone_e164,
          gcb.channel_type,
          gcb.external_user_id,
          gcb.external_username,
          gcb.external_display_name,
          gcb.status,
          c.id AS canonical_contact_id,
          cc.id AS canonical_contact_channel_id
        FROM guest_channel_bindings gcb
        LEFT JOIN contacts c
          ON c.phone_e164 = gcb.guest_phone_e164
        LEFT JOIN contact_channels cc
          ON cc.platform = lower(trim(gcb.channel_type))
         AND cc.external_user_id = gcb.external_user_id
         AND cc.status = 'active'
         AND cc.contact_id = c.id
        WHERE (
            c.id IS NULL
            OR cc.id IS NULL
            OR cc.contact_id IS NULL
            OR cc.contact_id <> c.id
        )
          {status_filter}
        ORDER BY gcb.id ASC
        """
    ).fetchall()


def backfill_contact_channels_from_guest_bindings(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
    active_only: bool = False,
) -> dict[str, Any]:
    rows = find_missing_contact_channels_from_guest_bindings(conn, active_only=active_only)
    applied = 0
    skipped = 0
    for row in rows:
        phone = str(row["guest_phone_e164"] or "").strip()
        channel = str(row["channel_type"] or "").strip().lower()
        external_user_id = str(row["external_user_id"] or "").strip()
        display_name = str(row["external_display_name"] or "").strip()
        username = str(row["external_username"] or "").strip()
        if not phone or not external_user_id or not channel:
            skipped += 1
            continue
        if dry_run:
            continue

        _upsert_contact_preferences(
            conn,
            phone_e164=phone,
            display_name=display_name,
            preferred_channel=channel,
            service_notifications_enabled=1,
        )
        _upsert_contact_channel(
            conn,
            guest_phone_e164=phone,
            channel_type=channel,
            external_user_id=external_user_id,
            profile_meta={
                "external_username": username,
                "external_display_name": display_name,
            },
        )
        applied += 1
    return {
        "scope": "contact_channels_from_guest_channel_bindings",
        "found": len(rows),
        "applied": applied,
        "skipped": skipped,
        "examples": [dict(row) for row in rows[:20]],
    }


def find_expired_pending_replies(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          id AS pending_reply_id,
          kind,
          booking_id,
          chat_id,
          actor_tg_id,
          prompt_message_id,
          expires_at
        FROM pending_replies
        WHERE datetime(expires_at) <= datetime('now')
        ORDER BY datetime(expires_at) ASC, id ASC
        """
    ).fetchall()


def cleanup_expired_pending_replies(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    rows = find_expired_pending_replies(conn)
    applied = 0
    if not dry_run:
        applied = int(delete_expired_pending_replies(conn))
    return {
        "scope": "cleanup_expired_pending_replies",
        "found": len(rows),
        "applied": applied,
        "examples": [dict(row) for row in rows[:20]],
    }


def find_superseded_pending_replies(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          pr.id AS pending_reply_id,
          pr.kind,
          pr.booking_id,
          pr.chat_id,
          pr.actor_tg_id,
          pr.prompt_message_id,
          pr.expires_at
        FROM pending_replies pr
        JOIN pending_replies newer
          ON newer.kind = pr.kind
         AND newer.chat_id = pr.chat_id
         AND newer.actor_tg_id = pr.actor_tg_id
         AND newer.id > pr.id
        WHERE datetime(pr.expires_at) > datetime('now')
          AND datetime(newer.expires_at) > datetime('now')
        GROUP BY pr.id
        ORDER BY pr.id ASC
        """
    ).fetchall()


def cleanup_superseded_pending_replies(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    rows = find_superseded_pending_replies(conn)
    applied = 0
    if not dry_run:
        applied = int(delete_superseded_pending_replies(conn))
    return {
        "scope": "cleanup_superseded_pending_replies",
        "found": len(rows),
        "applied": applied,
        "examples": [dict(row) for row in rows[:20]],
    }


def find_missing_bot_peers_from_vk_staff(conn: sqlite3.Connection, *, active_only: bool = False) -> list[sqlite3.Row]:
    status_filter = "WHERE vsp.is_active = 1" if active_only else "WHERE 1 = 1"
    return conn.execute(
        f"""
        SELECT
          vsp.peer_id,
          vsp.peer_external_id,
          vsp.bot_key,
          vsp.from_id,
          vsp.role_hint,
          vsp.last_message_text,
          vsp.is_active
        FROM vk_staff_peers vsp
        LEFT JOIN bot_peers bp
          ON bp.platform = 'vk'
         AND bp.bot_scope = COALESCE(NULLIF(trim(vsp.bot_key), ''), 'hostess')
         AND bp.external_peer_id = COALESCE(NULLIF(trim(vsp.peer_external_id), ''), trim(vsp.peer_id))
        {status_filter}
          AND bp.id IS NULL
        ORDER BY vsp.peer_id ASC
        """
    ).fetchall()


def backfill_bot_peers_from_vk_staff(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
    active_only: bool = False,
) -> dict[str, Any]:
    rows = find_missing_bot_peers_from_vk_staff(conn, active_only=active_only)
    applied = 0
    for row in rows:
        if dry_run:
            continue
        external_peer_id = str(row["peer_external_id"] or row["peer_id"] or "").strip()
        if not external_peer_id:
            continue
        upsert_bot_peer(
            conn,
            platform="vk",
            bot_scope=str(row["bot_key"] or "").strip() or "hostess",
            external_peer_id=external_peer_id,
            external_user_id=str(row["from_id"] or "").strip() or None,
            display_name=str(row["last_message_text"] or row["role_hint"] or "").strip(),
        )
        applied += 1
    return {
        "scope": "bot_peers_from_vk_staff",
        "found": len(rows),
        "applied": applied,
        "examples": [dict(row) for row in rows[:20]],
    }


def find_mirrored_active_vk_staff_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          vsp.peer_id,
          vsp.peer_external_id,
          vsp.bot_key,
          vsp.from_id,
          vsp.role_hint,
          vsp.is_active
        FROM vk_staff_peers vsp
        JOIN bot_peers bp
          ON bp.platform = 'vk'
         AND bp.bot_scope = COALESCE(NULLIF(trim(vsp.bot_key), ''), 'hostess')
         AND bp.external_peer_id = COALESCE(NULLIF(trim(vsp.peer_external_id), ''), trim(vsp.peer_id))
         AND bp.is_active = 1
        WHERE vsp.is_active = 1
        ORDER BY vsp.peer_id ASC
        """
    ).fetchall()


def deactivate_mirrored_vk_staff_rows(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    rows = find_mirrored_active_vk_staff_rows(conn)
    applied = 0
    for row in rows:
        if dry_run:
            continue
        conn.execute(
            """
            UPDATE vk_staff_peers
            SET is_active = 0,
                updated_at = datetime('now')
            WHERE peer_id = ?
            """,
            (str(row["peer_id"] or "").strip(),),
        )
        applied += 1
    return {
        "scope": "deactivate_mirrored_vk_staff",
        "found": len(rows),
        "applied": applied,
        "examples": [dict(row) for row in rows[:20]],
    }


def find_mirrored_active_guest_channel_binding_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          gcb.id AS legacy_binding_id,
          gcb.guest_phone_e164,
          gcb.channel_type,
          gcb.external_user_id,
          gcb.external_username,
          gcb.external_display_name,
          c.id AS canonical_contact_id,
          cc.id AS canonical_contact_channel_id
        FROM guest_channel_bindings gcb
        JOIN contacts c
          ON c.phone_e164 = gcb.guest_phone_e164
        JOIN contact_channels cc
          ON cc.platform = lower(trim(gcb.channel_type))
         AND cc.external_user_id = gcb.external_user_id
         AND cc.contact_id = c.id
         AND cc.status = 'active'
        WHERE lower(trim(COALESCE(gcb.status, 'active'))) = 'active'
        ORDER BY gcb.id ASC
        """
    ).fetchall()


def deactivate_mirrored_guest_channel_binding_rows(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    rows = find_mirrored_active_guest_channel_binding_rows(conn)
    applied = 0
    for row in rows:
        if dry_run:
            continue
        conn.execute(
            """
            UPDATE guest_channel_bindings
            SET status = 'inactive',
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (int(row["legacy_binding_id"]),),
        )
        applied += 1
    return {
        "scope": "deactivate_mirrored_guest_channel_bindings",
        "found": len(rows),
        "applied": applied,
        "examples": [dict(row) for row in rows[:20]],
    }


def build_report(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
    active_only: bool = False,
    deactivate_mirrored_guest_bindings: bool = False,
    deactivate_mirrored_vk_staff: bool = False,
) -> dict[str, Any]:
    token_result = backfill_public_tokens(conn, dry_run=dry_run, active_only=active_only)
    binding_token_result = backfill_channel_binding_tokens(conn, dry_run=dry_run, active_only=active_only)
    guest_binding_result = backfill_contact_channels_from_guest_bindings(conn, dry_run=dry_run, active_only=active_only)
    expired_pending_result = cleanup_expired_pending_replies(conn, dry_run=dry_run)
    superseded_pending_result = cleanup_superseded_pending_replies(conn, dry_run=dry_run)
    peer_result = backfill_bot_peers_from_vk_staff(conn, dry_run=dry_run, active_only=active_only)
    deactivate_guest_binding_result = {
        "scope": "deactivate_mirrored_guest_channel_bindings",
        "found": 0,
        "applied": 0,
        "examples": [],
    }
    deactivate_result = {
        "scope": "deactivate_mirrored_vk_staff",
        "found": 0,
        "applied": 0,
        "examples": [],
    }
    if deactivate_mirrored_guest_bindings:
        deactivate_guest_binding_result = deactivate_mirrored_guest_channel_binding_rows(conn, dry_run=dry_run)
    if deactivate_mirrored_vk_staff:
        deactivate_result = deactivate_mirrored_vk_staff_rows(conn, dry_run=dry_run)
    return {
        "db_path": "",
        "dry_run": bool(dry_run),
        "active_only": bool(active_only),
        "deactivate_mirrored_guest_bindings": bool(deactivate_mirrored_guest_bindings),
        "deactivate_mirrored_vk_staff": bool(deactivate_mirrored_vk_staff),
        "summary": {
            "token_rows_found": int(token_result["found"]),
            "token_rows_applied": int(token_result["applied"]),
            "binding_token_rows_found": int(binding_token_result["found"]),
            "binding_token_rows_applied": int(binding_token_result["applied"]),
            "binding_token_rows_skipped": int(binding_token_result["skipped"]),
            "guest_binding_rows_found": int(guest_binding_result["found"]),
            "guest_binding_rows_applied": int(guest_binding_result["applied"]),
            "guest_binding_rows_skipped": int(guest_binding_result["skipped"]),
            "mirrored_guest_binding_rows_found": int(deactivate_guest_binding_result["found"]),
            "mirrored_guest_binding_rows_applied": int(deactivate_guest_binding_result["applied"]),
            "expired_pending_reply_rows_found": int(expired_pending_result["found"]),
            "expired_pending_reply_rows_applied": int(expired_pending_result["applied"]),
            "superseded_pending_reply_rows_found": int(superseded_pending_result["found"]),
            "superseded_pending_reply_rows_applied": int(superseded_pending_result["applied"]),
            "bot_peer_rows_found": int(peer_result["found"]),
            "bot_peer_rows_applied": int(peer_result["applied"]),
            "mirrored_vk_staff_rows_found": int(deactivate_result["found"]),
            "mirrored_vk_staff_rows_applied": int(deactivate_result["applied"]),
        },
        "public_reservation_tokens": token_result,
        "channel_binding_tokens_from_guest_binding_tokens": binding_token_result,
        "contact_channels_from_guest_channel_bindings": guest_binding_result,
        "cleanup_expired_pending_replies": expired_pending_result,
        "cleanup_superseded_pending_replies": superseded_pending_result,
        "deactivate_mirrored_guest_channel_bindings": deactivate_guest_binding_result,
        "bot_peers_from_vk_staff": peer_result,
        "deactivate_mirrored_vk_staff": deactivate_result,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill canonical mirror-off prerequisites for BOT_LUCH.")
    parser.add_argument("--db-path", help="Explicit SQLite DB path to process instead of DB_PATH from env.")
    parser.add_argument("--dry-run", action="store_true", help="Only report missing rows without mutating the DB.")
    parser.add_argument("--active-only", action="store_true", help="Limit backfill to active runtime rows only.")
    parser.add_argument(
        "--deactivate-mirrored-guest-bindings",
        action="store_true",
        help="Deactivate legacy guest_channel_bindings rows that already have active canonical contacts/contact_channels mapping.",
    )
    parser.add_argument(
        "--deactivate-mirrored-vk-staff",
        action="store_true",
        help="Deactivate legacy vk_staff_peers rows that already have active canonical bot_peers mapping.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON report.")
    args = parser.parse_args()

    db_path = str(args.db_path or "").strip() or DB_PATH
    conn = connect_db(db_path)
    try:
        report = build_report(
            conn,
            dry_run=args.dry_run,
            active_only=args.active_only,
            deactivate_mirrored_guest_bindings=args.deactivate_mirrored_guest_bindings,
            deactivate_mirrored_vk_staff=args.deactivate_mirrored_vk_staff,
        )
        report["db_path"] = db_path
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()
    finally:
        conn.close()

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        summary = report["summary"]
        print(f"DB_PATH={db_path}")
        print(
            "backfill_mirror_off_prereqs: "
            f"token_found={summary['token_rows_found']} "
            f"token_applied={summary['token_rows_applied']} "
            f"binding_token_found={summary['binding_token_rows_found']} "
            f"binding_token_applied={summary['binding_token_rows_applied']} "
            f"binding_token_skipped={summary['binding_token_rows_skipped']} "
            f"guest_binding_found={summary['guest_binding_rows_found']} "
            f"guest_binding_applied={summary['guest_binding_rows_applied']} "
            f"guest_binding_skipped={summary['guest_binding_rows_skipped']} "
            f"legacy_guest_binding_found={summary['mirrored_guest_binding_rows_found']} "
            f"legacy_guest_binding_applied={summary['mirrored_guest_binding_rows_applied']} "
            f"expired_pending_found={summary['expired_pending_reply_rows_found']} "
            f"expired_pending_applied={summary['expired_pending_reply_rows_applied']} "
            f"superseded_pending_found={summary['superseded_pending_reply_rows_found']} "
            f"superseded_pending_applied={summary['superseded_pending_reply_rows_applied']} "
            f"bot_peer_found={summary['bot_peer_rows_found']} "
            f"bot_peer_applied={summary['bot_peer_rows_applied']} "
            f"legacy_vk_staff_found={summary['mirrored_vk_staff_rows_found']} "
            f"legacy_vk_staff_applied={summary['mirrored_vk_staff_rows_applied']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
