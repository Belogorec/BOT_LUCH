import argparse
import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Iterable, Optional

from db import DB_PATH, connect


REQUIRED_CANONICAL_TABLES = (
    "reservations",
    "reservation_events",
    "reservation_tables",
    "tables_core",
    "table_blocks",
    "public_reservation_tokens",
    "channel_binding_tokens",
    "contacts",
    "contact_channels",
    "bot_peers",
)


def _connect_for_verification(db_path: Optional[str] = None) -> sqlite3.Connection:
    explicit_path = str(db_path or "").strip()
    if not explicit_path:
        return connect()

    conn = sqlite3.connect(explicit_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _fetch_rows(conn, sql: str, params: Iterable[object] = ()) -> list[dict[str, Any]]:
    rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def _normalize_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    return value


def _issue(code: str, severity: str, message: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "count": len(rows),
        "examples": [{key: _normalize_value(value) for key, value in row.items()} for row in rows[:20]],
    }


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def _find_missing_required_tables(conn) -> list[str]:
    return [table for table in REQUIRED_CANONICAL_TABLES if not _table_exists(conn, table)]


def _collect_critical_issues(conn) -> list[dict[str, Any]]:
    critical: list[dict[str, Any]] = []

    rows = _fetch_rows(
        conn,
        """
        SELECT
          b.id AS booking_id,
          b.status AS legacy_status,
          b.reservation_dt,
          b.name,
          b.phone_e164
        FROM bookings b
        LEFT JOIN reservations r
          ON r.source = 'legacy_booking'
         AND r.external_ref = CAST(b.id AS TEXT)
        WHERE r.id IS NULL
        ORDER BY b.id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "legacy_bookings_without_canonical_reservation",
                "critical",
                "Legacy booking row has no canonical reservation mirror.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          b.id AS booking_id,
          b.status AS legacy_status,
          b.reservation_dt,
          b.reservation_token
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
          AND COALESCE(upper(trim(b.status)), 'WAITING') NOT IN ('DECLINED', 'CANCELLED', 'NO_SHOW', 'COMPLETED')
          AND prt.id IS NULL
        ORDER BY b.id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "active_legacy_booking_token_without_canonical_token",
                "critical",
                "Active booking still depends on legacy reservation_token without canonical public token coverage.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          b.id AS booking_id,
          b.assigned_table_number AS legacy_table_number,
          r.id AS reservation_id,
          tc.code AS canonical_table_number
        FROM bookings b
        LEFT JOIN reservations r
          ON r.source = 'legacy_booking'
         AND r.external_ref = CAST(b.id AS TEXT)
        LEFT JOIN reservation_tables rt
          ON rt.reservation_id = r.id
         AND rt.released_at IS NULL
        LEFT JOIN tables_core tc
          ON tc.id = rt.table_id
        WHERE b.assigned_table_number IS NOT NULL
          AND trim(CAST(b.assigned_table_number AS TEXT)) <> ''
          AND (tc.id IS NULL OR tc.code <> CAST(b.assigned_table_number AS TEXT))
        ORDER BY b.id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "legacy_booking_assignments_missing_core",
                "critical",
                "Legacy table assignment has no matching active canonical reservation_tables row.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          r.id AS reservation_id,
          r.external_ref AS booking_id,
          tc.code AS canonical_table_number,
          b.assigned_table_number AS legacy_table_number
        FROM reservations r
        JOIN reservation_tables rt
          ON rt.reservation_id = r.id
         AND rt.released_at IS NULL
        JOIN tables_core tc
          ON tc.id = rt.table_id
        LEFT JOIN bookings b
          ON b.id = CAST(r.external_ref AS INTEGER)
        WHERE r.source = 'legacy_booking'
          AND (
            b.id IS NULL
            OR COALESCE(CAST(b.assigned_table_number AS TEXT), '') <> tc.code
          )
        ORDER BY r.id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "core_assignments_missing_legacy",
                "critical",
                "Canonical active table assignment is not mirrored into bookings.assigned_table_number.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          vt.table_number,
          vt.restricted_until,
          vt.restriction_comment
        FROM venue_tables vt
        LEFT JOIN tables_core tc
          ON tc.code = CAST(vt.table_number AS TEXT)
        LEFT JOIN table_blocks tb
          ON tb.table_id = tc.id
         AND datetime(tb.ends_at) > datetime('now')
        WHERE vt.label = 'RESTRICTED'
          AND vt.restricted_until IS NOT NULL
          AND datetime(vt.restricted_until) > datetime('now')
          AND tb.id IS NULL
        ORDER BY vt.table_number ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "legacy_restrictions_missing_core",
                "critical",
                "Active legacy restriction has no active canonical table_blocks row.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          tb.id AS table_block_id,
          tc.code AS table_number,
          tb.ends_at,
          vt.label AS legacy_label,
          vt.restricted_until
        FROM table_blocks tb
        JOIN tables_core tc
          ON tc.id = tb.table_id
        LEFT JOIN venue_tables vt
          ON CAST(vt.table_number AS TEXT) = tc.code
        WHERE datetime(tb.ends_at) > datetime('now')
          AND (
            vt.table_number IS NULL
            OR vt.label <> 'RESTRICTED'
            OR vt.restricted_until IS NULL
          )
        ORDER BY tb.id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "core_restrictions_missing_legacy",
                "critical",
                "Active canonical restriction is not mirrored into venue_tables.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          id AS pending_reply_id,
          kind,
          booking_id,
          chat_id,
          actor_tg_id,
          expires_at
        FROM pending_replies
        WHERE datetime(expires_at) > datetime('now')
        ORDER BY datetime(expires_at) ASC, id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "pending_replies_still_used",
                "critical",
                "Runtime pending_replies rows still exist and block mirror-off readiness.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          gcb.id AS binding_id,
          gcb.guest_phone_e164,
          gcb.channel_type,
          gcb.external_user_id,
          gcb.status
        FROM guest_channel_bindings gcb
        LEFT JOIN contacts c
          ON c.phone_e164 = gcb.guest_phone_e164
        LEFT JOIN contact_channels cc
          ON cc.platform = lower(trim(gcb.channel_type))
         AND cc.external_user_id = gcb.external_user_id
         AND cc.status = 'active'
        WHERE lower(trim(COALESCE(gcb.status, 'active'))) = 'active'
          AND (
            c.id IS NULL
            OR cc.id IS NULL
            OR cc.contact_id IS NULL
            OR cc.contact_id <> c.id
          )
        ORDER BY gcb.id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "legacy_guest_channel_rows_without_canonical_mapping",
                "critical",
                "Active legacy guest bindings exist without matching canonical contacts/contact_channels mapping.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          id AS token_id,
          reservation_id,
          guest_phone_e164,
          channel_type,
          expires_at,
          status
        FROM guest_binding_tokens
        WHERE lower(trim(COALESCE(status, 'active'))) = 'active'
          AND datetime(expires_at) > datetime('now')
        ORDER BY datetime(expires_at) ASC, id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "guest_binding_tokens_still_used",
                "critical",
                "Active guest binding tokens still exist in legacy storage.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          vsp.peer_id,
          vsp.peer_external_id,
          vsp.bot_key,
          vsp.role_hint,
          vsp.last_seen_at
        FROM vk_staff_peers vsp
        LEFT JOIN bot_peers bp
          ON bp.platform = 'vk'
         AND bp.bot_scope = COALESCE(NULLIF(trim(vsp.bot_key), ''), 'hostess')
         AND bp.external_peer_id = COALESCE(NULLIF(trim(vsp.peer_external_id), ''), trim(vsp.peer_id))
         AND bp.is_active = 1
        WHERE vsp.is_active = 1
          AND bp.id IS NULL
        ORDER BY vsp.updated_at DESC, vsp.peer_id ASC
        """,
    )
    if rows:
        critical.append(
            _issue(
                "legacy_vk_staff_peers_without_canonical_mapping",
                "critical",
                "Active legacy vk_staff_peers rows exist without matching canonical bot_peers mapping.",
                rows,
            )
        )

    return critical


def _collect_warning_issues(conn) -> list[dict[str, Any]]:
    warning: list[dict[str, Any]] = []

    rows = _fetch_rows(
        conn,
        """
        SELECT
          r.id AS reservation_id,
          r.external_ref AS booking_id,
          r.status,
          r.reservation_at
        FROM reservations r
        LEFT JOIN bookings b
          ON b.id = CAST(r.external_ref AS INTEGER)
        WHERE r.source = 'legacy_booking'
          AND r.external_ref IS NOT NULL
          AND trim(r.external_ref) <> ''
          AND trim(r.external_ref) <> CAST(r.id AS TEXT)
          AND b.id IS NULL
        ORDER BY r.id ASC
        """,
    )
    if rows:
        warning.append(
            _issue(
                "canonical_reservations_without_legacy_booking",
                "warning",
                "Canonical legacy-sourced reservations no longer map back to bookings rows.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          r.id AS reservation_id,
          r.external_ref AS booking_id,
          r.status,
          r.reservation_at
        FROM reservations r
        LEFT JOIN reservation_events re
          ON re.reservation_id = r.id
        WHERE r.source = 'legacy_booking'
        GROUP BY r.id
        HAVING COUNT(re.id) = 0
        ORDER BY r.id ASC
        """,
    )
    if rows:
        warning.append(
            _issue(
                "legacy_reservations_without_events",
                "warning",
                "Canonical legacy-sourced reservations are missing reservation_events history.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          b.id AS booking_id,
          b.status AS legacy_status,
          b.reservation_dt,
          b.reservation_token
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
          AND COALESCE(upper(trim(b.status)), 'WAITING') IN ('DECLINED', 'CANCELLED', 'NO_SHOW', 'COMPLETED')
          AND prt.id IS NULL
        ORDER BY b.id ASC
        """,
    )
    if rows:
        warning.append(
            _issue(
                "historical_legacy_booking_token_without_canonical_token",
                "warning",
                "Historical booking keeps legacy reservation_token without canonical public token mapping.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          gcb.id AS binding_id,
          gcb.guest_phone_e164,
          gcb.channel_type,
          gcb.external_user_id,
          gcb.status
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
        """,
    )
    if rows:
        warning.append(
            _issue(
                "legacy_guest_bindings_still_present",
                "warning",
                "Active legacy guest bindings still exist even though canonical mapping is already present.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          vsp.peer_id,
          vsp.peer_external_id,
          vsp.bot_key,
          vsp.role_hint,
          vsp.last_seen_at
        FROM vk_staff_peers vsp
        JOIN bot_peers bp
          ON bp.platform = 'vk'
         AND bp.bot_scope = COALESCE(NULLIF(trim(vsp.bot_key), ''), 'hostess')
         AND bp.external_peer_id = COALESCE(NULLIF(trim(vsp.peer_external_id), ''), trim(vsp.peer_id))
         AND bp.is_active = 1
        WHERE vsp.is_active = 1
        ORDER BY vsp.updated_at DESC, vsp.peer_id ASC
        """,
    )
    if rows:
        warning.append(
            _issue(
                "legacy_vk_staff_peers_still_present",
                "warning",
                "Active legacy vk_staff_peers rows still exist even though canonical bot_peers mapping is present.",
                rows,
            )
        )

    return warning


def _collect_ignored_historical_issues(conn) -> list[dict[str, Any]]:
    ignored: list[dict[str, Any]] = []

    rows = _fetch_rows(
        conn,
        """
        SELECT
          id AS pending_reply_id,
          kind,
          booking_id,
          chat_id,
          actor_tg_id,
          expires_at
        FROM pending_replies
        WHERE datetime(expires_at) <= datetime('now')
        ORDER BY datetime(expires_at) DESC, id DESC
        """,
    )
    if rows:
        ignored.append(
            _issue(
                "historical_pending_replies",
                "ignored_historical",
                "Expired pending_replies rows remain as historical tail.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          id AS token_id,
          reservation_id,
          guest_phone_e164,
          channel_type,
          expires_at,
          status,
          used_at
        FROM guest_binding_tokens
        WHERE lower(trim(COALESCE(status, 'active'))) <> 'active'
           OR datetime(expires_at) <= datetime('now')
        ORDER BY id DESC
        """,
    )
    if rows:
        ignored.append(
            _issue(
                "historical_guest_binding_tokens",
                "ignored_historical",
                "Expired or consumed guest binding tokens remain in legacy storage.",
                rows,
            )
        )

    rows = _fetch_rows(
        conn,
        """
        SELECT
          id AS binding_id,
          guest_phone_e164,
          channel_type,
          external_user_id,
          status,
          updated_at
        FROM guest_channel_bindings
        WHERE lower(trim(COALESCE(status, 'active'))) <> 'active'
        ORDER BY id DESC
        """,
    )
    if rows:
        ignored.append(
            _issue(
                "historical_guest_channel_bindings",
                "ignored_historical",
                "Inactive legacy guest bindings remain as historical tail.",
                rows,
            )
        )

    return ignored


def build_report(db_path: Optional[str] = None) -> dict[str, Any]:
    conn = _connect_for_verification(db_path)
    try:
        critical: list[dict[str, Any]] = []
        warning: list[dict[str, Any]] = []
        ignored_historical: list[dict[str, Any]] = []

        missing_tables = _find_missing_required_tables(conn)
        if missing_tables:
            critical.append(
                {
                    "code": "missing_required_canonical_tables",
                    "severity": "critical",
                    "message": "Required canonical tables are missing for mirror-off verification.",
                    "count": len(missing_tables),
                    "examples": [{"table_name": table_name} for table_name in missing_tables],
                }
            )
        else:
            critical.extend(_collect_critical_issues(conn))
            warning.extend(_collect_warning_issues(conn))
            ignored_historical.extend(_collect_ignored_historical_issues(conn))

        return {
            "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "db_path": str(db_path or DB_PATH),
            "summary": {
                "critical_count": len(critical),
                "warning_count": len(warning),
                "ignored_historical_count": len(ignored_historical),
                "critical_rows": sum(int(item["count"]) for item in critical),
                "warning_rows": sum(int(item["count"]) for item in warning),
                "ignored_historical_rows": sum(int(item["count"]) for item in ignored_historical),
            },
            "critical": critical,
            "warning": warning,
            "ignored_historical": ignored_historical,
        }
    finally:
        conn.close()


def run_checks(db_path: Optional[str] = None) -> list[dict[str, Any]]:
    report = build_report(db_path)
    return list(report["critical"]) + list(report["warning"]) + list(report["ignored_historical"])


def _print_section(title: str, items: list[dict[str, Any]]) -> None:
    print(f"{title}:")
    if not items:
        print("- none")
        return
    for item in items:
        print(f"- {item['code']} count={item['count']}")
        print(f"  {item['message']}")
        for example in item.get("examples", [])[:3]:
            rendered = ", ".join(f"{key}={value}" for key, value in example.items())
            if rendered:
                print(f"  example: {rendered}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify BOT_LUCH readiness for LEGACY_MIRROR_ENABLED=0.")
    parser.add_argument("--db-path", help="Explicit SQLite DB path to verify instead of DB_PATH from env.")
    parser.add_argument("--strict", action="store_true", help="Exit with code 1 if any critical or warning issue is found.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON report.")
    args = parser.parse_args()

    explicit_db_path = str(args.db_path or "").strip() or None
    resolved_db_path = explicit_db_path or DB_PATH
    if explicit_db_path and not os.path.exists(explicit_db_path):
        if args.json:
            print(
                json.dumps(
                    {
                        "db_path": resolved_db_path,
                        "error": "db_path_missing",
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            print(f"DB_PATH={resolved_db_path}")
            print("mirror_off_verification:")
            print("critical:")
            print("- db_path_missing count=1")
            print("  Explicit DB path does not exist.")
        return 1 if args.strict else 0

    report = build_report(explicit_db_path)
    failing = list(report["critical"]) + list(report["warning"])

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"DB_PATH={resolved_db_path}")
        print("mirror_off_verification:")
        _print_section("critical", list(report["critical"]))
        _print_section("warning", list(report["warning"]))
        _print_section("ignored_historical", list(report["ignored_historical"]))
        summary = report["summary"]
        print(
            "summary: "
            f"critical={summary['critical_count']}({summary['critical_rows']} rows), "
            f"warning={summary['warning_count']}({summary['warning_rows']} rows), "
            f"ignored_historical={summary['ignored_historical_count']}({summary['ignored_historical_rows']} rows)"
        )

    if args.strict and failing:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
