import argparse
import os
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional

from db import connect, DB_PATH


@dataclass(frozen=True)
class CheckResult:
    name: str
    value: Optional[int]
    ok_when_zero: bool = True
    note: str = ""
    skipped: bool = False

    @property
    def is_ok(self) -> bool:
        if self.skipped:
            return True
        if self.ok_when_zero:
            return self.value == 0
        return bool(self.value and self.value > 0)


def _count(conn, sql: str, params: Iterable[object] = ()) -> int:
    row = conn.execute(sql, tuple(params)).fetchone()
    return int(row["c"] or 0) if row else 0


def _safe_check(name: str, conn, sql: str, note: str) -> CheckResult:
    try:
        return CheckResult(name=name, value=_count(conn, sql), note=note)
    except sqlite3.OperationalError as exc:
        return CheckResult(name=name, value=None, note=f"{note} Skipped: {exc}.", skipped=True)


def _connect_for_verification(db_path: Optional[str] = None) -> sqlite3.Connection:
    explicit_path = str(db_path or "").strip()
    if not explicit_path:
        return connect()

    conn = sqlite3.connect(explicit_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def run_checks(db_path: Optional[str] = None) -> list[CheckResult]:
    conn = _connect_for_verification(db_path)
    try:
        results: list[CheckResult] = []

        results.append(
            _safe_check(
                "legacy_bookings_without_reservation",
                conn,
                """
                SELECT COUNT(*) AS c
                FROM bookings b
                LEFT JOIN reservations r
                  ON r.source = 'legacy_booking'
                 AND r.external_ref = CAST(b.id AS TEXT)
                WHERE r.id IS NULL
                """,
                "Legacy booking rows that do not have canonical reservation mirror.",
            )
        )

        results.append(
            _safe_check(
                "legacy_reservations_without_booking",
                conn,
                """
                SELECT COUNT(*) AS c
                FROM reservations r
                LEFT JOIN bookings b
                  ON b.id = CAST(r.external_ref AS INTEGER)
                WHERE r.source = 'legacy_booking'
                  AND r.external_ref IS NOT NULL
                  AND trim(r.external_ref) <> ''
                  AND b.id IS NULL
                """,
                "Canonical legacy-sourced reservations that no longer map back to bookings.",
            )
        )

        results.append(
            _safe_check(
                "legacy_reservations_without_events",
                conn,
                """
                SELECT COUNT(*) AS c
                FROM (
                    SELECT r.id
                    FROM reservations r
                    LEFT JOIN reservation_events re
                      ON re.reservation_id = r.id
                    WHERE r.source = 'legacy_booking'
                    GROUP BY r.id
                    HAVING COUNT(re.id) = 0
                ) q
                """,
                "Canonical reservations missing all reservation_events.",
            )
        )

        results.append(
            _safe_check(
                "legacy_booking_assignments_missing_core",
                conn,
                """
                SELECT COUNT(*) AS c
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
                """,
                "Legacy table assignment has no matching active canonical reservation_tables row.",
            )
        )

        results.append(
            _safe_check(
                "core_assignments_missing_legacy",
                conn,
                """
                SELECT COUNT(*) AS c
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
                """,
                "Canonical active table assignment is not mirrored into bookings.assigned_table_number.",
            )
        )

        results.append(
            _safe_check(
                "legacy_restrictions_missing_core",
                conn,
                """
                SELECT COUNT(*) AS c
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
                """,
                "Active legacy restriction has no active canonical table_blocks row.",
            )
        )

        results.append(
            _safe_check(
                "core_restrictions_missing_legacy",
                conn,
                """
                SELECT COUNT(*) AS c
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
                """,
                "Active canonical restriction is not mirrored into venue_tables.",
            )
        )

        results.append(
            _safe_check(
                "legacy_booking_tokens_without_core_reservation",
                conn,
                """
                SELECT COUNT(*) AS c
                FROM bookings b
                LEFT JOIN reservations r
                  ON r.source = 'legacy_booking'
                 AND r.external_ref = CAST(b.id AS TEXT)
                WHERE b.reservation_token IS NOT NULL
                  AND trim(b.reservation_token) <> ''
                  AND r.id IS NULL
                """,
                "Legacy reservation_token still exists on booking row without canonical reservation.",
            )
        )

        return results
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify BOT_LUCH readiness for LEGACY_MIRROR_ENABLED=0.")
    parser.add_argument("--db-path", help="Explicit SQLite DB path to verify instead of DB_PATH from env.")
    parser.add_argument("--strict", action="store_true", help="Exit with code 1 if any check is non-zero.")
    args = parser.parse_args()

    explicit_db_path = str(args.db_path or "").strip() or None
    resolved_db_path = explicit_db_path or DB_PATH
    if explicit_db_path and not os.path.exists(explicit_db_path):
        print(f"DB_PATH={resolved_db_path}")
        print("mirror_off_verification:")
        print("- ERROR db_path_missing=1")
        print("  Explicit DB path does not exist.")
        return 1 if args.strict else 0

    results = run_checks(explicit_db_path)
    failing = [item for item in results if not item.is_ok]

    print(f"DB_PATH={resolved_db_path}")
    print("mirror_off_verification:")
    for item in results:
        status = "SKIP" if item.skipped else ("OK" if item.is_ok else "WARN")
        value = "n/a" if item.value is None else str(item.value)
        print(f"- {status} {item.name}={value}")
        if item.note:
            print(f"  {item.note}")

    if failing:
        print(f"summary: {len(failing)} warning checks")
    else:
        print("summary: all checks passed")

    if args.strict and failing:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
