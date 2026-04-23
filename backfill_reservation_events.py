import argparse
import json
import sqlite3
from pathlib import Path


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def find_missing_event_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            r.id,
            r.source,
            r.external_ref,
            r.guest_name,
            r.guest_phone,
            r.reservation_at,
            r.status,
            r.created_at
        FROM reservations r
        LEFT JOIN reservation_events re
          ON re.reservation_id = r.id
        WHERE r.source = 'legacy_booking'
        GROUP BY r.id
        HAVING COUNT(re.id) = 0
        ORDER BY r.id
        """
    ).fetchall()


def backfill_missing_events(conn: sqlite3.Connection, *, dry_run: bool) -> int:
    rows = find_missing_event_rows(conn)
    for row in rows:
        payload = {
            "source": str(row["source"] or "").strip() or "legacy_booking",
            "external_ref": str(row["external_ref"] or "").strip() or None,
            "guest_name": str(row["guest_name"] or "").strip() or None,
            "guest_phone": str(row["guest_phone"] or "").strip() or None,
            "reservation_at": str(row["reservation_at"] or "").strip() or None,
            "status": str(row["status"] or "").strip() or None,
            "backfill_reason": "missing_reservation_events",
        }
        print(
            f"reservation_id={int(row['id'])} "
            f"external_ref={str(row['external_ref'] or '').strip() or '-'} "
            f"created_at={str(row['created_at'] or '').strip() or '-'}"
        )
        if dry_run:
            continue
        conn.execute(
            """
            INSERT INTO reservation_events (reservation_id, event_type, actor, payload_json, created_at)
            VALUES (?, 'CREATED', 'system:backfill', ?, ?)
            """,
            (
                int(row["id"]),
                json.dumps(payload, ensure_ascii=False),
                str(row["created_at"] or "").strip() or None,
            ),
        )
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill missing reservation_events rows for legacy reservations.")
    parser.add_argument("--db-path", required=True, help="SQLite DB path to update.")
    parser.add_argument("--dry-run", action="store_true", help="Only print rows that would be backfilled.")
    args = parser.parse_args()

    db_path = str(args.db_path or "").strip()
    if not db_path or not Path(db_path).exists():
        print(f"db_path_missing: {db_path or '-'}")
        return 1

    conn = connect_db(db_path)
    try:
        count = backfill_missing_events(conn, dry_run=bool(args.dry_run))
        if args.dry_run:
            print(f"dry_run_missing_reservation_events={count}")
        else:
            conn.commit()
            print(f"backfilled_reservation_events={count}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
