import argparse
import time

from crm_sync import crm_sync_enabled, send_booking_event
from db import connect


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time backfill: push historical bookings from bot DB to CRM ingest API"
    )
    parser.add_argument("--start-id", type=int, default=0, help="Inclusive booking id lower bound")
    parser.add_argument("--end-id", type=int, default=0, help="Inclusive booking id upper bound (0 = no upper bound)")
    parser.add_argument("--limit", type=int, default=0, help="Max bookings to send (0 = all)")
    parser.add_argument("--sleep-ms", type=int, default=50, help="Delay between requests in ms")
    parser.add_argument("--dry-run", action="store_true", help="Only count rows, do not send")
    return parser.parse_args()


def _bookings_exist(conn) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bookings' LIMIT 1"
    ).fetchone()
    return bool(row)


def main() -> int:
    args = _parse_args()

    if not crm_sync_enabled():
        print("ERROR: CRM sync is disabled. Set CRM_API_URL in environment.")
        return 2

    conn = connect()
    try:
        if not _bookings_exist(conn):
            print("ERROR: bookings table not found in DB")
            return 2

        where = ["1=1"]
        params = []

        if args.start_id > 0:
            where.append("id >= ?")
            params.append(args.start_id)
        if args.end_id > 0:
            where.append("id <= ?")
            params.append(args.end_id)

        where_sql = " AND ".join(where)

        rows = conn.execute(
            f"""
            SELECT id
            FROM bookings
            WHERE {where_sql}
            ORDER BY id ASC
            """,
            params,
        ).fetchall()

        ids = [int(r["id"]) for r in rows]
        if args.limit > 0:
            ids = ids[: args.limit]

        total = len(ids)
        print(f"Found bookings for backfill: {total}")
        if total == 0:
            return 0

        sent_ok = 0
        sent_fail = 0

        for i, booking_id in enumerate(ids, start=1):
            if args.dry_run:
                print(f"[dry-run] {i}/{total} booking_id={booking_id}")
                continue

            ok = False
            try:
                ok = send_booking_event(
                    conn,
                    booking_id,
                    "BOOKING_BACKFILL",
                    {
                        "actor_tg_id": "system",
                        "actor_name": "backfill",
                        "payload": {"index": i, "total": total},
                    },
                )
            except Exception as exc:
                print(f"[{i}/{total}] booking_id={booking_id} exception={exc}")
                ok = False

            if ok:
                sent_ok += 1
                print(f"[{i}/{total}] booking_id={booking_id} ok")
            else:
                sent_fail += 1
                print(f"[{i}/{total}] booking_id={booking_id} fail")

            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

        print("Backfill done")
        print(f"total={total} ok={sent_ok} fail={sent_fail}")

        return 0 if sent_fail == 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
