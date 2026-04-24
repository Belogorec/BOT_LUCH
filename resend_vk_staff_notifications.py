import argparse

from db import connect
from vk_staff_notify import fetch_active_vk_staff_peers, notify_vk_staff_about_new_booking


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resend VK staff notifications for recent bookings.")
    parser.add_argument("--limit", type=int, default=2, help="How many latest reservations to resend.")
    parser.add_argument("--booking-id", type=int, action="append", default=[], help="Specific booking/reservation id to resend.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    conn = connect()
    try:
        peers = fetch_active_vk_staff_peers(conn, bot_key="hostess")
        peer_ids = [str(peer.get("peer_id") or "").strip() for peer in peers if str(peer.get("peer_id") or "").strip()]
        print(f"hostess_peers={len(peer_ids)}")
        if not peer_ids:
            print("status=skip reason=no_active_peers")
            return

        booking_ids = [int(value) for value in args.booking_id if int(value or 0) > 0]
        if not booking_ids:
            rows = conn.execute(
                """
                SELECT id
                FROM reservations
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (max(1, int(args.limit or 2)),),
            ).fetchall()
            booking_ids = [int(row["id"]) for row in rows]

        for booking_id in reversed(booking_ids):
            sent = notify_vk_staff_about_new_booking(conn, int(booking_id), source="manual_resend")
            print(f"booking_id={int(booking_id)} sent={int(sent)}")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
