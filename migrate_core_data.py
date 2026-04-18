import argparse

from core_migration import migrate_bookings_to_reservations, migrate_venue_tables_to_core
from core_schema import run_core_schema_migrations
from core_sync import sync_booking_state_to_core, sync_table_state_to_core
from db import connect, run_migrations


def main():
    parser = argparse.ArgumentParser(description="Migrate legacy booking data into core CRM tables.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply migration changes to the database. Without this flag the command only prints a dry-run summary.",
    )
    args = parser.parse_args()

    conn = connect()
    try:
        run_migrations(conn)
        run_core_schema_migrations(conn)

        legacy_bookings = conn.execute("SELECT COUNT(*) AS c FROM bookings").fetchone()
        legacy_tables = conn.execute("SELECT COUNT(*) AS c FROM venue_tables").fetchone()
        core_reservations = conn.execute("SELECT COUNT(*) AS c FROM reservations").fetchone()
        core_tables = conn.execute("SELECT COUNT(*) AS c FROM tables_core").fetchone()

        print(
            "DRY-RUN"
            if not args.apply
            else "APPLY",
            f"legacy_bookings={int(legacy_bookings['c'] or 0)}",
            f"legacy_tables={int(legacy_tables['c'] or 0)}",
            f"core_reservations={int(core_reservations['c'] or 0)}",
            f"core_tables={int(core_tables['c'] or 0)}",
        )

        if not args.apply:
            conn.rollback()
            return

        migrated_bookings = migrate_bookings_to_reservations(conn)
        migrated_tables = migrate_venue_tables_to_core(conn)

        booking_rows = conn.execute("SELECT id FROM bookings ORDER BY id ASC").fetchall()
        table_rows = conn.execute("SELECT table_number FROM venue_tables ORDER BY table_number ASC").fetchall()
        for row in booking_rows:
            sync_booking_state_to_core(conn, int(row["id"]))
        for row in table_rows:
            sync_table_state_to_core(conn, str(row["table_number"]))

        conn.commit()

        print(
            "DONE",
            f"migrated_bookings={migrated_bookings}",
            f"migrated_tables={migrated_tables}",
            f"synced_booking_states={len(booking_rows)}",
            f"synced_table_states={len(table_rows)}",
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
