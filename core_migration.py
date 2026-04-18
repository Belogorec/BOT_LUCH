import sqlite3

from core_sync import LEGACY_BOOKING_SOURCE


def _build_reservation_at(row: sqlite3.Row) -> str:
    reservation_dt = str((row["reservation_dt"] or "")).strip()
    if reservation_dt:
        return reservation_dt

    reservation_date = str((row["reservation_date"] or "")).strip()
    reservation_time = str((row["reservation_time"] or "")).strip()
    if reservation_date and reservation_time:
        return f"{reservation_date}T{reservation_time}"

    return ""


def migrate_bookings_to_reservations(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        """
        SELECT
          id,
          tranid,
          formname,
          name,
          phone_e164,
          reservation_date,
          reservation_time,
          reservation_dt,
          guests_count,
          comment,
          deposit_amount,
          deposit_comment,
          deposit_set_at,
          deposit_set_by,
          status,
          created_at,
          updated_at
        FROM bookings
        ORDER BY id ASC
        """
    ).fetchall()

    migrated = 0
    for row in rows:
        reservation_at = _build_reservation_at(row)
        if not reservation_at:
            continue

        source = LEGACY_BOOKING_SOURCE
        external_ref = str(int(row["id"]))

        exists = conn.execute(
            "SELECT id FROM reservations WHERE source = ? AND external_ref = ?",
            (source, external_ref),
        ).fetchone()
        params = (
            row["name"],
            row["phone_e164"],
            reservation_at,
            max(1, int(row["guests_count"] or 0)),
            row["comment"],
            row["deposit_amount"],
            row["deposit_comment"],
            row["deposit_set_at"],
            row["deposit_set_by"],
            str((row["status"] or "")).strip().lower() or "pending",
            row["created_at"],
            row["updated_at"],
            source,
            external_ref,
        )
        if exists:
            conn.execute(
                """
                UPDATE reservations
                SET guest_name = ?,
                    guest_phone = ?,
                    reservation_at = ?,
                    party_size = ?,
                    comment = ?,
                    deposit_amount = ?,
                    deposit_comment = ?,
                    deposit_set_at = ?,
                    deposit_set_by = ?,
                    status = ?,
                    created_at = ?,
                    updated_at = ?
                WHERE source = ? AND external_ref = ?
                """,
                params,
            )
        else:
            conn.execute(
                """
                INSERT INTO reservations (
                  guest_name,
                  guest_phone,
                  reservation_at,
                  party_size,
                  comment,
                  deposit_amount,
                  deposit_comment,
                  deposit_set_at,
                  deposit_set_by,
                  status,
                  created_at,
                  updated_at,
                  source,
                  external_ref
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
        migrated += 1

    return migrated


def migrate_venue_tables_to_core(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        """
        SELECT table_number, created_at, updated_at
        FROM venue_tables
        ORDER BY table_number ASC
        """
    ).fetchall()

    migrated = 0
    for row in rows:
        code = str((row["table_number"] or "")).strip()
        if not code:
            continue

        conn.execute(
            """
            INSERT INTO tables_core (code, title, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
              updated_at = excluded.updated_at
            """,
            (
                code,
                f"Table {code}",
                row["created_at"],
                row["updated_at"],
            ),
        )
        migrated += 1

    return migrated
