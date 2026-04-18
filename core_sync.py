from datetime import datetime
import sqlite3


LEGACY_BOOKING_SOURCE = "legacy_booking"
LEGACY_BLOCK_TYPE = "legacy_sync"


def _legacy_reservation_at(row: sqlite3.Row) -> str:
    reservation_dt = str((row["reservation_dt"] or "")).strip()
    if reservation_dt:
        return reservation_dt.replace(" ", "T") if "T" not in reservation_dt and " " in reservation_dt else reservation_dt

    reservation_date = str((row["reservation_date"] or "")).strip()
    reservation_time = str((row["reservation_time"] or "")).strip()
    if reservation_date and reservation_time:
        return f"{reservation_date}T{reservation_time}"

    return ""


def _core_status(status: str) -> str:
    normalized = str(status or "").strip().upper()
    return {
        "NEW": "pending",
        "WAITING": "pending",
        "CONFIRMED": "confirmed",
        "DECLINED": "declined",
        "CANCELLED": "cancelled",
        "NO_SHOW": "no_show",
        "COMPLETED": "completed",
    }.get(normalized, "pending")


def _ensure_table(conn: sqlite3.Connection, table_code: str) -> int:
    code = str(table_code or "").strip()
    if not code:
        raise ValueError("table_code_required")

    conn.execute(
        """
        INSERT INTO tables_core (code, title, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(code) DO UPDATE SET
          updated_at = datetime('now')
        """,
        (code, f"Table {code}"),
    )
    row = conn.execute("SELECT id FROM tables_core WHERE code = ?", (code,)).fetchone()
    if not row:
        raise ValueError("table_not_found_after_upsert")
    return int(row["id"])


def sync_booking_to_core(conn: sqlite3.Connection, booking_id: int) -> int:
    booking = conn.execute(
        """
        SELECT
          id,
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
        WHERE id = ?
        """,
        (int(booking_id),),
    ).fetchone()
    if not booking:
        raise ValueError("booking_not_found")

    reservation_at = _legacy_reservation_at(booking)
    if not reservation_at:
        raise ValueError("booking_has_no_reservation_at")

    external_ref = str(int(booking["id"]))
    reservation = conn.execute(
        "SELECT id FROM reservations WHERE source = ? AND external_ref = ?",
        (LEGACY_BOOKING_SOURCE, external_ref),
    ).fetchone()
    params = (
        booking["name"],
        booking["phone_e164"],
        reservation_at,
        max(1, int(booking["guests_count"] or 0)),
        booking["comment"],
        booking["deposit_amount"],
        booking["deposit_comment"],
        booking["deposit_set_at"],
        booking["deposit_set_by"],
        _core_status(booking["status"]),
        booking["created_at"],
        booking["updated_at"],
        LEGACY_BOOKING_SOURCE,
        external_ref,
    )
    if reservation:
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        reservation = conn.execute(
            "SELECT id FROM reservations WHERE source = ? AND external_ref = ?",
            (LEGACY_BOOKING_SOURCE, external_ref),
        ).fetchone()
    if not reservation:
        raise ValueError("reservation_not_found_after_upsert")
    return int(reservation["id"])


def sync_booking_assignment_to_core(conn: sqlite3.Connection, booking_id: int) -> None:
    reservation_id = sync_booking_to_core(conn, booking_id)
    booking = conn.execute(
        "SELECT assigned_table_number FROM bookings WHERE id = ?",
        (int(booking_id),),
    ).fetchone()
    if not booking:
        return

    table_code = str((booking["assigned_table_number"] or "")).strip()
    active_assignment = conn.execute(
        """
        SELECT id, table_id
        FROM reservation_tables
        WHERE reservation_id = ? AND released_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (reservation_id,),
    ).fetchone()

    if not table_code:
        if active_assignment:
            conn.execute(
                "UPDATE reservation_tables SET released_at = datetime('now') WHERE id = ?",
                (int(active_assignment["id"]),),
            )
        return

    table_id = _ensure_table(conn, table_code)
    if active_assignment and int(active_assignment["table_id"]) == table_id:
        return

    if active_assignment:
        conn.execute(
            "UPDATE reservation_tables SET released_at = datetime('now') WHERE id = ?",
            (int(active_assignment["id"]),),
        )

    conn.execute(
        """
        INSERT INTO reservation_tables (reservation_id, table_id, assigned_by)
        VALUES (?, ?, ?)
        """,
        (reservation_id, table_id, "legacy_sync"),
    )


def sync_table_to_core(conn: sqlite3.Connection, table_number: str) -> None:
    code = str(table_number or "").strip()
    if not code:
        return

    table_id = _ensure_table(conn, code)
    conn.execute(
        "DELETE FROM table_blocks WHERE table_id = ? AND block_type = ?",
        (table_id, LEGACY_BLOCK_TYPE),
    )

    row = conn.execute(
        """
        SELECT label, restricted_until, restriction_comment
        FROM venue_tables
        WHERE table_number = ?
        """,
        (code,),
    ).fetchone()
    if not row:
        return

    label = str((row["label"] or "")).strip().upper()
    restricted_until = str((row["restricted_until"] or "")).strip()
    if label != "RESTRICTED" or not restricted_until:
        return

    starts_at = datetime.utcnow().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO table_blocks (
          table_id,
          starts_at,
          ends_at,
          reason,
          block_type,
          created_by
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            table_id,
            starts_at,
            restricted_until.replace(" ", "T") if "T" not in restricted_until and " " in restricted_until else restricted_until,
            row["restriction_comment"],
            LEGACY_BLOCK_TYPE,
            "legacy_sync",
        ),
    )


def sync_booking_state_to_core(conn: sqlite3.Connection, booking_id: int) -> None:
    sync_booking_to_core(conn, booking_id)
    sync_booking_assignment_to_core(conn, booking_id)


def sync_table_state_to_core(conn: sqlite3.Connection, table_number: str) -> None:
    sync_table_to_core(conn, table_number)


def migrate_all_tables_to_core(conn: sqlite3.Connection) -> int:
    """Migrate all venue_tables to tables_core (idempotent)."""
    venue_tables = conn.execute(
        """
        SELECT DISTINCT table_number, label
        FROM venue_tables
        WHERE table_number IS NOT NULL AND trim(table_number) <> ''
        """
    ).fetchall()

    migrated = 0
    for row in venue_tables:
        table_number = str(row["table_number"]).strip()
        if not table_number:
            continue

        table_id = _ensure_table(conn, table_number)
        sync_table_to_core(conn, table_number)
        migrated += 1

    return migrated
