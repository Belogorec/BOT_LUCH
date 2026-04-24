import sqlite3
from typing import Iterable, Optional


def require_single_row_update(cursor: sqlite3.Cursor, *, error_code: str = "state_conflict") -> None:
    if cursor.rowcount != 1:
        raise ValueError(error_code)


def _row_version(
    conn: sqlite3.Connection,
    *,
    table: str,
    row_id: int,
    expected_version: Optional[int],
    missing_error_code: str,
) -> int:
    if expected_version is not None:
        return int(expected_version)
    row = conn.execute(f"SELECT version FROM {table} WHERE id = ? LIMIT 1", (int(row_id),)).fetchone()
    if not row:
        raise ValueError(missing_error_code)
    return int(row["version"] or 1)


def reservation_version(
    conn: sqlite3.Connection,
    reservation_id: int,
    *,
    expected_version: Optional[int] = None,
    missing_error_code: str = "booking_not_found",
) -> int:
    return _row_version(
        conn,
        table="reservations",
        row_id=reservation_id,
        expected_version=expected_version,
        missing_error_code=missing_error_code,
    )


def assignment_version(
    conn: sqlite3.Connection,
    assignment_id: int,
    *,
    expected_version: Optional[int] = None,
    missing_error_code: str = "state_conflict",
) -> int:
    return _row_version(
        conn,
        table="reservation_tables",
        row_id=assignment_id,
        expected_version=expected_version,
        missing_error_code=missing_error_code,
    )


def table_block_version(
    conn: sqlite3.Connection,
    table_block_id: int,
    *,
    expected_version: Optional[int] = None,
    missing_error_code: str = "state_conflict",
) -> int:
    return _row_version(
        conn,
        table="table_blocks",
        row_id=table_block_id,
        expected_version=expected_version,
        missing_error_code=missing_error_code,
    )


def update_reservation(
    conn: sqlite3.Connection,
    reservation_id: int,
    *,
    set_sql: str,
    params: Iterable[object] = (),
    expected_version: Optional[int] = None,
    missing_error_code: str = "booking_not_found",
) -> None:
    version = reservation_version(
        conn,
        reservation_id,
        expected_version=expected_version,
        missing_error_code=missing_error_code,
    )
    cursor = conn.execute(
        f"""
        UPDATE reservations
        SET {set_sql},
            updated_at = datetime('now'),
            version = COALESCE(version, 1) + 1
        WHERE id = ?
          AND COALESCE(version, 1) = ?
        """,
        tuple(params) + (int(reservation_id), version),
    )
    require_single_row_update(cursor, error_code="state_conflict")


def release_assignment(
    conn: sqlite3.Connection,
    assignment_id: int,
    *,
    expected_version: Optional[int] = None,
    missing_error_code: str = "state_conflict",
) -> None:
    version = assignment_version(
        conn,
        assignment_id,
        expected_version=expected_version,
        missing_error_code=missing_error_code,
    )
    cursor = conn.execute(
        """
        UPDATE reservation_tables
        SET released_at = datetime('now'),
            version = COALESCE(version, 1) + 1
        WHERE id = ?
          AND COALESCE(version, 1) = ?
        """,
        (int(assignment_id), version),
    )
    require_single_row_update(cursor, error_code="state_conflict")


def delete_table_block(
    conn: sqlite3.Connection,
    table_block_id: int,
    *,
    expected_version: Optional[int] = None,
    missing_error_code: str = "state_conflict",
) -> None:
    version = table_block_version(
        conn,
        table_block_id,
        expected_version=expected_version,
        missing_error_code=missing_error_code,
    )
    cursor = conn.execute(
        """
        DELETE FROM table_blocks
        WHERE id = ?
          AND COALESCE(version, 1) = ?
        """,
        (int(table_block_id), version),
    )
    require_single_row_update(cursor, error_code="state_conflict")
