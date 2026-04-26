import os
import sqlite3
import tempfile
import unittest

from booking_service import get_table_booking_conflicts
from core_schema import run_core_schema_migrations
from db import init_schema


class TableReservationConflictTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        conn = self._connect()
        try:
            init_schema(conn)
            run_core_schema_migrations(conn)
            conn.commit()
        finally:
            conn.close()

    def tearDown(self):
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def _connect(self):
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _insert_assigned_reservation(self, conn, *, external_ref: str, table_code: str, reservation_at: str) -> None:
        conn.execute(
            """
            INSERT INTO reservations (
                source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
            )
            VALUES ('legacy_booking', ?, ?, ?, ?, 2, 'confirmed')
            """,
            (external_ref, f"Гость {external_ref}", f"+7900000{external_ref.zfill(4)}", reservation_at),
        )
        reservation_id = int(conn.execute("SELECT id FROM reservations WHERE external_ref = ?", (external_ref,)).fetchone()["id"])
        conn.execute(
            "INSERT INTO tables_core (code, title) VALUES (?, ?) ON CONFLICT(code) DO NOTHING",
            (table_code, f"Стол {table_code}"),
        )
        table_id = int(conn.execute("SELECT id FROM tables_core WHERE code = ?", (table_code,)).fetchone()["id"])
        conn.execute(
            "INSERT INTO reservation_tables (reservation_id, table_id, assigned_by) VALUES (?, ?, 'test')",
            (reservation_id, table_id),
        )

    def test_same_table_non_overlapping_reservation_does_not_conflict(self):
        conn = self._connect()
        try:
            self._insert_assigned_reservation(
                conn,
                external_ref="1",
                table_code="221",
                reservation_at="2026-05-01T19:00",
            )

            conflicts = get_table_booking_conflicts(
                conn,
                "221",
                "2026-05-01T21:00",
                exclude_booking_id=2,
            )
        finally:
            conn.close()

        self.assertEqual(conflicts, [])

    def test_same_table_overlapping_reservation_conflicts(self):
        conn = self._connect()
        try:
            self._insert_assigned_reservation(
                conn,
                external_ref="1",
                table_code="221",
                reservation_at="2026-05-01T19:00",
            )

            conflicts = get_table_booking_conflicts(
                conn,
                "221",
                "2026-05-01T20:00",
                exclude_booking_id=2,
            )
        finally:
            conn.close()

        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["id"], 1)


if __name__ == "__main__":
    unittest.main()
