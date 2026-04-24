import os
import sqlite3
import tempfile
import unittest

from booking_service import set_booking_status
from core_schema import run_core_schema_migrations
from core_sync import sync_booking_assignment_to_core, sync_booking_to_core
from core_write_guards import update_reservation
from db import init_schema


class CoreWriteGuardsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
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

    def test_core_schema_defaults_version_columns_to_one(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('legacy_booking', '1', 'Анна', '+79000001001', '2026-05-01T19:00', 2, 'pending')
                """
            )
            reservation_id = int(conn.execute("SELECT id FROM reservations WHERE external_ref='1'").fetchone()["id"])
            conn.execute("INSERT INTO tables_core (code, title) VALUES ('221', 'Стол 221')")
            table_id = int(conn.execute("SELECT id FROM tables_core WHERE code='221'").fetchone()["id"])
            conn.execute(
                """
                INSERT INTO reservation_tables (reservation_id, table_id, assigned_by)
                VALUES (?, ?, 'tester')
                """,
                (reservation_id, table_id),
            )
            conn.execute(
                """
                INSERT INTO table_blocks (table_id, starts_at, ends_at, reason, block_type, created_by)
                VALUES (?, '2026-05-01T18:00', '2026-05-01T19:00', 'reserve', 'manual', 'tester')
                """,
                (table_id,),
            )
            conn.commit()

            reservation_version = int(conn.execute("SELECT version FROM reservations WHERE id=?", (reservation_id,)).fetchone()["version"])
            assignment_version = int(
                conn.execute("SELECT version FROM reservation_tables WHERE reservation_id=?", (reservation_id,)).fetchone()["version"]
            )
            block_version = int(conn.execute("SELECT version FROM table_blocks WHERE table_id=?", (table_id,)).fetchone()["version"])
        finally:
            conn.close()

        self.assertEqual(reservation_version, 1)
        self.assertEqual(assignment_version, 1)
        self.assertEqual(block_version, 1)

    def test_update_reservation_rejects_stale_expected_version(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('legacy_booking', '2', 'Иван', '+79000001002', '2026-05-01T20:00', 2, 'pending')
                """
            )
            reservation_id = int(conn.execute("SELECT id FROM reservations WHERE external_ref='2'").fetchone()["id"])

            update_reservation(
                conn,
                reservation_id,
                set_sql="status = ?",
                params=("confirmed",),
                expected_version=1,
            )
            conn.commit()

            with self.assertRaises(ValueError) as ctx:
                update_reservation(
                    conn,
                    reservation_id,
                    set_sql="status = ?",
                    params=("cancelled",),
                    expected_version=1,
                )
        finally:
            conn.close()

        self.assertEqual(str(ctx.exception), "state_conflict")

    def test_set_booking_status_bumps_canonical_reservation_version(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO bookings (
                    id, name, phone_e164, reservation_dt, guests_count, status
                )
                VALUES (10, 'Анна', '+79000001010', '2026-05-01 19:00', 2, 'WAITING')
                """
            )
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('legacy_booking', '10', 'Анна', '+79000001010', '2026-05-01T19:00', 2, 'pending')
                """
            )

            result = set_booking_status(conn, 10, "CONFIRMED", "tester", "Tester")
            conn.commit()

            row = conn.execute(
                "SELECT status, version FROM reservations WHERE source='legacy_booking' AND external_ref='10'"
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result, "CONFIRMED")
        self.assertEqual(row["status"], "confirmed")
        self.assertEqual(int(row["version"]), 2)

    def test_sync_booking_to_core_updates_existing_reservation_and_bumps_version(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO bookings (
                    id, name, phone_e164, reservation_dt, guests_count, status, comment
                )
                VALUES (20, 'Мария', '+79000001020', '2026-05-02 20:00', 4, 'CONFIRMED', 'window')
                """
            )
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status, comment
                )
                VALUES ('legacy_booking', '20', 'Old', '+79000000000', '2026-05-02T19:00', 2, 'pending', 'old')
                """
            )

            reservation_id = sync_booking_to_core(conn, 20)
            conn.commit()

            row = conn.execute("SELECT guest_name, party_size, status, comment, version FROM reservations WHERE id=?", (reservation_id,)).fetchone()
        finally:
            conn.close()

        self.assertEqual(row["guest_name"], "Мария")
        self.assertEqual(int(row["party_size"]), 4)
        self.assertEqual(row["status"], "confirmed")
        self.assertEqual(row["comment"], "window")
        self.assertEqual(int(row["version"]), 2)

    def test_sync_booking_assignment_to_core_releases_previous_assignment_with_version_bump(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO bookings (
                    id, name, phone_e164, reservation_dt, guests_count, status, assigned_table_number
                )
                VALUES (30, 'Олег', '+79000001030', '2026-05-03 21:00', 2, 'WAITING', '222')
                """
            )
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('legacy_booking', '30', 'Олег', '+79000001030', '2026-05-03T21:00', 2, 'pending')
                """
            )
            reservation_id = int(
                conn.execute("SELECT id FROM reservations WHERE source='legacy_booking' AND external_ref='30'").fetchone()["id"]
            )
            conn.execute("INSERT INTO tables_core (code, title) VALUES ('221', 'Стол 221')")
            old_table_id = int(conn.execute("SELECT id FROM tables_core WHERE code='221'").fetchone()["id"])
            conn.execute(
                """
                INSERT INTO reservation_tables (reservation_id, table_id, assigned_by)
                VALUES (?, ?, 'tester')
                """,
                (reservation_id, old_table_id),
            )

            sync_booking_assignment_to_core(conn, 30)
            conn.commit()

            old_row = conn.execute(
                """
                SELECT released_at, version
                FROM reservation_tables
                WHERE reservation_id = ? AND table_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (reservation_id, old_table_id),
            ).fetchone()
            new_row = conn.execute(
                """
                SELECT rt.version, tc.code
                FROM reservation_tables rt
                JOIN tables_core tc ON tc.id = rt.table_id
                WHERE rt.reservation_id = ? AND rt.released_at IS NULL
                ORDER BY rt.id DESC
                LIMIT 1
                """,
                (reservation_id,),
            ).fetchone()
        finally:
            conn.close()

        self.assertIsNotNone(old_row["released_at"])
        self.assertEqual(int(old_row["version"]), 2)
        self.assertEqual(new_row["code"], "222")
        self.assertEqual(int(new_row["version"]), 1)


if __name__ == "__main__":
    unittest.main()
