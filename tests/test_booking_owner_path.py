import os
import sqlite3
import tempfile
import unittest

from booking_service import (
    create_manual_booking,
    create_telegram_miniapp_booking_record,
    upsert_tilda_booking_record,
)
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations


class BookingOwnerPathTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        try:
            init_schema(conn)
            run_core_schema_migrations(conn)
            run_integration_schema_migrations(conn)
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

    def test_create_manual_booking_creates_canonical_reservation_and_binds_booking_id(self):
        conn = self._connect()
        try:
            result = create_manual_booking(
                conn,
                guest_name="Анна",
                guest_phone="+79000002001",
                reservation_date="2026-05-10",
                reservation_time="19:30",
                guests_count=3,
                comment="Manual create",
                actor_id="crm-1",
                actor_name="CRM",
            )
            conn.commit()

            booking = conn.execute("SELECT id, status FROM bookings WHERE id = ?", (int(result["booking_id"]),)).fetchone()
            reservation = conn.execute(
                """
                SELECT source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                FROM reservations
                WHERE source='legacy_booking' AND external_ref=?
                LIMIT 1
                """,
                (str(int(result["booking_id"])),),
            ).fetchone()
        finally:
            conn.close()

        self.assertIsNotNone(booking)
        self.assertEqual(booking["status"], "CONFIRMED")
        self.assertIsNotNone(reservation)
        self.assertEqual(reservation["guest_name"], "Анна")
        self.assertEqual(reservation["guest_phone"], "+79000002001")
        self.assertEqual(reservation["reservation_at"], "2026-05-10T19:30")
        self.assertEqual(int(reservation["party_size"]), 3)
        self.assertEqual(reservation["status"], "confirmed")

    def test_create_telegram_miniapp_booking_record_creates_canonical_reservation_and_public_token(self):
        conn = self._connect()
        try:
            result = create_telegram_miniapp_booking_record(
                conn,
                tg_user_id="tg-11",
                date_value="2026-05-11",
                time_value="20:15",
                guests_count=2,
                comment_value="Miniapp",
                reservation_token="miniapp-token-11",
                phone_e164="+79000002011",
                display_name="Mini User",
                raw_payload_json='{"src":"miniapp"}',
            )
            conn.commit()

            reservation = conn.execute(
                """
                SELECT id, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                FROM reservations
                WHERE source='legacy_booking' AND external_ref=?
                LIMIT 1
                """,
                (str(int(result["booking_id"])),),
            ).fetchone()
            token = conn.execute(
                """
                SELECT public_token, token_kind, status
                FROM public_reservation_tokens
                WHERE reservation_id = ?
                LIMIT 1
                """,
                (int(reservation["id"]),),
            ).fetchone()
        finally:
            conn.close()

        self.assertFalse(result["duplicate"])
        self.assertIsNotNone(reservation)
        self.assertEqual(reservation["guest_name"], "Mini User")
        self.assertEqual(reservation["guest_phone"], "+79000002011")
        self.assertEqual(reservation["reservation_at"], "2026-05-11T20:15:00")
        self.assertEqual(int(reservation["party_size"]), 2)
        self.assertEqual(reservation["status"], "pending")
        self.assertEqual(token["public_token"], "miniapp-token-11")
        self.assertEqual(token["token_kind"], "guest_access")
        self.assertEqual(token["status"], "active")

    def test_upsert_tilda_booking_record_updates_canonical_reservation_without_sync_helper(self):
        conn = self._connect()
        try:
            created = upsert_tilda_booking_record(
                conn,
                payload_json='{"src":"tilda","step":"create"}',
                name="Иван",
                phone_e164="+79000002021",
                phone_raw="+79000002021",
                date_raw="2026-05-12",
                time_raw="18:00",
                reservation_dt="2026-05-12T18:00:00",
                guests_count=2,
                comment="First",
                tranid="tran-21",
                formname="tilda_form",
                utm_source="tilda",
                utm_medium="site",
                utm_campaign="camp",
                utm_content="content",
                utm_term="term",
                guest_segment="NEW",
            )
            updated = upsert_tilda_booking_record(
                conn,
                payload_json='{"src":"tilda","step":"update"}',
                name="Иван Петров",
                phone_e164="+79000002021",
                phone_raw="+79000002021",
                date_raw="2026-05-12",
                time_raw="19:00",
                reservation_dt="2026-05-12T19:00:00",
                guests_count=4,
                comment="Updated",
                tranid="tran-21",
                formname="tilda_form",
                utm_source="tilda",
                utm_medium="site",
                utm_campaign="camp2",
                utm_content="content2",
                utm_term="term2",
                guest_segment="REGULAR",
            )
            conn.commit()

            reservation = conn.execute(
                """
                SELECT guest_name, reservation_at, party_size, comment, version
                FROM reservations
                WHERE source='legacy_booking' AND external_ref=?
                LIMIT 1
                """,
                (str(int(created["booking_id"])),),
            ).fetchone()
        finally:
            conn.close()

        self.assertFalse(created["existing"])
        self.assertTrue(updated["existing"])
        self.assertEqual(updated["booking_id"], created["booking_id"])
        self.assertEqual(reservation["guest_name"], "Иван Петров")
        self.assertEqual(reservation["reservation_at"], "2026-05-12T19:00:00")
        self.assertEqual(int(reservation["party_size"]), 4)
        self.assertEqual(reservation["comment"], "Updated")
        self.assertGreaterEqual(int(reservation["version"]), 2)


if __name__ == "__main__":
    unittest.main()
