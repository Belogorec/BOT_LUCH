import os
import sqlite3
import tempfile
import unittest

from application import miniapp_booking, tilda_booking
from db import init_schema


class AuthoritativeIncomingBookingTests(unittest.TestCase):
    def setUp(self):
        self.prev_tilda_authoritative = tilda_booking.CRM_AUTHORITATIVE
        self.prev_miniapp_authoritative = miniapp_booking.CRM_AUTHORITATIVE
        tilda_booking.CRM_AUTHORITATIVE = True
        miniapp_booking.CRM_AUTHORITATIVE = True
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        try:
            init_schema(conn)
            conn.commit()
        finally:
            conn.close()

    def tearDown(self):
        tilda_booking.CRM_AUTHORITATIVE = self.prev_tilda_authoritative
        miniapp_booking.CRM_AUTHORITATIVE = self.prev_miniapp_authoritative
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def _connect(self):
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def test_tilda_authoritative_create_calls_crm_without_local_booking(self):
        conn = self._connect()
        calls = []
        original_create = tilda_booking.crm_commands.create_reservation

        def _fake_create(*, payload, event_id, actor):
            calls.append({"payload": payload, "event_id": event_id, "actor": actor})
            return {
                "accepted": True,
                "ok": True,
                "reservation": {"reservation_id": 55, "booking_id": 55},
                "body": {"result": {"duplicate": False}},
            }

        tilda_booking.crm_commands.create_reservation = _fake_create
        try:
            result = tilda_booking.execute_tilda_booking_webhook(
                conn,
                payload={"tranid": "tran-55"},
                name="Анна",
                phone_raw="+79000000101",
                phone_e164="+79000000101",
                date_raw="2026-05-01",
                time_raw="19:00",
                guests_count=2,
                comment="у окна",
                tranid="tran-55",
                formname="tilda-form",
                utm_source="",
                utm_medium="",
                utm_campaign="",
                utm_content="",
                utm_term="",
            )
            local_count = int(conn.execute("SELECT COUNT(*) AS c FROM bookings").fetchone()["c"])
        finally:
            tilda_booking.crm_commands.create_reservation = original_create
            conn.close()

        self.assertTrue(result["ok"])
        self.assertEqual(result["reservation_id"], 55)
        self.assertEqual(result["tg_status"], "crm_authoritative_pending_delivery")
        self.assertEqual(local_count, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["event_id"], "tilda:tran-55")
        self.assertEqual(calls[0]["payload"]["source"], "tilda")
        self.assertEqual(calls[0]["payload"]["reservation_date"], "2026-05-01")
        self.assertEqual(calls[0]["payload"]["guests_count"], 2)

    def test_tilda_authoritative_defaults_missing_guest_count_to_two(self):
        conn = self._connect()
        calls = []
        original_create = tilda_booking.crm_commands.create_reservation

        def _fake_create(*, payload, event_id, actor):
            calls.append({"payload": payload, "event_id": event_id, "actor": actor})
            return {
                "accepted": True,
                "ok": True,
                "reservation": {"reservation_id": 56, "booking_id": 56},
                "body": {"result": {"duplicate": False}},
            }

        tilda_booking.crm_commands.create_reservation = _fake_create
        try:
            result = tilda_booking.execute_tilda_booking_webhook(
                conn,
                payload={"tranid": "tran-56"},
                name="Кристина",
                phone_raw="89195893420",
                phone_e164="+79195893420",
                date_raw="2026-05-09",
                time_raw="22:00",
                guests_count=None,
                comment="",
                tranid="tran-56",
                formname="Бронь стола (instagram)",
                utm_source="ig",
                utm_medium="social",
                utm_campaign="",
                utm_content="link_in_bio",
                utm_term="",
            )
        finally:
            tilda_booking.crm_commands.create_reservation = original_create
            conn.close()

        self.assertTrue(result["ok"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["payload"]["guests_count"], 2)

    def test_miniapp_authoritative_create_calls_crm_without_local_booking(self):
        conn = self._connect()
        calls = []
        original_create = miniapp_booking.crm_commands.create_reservation
        conn.execute(
            """
            INSERT INTO tg_bot_users (tg_user_id, first_name, has_shared_phone, phone_e164)
            VALUES ('777', 'Анна', 1, '+79000000101')
            """
        )
        conn.commit()

        def _fake_create(*, payload, event_id, actor):
            calls.append({"payload": payload, "event_id": event_id, "actor": actor})
            return {
                "accepted": True,
                "ok": True,
                "reservation": {"reservation_id": 77, "booking_id": 77},
                "body": {"result": {"duplicate": False}},
            }

        miniapp_booking.crm_commands.create_reservation = _fake_create
        try:
            result = miniapp_booking.execute_telegram_miniapp_booking(
                conn,
                tg_user_id="777",
                date_value="2026-05-02",
                time_value="20:00",
                guests_count=3,
                comment_value="день рождения",
                reservation_token="mini-token-77",
            )
            local_count = int(conn.execute("SELECT COUNT(*) AS c FROM bookings").fetchone()["c"])
        finally:
            miniapp_booking.crm_commands.create_reservation = original_create
            conn.close()

        self.assertTrue(result["ok"])
        self.assertEqual(result["reservation_id"], 77)
        self.assertEqual(local_count, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["event_id"], "telegram_miniapp:mini-token-77")
        self.assertEqual(calls[0]["payload"]["source"], "telegram_miniapp_api")
        self.assertEqual(calls[0]["payload"]["guest_phone"], "+79000000101")


if __name__ == "__main__":
    unittest.main()
