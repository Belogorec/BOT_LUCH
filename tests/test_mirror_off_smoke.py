import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import booking_service
import channel_binding_service
import waiter_notify
from application import miniapp_booking, tilda_booking
from contact_schema import run_contact_schema_migrations
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations


class MirrorOffSmokeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()

        self.prev_booking_legacy = booking_service.LEGACY_MIRROR_ENABLED
        self.prev_waiter_chat_id = waiter_notify.WAITER_CHAT_ID
        self.prev_miniapp_tg_chat_id = miniapp_booking.TG_CHAT_ID
        self.prev_tilda_tg_chat_id = tilda_booking.TG_CHAT_ID

        booking_service.LEGACY_MIRROR_ENABLED = False
        waiter_notify.WAITER_CHAT_ID = "waiter-smoke-chat"
        miniapp_booking.TG_CHAT_ID = "hostess-smoke-chat"
        tilda_booking.TG_CHAT_ID = "hostess-smoke-chat"

        conn = self._connect()
        try:
            init_schema(conn)
            run_core_schema_migrations(conn)
            run_integration_schema_migrations(conn)
            run_contact_schema_migrations(conn)
            conn.commit()
        finally:
            conn.close()

    def tearDown(self):
        booking_service.LEGACY_MIRROR_ENABLED = self.prev_booking_legacy
        waiter_notify.WAITER_CHAT_ID = self.prev_waiter_chat_id
        miniapp_booking.TG_CHAT_ID = self.prev_miniapp_tg_chat_id
        tilda_booking.TG_CHAT_ID = self.prev_tilda_tg_chat_id
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def _connect(self):
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def test_miniapp_booking_application_creates_canonical_token_in_mirror_off_mode(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO tg_bot_users (
                    tg_user_id, username, first_name, has_shared_phone, phone_e164, first_started_at, last_started_at, start_count
                ) VALUES ('tg-101', 'anna', 'Анна', 1, '+79000003001', datetime('now'), datetime('now'), 1)
                """
            )

            with patch.object(miniapp_booking, "dispatch_hostess_booking_card", return_value={"ok": True, "provider_message_id": "501"}), \
                 patch.object(miniapp_booking, "notify_vk_staff_about_new_booking", return_value=0), \
                 patch.object(miniapp_booking, "send_booking_event", return_value=True):
                result = miniapp_booking.execute_telegram_miniapp_booking(
                    conn,
                    tg_user_id="tg-101",
                    date_value="2026-05-21",
                    time_value="19:45",
                    guests_count=2,
                    comment_value="Mirror-off miniapp",
                    reservation_token="miniapp-smoke-token",
                )
            conn.commit()

            booking_id = int(result["booking_id"])
            booking_row = conn.execute(
                """
                SELECT reservation_token, deposit_amount
                FROM bookings
                WHERE id = ?
                """,
                (booking_id,),
            ).fetchone()
            reservation_row = conn.execute(
                """
                SELECT id, guest_name, guest_phone, reservation_at, party_size, status
                FROM reservations
                WHERE source='legacy_booking' AND external_ref=?
                LIMIT 1
                """,
                (str(booking_id),),
            ).fetchone()
            public_token_row = conn.execute(
                """
                SELECT public_token, status
                FROM public_reservation_tokens
                WHERE reservation_id = ?
                LIMIT 1
                """,
                (int(reservation_row["id"]),),
            ).fetchone()
        finally:
            conn.close()

        self.assertTrue(result["ok"])
        self.assertIsNone(booking_row)
        self.assertEqual(reservation_row["guest_name"], "Анна")
        self.assertEqual(reservation_row["guest_phone"], "+79000003001")
        self.assertEqual(reservation_row["reservation_at"], "2026-05-21T19:45:00")
        self.assertEqual(int(reservation_row["party_size"]), 2)
        self.assertEqual(reservation_row["status"], "pending")
        self.assertEqual(public_token_row["public_token"], "miniapp-smoke-token")
        self.assertEqual(public_token_row["status"], "active")

    def test_tilda_booking_application_returns_guest_page_url_and_token_resolves_canonically(self):
        conn = self._connect()
        try:
            with patch.object(tilda_booking, "dispatch_hostess_booking_card", return_value={"ok": True, "provider_message_id": "601"}), \
                 patch.object(tilda_booking, "notify_vk_staff_about_new_booking", return_value=0), \
                 patch.object(tilda_booking, "send_booking_event", return_value=True):
                result = tilda_booking.execute_tilda_booking_webhook(
                    conn,
                    payload={"source": "tilda-smoke"},
                    name="Иван",
                    phone_raw="+7 (900) 000-30-02",
                    phone_e164="+79000003002",
                    date_raw="2026-05-22",
                    time_raw="20:15",
                    guests_count=4,
                    comment="Mirror-off tilda",
                    tranid="smoke-tran-22",
                    formname="Tilda Smoke",
                    utm_source="tilda",
                    utm_medium="site",
                    utm_campaign="camp",
                    utm_content="content",
                    utm_term="term",
                )
            conn.commit()

            booking_id = int(result["booking_id"])
            reservation = conn.execute(
                """
                SELECT id
                FROM reservations
                WHERE source='legacy_booking' AND external_ref=?
                LIMIT 1
                """,
                (str(booking_id),),
            ).fetchone()
            token_row = conn.execute(
                """
                SELECT public_token
                FROM public_reservation_tokens
                WHERE reservation_id = ?
                LIMIT 1
                """,
                (int(reservation["id"]),),
            ).fetchone()
            payload = channel_binding_service.get_reservation_by_token(conn, str(token_row["public_token"]))
        finally:
            conn.close()

        self.assertTrue(result["ok"])
        self.assertTrue(str(result["guest_page_url"]).startswith("/guest/reservation/"))
        self.assertIsNotNone(payload)
        self.assertEqual(int(payload["reservation_id"]), int(reservation["id"]))
        self.assertEqual(int(payload["id"]), booking_id)
        self.assertEqual(payload["phone_e164"], "+79000003002")
        self.assertEqual(payload["formname"], "Tilda Smoke")
        self.assertEqual(payload["reservation_token"], token_row["public_token"])

    def test_manual_booking_deposit_flow_notifies_waiters_without_legacy_booking_mirror(self):
        conn = self._connect()
        try:
            create_result = booking_service.create_manual_booking(
                conn,
                guest_name="Мария",
                guest_phone="+79000003003",
                reservation_date="2026-05-23",
                reservation_time="21:00",
                guests_count=3,
                comment="Mirror-off manual",
                actor_id="crm-1",
                actor_name="CRM",
                table_number="12",
            )
            booking_id = int(create_result["booking_id"])
            booking_service.set_booking_deposit(
                conn,
                booking_id,
                5000,
                "crm-1",
                "CRM",
                comment="Smoke deposit",
            )

            with patch.object(waiter_notify, "dispatch_outbox_message", return_value={"ok": True, "provider_message_id": "9001"}), \
                 patch("vk_staff_notify.fetch_active_vk_staff_peers", return_value=[]):
                notify_result = waiter_notify.notify_waiters_about_deposit_booking(conn, booking_id)
            conn.commit()

            reservation = conn.execute(
                """
                SELECT id, deposit_amount, deposit_comment
                FROM reservations
                WHERE source='legacy_booking' AND external_ref=?
                LIMIT 1
                """,
                (str(booking_id),),
            ).fetchone()
            booking_row = conn.execute(
                """
                SELECT deposit_amount, assigned_table_number
                FROM bookings
                WHERE id = ?
                """,
                (booking_id,),
            ).fetchone()
            assignment_row = conn.execute(
                """
                SELECT tc.code AS table_code
                FROM reservation_tables rt
                JOIN tables_core tc ON tc.id = rt.table_id
                WHERE rt.reservation_id = ?
                  AND rt.released_at IS NULL
                ORDER BY rt.id DESC
                LIMIT 1
                """,
                (int(reservation["id"]),),
            ).fetchone()
            outbox_row = conn.execute(
                """
                SELECT platform, bot_scope, message_type, target_external_id
                FROM bot_outbox
                WHERE reservation_id = ?
                  AND message_type = 'waiter_deposit_notification'
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(reservation["id"]),),
            ).fetchone()
        finally:
            conn.close()

        self.assertTrue(notify_result)
        self.assertEqual(int(reservation["deposit_amount"]), 5000)
        self.assertEqual(reservation["deposit_comment"], "Smoke deposit")
        self.assertIsNone(booking_row)
        self.assertEqual(assignment_row["table_code"], "12")
        self.assertIsNotNone(outbox_row)
        self.assertEqual(outbox_row["platform"], "telegram")
        self.assertEqual(outbox_row["bot_scope"], "waiter")
        self.assertEqual(outbox_row["message_type"], "waiter_deposit_notification")
        self.assertEqual(outbox_row["target_external_id"], "waiter-smoke-chat")


if __name__ == "__main__":
    unittest.main()
