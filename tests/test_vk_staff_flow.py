import os
import sqlite3
import tempfile
import unittest

import booking_service
import outbox_dispatcher
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations
from vk_staff_flow import (
    VK_PENDING_EVENT_TYPE,
    VK_PROMPT_OUTBOX_TYPE,
    process_vk_booking_payload,
    process_vk_pending_text,
)


class VkStaffFlowTests(unittest.TestCase):
    def setUp(self):
        self.prev_core_only = booking_service.CORE_ONLY_MODE
        self.prev_legacy_mirror = booking_service.LEGACY_MIRROR_ENABLED
        booking_service.CORE_ONLY_MODE = False
        booking_service.LEGACY_MIRROR_ENABLED = True
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
        booking_service.CORE_ONLY_MODE = self.prev_core_only
        booking_service.LEGACY_MIRROR_ENABLED = self.prev_legacy_mirror
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def _connect(self):
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _seed_booking(self, conn, *, booking_id: int, phone: str = "+79000000101") -> None:
        conn.execute(
            """
            INSERT INTO bookings (
                id, formname, name, phone_e164, reservation_dt, guests_count, status
            )
            VALUES (?, 'vk', 'Анна', ?, '2026-05-01 19:00', 2, 'WAITING')
            """,
            (booking_id, phone),
        )

    def test_prompt_assign_table_uses_canonical_pending_event_and_outbox(self):
        conn = self._connect()
        original_dispatch_vk = outbox_dispatcher._dispatch_vk
        outbox_dispatcher._dispatch_vk = lambda target, payload, bot_scope: "vk-msg-1"
        try:
            self._seed_booking(conn, booking_id=1)

            handled = process_vk_booking_payload(
                conn,
                peer_id=2000000001,
                from_id=777,
                payload={"kind": "booking_action", "action": "prompt_assign_table", "booking_id": 1},
            )
            conn.commit()

            pending_reply_count = int(conn.execute("SELECT COUNT(*) AS c FROM pending_replies").fetchone()["c"])
            inbound = conn.execute(
                """
                SELECT event_type, processing_status, reservation_id, payload_json
                FROM bot_inbound_events
                WHERE event_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (VK_PENDING_EVENT_TYPE,),
            ).fetchone()
            outbox = conn.execute(
                """
                SELECT platform, bot_scope, message_type, target_external_id, delivery_status
                FROM bot_outbox
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            reservation = conn.execute(
                """
                SELECT id
                FROM reservations
                WHERE source = 'legacy_booking' AND external_ref = '1'
                LIMIT 1
                """
            ).fetchone()
        finally:
            outbox_dispatcher._dispatch_vk = original_dispatch_vk
            conn.close()

        self.assertTrue(handled)
        self.assertEqual(pending_reply_count, 0)
        self.assertIsNotNone(reservation)
        self.assertEqual(inbound["event_type"], VK_PENDING_EVENT_TYPE)
        self.assertEqual(inbound["processing_status"], "pending")
        self.assertEqual(int(inbound["reservation_id"]), int(reservation["id"]))
        self.assertIn('"mode": "assign_table"', inbound["payload_json"])
        self.assertEqual(outbox["platform"], "vk")
        self.assertEqual(outbox["bot_scope"], "hostess")
        self.assertEqual(outbox["message_type"], VK_PROMPT_OUTBOX_TYPE)
        self.assertEqual(outbox["target_external_id"], "2000000001")
        self.assertEqual(outbox["delivery_status"], "sent")

    def test_set_deposit_pending_text_reads_canonical_event_and_requeues_assign_table(self):
        conn = self._connect()
        original_dispatch_vk = outbox_dispatcher._dispatch_vk
        outbox_dispatcher._dispatch_vk = lambda target, payload, bot_scope: f"vk-{payload.get('text', '')[:8]}"
        try:
            self._seed_booking(conn, booking_id=2, phone="+79000000102")

            started = process_vk_booking_payload(
                conn,
                peer_id=2000000002,
                from_id=888,
                payload={"kind": "booking_action", "action": "prompt_set_deposit", "booking_id": 2},
            )
            handled = process_vk_pending_text(
                conn,
                peer_id=2000000002,
                from_id=888,
                text="5000",
            )
            conn.commit()

            pending_reply_count = int(conn.execute("SELECT COUNT(*) AS c FROM pending_replies").fetchone()["c"])
            booking = conn.execute(
                "SELECT deposit_amount, assigned_table_number FROM bookings WHERE id = 2"
            ).fetchone()
            inbound_rows = conn.execute(
                """
                SELECT processing_status, payload_json
                FROM bot_inbound_events
                WHERE event_type = ?
                ORDER BY id ASC
                """,
                (VK_PENDING_EVENT_TYPE,),
            ).fetchall()
            outbox_rows = conn.execute(
                """
                SELECT message_type, delivery_status
                FROM bot_outbox
                WHERE platform = 'vk' AND bot_scope = 'hostess'
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            outbox_dispatcher._dispatch_vk = original_dispatch_vk
            conn.close()

        self.assertTrue(started)
        self.assertTrue(handled)
        self.assertEqual(pending_reply_count, 0)
        self.assertEqual(int(booking["deposit_amount"]), 5000)
        self.assertIsNone(booking["assigned_table_number"])
        self.assertEqual(len(inbound_rows), 2)
        self.assertEqual(inbound_rows[0]["processing_status"], "processed")
        self.assertIn('"mode": "set_deposit"', inbound_rows[0]["payload_json"])
        self.assertEqual(inbound_rows[1]["processing_status"], "pending")
        self.assertIn('"mode": "assign_table"', inbound_rows[1]["payload_json"])
        self.assertEqual(len(outbox_rows), 2)
        self.assertTrue(all(row["message_type"] == VK_PROMPT_OUTBOX_TYPE for row in outbox_rows))
        self.assertTrue(all(row["delivery_status"] == "sent" for row in outbox_rows))


if __name__ == "__main__":
    unittest.main()
