import os
import sqlite3
import tempfile
import unittest

import outbox_dispatcher
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations
from telegram_pending_prompt import TG_PROMPT_OUTBOX_TYPE, complete_pending_prompt, load_pending_prompt, start_pending_prompt


class TelegramPendingPromptTests(unittest.TestCase):
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

    def _seed_booking(self, conn, *, booking_id: int, phone: str = "+79000000201") -> None:
        conn.execute(
            """
            INSERT INTO bookings (
                id, formname, name, phone_e164, reservation_dt, guests_count, status
            )
            VALUES (?, 'telegram', 'Анна', ?, '2026-05-01 19:00', 2, 'WAITING')
            """,
            (booking_id, phone),
        )

    def test_start_pending_prompt_creates_canonical_event_and_outbox(self):
        conn = self._connect()
        original_dispatch = outbox_dispatcher._dispatch_telegram
        outbox_dispatcher._dispatch_telegram = lambda target, payload: "tg-msg-1"
        try:
            self._seed_booking(conn, booking_id=1)
            event_id = start_pending_prompt(
                conn,
                event_type="telegram_guest_note_prompt",
                chat_id="chat-1",
                actor_id="actor-1",
                booking_id=1,
                payload={"guest_phone_e164": "+79000000201"},
                prompt_text="Введите комментарий",
                reply_markup={"force_reply": True, "selective": True},
            )
            conn.commit()

            pending_reply_count = int(conn.execute("SELECT COUNT(*) AS c FROM pending_replies").fetchone()["c"])
            inbound = conn.execute(
                """
                SELECT event_type, processing_status, reservation_id, payload_json
                FROM bot_inbound_events
                WHERE id = ?
                """,
                (event_id,),
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
            outbox_dispatcher._dispatch_telegram = original_dispatch
            conn.close()

        self.assertEqual(pending_reply_count, 0)
        self.assertIsNotNone(reservation)
        self.assertEqual(inbound["event_type"], "telegram_guest_note_prompt")
        self.assertEqual(inbound["processing_status"], "pending")
        self.assertEqual(int(inbound["reservation_id"]), int(reservation["id"]))
        self.assertIn('"guest_phone_e164": "+79000000201"', inbound["payload_json"])
        self.assertIn('"prompt_message_id": "tg-msg-1"', inbound["payload_json"])
        self.assertEqual(outbox["platform"], "telegram")
        self.assertEqual(outbox["bot_scope"], "hostess")
        self.assertEqual(outbox["message_type"], TG_PROMPT_OUTBOX_TYPE)
        self.assertEqual(outbox["target_external_id"], "chat-1")
        self.assertEqual(outbox["delivery_status"], "sent")

    def test_load_and_complete_pending_prompt_uses_canonical_state(self):
        conn = self._connect()
        original_dispatch = outbox_dispatcher._dispatch_telegram
        outbox_dispatcher._dispatch_telegram = lambda target, payload: "tg-msg-2"
        try:
            self._seed_booking(conn, booking_id=2, phone="+79000000202")
            event_id = start_pending_prompt(
                conn,
                event_type="telegram_guest_note_prompt",
                chat_id="chat-2",
                actor_id="actor-2",
                booking_id=2,
                payload={"guest_phone_e164": "+79000000202"},
                prompt_text="Введите комментарий",
            )
            row, payload = load_pending_prompt(
                conn,
                event_type="telegram_guest_note_prompt",
                chat_id="chat-2",
                actor_id="actor-2",
            )
            complete_pending_prompt(conn, int(event_id))
            conn.commit()

            processed = conn.execute(
                """
                SELECT processing_status
                FROM bot_inbound_events
                WHERE id = ?
                """,
                (event_id,),
            ).fetchone()
        finally:
            outbox_dispatcher._dispatch_telegram = original_dispatch
            conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(int(row["id"]), int(event_id))
        self.assertEqual(int(payload["booking_id"]), 2)
        self.assertEqual(payload["guest_phone_e164"], "+79000000202")
        self.assertEqual(processed["processing_status"], "processed")

    def test_start_pending_prompt_reuses_non_legacy_external_ref_reservation(self):
        conn = self._connect()
        original_dispatch = outbox_dispatcher._dispatch_telegram
        outbox_dispatcher._dispatch_telegram = lambda target, payload: "tg-msg-3"
        try:
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('telegram_miniapp', '77', 'Анна', '+79000000277', '2026-05-01T19:00', 2, 'pending')
                """
            )
            reservation_id = int(conn.execute("SELECT id FROM reservations").fetchone()["id"])

            event_id = start_pending_prompt(
                conn,
                event_type="telegram_guest_note_prompt",
                chat_id="chat-77",
                actor_id="actor-77",
                booking_id=77,
                payload={"guest_phone_e164": "+79000000277"},
                prompt_text="Введите комментарий",
            )
            conn.commit()

            inbound = conn.execute(
                """
                SELECT reservation_id
                FROM bot_inbound_events
                WHERE id = ?
                """,
                (event_id,),
            ).fetchone()
        finally:
            outbox_dispatcher._dispatch_telegram = original_dispatch
            conn.close()

        self.assertEqual(int(inbound["reservation_id"]), reservation_id)


if __name__ == "__main__":
    unittest.main()
