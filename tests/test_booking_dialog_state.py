import os
import sqlite3
import tempfile
import unittest

from booking_dialog import (
    BOOKING_DIALOG_EVENT_TYPE,
    BOOKING_QUESTION_EVENT_TYPE,
    STATE_AWAITING_DATE,
    clear_dialog_state,
    get_dialog_state,
    save_booking_question,
    save_dialog_state,
)
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations


class BookingDialogStateTests(unittest.TestCase):
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

    def test_save_and_get_dialog_state_use_canonical_inbound_events(self):
        conn = self._connect()
        try:
            save_dialog_state(
                conn,
                chat_id="chat-1",
                user_id="user-1",
                state=STATE_AWAITING_DATE,
                data={"guest_name": "Анна", "guest_phone": "+79000000301"},
                prompt_message_id="prompt-1",
            )
            conn.commit()

            state = get_dialog_state(conn, "chat-1", "user-1", prompt_message_id="prompt-1")
            pending_reply_count = int(conn.execute("SELECT COUNT(*) AS c FROM pending_replies").fetchone()["c"])
            inbound = conn.execute(
                """
                SELECT event_type, processing_status, payload_json
                FROM bot_inbound_events
                WHERE event_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (BOOKING_DIALOG_EVENT_TYPE,),
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(pending_reply_count, 0)
        self.assertEqual(state[0], STATE_AWAITING_DATE)
        self.assertEqual(state[1]["guest_name"], "Анна")
        self.assertEqual(state[1]["guest_phone"], "+79000000301")
        self.assertEqual(inbound["event_type"], BOOKING_DIALOG_EVENT_TYPE)
        self.assertEqual(inbound["processing_status"], "pending")
        self.assertIn('"prompt_message_id": "prompt-1"', inbound["payload_json"])

    def test_clear_dialog_state_marks_canonical_state_processed(self):
        conn = self._connect()
        try:
            save_dialog_state(
                conn,
                chat_id="chat-2",
                user_id="user-2",
                state=STATE_AWAITING_DATE,
                data={"guest_name": "Иван"},
                prompt_message_id="prompt-2",
            )
            clear_dialog_state(conn, "chat-2", "user-2")
            conn.commit()

            state = get_dialog_state(conn, "chat-2", "user-2")
            inbound = conn.execute(
                """
                SELECT processing_status
                FROM bot_inbound_events
                WHERE event_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (BOOKING_DIALOG_EVENT_TYPE,),
            ).fetchone()
        finally:
            conn.close()

        self.assertIsNone(state)
        self.assertEqual(inbound["processing_status"], "processed")

    def test_save_booking_question_uses_canonical_inbound_events(self):
        conn = self._connect()
        try:
            save_booking_question(
                conn,
                booking_id=77,
                phone_e164="+79000000377",
                question="Можно ли принести торт?",
                chat_id="chat-3",
                user_id="user-3",
            )
            conn.commit()

            pending_reply_count = int(conn.execute("SELECT COUNT(*) AS c FROM pending_replies").fetchone()["c"])
            inbound = conn.execute(
                """
                SELECT event_type, processing_status, payload_json
                FROM bot_inbound_events
                WHERE event_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (BOOKING_QUESTION_EVENT_TYPE,),
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(pending_reply_count, 0)
        self.assertEqual(inbound["event_type"], BOOKING_QUESTION_EVENT_TYPE)
        self.assertEqual(inbound["processing_status"], "pending")
        self.assertIn('"booking_id": 77', inbound["payload_json"])
        self.assertIn('Можно ли принести торт?', inbound["payload_json"])


if __name__ == "__main__":
    unittest.main()
