import sqlite3
import unittest

import outbox_dispatcher
from outbox_dispatcher import dispatch_pending_outbox


def _connect():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE bot_outbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          reservation_id INTEGER,
          platform TEXT NOT NULL,
          bot_scope TEXT NOT NULL,
          target_peer_id INTEGER,
          target_external_id TEXT,
          message_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          delivery_status TEXT NOT NULL DEFAULT 'new',
          attempts INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          sent_at TEXT
        )
        """
    )
    return conn


class CrmOutboxTests(unittest.TestCase):
    def test_http_outbox_marks_sent(self):
        conn = _connect()
        conn.execute(
            """
            INSERT INTO bot_outbox (
                platform, bot_scope, target_external_id, message_type, payload_json
            )
            VALUES ('http', 'crm_sync', 'https://crm.example/api/events', 'crm_booking:BOOKING_UPSERT', '{"ok": true}')
            """
        )
        original = outbox_dispatcher._dispatch_http_post
        outbox_dispatcher._dispatch_http_post = lambda target, payload: "200"
        try:
            result = dispatch_pending_outbox(conn, platform="http", bot_scope="crm_sync")
        finally:
            outbox_dispatcher._dispatch_http_post = original

        row = conn.execute("SELECT delivery_status, attempts, last_error FROM bot_outbox").fetchone()
        self.assertEqual(result["sent"], 1)
        self.assertEqual(row["delivery_status"], "sent")
        self.assertEqual(row["attempts"], 1)
        self.assertIsNone(row["last_error"])

    def test_http_outbox_dead_letters_after_max_attempts(self):
        conn = _connect()
        conn.execute(
            """
            INSERT INTO bot_outbox (
                platform, bot_scope, target_external_id, message_type, payload_json, attempts
            )
            VALUES ('http', 'crm_sync', 'https://crm.example/api/events', 'crm_booking:BOOKING_UPSERT', '{"ok": true}', 1)
            """
        )
        original = outbox_dispatcher._dispatch_http_post

        def _fail(_target, _payload):
            raise RuntimeError("crm_down")

        outbox_dispatcher._dispatch_http_post = _fail
        try:
            result = dispatch_pending_outbox(conn, platform="http", bot_scope="crm_sync", max_attempts=2)
        finally:
            outbox_dispatcher._dispatch_http_post = original

        row = conn.execute("SELECT delivery_status, attempts, last_error FROM bot_outbox").fetchone()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(row["delivery_status"], "dead_letter")
        self.assertEqual(row["attempts"], 2)
        self.assertIn("crm_down", row["last_error"])


if __name__ == "__main__":
    unittest.main()
