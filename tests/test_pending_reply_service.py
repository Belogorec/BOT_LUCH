import os
import sqlite3
import tempfile
import unittest

from db import init_schema
from pending_reply_service import (
    delete_expired_pending_replies,
    delete_superseded_pending_replies,
    replace_pending_reply,
)


class PendingReplyServiceTests(unittest.TestCase):
    def setUp(self):
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
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def _connect(self):
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def test_replace_pending_reply_keeps_single_active_row_per_scope(self):
        conn = self._connect()
        try:
            first_id = replace_pending_reply(
                conn,
                kind="booking_dialog",
                booking_id=0,
                payload_text='{"state":"one"}',
                chat_id="chat-1",
                actor_tg_id="actor-1",
                prompt_message_id="prompt-1",
                expires_at="2099-01-01T00:00:00",
            )
            second_id = replace_pending_reply(
                conn,
                kind="booking_dialog",
                booking_id=0,
                payload_text='{"state":"two"}',
                chat_id="chat-1",
                actor_tg_id="actor-1",
                prompt_message_id="prompt-2",
                expires_at="2099-01-01T00:00:00",
            )
            conn.commit()

            rows = conn.execute(
                """
                SELECT id, phone_e164, prompt_message_id
                FROM pending_replies
                WHERE kind='booking_dialog' AND chat_id='chat-1' AND actor_tg_id='actor-1'
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            conn.close()

        self.assertNotEqual(first_id, second_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], second_id)
        self.assertEqual(rows[0]["prompt_message_id"], "prompt-2")

    def test_delete_expired_pending_replies_removes_invalid_or_stale_rows(self):
        conn = self._connect()
        try:
            conn.executemany(
                """
                INSERT INTO pending_replies (
                    kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("guest_note", 1, "+79000000001", "chat-2", "actor-2", "prompt-a", "2020-01-01T00:00:00"),
                    ("guest_note", 1, "+79000000001", "chat-2", "actor-2", "prompt-b", "bad-date"),
                    ("guest_note", 1, "+79000000001", "chat-2", "actor-2", "prompt-c", "2099-01-01T00:00:00"),
                ],
            )

            deleted = delete_expired_pending_replies(conn, kind="guest_note", chat_id="chat-2", actor_tg_id="actor-2")
            conn.commit()

            rows = conn.execute(
                """
                SELECT prompt_message_id
                FROM pending_replies
                WHERE kind='guest_note' AND chat_id='chat-2' AND actor_tg_id='actor-2'
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(deleted, 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["prompt_message_id"], "prompt-c")

    def test_delete_superseded_pending_replies_keeps_latest_per_scope(self):
        conn = self._connect()
        try:
            conn.executemany(
                """
                INSERT INTO pending_replies (
                    kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("lineup_upload", 0, "", "chat-3", "actor-3", "prompt-old", "2099-01-01T00:00:00"),
                    ("lineup_upload", 0, "", "chat-3", "actor-3", "prompt-new", "2099-01-01T00:00:00"),
                    ("lineup_upload", 0, "", "chat-4", "actor-4", "prompt-keep", "2099-01-01T00:00:00"),
                ],
            )

            deleted = delete_superseded_pending_replies(conn)
            conn.commit()

            rows = conn.execute(
                """
                SELECT chat_id, actor_tg_id, prompt_message_id
                FROM pending_replies
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(deleted, 1)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["prompt_message_id"], "prompt-new")
        self.assertEqual(rows[1]["prompt_message_id"], "prompt-keep")


if __name__ == "__main__":
    unittest.main()
