import json
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta

try:
    import tg_handlers

    _FAKE_REQUEST = None
except ModuleNotFoundError as exc:
    if exc.name != "flask":
        raise

    class _FakeRequest:
        headers = {}
        _payload = {}

        def get_json(self, silent=False):
            return self._payload

    _FAKE_REQUEST = _FakeRequest()
    _fake_flask = types.ModuleType("flask")
    _fake_flask.request = _FAKE_REQUEST
    _fake_flask.g = types.SimpleNamespace()
    _fake_flask.has_request_context = lambda: False

    def _fake_abort(code):
        raise RuntimeError(f"abort:{code}")

    _fake_flask.abort = _fake_abort
    sys.modules["flask"] = _fake_flask
    sys.modules.pop("tg_handlers", None)
    import tg_handlers

import booking_service
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations


class TelegramAuthoritativeFlowTests(unittest.TestCase):
    def setUp(self):
        self.prev_core_only = booking_service.CORE_ONLY_MODE
        self.prev_legacy_mirror = booking_service.LEGACY_MIRROR_ENABLED
        self.prev_tg_authoritative = tg_handlers.CRM_AUTHORITATIVE
        self.prev_tg_secret = tg_handlers.TG_WEBHOOK_SECRET
        booking_service.CORE_ONLY_MODE = False
        booking_service.LEGACY_MIRROR_ENABLED = True
        tg_handlers.CRM_AUTHORITATIVE = True
        tg_handlers.TG_WEBHOOK_SECRET = ""
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
        tg_handlers.CRM_AUTHORITATIVE = self.prev_tg_authoritative
        tg_handlers.TG_WEBHOOK_SECRET = self.prev_tg_secret
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def _connect(self):
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _seed_booking(self, conn, *, booking_id: int, status: str = "WAITING") -> None:
        conn.execute(
            """
            INSERT INTO bookings (
                id, formname, name, phone_e164, phone_raw, reservation_date, reservation_time,
                reservation_dt, guests_count, status
            )
            VALUES (?, 'telegram', 'Анна', '+79000000101', '+79000000101', '2026-05-01',
                    '19:00', '2026-05-01 19:00', 2, ?)
            """,
            (booking_id, status),
        )

    def _seed_core_reservation(self, conn, *, booking_id: int, status: str = "pending") -> int:
        conn.execute(
            """
            INSERT INTO reservations (
                source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
            )
            VALUES ('legacy_booking', ?, 'Анна', '+79000000101', '2026-05-01T19:00', 2, ?)
            """,
            (str(booking_id), status),
        )
        return int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    def _insert_table_prompt(self, conn, *, booking_id: int, reservation_id, chat_id: str, actor_id: str) -> int:
        payload = {
            "mode": "assign_table",
            "booking_id": booking_id,
            "expires_at": (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds"),
        }
        conn.execute(
            """
            INSERT INTO bot_inbound_events (
                platform, bot_scope, event_type, actor_external_id, peer_external_id,
                reservation_id, payload_json, processing_status
            )
            VALUES ('telegram', 'hostess', ?, ?, ?, ?, ?, 'pending')
            """,
            (
                tg_handlers.TG_TABLE_FLOW_EVENT_TYPE,
                str(actor_id),
                str(chat_id),
                int(reservation_id) if reservation_id else None,
                json.dumps(payload),
            ),
        )
        return int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    def _run_update(self, update):
        old_connect = tg_handlers.connect
        tg_handlers.connect = self._connect
        if _FAKE_REQUEST is not None:
            _FAKE_REQUEST.headers = {}
            _FAKE_REQUEST._payload = update
            try:
                return tg_handlers.tg_webhook_impl()
            finally:
                tg_handlers.connect = old_connect

        from flask import Flask

        app = Flask(__name__)
        try:
            with app.test_request_context("/telegram", method="POST", json=update):
                return tg_handlers.tg_webhook_impl()
        finally:
            tg_handlers.connect = old_connect

    def test_confirm_callback_sends_crm_command_without_local_status_mutation(self):
        conn = self._connect()
        crm_calls = []
        answers = []
        original_status = tg_handlers.crm_commands.reservation_status
        original_sync_card = tg_handlers._sync_admin_booking_card
        original_answer = tg_handlers.safe_answer_callback
        tg_handlers._sync_admin_booking_card = lambda *args, **kwargs: None
        tg_handlers.safe_answer_callback = lambda callback_id, text="": answers.append((callback_id, text))

        def _fake_status(reservation_id, *, status, event_id, actor):
            crm_calls.append({"reservation_id": reservation_id, "status": status, "event_id": event_id, "actor": actor})
            return {"accepted": True, "ok": True}

        tg_handlers.crm_commands.reservation_status = _fake_status
        try:
            self._seed_booking(conn, booking_id=1)
            reservation_id = self._seed_core_reservation(conn, booking_id=1)
            conn.commit()
        finally:
            conn.close()

        try:
            result = self._run_update(
                {
                    "update_id": 1001,
                    "callback_query": {
                        "id": "cq-confirm-1",
                        "data": "b:1:booking:confirm",
                        "from": {"id": 777, "username": "hostess"},
                        "message": {"message_id": 55, "chat": {"id": -100}},
                    },
                }
            )
            conn = self._connect()
            booking = conn.execute("SELECT status FROM bookings WHERE id = 1").fetchone()
        finally:
            tg_handlers.crm_commands.reservation_status = original_status
            tg_handlers._sync_admin_booking_card = original_sync_card
            tg_handlers.safe_answer_callback = original_answer
            conn.close()

        self.assertEqual(result, {"ok": True})
        self.assertEqual(booking["status"], "WAITING")
        self.assertEqual(len(crm_calls), 1)
        self.assertEqual(crm_calls[0]["reservation_id"], reservation_id)
        self.assertEqual(crm_calls[0]["status"], "confirmed")
        self.assertIn(("cq-confirm-1", "Подтверждено"), answers)

    def test_confirm_callback_without_local_booking_uses_crm_reservation_id_and_snapshot(self):
        crm_calls = []
        edits = []
        answers = []
        original_status = tg_handlers.crm_commands.reservation_status
        original_sync_card = tg_handlers._sync_admin_booking_card
        original_edit = tg_handlers.tg_edit_message
        original_answer = tg_handlers.safe_answer_callback
        tg_handlers._sync_admin_booking_card = lambda *args, **kwargs: None
        tg_handlers.tg_edit_message = lambda chat_id, message_id, text, reply_markup=None: edits.append(
            {"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup}
        )
        tg_handlers.safe_answer_callback = lambda callback_id, text="": answers.append((callback_id, text))

        def _fake_status(reservation_id, *, status, event_id, actor):
            crm_calls.append({"reservation_id": reservation_id, "status": status, "event_id": event_id, "actor": actor})
            return {
                "accepted": True,
                "ok": True,
                "reservation": {
                    "reservation_id": reservation_id,
                    "booking_id": reservation_id,
                    "status": status,
                    "guest_name": "Анна",
                    "guest_phone": "+79000000101",
                    "reservation_date": "2026-05-01",
                    "reservation_time": "19:00",
                    "party_size": 2,
                },
            }

        tg_handlers.crm_commands.reservation_status = _fake_status
        conn = None
        try:
            result = self._run_update(
                {
                    "update_id": 1010,
                    "callback_query": {
                        "id": "cq-confirm-no-local",
                        "data": "b:55:booking:confirm",
                        "from": {"id": 777, "username": "hostess"},
                        "message": {"message_id": 88, "chat": {"id": -100}},
                    },
                }
            )
            conn = self._connect()
            local_count = int(conn.execute("SELECT COUNT(*) AS c FROM bookings").fetchone()["c"])
        finally:
            tg_handlers.crm_commands.reservation_status = original_status
            tg_handlers._sync_admin_booking_card = original_sync_card
            tg_handlers.tg_edit_message = original_edit
            tg_handlers.safe_answer_callback = original_answer
            if conn is not None:
                conn.close()

        self.assertEqual(result, {"ok": True})
        self.assertEqual(local_count, 0)
        self.assertEqual(crm_calls[0]["reservation_id"], 55)
        self.assertEqual(crm_calls[0]["status"], "confirmed")
        self.assertEqual(edits[0]["message_id"], "88")
        self.assertIn("<b>Статус:</b> CONFIRMED", edits[0]["text"])
        self.assertIn(("cq-confirm-no-local", "Подтверждено"), answers)

    def test_assign_table_rejection_keeps_local_assignment_and_prompt_pending(self):
        conn = self._connect()
        sent_messages = []
        original_assign = tg_handlers.crm_commands.assign_table
        original_send = tg_handlers.tg_send_message
        original_sync_card = tg_handlers._sync_admin_booking_card
        original_notify = tg_handlers.notify_waiters_about_deposit_booking
        tg_handlers.crm_commands.assign_table = lambda *args, **kwargs: {
            "accepted": False,
            "ok": False,
            "error": "table_time_conflict",
        }
        tg_handlers.tg_send_message = lambda chat_id, text, reply_markup=None: sent_messages.append((chat_id, text, reply_markup))
        tg_handlers._sync_admin_booking_card = lambda *args, **kwargs: None
        tg_handlers.notify_waiters_about_deposit_booking = lambda *args, **kwargs: None
        try:
            self._seed_booking(conn, booking_id=2)
            reservation_id = self._seed_core_reservation(conn, booking_id=2, status="confirmed")
            prompt_id = self._insert_table_prompt(
                conn,
                booking_id=2,
                reservation_id=reservation_id,
                chat_id="-100",
                actor_id="777",
            )
            conn.commit()
        finally:
            conn.close()

        try:
            result = self._run_update(
                {
                    "update_id": 1002,
                    "message": {
                        "message_id": 56,
                        "text": "221",
                        "chat": {"id": -100},
                        "from": {"id": 777, "username": "hostess"},
                    },
                }
            )
            conn = self._connect()
            booking = conn.execute("SELECT assigned_table_number FROM bookings WHERE id = 2").fetchone()
            prompt = conn.execute("SELECT processing_status FROM bot_inbound_events WHERE id = ?", (prompt_id,)).fetchone()
        finally:
            tg_handlers.crm_commands.assign_table = original_assign
            tg_handlers.tg_send_message = original_send
            tg_handlers._sync_admin_booking_card = original_sync_card
            tg_handlers.notify_waiters_about_deposit_booking = original_notify
            conn.close()

        self.assertEqual(result, {"ok": True})
        self.assertIsNone(booking["assigned_table_number"])
        self.assertEqual(prompt["processing_status"], "pending")
        self.assertTrue(any("стол занят" in text.lower() for _chat, text, _markup in sent_messages))

    def test_assign_table_pending_without_local_booking_calls_crm(self):
        conn = self._connect()
        crm_calls = []
        sent_messages = []
        original_assign = tg_handlers.crm_commands.assign_table
        original_send = tg_handlers.tg_send_message
        original_sync_card = tg_handlers._sync_admin_booking_card
        tg_handlers.tg_send_message = lambda chat_id, text, reply_markup=None: sent_messages.append((chat_id, text, reply_markup))
        tg_handlers._sync_admin_booking_card = lambda *args, **kwargs: None

        def _fake_assign(reservation_id, *, table_number, guests_count, guest_name="", guest_phone="", event_id, actor):
            crm_calls.append(
                {
                    "reservation_id": reservation_id,
                    "table_number": table_number,
                    "guests_count": guests_count,
                    "guest_name": guest_name,
                    "guest_phone": guest_phone,
                }
            )
            return {
                "accepted": True,
                "ok": True,
                "reservation": {
                    "reservation_id": reservation_id,
                    "booking_id": reservation_id,
                    "status": "confirmed",
                    "guest_name": "Анна",
                    "reservation_date": "2026-05-01",
                    "reservation_time": "19:00",
                    "party_size": 2,
                    "table_number": table_number,
                },
            }

        tg_handlers.crm_commands.assign_table = _fake_assign
        try:
            prompt_id = self._insert_table_prompt(
                conn,
                booking_id=77,
                reservation_id=None,
                chat_id="-100",
                actor_id="777",
            )
            conn.commit()
        finally:
            conn.close()

        try:
            result = self._run_update(
                {
                    "update_id": 1011,
                    "message": {
                        "message_id": 89,
                        "text": "221",
                        "chat": {"id": -100},
                        "from": {"id": 777, "username": "hostess"},
                    },
                }
            )
            conn = self._connect()
            prompt = conn.execute("SELECT processing_status FROM bot_inbound_events WHERE id = ?", (prompt_id,)).fetchone()
            local_count = int(conn.execute("SELECT COUNT(*) AS c FROM bookings").fetchone()["c"])
        finally:
            tg_handlers.crm_commands.assign_table = original_assign
            tg_handlers.tg_send_message = original_send
            tg_handlers._sync_admin_booking_card = original_sync_card
            conn.close()

        self.assertEqual(result, {"ok": True})
        self.assertEqual(local_count, 0)
        self.assertEqual(prompt["processing_status"], "processed")
        self.assertEqual(crm_calls[0]["reservation_id"], 77)
        self.assertEqual(crm_calls[0]["table_number"], "221")
        self.assertEqual(crm_calls[0]["guests_count"], 0)
        self.assertTrue(any("назначен" in text.lower() for _chat, text, _markup in sent_messages))

    def test_booking_restriction_uses_reservation_command_without_local_block(self):
        conn = self._connect()
        crm_calls = []
        sent_messages = []
        original_restrict = tg_handlers.crm_commands.restrict_reservation_table
        original_send = tg_handlers.tg_send_message
        original_sync_card = tg_handlers._sync_admin_booking_card
        tg_handlers.tg_send_message = lambda chat_id, text, reply_markup=None: sent_messages.append((chat_id, text, reply_markup))
        tg_handlers._sync_admin_booking_card = lambda *args, **kwargs: None

        def _fake_restrict(reservation_id, *, table_number, restricted_until, event_id, actor, force_override=False, comment=""):
            crm_calls.append(
                {
                    "reservation_id": reservation_id,
                    "table_number": table_number,
                    "restricted_until": restricted_until,
                    "event_id": event_id,
                    "force_override": force_override,
                }
            )
            return {"accepted": True, "ok": True}

        tg_handlers.crm_commands.restrict_reservation_table = _fake_restrict
        try:
            self._seed_booking(conn, booking_id=3)
            reservation_id = self._seed_core_reservation(conn, booking_id=3, status="confirmed")
            prompt_id = self._insert_table_prompt(
                conn,
                booking_id=3,
                reservation_id=reservation_id,
                chat_id="-100",
                actor_id="777",
            )
            conn.execute(
                "UPDATE bot_inbound_events SET payload_json = ? WHERE id = ?",
                (
                    json.dumps(
                        {
                            "mode": "restrict_until",
                            "booking_id": 3,
                            "table_number": "221",
                            "force_override": True,
                            "expires_at": (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds"),
                        }
                    ),
                    prompt_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        try:
            result = self._run_update(
                {
                    "update_id": 1003,
                    "message": {
                        "message_id": 57,
                        "text": "3",
                        "chat": {"id": -100},
                        "from": {"id": 777, "username": "hostess"},
                    },
                }
            )
            conn = self._connect()
            prompt = conn.execute("SELECT processing_status FROM bot_inbound_events WHERE id = ?", (prompt_id,)).fetchone()
            block_count = int(conn.execute("SELECT COUNT(*) AS c FROM table_blocks").fetchone()["c"])
        finally:
            tg_handlers.crm_commands.restrict_reservation_table = original_restrict
            tg_handlers.tg_send_message = original_send
            tg_handlers._sync_admin_booking_card = original_sync_card
            conn.close()

        self.assertEqual(result, {"ok": True})
        self.assertEqual(prompt["processing_status"], "processed")
        self.assertEqual(block_count, 0)
        self.assertEqual(len(crm_calls), 1)
        self.assertEqual(crm_calls[0]["reservation_id"], reservation_id)
        self.assertEqual(crm_calls[0]["table_number"], "221")
        self.assertTrue(crm_calls[0]["force_override"])
        self.assertTrue(any("ограничен" in text.lower() for _chat, text, _markup in sent_messages))

    def test_sync_admin_card_prefers_crm_command_snapshot(self):
        conn = self._connect()
        edits = []
        original_edit = tg_handlers.tg_edit_message
        tg_handlers.tg_edit_message = lambda chat_id, message_id, text, reply_markup=None: edits.append(
            {"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup}
        )
        try:
            self._seed_booking(conn, booking_id=4, status="WAITING")
            reservation_id = self._seed_core_reservation(conn, booking_id=4, status="pending")
            conn.execute(
                """
                INSERT INTO bot_message_links (
                    reservation_id, platform, bot_scope, external_chat_id, external_message_id, message_kind
                )
                VALUES (?, 'telegram', 'hostess', '-100', '77', 'reservation_card')
                """,
                (reservation_id,),
            )
            conn.commit()
            tg_handlers._sync_admin_booking_card(
                conn,
                4,
                {
                    "reservation": {
                        "reservation_id": reservation_id,
                        "booking_id": 4,
                        "status": "confirmed",
                        "guest_name": "Анна",
                        "guest_phone": "+79000000101",
                        "reservation_date": "2026-05-01",
                        "reservation_time": "19:00",
                        "party_size": 2,
                        "table_number": "221",
                        "deposit_amount": 5000,
                        "deposit_comment": "предоплата",
                        "comment": "у окна",
                    }
                },
            )
            booking = conn.execute("SELECT status FROM bookings WHERE id = 4").fetchone()
        finally:
            tg_handlers.tg_edit_message = original_edit
            conn.close()

        self.assertEqual(booking["status"], "WAITING")
        self.assertEqual(len(edits), 1)
        self.assertEqual(edits[0]["chat_id"], "-100")
        self.assertEqual(edits[0]["message_id"], "77")
        self.assertIn("<b>Статус:</b> CONFIRMED", edits[0]["text"])
        self.assertIn("<b>Стол:</b> 221", edits[0]["text"])
        self.assertIn("<b>Депозит:</b> 5000", edits[0]["text"])

    def test_authoritative_start_does_not_register_client_user(self):
        sent_messages = []
        original_send = tg_handlers.tg_send_message
        tg_handlers.tg_send_message = lambda chat_id, text, reply_markup=None: sent_messages.append((chat_id, text, reply_markup))
        try:
            result = self._run_update(
                {
                    "update_id": 1004,
                    "message": {
                        "message_id": 58,
                        "text": "/start",
                        "chat": {"id": 777},
                        "from": {"id": 777, "username": "guest"},
                    },
                }
            )
            conn = self._connect()
            user_count = int(conn.execute("SELECT COUNT(*) AS c FROM tg_bot_users").fetchone()["c"])
        finally:
            tg_handlers.tg_send_message = original_send
            conn.close()

        self.assertEqual(result, {"ok": True})
        self.assertEqual(user_count, 0)
        self.assertTrue(any("рабочих уведомлений" in text for _chat, text, _markup in sent_messages))

    def test_authoritative_contact_share_does_not_store_phone(self):
        sent_messages = []
        original_send = tg_handlers.tg_send_message
        tg_handlers.tg_send_message = lambda chat_id, text, reply_markup=None: sent_messages.append((chat_id, text, reply_markup))
        try:
            result = self._run_update(
                {
                    "update_id": 1005,
                    "message": {
                        "message_id": 59,
                        "chat": {"id": 777},
                        "from": {"id": 777, "username": "guest"},
                        "contact": {"phone_number": "+79000000101", "first_name": "Анна"},
                    },
                }
            )
            conn = self._connect()
            user_count = int(conn.execute("SELECT COUNT(*) AS c FROM tg_bot_users").fetchone()["c"])
            guest_count = int(conn.execute("SELECT COUNT(*) AS c FROM guests").fetchone()["c"])
        finally:
            tg_handlers.tg_send_message = original_send
            conn.close()

        self.assertEqual(result, {"ok": True})
        self.assertEqual(user_count, 0)
        self.assertEqual(guest_count, 0)
        self.assertTrue(any("рабочих уведомлений" in text for _chat, text, _markup in sent_messages))


if __name__ == "__main__":
    unittest.main()
