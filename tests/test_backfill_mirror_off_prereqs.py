import os
import sqlite3
import tempfile
import unittest

from backfill_mirror_off_prereqs import (
    backfill_bot_peers_from_vk_staff,
    backfill_channel_binding_tokens,
    backfill_contact_channels_from_guest_bindings,
    backfill_public_tokens,
    cleanup_expired_pending_replies,
    cleanup_superseded_pending_replies,
    deactivate_mirrored_guest_channel_binding_rows,
    deactivate_mirrored_vk_staff_rows,
)
from contact_schema import run_contact_schema_migrations
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations
from vk_staff_notify import fetch_active_vk_staff_peers, upsert_vk_staff_peer


class BackfillMirrorOffPrereqsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        try:
            init_schema(conn)
            run_core_schema_migrations(conn)
            run_integration_schema_migrations(conn)
            run_contact_schema_migrations(conn)
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

    def _seed_booking(self, conn, *, booking_id: int, phone: str) -> None:
        conn.execute(
            """
            INSERT INTO bookings (
                id, name, phone_e164, reservation_dt, guests_count, status, reservation_token
            )
            VALUES (?, 'Анна', ?, '2026-05-01 19:00', 2, 'CONFIRMED', ?)
            """,
            (booking_id, phone, f"seed-token-{booking_id}"),
        )

    def test_backfill_public_tokens_creates_missing_canonical_rows(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO bookings (
                    id, name, phone_e164, reservation_dt, guests_count, status, reservation_token
                )
                VALUES (1, 'Анна', '+79000000001', '2026-05-01 19:00', 2, 'CONFIRMED', 'token-1')
                """
            )
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('legacy_booking', '1', 'Анна', '+79000000001', '2026-05-01T19:00', 2, 'confirmed')
                """
            )

            result = backfill_public_tokens(conn)
            conn.commit()

            row = conn.execute(
                """
                SELECT public_token, token_kind, status
                FROM public_reservation_tokens
                WHERE public_token='token-1'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(row["token_kind"], "guest_access")
        self.assertEqual(row["status"], "active")

    def test_backfill_bot_peers_creates_missing_vk_mapping(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO vk_staff_peers (
                    peer_id, peer_external_id, bot_key, from_id, is_active, role_hint, last_message_text
                )
                VALUES ('hostess:2000000001', '2000000001', 'hostess', '123', 1, 'hostess', 'Hostess chat')
                """
            )

            result = backfill_bot_peers_from_vk_staff(conn, active_only=True)
            conn.commit()

            row = conn.execute(
                """
                SELECT platform, bot_scope, external_peer_id, external_user_id, is_active
                FROM bot_peers
                WHERE platform='vk' AND bot_scope='hostess' AND external_peer_id='2000000001'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(row["external_user_id"], "123")
        self.assertEqual(row["is_active"], 1)

    def test_backfill_channel_binding_tokens_creates_canonical_rows_for_active_legacy_tokens(self):
        conn = self._connect()
        try:
            self._seed_booking(conn, booking_id=11, phone="+79000000021")
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('legacy_booking', '11', 'Анна', '+79000000021', '2026-05-01T19:00', 2, 'confirmed')
                """
            )
            core_reservation_id = int(conn.execute("SELECT id FROM reservations WHERE external_ref='11'").fetchone()["id"])
            conn.execute(
                """
                INSERT INTO guest_binding_tokens (
                    token_hash, reservation_id, guest_phone_e164, channel_type, status, expires_at, created_at, updated_at
                )
                VALUES ('hash-11', 11, '+79000000021', 'telegram', 'active', datetime('now', '+30 minutes'), datetime('now'), datetime('now'))
                """
            )

            result = backfill_channel_binding_tokens(conn, active_only=True)
            conn.commit()

            row = conn.execute(
                """
                SELECT reservation_id, token_hash, guest_phone_e164, channel_type, status
                FROM channel_binding_tokens
                WHERE token_hash='hash-11'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(result["skipped"], 0)
        self.assertEqual(int(row["reservation_id"]), core_reservation_id)
        self.assertEqual(row["guest_phone_e164"], "+79000000021")
        self.assertEqual(row["channel_type"], "telegram")
        self.assertEqual(row["status"], "active")

    def test_backfill_channel_binding_tokens_syncs_missing_canonical_reservation(self):
        conn = self._connect()
        try:
            self._seed_booking(conn, booking_id=12, phone="+79000000022")
            conn.execute(
                """
                INSERT INTO guest_binding_tokens (
                    token_hash, reservation_id, guest_phone_e164, channel_type, status, expires_at, created_at, updated_at
                )
                VALUES ('hash-12', 12, '+79000000022', 'vk', 'active', datetime('now', '+30 minutes'), datetime('now'), datetime('now'))
                """
            )

            result = backfill_channel_binding_tokens(conn, active_only=True)
            conn.commit()

            reservation = conn.execute(
                """
                SELECT id
                FROM reservations
                WHERE source='legacy_booking' AND external_ref='12'
                LIMIT 1
                """
            ).fetchone()
            row = conn.execute(
                """
                SELECT reservation_id, channel_type, status
                FROM channel_binding_tokens
                WHERE token_hash='hash-12'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(result["skipped"], 0)
        self.assertIsNotNone(reservation)
        self.assertEqual(int(row["reservation_id"]), int(reservation["id"]))
        self.assertEqual(row["channel_type"], "vk")
        self.assertEqual(row["status"], "active")

    def test_backfill_contact_channels_from_guest_bindings_creates_canonical_mapping(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO guest_channel_bindings (
                    guest_phone_e164, channel_type, external_user_id, external_username, external_display_name, status
                )
                VALUES ('+79000000031', 'telegram', 'tg-user-31', 'anna31', 'Anna 31', 'active')
                """
            )

            result = backfill_contact_channels_from_guest_bindings(conn, active_only=True)
            conn.commit()

            contact = conn.execute(
                """
                SELECT phone_e164, preferred_channel, service_notifications_enabled
                FROM contacts
                WHERE phone_e164 = '+79000000031'
                """
            ).fetchone()
            channel = conn.execute(
                """
                SELECT platform, external_user_id, username, display_name, status
                FROM contact_channels
                WHERE platform = 'telegram' AND external_user_id = 'tg-user-31'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(result["skipped"], 0)
        self.assertEqual(contact["phone_e164"], "+79000000031")
        self.assertEqual(contact["preferred_channel"], "telegram")
        self.assertEqual(int(contact["service_notifications_enabled"]), 1)
        self.assertEqual(channel["platform"], "telegram")
        self.assertEqual(channel["external_user_id"], "tg-user-31")
        self.assertEqual(channel["username"], "anna31")
        self.assertEqual(channel["display_name"], "Anna 31")
        self.assertEqual(channel["status"], "active")

    def test_deactivate_mirrored_guest_channel_binding_rows_marks_legacy_row_inactive(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO contacts (
                    phone_e164, display_name, preferred_channel, source
                )
                VALUES ('+79000000032', 'Иван', 'telegram', 'test')
                """
            )
            contact_id = int(
                conn.execute("SELECT id FROM contacts WHERE phone_e164 = '+79000000032'").fetchone()["id"]
            )
            conn.execute(
                """
                INSERT INTO contact_channels (
                    contact_id, platform, external_user_id, display_name, status
                )
                VALUES (?, 'telegram', 'tg-user-32', 'Иван', 'active')
                """,
                (contact_id,),
            )
            conn.execute(
                """
                INSERT INTO guest_channel_bindings (
                    guest_phone_e164, channel_type, external_user_id, external_display_name, status
                )
                VALUES ('+79000000032', 'telegram', 'tg-user-32', 'Иван', 'active')
                """
            )

            result = deactivate_mirrored_guest_channel_binding_rows(conn)
            conn.commit()

            row = conn.execute(
                """
                SELECT status
                FROM guest_channel_bindings
                WHERE guest_phone_e164 = '+79000000032' AND external_user_id = 'tg-user-32'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(row["status"], "inactive")

    def test_cleanup_expired_pending_replies_removes_historical_rows(self):
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
                    ("booking_dialog", 1, "{}", "chat-1", "actor-1", "prompt-1", "2020-01-01T00:00:00"),
                    ("booking_dialog", 2, "{}", "chat-2", "actor-2", "prompt-2", "2099-01-01T00:00:00"),
                ],
            )

            result = cleanup_expired_pending_replies(conn)
            conn.commit()

            rows = conn.execute(
                """
                SELECT kind, chat_id, actor_tg_id
                FROM pending_replies
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["chat_id"], "chat-2")

    def test_cleanup_superseded_pending_replies_keeps_latest_active_row(self):
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
                    ("table_flow", 1, '{"mode":"assign_table"}', "chat-3", "actor-3", "prompt-old", "2099-01-01T00:00:00"),
                    ("table_flow", 1, '{"mode":"assign_table"}', "chat-3", "actor-3", "prompt-new", "2099-01-01T00:00:00"),
                    ("table_flow", 2, '{"mode":"assign_table"}', "chat-4", "actor-4", "prompt-other", "2099-01-01T00:00:00"),
                ],
            )

            result = cleanup_superseded_pending_replies(conn)
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

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["prompt_message_id"], "prompt-new")
        self.assertEqual(rows[1]["prompt_message_id"], "prompt-other")

    def test_upsert_vk_staff_peer_writes_only_canonical_bot_peer(self):
        conn = self._connect()
        try:
            created = upsert_vk_staff_peer(
                conn,
                bot_key="hostess",
                role_hint="hostess",
                peer_id="2000000002",
                from_id="777",
                message_text="Привет",
            )
            conn.commit()

            row = conn.execute(
                """
                SELECT platform, bot_scope, external_peer_id, external_user_id, is_active
                FROM bot_peers
                WHERE platform='vk' AND bot_scope='hostess' AND external_peer_id='2000000002'
                """
            ).fetchone()
            legacy_count = int(
                conn.execute("SELECT COUNT(*) AS c FROM vk_staff_peers WHERE peer_external_id='2000000002'").fetchone()["c"]
            )
        finally:
            conn.close()

        self.assertTrue(created)
        self.assertIsNotNone(row)
        self.assertEqual(row["external_user_id"], "777")
        self.assertEqual(row["is_active"], 1)
        self.assertEqual(legacy_count, 0)

    def test_fetch_active_vk_staff_peers_prefers_canonical_bot_peers(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO vk_staff_peers (
                    peer_id, peer_external_id, bot_key, is_active, role_hint, last_message_text
                )
                VALUES ('hostess:2000000003', '2000000003', 'hostess', 1, 'hostess', 'legacy row')
                """
            )
            conn.execute(
                """
                INSERT INTO bot_peers (
                    platform, bot_scope, external_peer_id, external_user_id, display_name, is_active
                )
                VALUES ('vk', 'hostess', '2000000003', '900', 'canonical row', 1)
                """
            )
            conn.commit()

            rows = fetch_active_vk_staff_peers(conn, bot_key="hostess")
        finally:
            conn.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["peer_id"], "2000000003")
        self.assertEqual(rows[0]["from_id"], "900")
        self.assertEqual(rows[0]["last_message_text"], "canonical row")

    def test_fetch_active_vk_staff_peers_ignores_legacy_rows_without_canonical_peer(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO vk_staff_peers (
                    peer_id, peer_external_id, bot_key, is_active, role_hint, last_message_text
                )
                VALUES ('hostess:2000000099', '2000000099', 'hostess', 1, 'hostess', 'legacy only')
                """
            )
            conn.commit()

            rows = fetch_active_vk_staff_peers(conn, bot_key="hostess")
        finally:
            conn.close()

        self.assertEqual(rows, [])

    def test_deactivate_mirrored_vk_staff_rows_marks_legacy_row_inactive(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO vk_staff_peers (
                    peer_id, peer_external_id, bot_key, is_active, role_hint
                )
                VALUES ('hostess:2000000004', '2000000004', 'hostess', 1, 'hostess')
                """
            )
            conn.execute(
                """
                INSERT INTO bot_peers (
                    platform, bot_scope, external_peer_id, is_active
                )
                VALUES ('vk', 'hostess', '2000000004', 1)
                """
            )

            result = deactivate_mirrored_vk_staff_rows(conn)
            conn.commit()

            row = conn.execute(
                "SELECT is_active FROM vk_staff_peers WHERE peer_id='hostess:2000000004'"
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(result["found"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(row["is_active"], 0)


if __name__ == "__main__":
    unittest.main()
