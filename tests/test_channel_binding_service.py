import os
import sqlite3
import tempfile
import unittest

import channel_binding_service
from contact_schema import run_contact_schema_migrations
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations


class ChannelBindingServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.prev_guest_comm_enabled = channel_binding_service.GUEST_COMM_ENABLED
        channel_binding_service.GUEST_COMM_ENABLED = True

        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        try:
            init_schema(conn)
            run_core_schema_migrations(conn)
            run_integration_schema_migrations(conn)
            run_contact_schema_migrations(conn)
        finally:
            conn.close()

    def tearDown(self):
        channel_binding_service.GUEST_COMM_ENABLED = self.prev_guest_comm_enabled
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def _connect(self):
        conn = sqlite3.connect(self.tmp.name)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _seed_booking_pair(self, conn, *, booking_id: int = 1, phone: str = "+79000000011") -> int:
        conn.execute(
            """
            INSERT INTO bookings (
                id, name, phone_e164, reservation_dt, guests_count, status, reservation_token
            )
            VALUES (?, 'Анна', ?, '2026-05-01 19:00', 2, 'WAITING', 'pubtok-seed')
            """,
            (booking_id, phone),
        )
        conn.execute(
            """
            INSERT INTO reservations (
                source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
            )
            VALUES ('legacy_booking', ?, 'Анна', ?, '2026-05-01T19:00', 2, 'pending')
            """,
            (str(booking_id), phone),
        )
        return int(conn.execute("SELECT id FROM reservations").fetchone()["id"])

    def test_create_binding_token_writes_only_canonical_storage(self):
        conn = self._connect()
        try:
            core_reservation_id = self._seed_booking_pair(conn, booking_id=1)

            result = channel_binding_service.create_binding_token(
                conn,
                reservation_id=1,
                guest_phone_e164="+79000000011",
                channel_type="telegram",
            )

            canonical = conn.execute(
                """
                SELECT reservation_id, guest_phone_e164, channel_type, status
                FROM channel_binding_tokens
                WHERE id = ?
                """,
                (int(result["token_id"]),),
            ).fetchone()
            legacy = conn.execute("SELECT COUNT(*) AS c FROM guest_binding_tokens").fetchone()
        finally:
            conn.close()

        self.assertEqual(int(result["core_reservation_id"]), core_reservation_id)
        self.assertEqual(int(canonical["reservation_id"]), core_reservation_id)
        self.assertEqual(canonical["guest_phone_e164"], "+79000000011")
        self.assertEqual(canonical["channel_type"], "telegram")
        self.assertEqual(canonical["status"], "active")
        self.assertEqual(int(legacy["c"] or 0), 0)

    def test_consume_canonical_binding_token_creates_contact_channel_without_legacy_binding_row(self):
        conn = self._connect()
        try:
            self._seed_booking_pair(conn, booking_id=2, phone="+79000000012")
            created = channel_binding_service.create_binding_token(
                conn,
                reservation_id=2,
                guest_phone_e164="+79000000012",
                channel_type="telegram",
            )

            result = channel_binding_service.consume_binding_token_once(
                conn,
                token_plain=str(created["token"]),
                channel_type="telegram",
                external_user_id="tg-user-77",
                profile_meta={
                    "external_username": "anna",
                    "external_display_name": "Anna",
                },
            )

            contact_channel = conn.execute(
                """
                SELECT platform, external_user_id, status
                FROM contact_channels
                WHERE external_user_id = 'tg-user-77'
                LIMIT 1
                """
            ).fetchone()
            canonical_token = conn.execute(
                """
                SELECT status, used_by_external_user_id
                FROM channel_binding_tokens
                WHERE id = ?
                """,
                (int(created["token_id"]),),
            ).fetchone()
            legacy_bindings = conn.execute("SELECT COUNT(*) AS c FROM guest_channel_bindings").fetchone()
        finally:
            conn.close()

        self.assertTrue(result["ok"])
        self.assertEqual(contact_channel["platform"], "telegram")
        self.assertEqual(contact_channel["external_user_id"], "tg-user-77")
        self.assertEqual(contact_channel["status"], "active")
        self.assertEqual(canonical_token["status"], "used")
        self.assertEqual(canonical_token["used_by_external_user_id"], "tg-user-77")
        self.assertEqual(int(legacy_bindings["c"] or 0), 0)

    def test_consume_legacy_token_without_canonical_mapping_is_rejected(self):
        conn = self._connect()
        try:
            self._seed_booking_pair(conn, booking_id=3, phone="+79000000013")
            raw_token = "legacy-bind-token"
            conn.execute(
                """
                INSERT INTO guest_binding_tokens (
                    token_hash, reservation_id, guest_phone_e164, channel_type, status, expires_at, created_at, updated_at
                )
                VALUES (?, 3, '+79000000013', 'telegram', 'active', datetime('now', '+30 minutes'), datetime('now'), datetime('now'))
                """,
                (channel_binding_service._hash_token(raw_token),),
            )

            result = channel_binding_service.consume_binding_token_once(
                conn,
                token_plain=raw_token,
                channel_type="telegram",
                external_user_id="tg-user-legacy",
                profile_meta={"external_display_name": "Legacy User"},
            )

            legacy_token = conn.execute(
                """
                SELECT status, used_by_external_user_id
                FROM guest_binding_tokens
                WHERE reservation_id = 3
                LIMIT 1
                """
            ).fetchone()
            legacy_bindings = conn.execute("SELECT COUNT(*) AS c FROM guest_channel_bindings").fetchone()
            contact_channels = int(conn.execute("SELECT COUNT(*) AS c FROM contact_channels").fetchone()["c"])
        finally:
            conn.close()

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "token_invalid")
        self.assertEqual(legacy_token["status"], "active")
        self.assertIsNone(legacy_token["used_by_external_user_id"])
        self.assertEqual(int(legacy_bindings["c"] or 0), 0)
        self.assertEqual(contact_channels, 0)

    def test_legacy_token_fallback_is_disabled(self):
        conn = self._connect()
        try:
            self._seed_booking_pair(conn, booking_id=4, phone="+79000000014")
            raw_token = "legacy-bind-token-off"
            conn.execute(
                """
                INSERT INTO guest_binding_tokens (
                    token_hash, reservation_id, guest_phone_e164, channel_type, status, expires_at, created_at, updated_at
                )
                VALUES (?, 4, '+79000000014', 'telegram', 'active', datetime('now', '+30 minutes'), datetime('now'), datetime('now'))
                """,
                (channel_binding_service._hash_token(raw_token),),
            )

            result = channel_binding_service.consume_binding_token_once(
                conn,
                token_plain=raw_token,
                channel_type="telegram",
                external_user_id="tg-user-legacy-off",
                profile_meta={"external_display_name": "Legacy Off"},
            )

            legacy_token = conn.execute(
                """
                SELECT status, used_by_external_user_id
                FROM guest_binding_tokens
                WHERE reservation_id = 4
                LIMIT 1
                """
            ).fetchone()
            contact_channels = int(conn.execute("SELECT COUNT(*) AS c FROM contact_channels").fetchone()["c"])
        finally:
            conn.close()

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "token_invalid")
        self.assertEqual(legacy_token["status"], "active")
        self.assertIsNone(legacy_token["used_by_external_user_id"])
        self.assertEqual(contact_channels, 0)

    def test_create_binding_token_resolves_non_legacy_external_ref_mapping(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('telegram_miniapp', '77', 'Анна', '+79000000077', '2026-05-01T19:00', 2, 'pending')
                """
            )
            reservation_id = int(conn.execute("SELECT id FROM reservations").fetchone()["id"])

            result = channel_binding_service.create_binding_token(
                conn,
                reservation_id=77,
                guest_phone_e164="+79000000077",
                channel_type="telegram",
            )
        finally:
            conn.close()

        self.assertEqual(int(result["core_reservation_id"]), reservation_id)

    def test_get_reservation_by_token_uses_canonical_reservation_without_legacy_booking_row(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, comment, status, created_at, updated_at
                )
                VALUES ('crm_manual', '', 'Анна', '+79000000088', '2026-05-01T20:00', 3, 'window', 'confirmed', '2026-05-01 10:00:00', '2026-05-01 10:05:00')
                """
            )
            reservation_id = int(conn.execute("SELECT id FROM reservations").fetchone()["id"])
            conn.execute(
                """
                INSERT INTO public_reservation_tokens (reservation_id, public_token, token_kind, status, expires_at)
                VALUES (?, 'public-token-88', 'guest_access', 'active', NULL)
                """,
                (reservation_id,),
            )
            conn.commit()

            payload = channel_binding_service.get_reservation_by_token(conn, "public-token-88")
        finally:
            conn.close()

        self.assertIsNotNone(payload)
        self.assertEqual(int(payload["reservation_id"]), reservation_id)
        self.assertEqual(int(payload["id"]), reservation_id)
        self.assertEqual(payload["formname"], "crm_manual")
        self.assertEqual(payload["phone_e164"], "+79000000088")
        self.assertEqual(payload["reservation_token"], "public-token-88")

    def test_get_reservation_by_token_does_not_fallback_to_legacy_booking_token(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO bookings (
                    id, name, phone_e164, reservation_dt, guests_count, status, reservation_token
                )
                VALUES (99, 'Анна', '+79000000099', '2026-05-01 21:00', 2, 'WAITING', 'legacy-public-token-99')
                """
            )
            payload = channel_binding_service.get_reservation_by_token(conn, "legacy-public-token-99")
        finally:
            conn.close()

        self.assertIsNone(payload)

    def test_get_guest_bindings_ignores_legacy_rows_without_canonical_channels(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO guest_channel_bindings (
                    guest_phone_e164, channel_type, external_user_id, external_username, external_display_name, status
                )
                VALUES ('+79000000123', 'telegram', 'legacy-only-user', 'legacy', 'Legacy User', 'active')
                """
            )
            conn.commit()

            rows = channel_binding_service.get_guest_bindings(conn, "+79000000123")
        finally:
            conn.close()

        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
