import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta

from contact_schema import run_contact_schema_migrations
from core_schema import run_core_schema_migrations
from db import init_schema
from integration_schema import run_integration_schema_migrations
from verify_mirror_off import build_report


class VerifyMirrorOffTests(unittest.TestCase):
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

    def test_clean_db_has_no_critical_or_warning_issues(self):
        report = build_report(self.tmp.name)

        self.assertEqual(report["summary"]["critical_count"], 0)
        self.assertEqual(report["summary"]["warning_count"], 0)

    def test_core_only_self_owned_reservation_without_legacy_booking_is_clean(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO reservations (
                    id, source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES (11, 'legacy_booking', '11', 'Core Only', '+79000000011', '2026-05-01T19:00', 2, 'pending')
                """
            )
            conn.commit()
        finally:
            conn.close()

        report = build_report(self.tmp.name)
        warning_codes = {item["code"] for item in report["warning"]}

        self.assertNotIn("canonical_reservations_without_legacy_booking", warning_codes)

    def test_missing_legacy_booking_for_mirrored_reservation_stays_warning(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO reservations (
                    id, source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES (12, 'legacy_booking', '99', 'Old Mirror', '+79000000012', '2026-05-01T20:00', 2, 'pending')
                """
            )
            conn.commit()
        finally:
            conn.close()

        report = build_report(self.tmp.name)
        warning_codes = {item["code"] for item in report["warning"]}

        self.assertIn("canonical_reservations_without_legacy_booking", warning_codes)

    def test_active_legacy_booking_token_without_public_token_is_critical(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO bookings (
                    id, name, phone_e164, reservation_dt, guests_count, status, reservation_token
                )
                VALUES (1, 'Анна', '+79000000001', '2026-05-01 19:00', 2, 'WAITING', 'pubtok-1')
                """
            )
            conn.execute(
                """
                INSERT INTO reservations (
                    source, external_ref, guest_name, guest_phone, reservation_at, party_size, status
                )
                VALUES ('legacy_booking', '1', 'Анна', '+79000000001', '2026-05-01T19:00', 2, 'pending')
                """
            )
            conn.commit()
        finally:
            conn.close()

        report = build_report(self.tmp.name)
        critical_codes = {item["code"] for item in report["critical"]}

        self.assertIn("active_legacy_booking_token_without_canonical_token", critical_codes)

    def test_pending_replies_split_between_active_and_historical(self):
        now = datetime.utcnow().replace(microsecond=0)
        active_expires = (now + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
        expired_expires = (now - timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")

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
                    ("booking_dialog", 1, "+79000000002", "100", "admin", "501", active_expires),
                    ("booking_dialog", 2, "+79000000003", "101", "admin", "502", expired_expires),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        report = build_report(self.tmp.name)
        critical_codes = {item["code"] for item in report["critical"]}
        ignored_codes = {item["code"] for item in report["ignored_historical"]}

        self.assertIn("pending_replies_still_used", critical_codes)
        self.assertIn("historical_pending_replies", ignored_codes)

    def test_active_legacy_guest_binding_without_canonical_mapping_is_critical(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO guest_channel_bindings (
                    guest_phone_e164, channel_type, external_user_id, external_username, status
                )
                VALUES ('+79000000004', 'telegram', 'tg-user-1', 'anna', 'active')
                """
            )
            conn.commit()
        finally:
            conn.close()

        report = build_report(self.tmp.name)
        critical_codes = {item["code"] for item in report["critical"]}

        self.assertIn("legacy_guest_channel_rows_without_canonical_mapping", critical_codes)

    def test_mapped_legacy_guest_binding_becomes_warning(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO contacts (
                    phone_e164, display_name, source
                )
                VALUES ('+79000000005', 'Иван', 'test')
                """
            )
            contact_id = int(conn.execute("SELECT id FROM contacts WHERE phone_e164='+79000000005'").fetchone()["id"])
            conn.execute(
                """
                INSERT INTO contact_channels (
                    contact_id, platform, external_user_id, display_name, status
                )
                VALUES (?, 'telegram', 'tg-user-2', 'Иван', 'active')
                """,
                (contact_id,),
            )
            conn.execute(
                """
                INSERT INTO guest_channel_bindings (
                    guest_phone_e164, channel_type, external_user_id, external_display_name, status
                )
                VALUES ('+79000000005', 'telegram', 'tg-user-2', 'Иван', 'active')
                """
            )
            conn.commit()
        finally:
            conn.close()

        report = build_report(self.tmp.name)
        warning_codes = {item["code"] for item in report["warning"]}
        critical_codes = {item["code"] for item in report["critical"]}

        self.assertIn("legacy_guest_bindings_still_present", warning_codes)
        self.assertNotIn("legacy_guest_channel_rows_without_canonical_mapping", critical_codes)

    def test_active_vk_staff_peer_without_bot_peer_mapping_is_critical(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO vk_staff_peers (
                    peer_id, peer_external_id, bot_key, is_active, role_hint
                )
                VALUES ('2000000001', '2000000001', 'hostess', 1, 'hostess')
                """
            )
            conn.commit()
        finally:
            conn.close()

        report = build_report(self.tmp.name)
        critical_codes = {item["code"] for item in report["critical"]}

        self.assertIn("legacy_vk_staff_peers_without_canonical_mapping", critical_codes)


if __name__ == "__main__":
    unittest.main()
