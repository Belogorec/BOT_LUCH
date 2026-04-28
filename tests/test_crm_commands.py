import json
import unittest
from unittest.mock import patch

import crm_commands


class FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class CrmCommandsTests(unittest.TestCase):
    def test_send_command_normalizes_success_and_headers(self):
        calls = []

        def fake_request(method, url, headers, data, timeout):
            calls.append({"method": method, "url": url, "headers": headers, "data": data, "timeout": timeout})
            return FakeResponse(200, {"ok": True, "event_id": "evt-1"})

        with patch.object(crm_commands, "CRM_COMMAND_API_URL", "https://crm.example"), \
            patch.object(crm_commands, "CRM_COMMAND_API_KEY", "secret-key"), \
            patch.object(crm_commands.requests, "request", side_effect=fake_request):
            result = crm_commands.send_command(
                method="POST",
                path="/api/commands/reservations/1/status",
                event_id="evt-1",
                payload={"status": "confirmed"},
            )

        self.assertTrue(result["accepted"])
        self.assertEqual(result["status_code"], 200)
        self.assertEqual(calls[0]["headers"]["X-CRM-Command-Key"], "secret-key")
        self.assertEqual(calls[0]["headers"]["X-Idempotency-Key"], "evt-1")
        self.assertIn('"event_id": "evt-1"', calls[0]["data"])

    def test_send_command_normalizes_rejection(self):
        with patch.object(crm_commands, "CRM_COMMAND_API_URL", "https://crm.example"), \
            patch.object(crm_commands, "CRM_COMMAND_API_KEY", "secret-key"), \
            patch.object(crm_commands.requests, "request", return_value=FakeResponse(400, {"ok": False, "error": "table_time_conflict"})):
            result = crm_commands.assign_table(
                10,
                table_number="221",
                guests_count=2,
                event_id="evt-2",
                actor={"id": "vk:1", "name": "VK 1"},
            )

        self.assertFalse(result["accepted"])
        self.assertEqual(result["error"], "table_time_conflict")
        self.assertEqual(result["status_code"], 400)

    def test_missing_config_raises_without_secret_leak(self):
        with patch.object(crm_commands, "CRM_COMMAND_API_URL", ""), \
            patch.object(crm_commands, "CRM_COMMAND_API_KEY", "secret-key"):
            with self.assertRaises(crm_commands.CrmCommandError) as ctx:
                crm_commands.send_command(method="POST", path="/x", event_id="evt", payload={})
        self.assertEqual(str(ctx.exception), "crm_command_api_url_missing")

    def test_reservation_restriction_uses_booking_bound_endpoint(self):
        calls = []

        def fake_request(method, url, headers, data, timeout):
            calls.append({"method": method, "url": url, "data": json.loads(data)})
            return FakeResponse(200, {"ok": True, "event_id": "evt-3"})

        with patch.object(crm_commands, "CRM_COMMAND_API_URL", "https://crm.example"), \
            patch.object(crm_commands, "CRM_COMMAND_API_KEY", "secret-key"), \
            patch.object(crm_commands.requests, "request", side_effect=fake_request):
            result = crm_commands.restrict_reservation_table(
                10,
                table_number="221",
                restricted_until="2026-05-01T23:00",
                event_id="evt-3",
                actor={"id": "tg:1", "name": "TG 1"},
                force_override=True,
            )

        self.assertTrue(result["accepted"])
        self.assertEqual(calls[0]["method"], "POST")
        self.assertEqual(calls[0]["url"], "https://crm.example/api/commands/reservations/10/restriction")
        self.assertEqual(calls[0]["data"]["table_number"], "221")
        self.assertTrue(calls[0]["data"]["force_override"])


if __name__ == "__main__":
    unittest.main()
