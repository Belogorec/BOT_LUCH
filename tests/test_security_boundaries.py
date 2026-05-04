import os
import tempfile
import unittest


os.environ["DB_PATH"] = os.path.join(tempfile.gettempdir(), "bot_luch_security_boundaries_test.db")
os.environ["BOT_TOKEN"] = "123456:test-token"
os.environ["DASHBOARD_SECRET"] = "dashboard-test-secret"
os.environ["MINIAPP_URL"] = "https://bot.example.test/miniapp/reserve"
os.environ["TG_WEBHOOK_SECRET"] = "tg-test-secret"
os.environ["TILDA_SECRET"] = "tilda-test-secret"

try:
    import flask_app  # noqa: E402
    from flask_app import app  # noqa: E402
except (ModuleNotFoundError, ImportError) as exc:
    if getattr(exc, "name", "") != "flask" and "flask" not in str(exc).lower():
        raise
    app = None
    flask_app = None


class SecurityBoundaryTests(unittest.TestCase):
    def setUp(self):
        if app is None:
            self.skipTest("Flask is not installed in this Python environment")
        self.client = app.test_client()

    def test_public_guest_lookup_is_disabled(self):
        response = self.client.get("/public/api/guest?phone=%2B79991234567")

        self.assertEqual(response.status_code, 410)
        self.assertEqual(response.json["error"], "guest_lookup_disabled")
        self.assertNotIn("phone_e164", response.json)
        self.assertNotIn("name", response.json)

    def test_public_api_does_not_emit_wildcard_cors(self):
        response = self.client.get(
            "/public/api/guest?phone=%2B79991234567",
            headers={"Origin": "https://attacker.example"},
        )

        self.assertNotEqual(response.headers.get("Access-Control-Allow-Origin"), "*")
        self.assertIsNone(response.headers.get("Access-Control-Allow-Origin"))

    def test_legacy_crm_sync_booking_route_is_removed(self):
        response = self.client.post("/admin/api/crm-sync/booking/42", json={"action": "confirm"})
        self.assertEqual(response.status_code, 404)

    def test_legacy_crm_sync_manual_booking_route_is_removed(self):
        response = self.client.post("/admin/api/crm-sync/manual-booking", json={"payload": {"guest_name": "Test"}})
        self.assertEqual(response.status_code, 404)

    def test_legacy_crm_sync_table_route_is_removed(self):
        response = self.client.post("/admin/api/crm-sync/table", json={"action": "set_table_label"})
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
