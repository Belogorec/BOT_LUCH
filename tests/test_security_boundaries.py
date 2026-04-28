import os
import tempfile
import unittest


os.environ["DB_PATH"] = os.path.join(tempfile.gettempdir(), "bot_luch_security_boundaries_test.db")
os.environ["BOT_TOKEN"] = "123456:test-token"
os.environ["CRM_SYNC_SHARED_SECRET"] = "crm-test-secret"
os.environ["DASHBOARD_SECRET"] = "dashboard-test-secret"
os.environ["MINIAPP_URL"] = "https://bot.example.test/miniapp/reserve"
os.environ["TG_WEBHOOK_SECRET"] = "tg-test-secret"
os.environ["TILDA_SECRET"] = "tilda-test-secret"

try:
    from flask import request  # noqa: E402
    import flask_app  # noqa: E402
    from flask_app import _crm_sync_authorized, app  # noqa: E402
except (ModuleNotFoundError, ImportError) as exc:
    if getattr(exc, "name", "") != "flask" and "flask" not in str(exc).lower():
        raise
    request = None
    app = None
    flask_app = None
    _crm_sync_authorized = None


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

    def test_crm_sync_rejects_legacy_bot_token_payload(self):
        with app.test_request_context(
            "/admin/api/crm-sync/bookings/recent",
            method="GET",
            json={"bot_token": "crm-test-secret"},
        ):
            self.assertFalse(_crm_sync_authorized(request))

    def test_crm_sync_rejects_legacy_crm_api_key_header(self):
        with app.test_request_context(
            "/admin/api/crm-sync/bookings/recent",
            method="GET",
            headers={"X-CRM-API-Key": "crm-test-secret"},
        ):
            self.assertFalse(_crm_sync_authorized(request))

    def test_crm_sync_endpoints_are_disabled_in_authoritative_mode(self):
        previous = flask_app.CRM_AUTHORITATIVE
        flask_app.CRM_AUTHORITATIVE = True
        try:
            response = self.client.get(
                "/admin/api/crm-sync/bookings/recent",
                headers={"X-CRM-Sync-Secret": flask_app.CRM_SYNC_SHARED_SECRET},
            )
        finally:
            flask_app.CRM_AUTHORITATIVE = previous

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json["error"], "crm_authoritative_mode")


if __name__ == "__main__":
    unittest.main()
