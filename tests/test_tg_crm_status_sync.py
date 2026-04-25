import sys
import types
import unittest

sys.modules.setdefault(
    "flask",
    types.SimpleNamespace(
        abort=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("flask.abort called")),
        g=types.SimpleNamespace(),
        has_request_context=lambda: False,
        request=types.SimpleNamespace(headers={}, get_json=lambda *args, **kwargs: {}),
    ),
)
import tg_handlers


class TelegramCrmStatusSyncTests(unittest.TestCase):
    def test_status_event_dispatches_to_crm_immediately(self):
        calls = []
        original_send_booking_event = tg_handlers.send_booking_event
        original_log_booking_event = tg_handlers.log_booking_event

        def _fake_send(conn, booking_id, event_name, meta, *, dispatch_now=False):
            calls.append(
                {
                    "booking_id": booking_id,
                    "event_name": event_name,
                    "meta": meta,
                    "dispatch_now": dispatch_now,
                }
            )
            return True

        tg_handlers.send_booking_event = _fake_send
        tg_handlers.log_booking_event = lambda *args, **kwargs: None
        try:
            tg_handlers._send_booking_event_to_crm(
                object(),
                42,
                "BOOKING_STATUS_CONFIRMED",
                {"payload": {"status": "CONFIRMED"}},
            )
        finally:
            tg_handlers.send_booking_event = original_send_booking_event
            tg_handlers.log_booking_event = original_log_booking_event

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["booking_id"], 42)
        self.assertEqual(calls[0]["event_name"], "BOOKING_STATUS_CONFIRMED")
        self.assertTrue(calls[0]["dispatch_now"])


if __name__ == "__main__":
    unittest.main()
