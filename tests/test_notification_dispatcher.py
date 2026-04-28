import sqlite3
import unittest

import notification_dispatcher


class NotificationDispatcherTests(unittest.TestCase):
    def test_authoritative_mode_rejects_guest_service_notifications_without_outbox(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        previous_authoritative = notification_dispatcher.CRM_AUTHORITATIVE
        previous_guest_comm = notification_dispatcher.GUEST_COMM_ENABLED
        notification_dispatcher.CRM_AUTHORITATIVE = True
        notification_dispatcher.GUEST_COMM_ENABLED = True
        try:
            result = notification_dispatcher.send_service_notification(
                conn,
                event_type="BOOKING_STATUS_CONFIRMED",
                text="test",
                reservation_id=1,
            )
        finally:
            notification_dispatcher.CRM_AUTHORITATIVE = previous_authoritative
            notification_dispatcher.GUEST_COMM_ENABLED = previous_guest_comm
            conn.close()

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "guest_comm_disabled_in_authoritative_mode")


if __name__ == "__main__":
    unittest.main()
