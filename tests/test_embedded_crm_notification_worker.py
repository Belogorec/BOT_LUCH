import os
import unittest

import embedded_crm_notification_worker as worker


class EmbeddedCrmNotificationWorkerTests(unittest.TestCase):
    def setUp(self):
        self.prev_started = worker._STARTED
        self.prev_env = os.environ.get("CRM_NOTIFICATION_EMBEDDED_WORKER")
        self.prev_threading = worker.threading.Thread
        worker._STARTED = False

    def tearDown(self):
        worker._STARTED = self.prev_started
        if self.prev_env is None:
            os.environ.pop("CRM_NOTIFICATION_EMBEDDED_WORKER", None)
        else:
            os.environ["CRM_NOTIFICATION_EMBEDDED_WORKER"] = self.prev_env
        worker.threading.Thread = self.prev_threading

    def test_disabled_by_default(self):
        os.environ["CRM_NOTIFICATION_EMBEDDED_WORKER"] = "0"
        started = worker.start_embedded_crm_notification_worker()
        self.assertFalse(started)

    def test_starts_once_when_enabled(self):
        starts = []

        class FakeThread:
            def __init__(self, target, name, daemon):
                self.target = target
                self.name = name
                self.daemon = daemon

            def start(self):
                starts.append({"name": self.name, "daemon": self.daemon})

        os.environ["CRM_NOTIFICATION_EMBEDDED_WORKER"] = "1"
        worker.threading.Thread = FakeThread
        first = worker.start_embedded_crm_notification_worker()
        second = worker.start_embedded_crm_notification_worker()

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(starts, [{"name": "crm-notification-embedded", "daemon": True}])


if __name__ == "__main__":
    unittest.main()
