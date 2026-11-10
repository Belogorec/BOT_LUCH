import unittest
from unittest.mock import patch

import crm_notification_worker
import crm_staff_links
from booking_render import booking_crm_keyboard


class CrmNotificationWorkerTests(unittest.TestCase):
    def test_staff_restore_link_uses_exact_reservation_and_preserves_active_controls(self):
        reservation = {"reservation_id": 55, "booking_id": 42, "status": "cancelled"}
        job = {
            "platform": "telegram",
            "bot_scope": "hostess",
            "target_external_id": "-100",
            "message_type": "reservation_card_upsert",
            "payload": {"reservation": reservation, "message_id": "msg-1"},
        }
        expected_url = "https://crm.example.test/crm/bookings/42?reservation_id=55#restore-booking"
        with (
            patch.object(crm_staff_links, "CRM_COMMAND_API_URL", "https://crm.example.test"),
            patch.object(crm_notification_worker, "tg_edit_message") as edit,
        ):
            crm_notification_worker.dispatch_notification_job(job)
            self.assertEqual(
                edit.call_args.args[3],
                {"inline_keyboard": [[{"text": "Восстановить в CRM", "url": expected_url}]]},
            )
            reservation["status"] = "pending"
            crm_notification_worker.dispatch_notification_job(job)
            keyboard = edit.call_args.args[3]["inline_keyboard"]
            self.assertEqual(keyboard[0][0]["callback_data"], "b:42:booking:confirm")
            self.assertEqual(keyboard[-1][0], {"text": "Открыть в CRM", "url": expected_url})

    def test_vk_restore_link_only_in_hostess_jobs(self):
        job = {
            "platform": "vk",
            "bot_scope": "hostess",
            "target_external_id": "101",
            "message_type": "reservation_card_upsert",
            "payload": {"reservation": {"reservation_id": 55, "booking_id": 42, "status": "completed"}},
        }
        with (
            patch.object(crm_staff_links, "CRM_COMMAND_API_URL", "https://crm.example.test"),
            patch.object(crm_notification_worker, "vk_send_message") as send,
        ):
            crm_notification_worker.dispatch_notification_job(job)
            self.assertIn(
                "Восстановить в CRM: https://crm.example.test/crm/bookings/42?reservation_id=55#restore-booking",
                send.call_args.args[1],
            )
            job["bot_scope"] = "waiter"
            crm_notification_worker.dispatch_notification_job(job)
            self.assertNotIn("crm.example.test", send.call_args.args[1])

    def test_legacy_link_uses_archive_without_api_path_or_credentials(self):
        with (
            patch.object(crm_staff_links, "CRM_COMMAND_API_URL", ""),
            patch.object(crm_staff_links, "CRM_API_URL", "https://test-user:test-password@crm.example.test/api/ingest?key=test"),
        ):
            self.assertEqual(
                booking_crm_keyboard(archived=True),
                {"inline_keyboard": [[{"text": "Восстановить в CRM", "url": "https://crm.example.test/crm/archive"}]]},
            )

    def test_process_batch_renders_reservation_card_and_acks_success(self):
        completed = []
        sent = []
        original_claim = crm_notification_worker.claim_notifications
        original_complete = crm_notification_worker.complete_notification
        original_send = crm_notification_worker.tg_send_message
        crm_notification_worker.claim_notifications = lambda limit=50, max_attempts=5: [
            {
                "id": 10,
                "platform": "telegram",
                "bot_scope": "hostess",
                "target_external_id": "-100",
                "message_type": "reservation_card_upsert",
                "payload": {
                    "reservation": {
                        "reservation_id": 55,
                        "booking_id": 55,
                        "status": "pending",
                        "guest_name": "Анна",
                        "guest_phone": "+79000000101",
                        "reservation_date": "2026-05-01",
                        "reservation_time": "19:00",
                        "party_size": 2,
                        "table_number": "221",
                    }
                },
            }
        ]
        crm_notification_worker.complete_notification = lambda job_id, **kwargs: completed.append({"job_id": job_id, **kwargs})
        crm_notification_worker.tg_send_message = lambda chat_id, text, reply_markup=None: sent.append(
            {"chat_id": chat_id, "text": text, "reply_markup": reply_markup}
        ) or "msg-55"
        try:
            result = crm_notification_worker.process_crm_notification_batch()
        finally:
            crm_notification_worker.claim_notifications = original_claim
            crm_notification_worker.complete_notification = original_complete
            crm_notification_worker.tg_send_message = original_send

        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(sent[0]["chat_id"], "-100")
        self.assertIn("Бронь #55", sent[0]["text"])
        self.assertIn("<b>Стол:</b> 221", sent[0]["text"])
        self.assertEqual(completed[0]["job_id"], 10)
        self.assertTrue(completed[0]["ok"])
        self.assertEqual(completed[0]["provider_message_id"], "msg-55")

    def test_process_batch_edits_existing_telegram_reservation_card(self):
        completed = []
        edited = []
        sent = []
        original_claim = crm_notification_worker.claim_notifications
        original_complete = crm_notification_worker.complete_notification
        original_send = crm_notification_worker.tg_send_message
        original_edit = crm_notification_worker.tg_edit_message
        crm_notification_worker.claim_notifications = lambda limit=50, max_attempts=5: [
            {
                "id": 11,
                "platform": "telegram",
                "bot_scope": "hostess",
                "target_external_id": "-100",
                "message_type": "reservation_card_upsert",
                "payload": {
                    "message_id": "msg-existing",
                    "reservation": {
                        "reservation_id": 55,
                        "booking_id": 55,
                        "status": "confirmed",
                        "guest_name": "Анна",
                        "guest_phone": "+79000000101",
                        "reservation_date": "2026-05-03",
                        "reservation_time": "21:15",
                        "party_size": 2,
                        "table_number": "221",
                    }
                },
            }
        ]
        crm_notification_worker.complete_notification = lambda job_id, **kwargs: completed.append({"job_id": job_id, **kwargs})
        crm_notification_worker.tg_send_message = lambda chat_id, text, reply_markup=None: sent.append(
            {"chat_id": chat_id, "text": text, "reply_markup": reply_markup}
        ) or "unexpected-send"
        crm_notification_worker.tg_edit_message = lambda chat_id, message_id, text, reply_markup=None: edited.append(
            {"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup}
        ) or True
        try:
            result = crm_notification_worker.process_crm_notification_batch()
        finally:
            crm_notification_worker.claim_notifications = original_claim
            crm_notification_worker.complete_notification = original_complete
            crm_notification_worker.tg_send_message = original_send
            crm_notification_worker.tg_edit_message = original_edit

        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(len(sent), 0)
        self.assertEqual(edited[0]["chat_id"], "-100")
        self.assertEqual(edited[0]["message_id"], "msg-existing")
        self.assertIn("2026-05-03 21:15", edited[0]["text"])
        self.assertEqual(completed[0]["job_id"], 11)
        self.assertTrue(completed[0]["ok"])
        self.assertEqual(completed[0]["provider_message_id"], "msg-existing")


if __name__ == "__main__":
    unittest.main()
