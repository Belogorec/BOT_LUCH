import unittest

import crm_notification_worker


class CrmNotificationWorkerTests(unittest.TestCase):
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
