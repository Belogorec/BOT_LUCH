import os
import time

from crm_notification_worker import process_crm_notification_batch


def main() -> None:
    interval = float(os.getenv("CRM_NOTIFICATION_INTERVAL_SEC", "5") or "5")
    limit = int(os.getenv("CRM_NOTIFICATION_BATCH_LIMIT", "50") or "50")
    max_attempts = int(os.getenv("CRM_NOTIFICATION_MAX_ATTEMPTS", "5") or "5")

    while True:
        try:
            result = process_crm_notification_batch(limit=limit, max_attempts=max_attempts)
            if result.get("count"):
                print(
                    "[CRM-NOTIFICATION] "
                    f"sent={result.get('sent')} failed={result.get('failed')} count={result.get('count')}",
                    flush=True,
                )
        except Exception as exc:
            print(f"[CRM-NOTIFICATION] error={exc}", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
