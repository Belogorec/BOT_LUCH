import os
import time

from db import connect
from outbox_dispatcher import dispatch_pending_outbox


def main() -> None:
    interval = float(os.getenv("CRM_OUTBOX_INTERVAL_SEC", "5") or "5")
    limit = int(os.getenv("CRM_OUTBOX_BATCH_LIMIT", "50") or "50")
    max_attempts = int(os.getenv("CRM_OUTBOX_MAX_ATTEMPTS", "5") or "5")

    while True:
        conn = connect()
        try:
            result = dispatch_pending_outbox(
                conn,
                platform="http",
                bot_scope="crm_sync",
                limit=limit,
                max_attempts=max_attempts,
            )
            conn.commit()
            if result.get("count"):
                print(
                    "[CRM-OUTBOX] "
                    f"sent={result.get('sent')} failed={result.get('failed')} count={result.get('count')}",
                    flush=True,
                )
        except Exception as exc:
            conn.rollback()
            print(f"[CRM-OUTBOX] error={exc}", flush=True)
        finally:
            conn.close()
        time.sleep(interval)


if __name__ == "__main__":
    main()
