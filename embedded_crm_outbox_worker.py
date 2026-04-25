import os
import threading
import time

try:
    import fcntl
except Exception:  # pragma: no cover - fcntl is available on Railway/Linux.
    fcntl = None

from config import CRM_API_URL
from db import connect
from outbox_dispatcher import dispatch_pending_outbox

_STARTED = False


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _falsy(value: str) -> bool:
    return value.strip().lower() in {"0", "false", "no", "off"}


def _enabled() -> bool:
    value = os.getenv("CRM_OUTBOX_EMBEDDED_WORKER", "0")
    if _truthy(value):
        return True
    if _falsy(value):
        return False
    return bool(CRM_API_URL) and bool(
        os.getenv("RAILWAY_SERVICE_ID")
        or os.getenv("RAILWAY_ENVIRONMENT")
        or os.getenv("RAILWAY_PROJECT_ID")
    )


def _acquire_lock():
    if fcntl is None:
        return None
    lock_path = os.getenv("CRM_OUTBOX_LOCK_PATH", "/tmp/luch_crm_outbox_worker.lock")
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.close()
        return None
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file


def _run_loop() -> None:
    if fcntl is None:
        print("[CRM-OUTBOX-EMBEDDED] status=disabled reason=file_lock_unavailable", flush=True)
        return

    interval = float(os.getenv("CRM_OUTBOX_INTERVAL_SEC", "5") or "5")
    limit = int(os.getenv("CRM_OUTBOX_BATCH_LIMIT", "50") or "50")
    max_attempts = int(os.getenv("CRM_OUTBOX_MAX_ATTEMPTS", "5") or "5")

    lock_file = None
    while lock_file is None:
        lock_file = _acquire_lock()
        if lock_file is None:
            time.sleep(max(5.0, interval))

    print("[CRM-OUTBOX-EMBEDDED] status=started", flush=True)

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
                    "[CRM-OUTBOX-EMBEDDED] "
                    f"sent={result.get('sent')} failed={result.get('failed')} count={result.get('count')}",
                    flush=True,
                )
        except Exception as exc:
            conn.rollback()
            print(f"[CRM-OUTBOX-EMBEDDED] status=error error={exc}", flush=True)
        finally:
            conn.close()
        time.sleep(interval)


def start_embedded_crm_outbox_worker() -> bool:
    global _STARTED
    if _STARTED or not _enabled():
        return False
    _STARTED = True
    thread = threading.Thread(target=_run_loop, name="crm-outbox-embedded", daemon=True)
    thread.start()
    return True
