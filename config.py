import os
import json
import re

from dotenv import load_dotenv


load_dotenv()


def _parse_admin_ids(raw: str) -> list[str]:
    if not raw:
        return []

    s = str(raw).strip()
    if not s:
        return []

    # Support JSON list format: ["123", "456"]
    if s.startswith("[") and s.endswith("]"):
        try:
            data = json.loads(s)
            ids = [str(x).strip() for x in data if str(x).strip()]
            return sorted(set(ids))
        except Exception:
            pass

    # Support comma/space/newline/semicolon separated values.
    parts = [p.strip() for p in re.split(r"[\s,;]+", s) if p.strip()]

    normalized: list[str] = []
    for p in parts:
        # Remove common accidental wrappers from env UIs, e.g. "12345"
        v = p.strip().strip('"').strip("'").strip()
        if not v:
            continue
        normalized.append(v)

    return sorted(set(normalized))


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
TILDA_SECRET = os.getenv("TILDA_SECRET", "").strip()
TG_WEBHOOK_SECRET = os.getenv("TG_WEBHOOK_SECRET", "").strip()
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "").strip()
ANALYTICS_TZ_OFFSET_HOURS = int(os.getenv("ANALYTICS_TZ_OFFSET_HOURS", "3").strip() or "3")
CRM_AUTH_CONFIRM_URL = os.getenv(
    "CRM_AUTH_CONFIRM_URL",
    "https://luchcrm-production.up.railway.app/api/auth/confirm-code",
).strip()
CRM_AUTH_TIMEOUT_SEC = int(os.getenv("CRM_AUTH_TIMEOUT_SEC", "8").strip() or "8")

# Supports: "12345,67890", "12345 67890", JSON list, or single PROMO_ADMIN_ID.
_admins_multi = _parse_admin_ids(os.getenv("PROMO_ADMIN_IDS", ""))
_admin_single = os.getenv("PROMO_ADMIN_ID", "").strip()
PROMO_ADMIN_IDS = sorted(set(_admins_multi + ([_admin_single] if _admin_single else [])))

if BOT_TOKEN:
    TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
else:
    TG_API = ""
