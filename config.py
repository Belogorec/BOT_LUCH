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
WAITER_CHAT_ID = os.getenv("WAITER_CHAT_ID", os.getenv("WAITERS_CHAT_ID", "-1001763474308")).strip()
TILDA_SECRET = os.getenv("TILDA_SECRET", "").strip()
TG_WEBHOOK_SECRET = os.getenv("TG_WEBHOOK_SECRET", "").strip()
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "").strip()
ANALYTICS_TZ_OFFSET_HOURS = int(os.getenv("ANALYTICS_TZ_OFFSET_HOURS", "3").strip() or "3")
BUSINESS_TZ_OFFSET_HOURS = int(os.getenv("BUSINESS_TZ_OFFSET_HOURS", str(ANALYTICS_TZ_OFFSET_HOURS)).strip() or str(ANALYTICS_TZ_OFFSET_HOURS))
CRM_API_URL = os.getenv("CRM_API_URL", "").strip()
CRM_API_KEY = os.getenv("CRM_API_KEY", "").strip()
CRM_SYNC_TIMEOUT = int(os.getenv("CRM_SYNC_TIMEOUT", "8").strip() or "8")
CRM_AUTH_CONFIRM_URL = os.getenv(
    "CRM_AUTH_CONFIRM_URL",
    "https://luchcrm-production.up.railway.app/api/auth/confirm-code",
).strip()
CRM_AUTH_TIMEOUT_SEC = int(os.getenv("CRM_AUTH_TIMEOUT_SEC", "8").strip() or "8")
VK_HOSTESS_GROUP_ID = os.getenv("VK_HOSTESS_GROUP_ID", os.getenv("VK_GROUP_ID", "")).strip()
VK_HOSTESS_ACCESS_TOKEN = os.getenv("VK_HOSTESS_ACCESS_TOKEN", os.getenv("VK_ACCESS_TOKEN", "")).strip()
VK_HOSTESS_CALLBACK_SECRET = os.getenv("VK_HOSTESS_CALLBACK_SECRET", os.getenv("VK_CALLBACK_SECRET", "")).strip()
VK_HOSTESS_CONFIRMATION_TOKEN = os.getenv("VK_HOSTESS_CONFIRMATION_TOKEN", os.getenv("VK_CONFIRMATION_TOKEN", "")).strip()

VK_WAITER_GROUP_ID = os.getenv("VK_WAITER_GROUP_ID", "").strip()
VK_WAITER_ACCESS_TOKEN = os.getenv("VK_WAITER_ACCESS_TOKEN", "").strip()
VK_WAITER_CALLBACK_SECRET = os.getenv("VK_WAITER_CALLBACK_SECRET", "").strip()
VK_WAITER_CONFIRMATION_TOKEN = os.getenv("VK_WAITER_CONFIRMATION_TOKEN", "").strip()

# Backward-compatible aliases for the original hostess bot env names.
VK_GROUP_ID = VK_HOSTESS_GROUP_ID
VK_ACCESS_TOKEN = VK_HOSTESS_ACCESS_TOKEN
VK_CALLBACK_SECRET = VK_HOSTESS_CALLBACK_SECRET
VK_CONFIRMATION_TOKEN = VK_HOSTESS_CONFIRMATION_TOKEN
VK_API_VERSION = os.getenv("VK_API_VERSION", "5.199").strip() or "5.199"


def _build_vk_bot_config(
    *,
    bot_key: str,
    role_hint: str,
    group_id: str,
    access_token: str,
    callback_secret: str,
    confirmation_token: str,
) -> dict[str, str]:
    return {
        "bot_key": bot_key,
        "role_hint": role_hint,
        "group_id": str(group_id or "").strip(),
        "access_token": str(access_token or "").strip(),
        "callback_secret": str(callback_secret or "").strip(),
        "confirmation_token": str(confirmation_token or "").strip(),
    }


VK_BOTS: dict[str, dict[str, str]] = {
    "hostess": _build_vk_bot_config(
        bot_key="hostess",
        role_hint="hostess",
        group_id=VK_HOSTESS_GROUP_ID,
        access_token=VK_HOSTESS_ACCESS_TOKEN,
        callback_secret=VK_HOSTESS_CALLBACK_SECRET,
        confirmation_token=VK_HOSTESS_CONFIRMATION_TOKEN,
    ),
    "waiter": _build_vk_bot_config(
        bot_key="waiter",
        role_hint="waiter",
        group_id=VK_WAITER_GROUP_ID,
        access_token=VK_WAITER_ACCESS_TOKEN,
        callback_secret=VK_WAITER_CALLBACK_SECRET,
        confirmation_token=VK_WAITER_CONFIRMATION_TOKEN,
    ),
}


def get_vk_bot_config(bot_key: str = "hostess") -> dict[str, str]:
    return dict(VK_BOTS.get(bot_key, {}))


def find_vk_bot_config_by_group_id(group_id: str) -> dict[str, str]:
    normalized_group_id = str(group_id or "").strip()
    if not normalized_group_id:
        return {}

    for bot in VK_BOTS.values():
        if str(bot.get("group_id") or "").strip() == normalized_group_id:
            return dict(bot)
    return {}

# Supports: "12345,67890", "12345 67890", JSON list, or single PROMO_ADMIN_ID.
_admins_multi = _parse_admin_ids(os.getenv("PROMO_ADMIN_IDS", ""))
_admin_single = os.getenv("PROMO_ADMIN_ID", "").strip()
PROMO_ADMIN_IDS = sorted(set(_admins_multi + ([_admin_single] if _admin_single else [])))

if BOT_TOKEN:
    TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
else:
    TG_API = ""
