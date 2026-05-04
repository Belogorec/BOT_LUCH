import os
import json
import re

from dotenv import load_dotenv


load_dotenv()


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "enabled"}


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


def _parse_peer_ids(raw: str) -> list[str]:
    if not raw:
        return []
    parts = [p.strip().strip('"').strip("'").strip() for p in re.split(r"[\s,;]+", str(raw)) if p.strip()]
    return [p for p in parts if p]


ALLOW_INSECURE_DEFAULTS = _env_flag("ALLOW_INSECURE_DEFAULTS", default=False)
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
WAITER_CHAT_ID = os.getenv("WAITER_CHAT_ID", os.getenv("WAITERS_CHAT_ID", "-1001763474308")).strip()
TILDA_SECRET = os.getenv("TILDA_SECRET", "").strip()
TG_WEBHOOK_SECRET = os.getenv("TG_WEBHOOK_SECRET", "").strip()
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "").strip()
MINIAPP_URL = os.getenv("MINIAPP_URL", "").strip()
MINIAPP_MIN_LEAD_MINUTES = int(os.getenv("MINIAPP_MIN_LEAD_MINUTES", "20").strip() or "20")
TELEGRAM_INIT_DATA_MAX_AGE_SEC = int(os.getenv("TELEGRAM_INIT_DATA_MAX_AGE_SEC", "86400").strip() or "86400")
DASHBOARD_CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("DASHBOARD_CORS_ORIGINS", "").split(",")
    if origin.strip()
]
PUBLIC_CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("PUBLIC_CORS_ORIGINS", "").split(",")
    if origin.strip()
]
ANALYTICS_TZ_OFFSET_HOURS = int(os.getenv("ANALYTICS_TZ_OFFSET_HOURS", "3").strip() or "3")
BUSINESS_TZ_OFFSET_HOURS = int(os.getenv("BUSINESS_TZ_OFFSET_HOURS", str(ANALYTICS_TZ_OFFSET_HOURS)).strip() or str(ANALYTICS_TZ_OFFSET_HOURS))
TABLE_RESERVATION_DURATION_MINUTES = int(os.getenv("TABLE_RESERVATION_DURATION_MINUTES", "120").strip() or "120")
TABLE_RESERVATION_BUFFER_MINUTES = int(os.getenv("TABLE_RESERVATION_BUFFER_MINUTES", "0").strip() or "0")
CRM_API_URL = os.getenv("CRM_API_URL", "").strip()
CRM_API_KEY = os.getenv("CRM_API_KEY", "").strip()
CRM_SYNC_TIMEOUT = int(os.getenv("CRM_SYNC_TIMEOUT", "8").strip() or "8")
CRM_AUTHORITATIVE = _env_flag("CRM_AUTHORITATIVE", default=False)
CRM_COMMAND_API_URL = os.getenv("CRM_COMMAND_API_URL", "").strip()
CRM_COMMAND_API_KEY = os.getenv("CRM_COMMAND_API_KEY", "").strip()
CRM_COMMAND_TIMEOUT = int(os.getenv("CRM_COMMAND_TIMEOUT", os.getenv("CRM_SYNC_TIMEOUT", "8")).strip() or "8")
VK_HOSTESS_GROUP_ID = os.getenv("VK_HOSTESS_GROUP_ID", os.getenv("VK_GROUP_ID", "")).strip()
VK_HOSTESS_ACCESS_TOKEN = os.getenv("VK_HOSTESS_ACCESS_TOKEN", os.getenv("VK_ACCESS_TOKEN", "")).strip()
VK_HOSTESS_CALLBACK_SECRET = os.getenv("VK_HOSTESS_CALLBACK_SECRET", os.getenv("VK_CALLBACK_SECRET", "")).strip()
VK_HOSTESS_CONFIRMATION_TOKEN = os.getenv("VK_HOSTESS_CONFIRMATION_TOKEN", os.getenv("VK_CONFIRMATION_TOKEN", "")).strip()
VK_HOSTESS_PEER_IDS = _parse_peer_ids(os.getenv("VK_HOSTESS_PEER_IDS", ""))

VK_WAITER_GROUP_ID = os.getenv("VK_WAITER_GROUP_ID", "").strip()
VK_WAITER_ACCESS_TOKEN = os.getenv("VK_WAITER_ACCESS_TOKEN", "").strip()
VK_WAITER_CALLBACK_SECRET = os.getenv("VK_WAITER_CALLBACK_SECRET", "").strip()
VK_WAITER_CONFIRMATION_TOKEN = os.getenv("VK_WAITER_CONFIRMATION_TOKEN", "").strip()
VK_WAITER_PEER_IDS = _parse_peer_ids(os.getenv("VK_WAITER_PEER_IDS", ""))
VK_GUEST_GROUP_ID = os.getenv("VK_GUEST_GROUP_ID", "").strip()
VK_GUEST_ACCESS_TOKEN = os.getenv("VK_GUEST_ACCESS_TOKEN", "").strip()
VK_GUEST_CALLBACK_SECRET = os.getenv("VK_GUEST_CALLBACK_SECRET", "").strip()
VK_GUEST_CONFIRMATION_TOKEN = os.getenv("VK_GUEST_CONFIRMATION_TOKEN", "").strip()

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
    "guest": _build_vk_bot_config(
        bot_key="guest",
        role_hint="guest",
        group_id=VK_GUEST_GROUP_ID,
        access_token=VK_GUEST_ACCESS_TOKEN,
        callback_secret=VK_GUEST_CALLBACK_SECRET,
        confirmation_token=VK_GUEST_CONFIRMATION_TOKEN,
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

GUEST_COMM_ENABLED = _env_flag("GUEST_COMM_ENABLED", default=False)
CORE_ONLY_MODE = _env_flag("CORE_ONLY_MODE", default=False)
LEGACY_MIRROR_ENABLED = _env_flag("LEGACY_MIRROR_ENABLED", default=(not CORE_ONLY_MODE))
GUEST_BINDING_TOKEN_TTL_MIN = int(os.getenv("GUEST_BINDING_TOKEN_TTL_MIN", "45").strip() or "45")
GUEST_BINDING_TOKEN_PEPPER = os.getenv("GUEST_BINDING_TOKEN_PEPPER", "").strip()
GUEST_PUBLIC_BASE_URL = os.getenv("GUEST_PUBLIC_BASE_URL", "").strip()
TG_BOT_USERNAME = os.getenv("TG_BOT_USERNAME", "").strip().lstrip("@")
TG_BINDING_START_PREFIX = os.getenv("TG_BINDING_START_PREFIX", "bind_").strip() or "bind_"
GUEST_NOTIFICATION_TEST_MODE = _env_flag("GUEST_NOTIFICATION_TEST_MODE", default=False)


def _configured(*values: str) -> bool:
    return any(str(value or "").strip() for value in values)


def validate_security_config() -> None:
    missing: list[str] = []

    for name, value in {
        "BOT_TOKEN": BOT_TOKEN,
        "TG_WEBHOOK_SECRET": TG_WEBHOOK_SECRET,
        "DASHBOARD_SECRET": DASHBOARD_SECRET,
        "TILDA_SECRET": TILDA_SECRET,
        "MINIAPP_URL": MINIAPP_URL,
    }.items():
        if not value:
            missing.append(name)

    if CRM_API_URL and not CRM_API_KEY:
        missing.append("CRM_API_KEY")
    if CRM_API_KEY and not CRM_API_URL:
        missing.append("CRM_API_URL")
    if CRM_AUTHORITATIVE:
        if not CRM_COMMAND_API_URL:
            missing.append("CRM_COMMAND_API_URL")
        if not CRM_COMMAND_API_KEY:
            missing.append("CRM_COMMAND_API_KEY")

    for prefix, group_id, access_token, callback_secret, confirmation_token in (
        ("VK_HOSTESS", VK_HOSTESS_GROUP_ID, VK_HOSTESS_ACCESS_TOKEN, VK_HOSTESS_CALLBACK_SECRET, VK_HOSTESS_CONFIRMATION_TOKEN),
        ("VK_WAITER", VK_WAITER_GROUP_ID, VK_WAITER_ACCESS_TOKEN, VK_WAITER_CALLBACK_SECRET, VK_WAITER_CONFIRMATION_TOKEN),
        ("VK_GUEST", VK_GUEST_GROUP_ID, VK_GUEST_ACCESS_TOKEN, VK_GUEST_CALLBACK_SECRET, VK_GUEST_CONFIRMATION_TOKEN),
    ):
        if _configured(group_id, access_token, callback_secret, confirmation_token):
            if not group_id:
                missing.append(f"{prefix}_GROUP_ID")
            if not access_token:
                missing.append(f"{prefix}_ACCESS_TOKEN")
            if not callback_secret:
                missing.append(f"{prefix}_CALLBACK_SECRET")
            if not confirmation_token:
                missing.append(f"{prefix}_CONFIRMATION_TOKEN")

    if (missing) and not ALLOW_INSECURE_DEFAULTS:
        details = "missing: " + ", ".join(sorted(set(missing)))
        raise RuntimeError("Security-critical BOT env is not configured (" + details + ")")
