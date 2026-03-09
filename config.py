import os

from dotenv import load_dotenv


load_dotenv()


def _split_csv_ids(raw: str) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
TILDA_SECRET = os.getenv("TILDA_SECRET", "").strip()
TG_WEBHOOK_SECRET = os.getenv("TG_WEBHOOK_SECRET", "").strip()
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "").strip()
ANALYTICS_TZ_OFFSET_HOURS = int(os.getenv("ANALYTICS_TZ_OFFSET_HOURS", "3").strip() or "3")

# Comma-separated list: "12345,67890"
PROMO_ADMIN_IDS = _split_csv_ids(os.getenv("PROMO_ADMIN_IDS", ""))

if BOT_TOKEN:
    TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
else:
    TG_API = ""
