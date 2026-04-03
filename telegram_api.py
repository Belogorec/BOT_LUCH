import json
import time
from typing import Optional
import requests

from config import TG_API

session = requests.Session()
session.trust_env = True


def tg_post(method: str, data: dict):
    if not TG_API:
        raise RuntimeError("BOT_TOKEN missing")

    max_attempts = 6
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = session.post(f"{TG_API}/{method}", data=data, timeout=25)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                raise
            # Short backoff for transient Telegram edge/network hiccups.
            time.sleep(0.6 * attempt)
            continue

        # Retry transient HTTP statuses (429 throttling, 5xx Telegram edge issues).
        if r.status_code == 429 or 500 <= r.status_code < 600:
            retry_after = 0
            try:
                payload_hint = r.json()
                params = payload_hint.get("parameters") or {}
                retry_after = int(params.get("retry_after") or 0)
            except Exception:
                retry_after = 0

            if attempt < max_attempts:
                wait_sec = max(0.8 * attempt, float(retry_after))
                time.sleep(wait_sec)
                continue

            r.raise_for_status()

        try:
            payload = r.json()
        except Exception as e:
            raise RuntimeError(f"Telegram API returned non-JSON response for {method}: {e}")

        if payload.get("ok", False):
            return payload

        description = payload.get("description") or "Unknown Telegram API error"
        error_code = payload.get("error_code")
        params = payload.get("parameters") or {}

        # Telegram returns 400 when the edited text/markup is identical.
        # This should not break the surrounding business flow.
        if (
            method == "editMessageText"
            and error_code == 400
            and "message is not modified" in str(description).lower()
        ):
            return payload

        if error_code == 429 and attempt < max_attempts:
            retry_after = int(params.get("retry_after") or 0)
            wait_sec = max(0.8 * attempt, float(retry_after))
            time.sleep(wait_sec)
            continue

        raise RuntimeError(
            f"Telegram API error in {method}: "
            f"error_code={error_code}, description={description}, parameters={params}"
        )

    raise RuntimeError(f"Telegram API request failed for {method}: {last_exc}")


def tg_send_message(chat_id: str, text: str, reply_markup: Optional[dict] = None, parse_mode: str = "HTML"):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    data = tg_post("sendMessage", payload)
    return (data.get("result") or {}).get("message_id")


def tg_edit_message(chat_id: str, message_id: str, text: str, reply_markup: Optional[dict] = None, parse_mode: str = "HTML"):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    tg_post("editMessageText", payload)


def tg_answer_callback(callback_query_id: str, text: str = ""):
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    tg_post("answerCallbackQuery", payload)


def tg_send_photo(chat_id: str, file_id: str, caption: Optional[str] = None, reply_markup: Optional[dict] = None, parse_mode: str = "HTML"):
    payload = {
        "chat_id": chat_id,
        "photo": file_id,
    }
    if caption:
        payload["caption"] = caption
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    data = tg_post("sendPhoto", payload)
    return (data.get("result") or {}).get("message_id")
