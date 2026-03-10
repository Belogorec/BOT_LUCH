import json
import requests

from config import TG_API

session = requests.Session()
session.trust_env = True


def tg_post(method: str, data: dict):
    if not TG_API:
        raise RuntimeError("BOT_TOKEN missing")

    r = session.post(f"{TG_API}/{method}", data=data, timeout=25)
    r.raise_for_status()

    try:
        payload = r.json()
    except Exception as e:
        raise RuntimeError(f"Telegram API returned non-JSON response for {method}: {e}")

    if not payload.get("ok", False):
        description = payload.get("description") or "Unknown Telegram API error"
        error_code = payload.get("error_code")
        params = payload.get("parameters") or {}
        raise RuntimeError(
            f"Telegram API error in {method}: "
            f"error_code={error_code}, description={description}, parameters={params}"
        )

    return payload


def tg_send_message(chat_id: str, text: str, reply_markup: dict | None = None, parse_mode: str = "HTML"):
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


def tg_edit_message(chat_id: str, message_id: str, text: str, reply_markup: dict | None = None, parse_mode: str = "HTML"):
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


def tg_send_photo(chat_id: str, file_id: str, caption: str | None = None, reply_markup: dict | None = None, parse_mode: str = "HTML"):
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