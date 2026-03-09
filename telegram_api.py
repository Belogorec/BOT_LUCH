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
    return r.json()


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