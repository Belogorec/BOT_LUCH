import json
import sqlite3
from typing import Any

from telegram_api import tg_edit_message, tg_send_message
from vk_api import vk_send_message


def _load_outbox_row(conn: sqlite3.Connection, outbox_id: int):
    return conn.execute(
        """
        SELECT
            id,
            reservation_id,
            platform,
            bot_scope,
            target_external_id,
            message_type,
            payload_json,
            delivery_status,
            attempts
        FROM bot_outbox
        WHERE id=?
        LIMIT 1
        """,
        (int(outbox_id),),
    ).fetchone()


def _parse_payload(payload_json: str) -> dict[str, Any]:
    try:
        payload = json.loads(payload_json or "{}")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _extract_text(payload: dict[str, Any]) -> str:
    text = str(payload.get("text") or "").strip()
    if text:
        return text
    return str(payload.get("message") or "").strip()


def _extract_reply_markup(payload: dict[str, Any]) -> dict[str, Any] | None:
    value = payload.get("reply_markup")
    if isinstance(value, dict):
        return value
    return None


def _extract_keyboard(payload: dict[str, Any]) -> dict[str, Any] | None:
    value = payload.get("keyboard")
    if isinstance(value, dict):
        return value
    return None


def _dispatch_telegram(target_external_id: str, payload: dict[str, Any]) -> str:
    chat_id = str(target_external_id or "").strip()
    text = _extract_text(payload)
    reply_markup = _extract_reply_markup(payload)
    message_id = str(payload.get("message_id") or "").strip()
    if message_id:
        tg_edit_message(chat_id, message_id, text, reply_markup)
        return message_id
    message_id = tg_send_message(chat_id, text, reply_markup)
    return str(message_id or "")


def _dispatch_vk(target_external_id: str, payload: dict[str, Any], *, bot_scope: str) -> str:
    response = vk_send_message(
        int(str(target_external_id or "").strip()),
        _extract_text(payload),
        bot_key=str(bot_scope or "").strip() or "hostess",
        keyboard=_extract_keyboard(payload),
    )
    if isinstance(response, dict):
        value = response.get("message_id") or response.get("conversation_message_id") or response
        return str(value)
    return str(response or "")


def dispatch_outbox_message(conn: sqlite3.Connection, outbox_id: int) -> dict[str, Any]:
    row = _load_outbox_row(conn, outbox_id)
    if not row:
        return {"ok": False, "error": "outbox_not_found", "outbox_id": int(outbox_id)}

    payload = _parse_payload(row["payload_json"])
    platform = str(row["platform"] or "").strip().lower()
    bot_scope = str(row["bot_scope"] or "").strip()
    target_external_id = str(row["target_external_id"] or "").strip()
    text = _extract_text(payload)
    message_type = str(row["message_type"] or "").strip()

    if not target_external_id:
        error = "target_external_id_missing"
    elif not text:
        error = "message_text_missing"
    else:
        error = ""

    attempts = int(row["attempts"] or 0) + 1

    if error:
        conn.execute(
            """
            UPDATE bot_outbox
            SET delivery_status='failed',
                attempts=?,
                last_error=?,
                sent_at=datetime('now')
            WHERE id=?
            """,
            (attempts, error, int(outbox_id)),
        )
        return {"ok": False, "error": error, "outbox_id": int(outbox_id)}

    try:
        if platform == "telegram":
            provider_message_id = _dispatch_telegram(target_external_id, payload)
        elif platform == "vk":
            provider_message_id = _dispatch_vk(target_external_id, payload, bot_scope=bot_scope)
        else:
            raise ValueError(f"unsupported_platform:{platform or '-'}")
    except Exception as exc:
        conn.execute(
            """
            UPDATE bot_outbox
            SET delivery_status='failed',
                attempts=?,
                last_error=?,
                sent_at=datetime('now')
            WHERE id=?
            """,
            (attempts, str(exc), int(outbox_id)),
        )
        return {"ok": False, "error": str(exc), "outbox_id": int(outbox_id)}

    conn.execute(
        """
        UPDATE bot_outbox
        SET delivery_status='sent',
            attempts=?,
            last_error=NULL,
            sent_at=datetime('now')
        WHERE id=?
        """,
        (attempts, int(outbox_id)),
    )
    return {
        "ok": True,
        "outbox_id": int(outbox_id),
        "provider_message_id": provider_message_id,
        "platform": platform,
        "bot_scope": bot_scope,
        "message_type": message_type,
    }
