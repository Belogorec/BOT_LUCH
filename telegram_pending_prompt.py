from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Optional

from booking_service import resolve_core_reservation_id
from integration_service import create_outbox_message, mark_inbound_event_processed, record_inbound_event
from outbox_dispatcher import dispatch_outbox_message


TG_PROMPT_OUTBOX_TYPE = "telegram_prompt"


def _resolve_core_reservation_id(conn, booking_id: int) -> Optional[int]:
    return resolve_core_reservation_id(conn, int(booking_id or 0), allow_booking_sync=True)


def _send_telegram_prompt(
    conn,
    *,
    chat_id: str,
    text: str,
    reply_markup: Optional[dict[str, Any]] = None,
    bot_scope: str = "hostess",
) -> str:
    payload: dict[str, Any] = {"text": str(text or "").strip()}
    if isinstance(reply_markup, dict) and reply_markup:
        payload["reply_markup"] = reply_markup
    outbox_id = create_outbox_message(
        conn,
        reservation_id=None,
        platform="telegram",
        bot_scope=str(bot_scope or "").strip() or "hostess",
        message_type=TG_PROMPT_OUTBOX_TYPE,
        payload=payload,
        target_external_id=str(chat_id or "").strip(),
    )
    result = dispatch_outbox_message(conn, outbox_id)
    if not result.get("ok"):
        raise RuntimeError(str(result.get("error") or "telegram_prompt_dispatch_failed"))
    return str(result.get("provider_message_id") or "")


def _mark_active_pending_prompts(
    conn,
    *,
    event_type: str,
    chat_id: str,
    actor_id: str,
    bot_scope: str = "hostess",
    status: str,
    error_text: str = "",
) -> None:
    rows = conn.execute(
        """
        SELECT id
        FROM bot_inbound_events
        WHERE platform = 'telegram'
          AND bot_scope = ?
          AND event_type = ?
          AND actor_external_id = ?
          AND peer_external_id = ?
          AND processing_status = 'pending'
        ORDER BY id ASC
        """,
        (str(bot_scope or "").strip() or "hostess", str(event_type or ""), str(actor_id or ""), str(chat_id or "")),
    ).fetchall()
    for row in rows:
        mark_inbound_event_processed(conn, int(row["id"]), status=status, error_text=error_text)


def start_pending_prompt(
    conn,
    *,
    event_type: str,
    chat_id: str,
    actor_id: str,
    booking_id: int,
    payload: Optional[dict[str, Any]],
    prompt_text: str,
    reply_markup: Optional[dict[str, Any]] = None,
    ttl_minutes: int = 10,
    bot_scope: str = "hostess",
) -> int:
    prompt_message_id = _send_telegram_prompt(
        conn,
        chat_id=chat_id,
        text=prompt_text,
        reply_markup=reply_markup,
        bot_scope=bot_scope,
    )
    _mark_active_pending_prompts(
        conn,
        event_type=event_type,
        chat_id=chat_id,
        actor_id=actor_id,
        bot_scope=bot_scope,
        status="superseded",
        error_text="replaced_by_newer_prompt",
    )
    final_payload = dict(payload or {})
    final_payload["booking_id"] = int(booking_id or 0)
    final_payload["prompt_message_id"] = prompt_message_id
    final_payload["expires_at"] = (datetime.utcnow() + timedelta(minutes=max(1, int(ttl_minutes or 10)))).isoformat(
        timespec="seconds"
    )
    event_id = record_inbound_event(
        conn,
        platform="telegram",
        bot_scope=str(bot_scope or "").strip() or "hostess",
        event_type=str(event_type or "").strip(),
        payload=final_payload,
        actor_external_id=str(actor_id or ""),
        actor_display_name="",
        peer_external_id=str(chat_id or ""),
        reservation_id=_resolve_core_reservation_id(conn, int(booking_id or 0)),
    )
    conn.execute(
        """
        UPDATE bot_inbound_events
        SET processing_status = 'pending',
            error_text = NULL,
            processed_at = NULL
        WHERE id = ?
        """,
        (int(event_id),),
    )
    return int(event_id)


def load_pending_prompt(
    conn,
    *,
    event_type: str,
    chat_id: str,
    actor_id: str,
    bot_scope: str = "hostess",
):
    row = conn.execute(
        """
        SELECT id, reservation_id, payload_json
        FROM bot_inbound_events
        WHERE platform = 'telegram'
          AND bot_scope = ?
          AND event_type = ?
          AND actor_external_id = ?
          AND peer_external_id = ?
          AND processing_status = 'pending'
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(bot_scope or "").strip() or "hostess", str(event_type or "").strip(), str(actor_id or ""), str(chat_id or "")),
    ).fetchone()
    if not row:
        return None, {}
    try:
        payload = json.loads(row["payload_json"] or "{}")
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    try:
        expires_at = datetime.fromisoformat(str(payload.get("expires_at") or ""))
        if datetime.utcnow() > expires_at:
            mark_inbound_event_processed(conn, int(row["id"]), status="expired", error_text="expired_before_use")
            return None, {}
    except Exception:
        pass
    return row, payload


def complete_pending_prompt(conn, event_id: int, *, status: str = "processed", error_text: str = "") -> None:
    mark_inbound_event_processed(conn, int(event_id), status=status, error_text=error_text)
