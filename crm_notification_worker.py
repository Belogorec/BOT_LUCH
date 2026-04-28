import re
from typing import Any

import requests

from booking_render import render_booking_card_from_reservation
from config import CRM_COMMAND_API_KEY, CRM_COMMAND_API_URL, CRM_COMMAND_TIMEOUT
from telegram_api import tg_edit_message, tg_send_message
from vk_api import vk_send_message


class CrmNotificationError(RuntimeError):
    pass


def _base_url() -> str:
    value = str(CRM_COMMAND_API_URL or "").strip().rstrip("/")
    if not value:
        raise CrmNotificationError("crm_command_api_url_missing")
    return value


def _headers() -> dict[str, str]:
    if not CRM_COMMAND_API_KEY:
        raise CrmNotificationError("crm_command_api_key_missing")
    return {
        "Content-Type": "application/json",
        "X-CRM-Command-Key": CRM_COMMAND_API_KEY,
    }


def claim_notifications(*, limit: int = 50, max_attempts: int = 5) -> list[dict[str, Any]]:
    response = requests.post(
        f"{_base_url()}/api/notification-outbox/claim",
        headers=_headers(),
        json={"limit": int(limit or 50), "max_attempts": int(max_attempts or 5)},
        timeout=max(3, int(CRM_COMMAND_TIMEOUT or 8)),
    )
    response.raise_for_status()
    body = response.json()
    jobs = body.get("jobs") if isinstance(body, dict) else []
    return jobs if isinstance(jobs, list) else []


def complete_notification(job_id: int, *, ok: bool, provider_message_id: str = "", error: str = "") -> None:
    response = requests.post(
        f"{_base_url()}/api/notification-outbox/{int(job_id)}/complete",
        headers=_headers(),
        json={
            "ok": bool(ok),
            "provider_message_id": str(provider_message_id or ""),
            "error": str(error or ""),
        },
        timeout=max(3, int(CRM_COMMAND_TIMEOUT or 8)),
    )
    response.raise_for_status()


def _plain_text(html_text: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", str(html_text or ""), flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text


def dispatch_notification_job(job: dict[str, Any]) -> str:
    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
    message_type = str(job.get("message_type") or "").strip()
    platform = str(job.get("platform") or "").strip().lower()
    bot_scope = str(job.get("bot_scope") or "").strip() or "hostess"
    target = str(job.get("target_external_id") or "").strip()

    if message_type != "reservation_card_upsert":
        raise ValueError(f"unsupported_notification_type:{message_type or '-'}")
    reservation = payload.get("reservation") if isinstance(payload.get("reservation"), dict) else {}
    text, reply_markup = render_booking_card_from_reservation(reservation)
    message_id = str(payload.get("message_id") or "").strip()

    if platform == "telegram":
        if message_id:
            tg_edit_message(target, message_id, text, reply_markup)
            return message_id
        return str(tg_send_message(target, text, reply_markup) or "")

    if platform == "vk":
        response = vk_send_message(
            int(target),
            _plain_text(text),
            bot_key=bot_scope,
        )
        if isinstance(response, dict):
            return str(response.get("message_id") or response.get("conversation_message_id") or "")
        return str(response or "")

    raise ValueError(f"unsupported_platform:{platform or '-'}")


def process_crm_notification_batch(*, limit: int = 50, max_attempts: int = 5) -> dict[str, Any]:
    jobs = claim_notifications(limit=limit, max_attempts=max_attempts)
    sent = 0
    failed = 0
    results = []
    for job in jobs:
        job_id = int(job.get("id") or 0)
        try:
            provider_message_id = dispatch_notification_job(job)
            complete_notification(job_id, ok=True, provider_message_id=provider_message_id)
            sent += 1
            results.append({"id": job_id, "ok": True, "provider_message_id": provider_message_id})
        except Exception as exc:
            complete_notification(job_id, ok=False, error=str(exc))
            failed += 1
            results.append({"id": job_id, "ok": False, "error": str(exc)})
    return {"ok": True, "count": len(jobs), "sent": sent, "failed": failed, "results": results}
