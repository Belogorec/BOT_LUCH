import json
from typing import Any, Optional

from booking_render import render_booking_card
from booking_service import (
    compute_segment,
    log_booking_event,
    upsert_guest_if_missing,
    upsert_tilda_booking_record,
)
from config import TG_CHAT_ID
from core_sync import sync_booking_to_core
from crm_sync import send_booking_event
from db import get_tags
from integration_service import create_outbox_message
from hostess_card_delivery import dispatch_hostess_booking_card, get_hostess_card_link
from vk_staff_notify import notify_vk_staff_about_new_booking
from channel_binding_service import build_guest_page_public_url


def execute_tilda_booking_webhook(
    conn,
    *,
    payload: dict[str, Any],
    name: str,
    phone_raw: str,
    phone_e164: str,
    date_raw: str,
    time_raw: str,
    guests_count: Optional[int],
    comment: str,
    tranid: str,
    formname: str,
    utm_source: str,
    utm_medium: str,
    utm_campaign: str,
    utm_content: str,
    utm_term: str,
) -> dict[str, Any]:
    reservation_dt = f"{date_raw}T{time_raw}" if (date_raw and time_raw) else ""
    booking_id = None
    tg_status = "skipped"
    reservation_token = ""

    if phone_e164:
        upsert_guest_if_missing(conn, phone_e164, name)

    tags = get_tags(conn, phone_e164) if phone_e164 else []
    g_row = conn.execute(
        "SELECT visits_count FROM guests WHERE phone_e164=?",
        (phone_e164,),
    ).fetchone() if phone_e164 else None
    visits_count = int(g_row["visits_count"] or 0) if g_row else 0
    guest_segment = compute_segment(visits_count, tags)

    upsert_result = upsert_tilda_booking_record(
        conn,
        payload_json=json.dumps(payload, ensure_ascii=False),
        name=name,
        phone_e164=phone_e164,
        phone_raw=phone_raw,
        date_raw=date_raw,
        time_raw=time_raw,
        reservation_dt=reservation_dt,
        guests_count=guests_count,
        comment=comment,
        tranid=tranid,
        formname=formname,
        utm_source=utm_source,
        utm_medium=utm_medium,
        utm_campaign=utm_campaign,
        utm_content=utm_content,
        utm_term=utm_term,
        guest_segment=guest_segment,
        source="tilda",
    )
    booking_id = int(upsert_result["booking_id"])
    reservation_token = str(upsert_result["reservation_token"] or "").strip()
    existing = bool(upsert_result["existing"])

    core_reservation_id = sync_booking_to_core(conn, booking_id)
    create_outbox_message(
        conn,
        reservation_id=core_reservation_id,
        platform="telegram",
        bot_scope="hostess",
        target_external_id=str(TG_CHAT_ID or "").strip() or None,
        message_type="reservation_created",
        payload={"legacy_booking_id": booking_id, "source": "tilda"},
    )
    conn.commit()

    try:
        sync_ok = send_booking_event(
            conn,
            booking_id,
            "BOOKING_UPSERT",
            {
                "actor_tg_id": "system",
                "actor_name": "tilda",
                "payload": {"source": "tilda", "tg_status": tg_status},
            },
            dispatch_now=True,
        )
        if not sync_ok:
            log_booking_event(
                conn,
                booking_id,
                "CRM_SYNC_FAIL",
                "system",
                "system",
                {"source": "tilda", "reason": "send_booking_event_false"},
            )
    except Exception:
        log_booking_event(
            conn,
            booking_id,
            "CRM_SYNC_FAIL",
            "system",
            "system",
            {"source": "tilda", "reason": "send_booking_event_exception"},
        )

    try:
        text, kb = render_booking_card(conn, booking_id)
        existing_link = get_hostess_card_link(conn, reservation_id=core_reservation_id) if existing else None

        if existing and existing_link and existing_link["chat_id"] and existing_link["message_id"]:
            dispatch_result = dispatch_hostess_booking_card(
                conn,
                booking_id=booking_id,
                reservation_id=core_reservation_id,
                chat_id=existing_link["chat_id"],
                text=text,
                reply_markup=kb,
                message_id=existing_link["message_id"],
            )
            tg_status = "edited" if dispatch_result.get("ok") else "error"
        else:
            dispatch_result = dispatch_hostess_booking_card(
                conn,
                booking_id=booking_id,
                reservation_id=core_reservation_id,
                chat_id=str(TG_CHAT_ID),
                text=text,
                reply_markup=kb,
            )
            tg_status = "sent" if dispatch_result.get("ok") else "error"

        if dispatch_result.get("ok"):
            log_booking_event(
                conn,
                booking_id,
                "TG_SYNC_OK",
                "system",
                "system",
                {"status": tg_status},
            )
        else:
            log_booking_event(
                conn,
                booking_id,
                "TG_SYNC_ERROR",
                "system",
                "system",
                {"error": str(dispatch_result.get("error") or "dispatch_failed"), "stage": "dispatch_outbox"},
            )
        conn.commit()

    except Exception as exc:
        log_booking_event(
            conn,
            booking_id,
            "TG_SYNC_ERROR",
            "system",
            "system",
            {
                "error": str(exc),
                "stage": "render_or_send",
            },
        )
        conn.commit()
        tg_status = "error"

    try:
        sent_count = notify_vk_staff_about_new_booking(conn, booking_id, source="tilda")
        if sent_count:
            log_booking_event(
                conn,
                booking_id,
                "VK_STAFF_SYNC_OK",
                "system",
                "system",
                {"source": "tilda", "sent_count": sent_count},
            )
            conn.commit()
    except Exception as exc:
        log_booking_event(
            conn,
            booking_id,
            "VK_STAFF_SYNC_FAIL",
            "system",
            "system",
            {"source": "tilda", "error": str(exc)},
        )
        conn.commit()

    return {
        "ok": True,
        "booking_id": booking_id,
        "tg_status": tg_status,
        "guest_page_url": build_guest_page_public_url(reservation_token),
    }
