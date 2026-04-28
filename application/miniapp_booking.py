import json
from typing import Any

import crm_commands
from booking_render import render_booking_card
from booking_service import create_telegram_miniapp_booking_record, log_booking_event
from config import CRM_AUTHORITATIVE, TG_CHAT_ID
from core_sync import sync_booking_to_core
from crm_sync import send_booking_event
from integration_service import create_outbox_message
from hostess_card_delivery import dispatch_hostess_booking_card
from vk_staff_notify import notify_vk_staff_about_new_booking


def execute_telegram_miniapp_booking(
    conn,
    *,
    tg_user_id: str,
    date_value: str,
    time_value: str,
    guests_count: int,
    comment_value: str,
    reservation_token: str,
) -> dict[str, Any]:
    user_row = conn.execute(
        "SELECT phone_e164, first_name FROM tg_bot_users WHERE tg_user_id=? AND has_shared_phone=1",
        (tg_user_id,),
    ).fetchone()
    phone_e164 = user_row["phone_e164"] if user_row else None
    saved_name = (user_row["first_name"] if user_row else None) or ""

    raw_payload = json.dumps(
        {
            "source": "telegram_miniapp_api",
            "requester_tg_user_id": tg_user_id,
            "requester_chat_id": tg_user_id,
            "requester_name": saved_name,
            "reservation_token": reservation_token,
            "date": date_value,
            "time": time_value,
            "guests": guests_count,
            "comment": comment_value,
        },
        ensure_ascii=False,
    )

    if CRM_AUTHORITATIVE:
        command_payload = {
            "source": "telegram_miniapp_api",
            "external_ref": reservation_token,
            "reservation_token": reservation_token,
            "guest_name": saved_name or "Telegram",
            "guest_phone": phone_e164 or "",
            "reservation_date": date_value,
            "reservation_time": time_value,
            "guests_count": guests_count,
            "comment": comment_value,
            "requester_tg_user_id": tg_user_id,
            "requester_chat_id": tg_user_id,
        }
        result = crm_commands.create_reservation(
            payload=command_payload,
            event_id=f"telegram_miniapp:{reservation_token}",
            actor={"id": str(tg_user_id or "telegram_miniapp"), "name": saved_name or "telegram_miniapp_api"},
        )
        reservation = result.get("reservation") or {}
        return {
            "ok": bool(result.get("accepted")),
            "booking_id": int(reservation.get("booking_id") or reservation.get("reservation_id") or 0),
            "reservation_id": int(reservation.get("reservation_id") or 0),
            "duplicate": bool(result.get("duplicate") or (result.get("body") or {}).get("result", {}).get("duplicate")),
            "error": "" if result.get("accepted") else str(result.get("error") or "crm_command_failed"),
        }

    create_result = create_telegram_miniapp_booking_record(
        conn,
        tg_user_id=tg_user_id,
        date_value=date_value,
        time_value=time_value,
        guests_count=guests_count,
        comment_value=comment_value,
        reservation_token=reservation_token,
        phone_e164=phone_e164,
        display_name=saved_name or "Telegram",
        raw_payload_json=raw_payload,
        source="telegram_miniapp_api",
    )
    booking_id = int(create_result["booking_id"])
    if create_result.get("duplicate"):
        return {"ok": True, "booking_id": booking_id, "duplicate": True}

    core_reservation_id = sync_booking_to_core(conn, booking_id)
    create_outbox_message(
        conn,
        reservation_id=core_reservation_id,
        platform="telegram",
        bot_scope="hostess",
        target_external_id=str(TG_CHAT_ID or "").strip() or None,
        message_type="reservation_created",
        payload={"legacy_booking_id": booking_id, "source": "telegram_miniapp_api"},
    )

    if TG_CHAT_ID:
        card_text, kb = render_booking_card(conn, booking_id)
        try:
            dispatch_result = dispatch_hostess_booking_card(
                conn,
                booking_id=booking_id,
                reservation_id=core_reservation_id,
                chat_id=str(TG_CHAT_ID),
                text=card_text,
                reply_markup=kb,
            )
            if dispatch_result.get("ok"):
                log_booking_event(conn, booking_id, "TG_SYNC_OK", "system", "system", {"target_chat_id": str(TG_CHAT_ID)})
            else:
                log_booking_event(
                    conn,
                    booking_id,
                    "TG_SYNC_FAIL",
                    "system",
                    "system",
                    {"error": str(dispatch_result.get("error") or "dispatch_failed")},
                )
        except Exception as exc:
            log_booking_event(conn, booking_id, "TG_SYNC_FAIL", "system", "system", {"error": str(exc)})

    try:
        sent_count = notify_vk_staff_about_new_booking(conn, booking_id, source="telegram_miniapp_api")
        if sent_count:
            log_booking_event(conn, booking_id, "VK_STAFF_SYNC_OK", "system", "system", {"sent_count": sent_count})
    except Exception as exc:
        log_booking_event(conn, booking_id, "VK_STAFF_SYNC_FAIL", "system", "system", {"error": str(exc)})

    try:
        sync_ok = send_booking_event(
            conn,
            booking_id,
            "BOOKING_UPSERT",
            {
                "actor_tg_id": tg_user_id or "system",
                "actor_name": saved_name or "telegram_miniapp_api",
                "payload": {"source": "telegram_miniapp_api"},
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
                {"source": "telegram_miniapp_api", "reason": "send_booking_event_false"},
            )
    except Exception as exc:
        log_booking_event(
            conn,
            booking_id,
            "CRM_SYNC_FAIL",
            "system",
            "system",
            {"source": "telegram_miniapp_api", "reason": str(exc)},
        )

    return {"ok": True, "booking_id": booking_id}
