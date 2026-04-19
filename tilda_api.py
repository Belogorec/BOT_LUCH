from flask import abort, request

from application import execute_tilda_booking_webhook
from config import TILDA_SECRET
from db import connect
from integration.inbound.tilda_parser import parse_tilda_booking_payload


def ensure_db():
    return connect()


def tilda_webhook_impl(normalize_name, normalize_phone_e164, normalize_time_hhmm):
    key = (request.args.get("key") or "").strip()
    if not TILDA_SECRET or key != TILDA_SECRET:
        abort(403)

    payload = request.get_json(silent=True)
    if payload is None:
        payload = request.form.to_dict(flat=True)
    payload = payload or {}
    parsed = parse_tilda_booking_payload(
        payload,
        normalize_name=normalize_name,
        normalize_phone_e164=normalize_phone_e164,
        normalize_time_hhmm=normalize_time_hhmm,
    )

    conn = ensure_db()
    try:
        return execute_tilda_booking_webhook(
            conn,
            payload=parsed["payload"],
            name=parsed["name"],
            phone_raw=parsed["phone_raw"],
            phone_e164=parsed["phone_e164"],
            date_raw=parsed["date_raw"],
            time_raw=parsed["time_raw"],
            guests_count=parsed["guests_count"],
            comment=parsed["comment"],
            tranid=parsed["tranid"],
            formname=parsed["formname"],
            utm_source=parsed["utm_source"],
            utm_medium=parsed["utm_medium"],
            utm_campaign=parsed["utm_campaign"],
            utm_content=parsed["utm_content"],
            utm_term=parsed["utm_term"],
        )
    finally:
        conn.close()
