from __future__ import annotations

import re
from typing import Any, Callable


def _normalize_payload_key(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("ё", "е")
    return re.sub(r"[^0-9a-zа-я]+", "", normalized)


def parse_tilda_booking_payload(
    payload: dict[str, Any],
    *,
    normalize_name: Callable[[str], str],
    normalize_phone_e164: Callable[..., str],
    normalize_time_hhmm: Callable[[str], str],
) -> dict[str, Any]:
    payload = payload or {}

    normalized_payload: dict[str, Any] = {}
    for key, value in payload.items():
        normalized_key = _normalize_payload_key(key)
        if normalized_key and normalized_key not in normalized_payload:
            normalized_payload[normalized_key] = value

    def pick(*keys: str, default: str = "") -> str:
        for key in keys:
            value = payload.get(key)
            if value is None:
                value = normalized_payload.get(_normalize_payload_key(key))
            if value is not None:
                text = str(value).strip()
                if text:
                    return text
        return default

    def pick_int_from_text(*keys: str) -> int | None:
        raw = pick(*keys, default="")
        if not raw:
            return None
        match = re.search(r"\d+", raw)
        if not match:
            return None
        value = int(match.group(0))
        return value if 1 <= value <= 50 else None

    name = normalize_name(
        pick(
            "Name", "name", "NAME",
            "Имя", "имя",
            default="",
        )
    )

    phone_raw = pick(
        "Phone", "phone", "PHONE",
        "Телефон", "телефон",
        "Mobile", "mobile",
        default="",
    )
    phone_e164 = normalize_phone_e164(phone_raw, default_region="RU")

    date_raw = pick(
        "date", "Date", "DATE",
        "reservation_date", "Reservation date",
        "Дата", "дата",
        default="",
    ).strip()

    time_raw_src = pick(
        "time", "Time", "TIME",
        "reservation_time", "Reservation time",
        "Время", "время",
        default="",
    ).strip()
    time_raw = normalize_time_hhmm(time_raw_src)

    guests_count = pick_int_from_text(
        "amountofguests", "guests", "Guests", "guests_count", "guestscount",
        "guest_count", "guestcount", "number_of_guests", "guests_number",
        "persons", "people", "qty", "count",
        "kolichestvogostey", "kolichestvogostei",
        "Количество гостей", "количество гостей",
        "Количествогостей", "количествогостеи", "количествогостеи",
        "Гостей", "гостей",
    )

    if guests_count is None:
        for key, value in payload.items():
            key_norm = _normalize_payload_key(key)
            if not key_norm:
                continue

            looks_like_guest_count = (
                key_norm in {
                    "гостей",
                    "гости",
                    "guests",
                    "guestscount",
                    "guestcount",
                    "persons",
                    "people",
                }
                or ("guest" in key_norm and any(token in key_norm for token in ("count", "qty", "amount", "number", "num")))
                or ("гост" in key_norm and any(token in key_norm for token in ("кол", "числ", "сколь")))
            )
            if not looks_like_guest_count:
                continue

            raw_val = str(value or "").strip()
            match = re.search(r"\d+", raw_val)
            if not match:
                continue
            parsed = int(match.group(0))
            if 1 <= parsed <= 50:
                guests_count = parsed
                break

    comment = pick(
        "comment", "Comment", "Comments",
        "Комментарий", "комментарий",
        "Комментарий к бронированию", "commentary",
        default="",
    )

    tranid = pick("tranid", "Tranid", "TRANID", default="")
    formname = pick("formname", "Formname", "FORMNAME", default="Бронь стола")

    utm_source = pick("utm_source", default="")
    utm_medium = pick("utm_medium", default="")
    utm_campaign = pick("utm_campaign", default="")
    utm_content = pick("utm_content", default="")
    utm_term = pick("utm_term", default="")

    return {
        "payload": payload,
        "name": name,
        "phone_raw": phone_raw,
        "phone_e164": phone_e164,
        "date_raw": date_raw,
        "time_raw": time_raw,
        "guests_count": guests_count,
        "comment": comment,
        "tranid": tranid,
        "formname": formname,
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": utm_campaign,
        "utm_content": utm_content,
        "utm_term": utm_term,
    }
