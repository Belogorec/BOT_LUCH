from typing import Optional
from urllib.parse import urlsplit, urlunsplit

from config import CRM_API_URL, CRM_COMMAND_API_URL


ARCHIVED_BOOKING_STATUSES = {"declined", "cancelled", "no_show", "completed"}


def crm_staff_booking_link(reservation: Optional[dict] = None, *, archived: bool = False) -> tuple[str, str]:
    """Return a label and a CRM URL without API credentials or guest data."""
    for configured_url in (CRM_COMMAND_API_URL, CRM_API_URL):
        try:
            endpoint = urlsplit(configured_url)
            if endpoint.scheme not in {"http", "https"} or not endpoint.hostname:
                continue
            origin = urlunsplit((endpoint.scheme, endpoint.netloc.rsplit("@", 1)[-1], "", "", ""))
        except ValueError:
            continue
        break
    else:
        return "", ""

    if reservation:
        reservation_id = str(reservation.get("reservation_id") or "")
        booking_id = str(reservation.get("booking_id") or reservation_id)
        archived = str(reservation.get("status") or "").strip().lower() in ARCHIVED_BOOKING_STATUSES
        if reservation_id.isdecimal() and int(reservation_id) > 0 and booking_id.isdecimal() and int(booking_id) > 0:
            label = "Восстановить в CRM" if archived else "Открыть в CRM"
            return label, f"{origin}/crm/bookings/{int(booking_id)}?reservation_id={int(reservation_id)}#restore-booking"

    return ("Восстановить в CRM" if archived else "Архив CRM"), f"{origin}/crm/archive"


def append_crm_staff_booking_link(text: str, reservation: Optional[dict] = None, *, archived: bool = False) -> str:
    label, url = crm_staff_booking_link(reservation, archived=archived)
    return f"{text}\n\n{label}: {url}" if url else text
