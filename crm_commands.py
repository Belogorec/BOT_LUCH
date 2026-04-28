import json
from typing import Any, Optional

import requests

from config import CRM_COMMAND_API_KEY, CRM_COMMAND_API_URL, CRM_COMMAND_TIMEOUT


class CrmCommandError(RuntimeError):
    pass


def _base_url() -> str:
    value = str(CRM_COMMAND_API_URL or "").strip().rstrip("/")
    if not value:
        raise CrmCommandError("crm_command_api_url_missing")
    return value


def _headers(event_id: str = "") -> dict[str, str]:
    if not CRM_COMMAND_API_KEY:
        raise CrmCommandError("crm_command_api_key_missing")
    headers = {
        "Content-Type": "application/json",
        "X-CRM-Command-Key": CRM_COMMAND_API_KEY,
    }
    if event_id:
        headers["X-Idempotency-Key"] = event_id
    return headers


def _normalize_response(status_code: int, body: Any) -> dict[str, Any]:
    payload = body if isinstance(body, dict) else {}
    ok = bool(payload.get("ok"))
    return {
        "ok": ok,
        "accepted": ok,
        "duplicate": bool(payload.get("duplicate")),
        "status_code": int(status_code or 0),
        "error": "" if ok else str(payload.get("error") or f"http_{status_code}"),
        "reservation": payload.get("reservation") if isinstance(payload.get("reservation"), dict) else {},
        "body": payload,
    }


def send_command(
    *,
    method: str,
    path: str,
    payload: Optional[dict[str, Any]] = None,
    event_id: str = "",
) -> dict[str, Any]:
    body = dict(payload or {})
    if event_id and not body.get("event_id"):
        body["event_id"] = event_id
    event_key = str(body.get("event_id") or event_id or "").strip()
    url = f"{_base_url()}{path}"
    try:
        resp = requests.request(
            method.upper(),
            url,
            headers=_headers(event_key),
            data=json.dumps(body, ensure_ascii=False),
            timeout=max(3, int(CRM_COMMAND_TIMEOUT or 8)),
        )
        try:
            parsed = resp.json()
        except ValueError:
            parsed = {}
        return _normalize_response(resp.status_code, parsed)
    except CrmCommandError:
        raise
    except requests.RequestException as exc:
        return {
            "ok": False,
            "accepted": False,
            "duplicate": False,
            "status_code": 0,
            "error": str(exc),
            "body": {},
        }


def reservation_status(reservation_id: int, *, status: str, event_id: str, actor: dict[str, Any]) -> dict[str, Any]:
    return send_command(
        method="POST",
        path=f"/api/commands/reservations/{int(reservation_id)}/status",
        event_id=event_id,
        payload={"status": status, "actor": actor},
    )


def create_reservation(*, payload: dict[str, Any], event_id: str, actor: dict[str, Any]) -> dict[str, Any]:
    body = dict(payload or {})
    body["actor"] = actor
    return send_command(
        method="POST",
        path="/api/commands/reservations",
        event_id=event_id,
        payload=body,
    )


def assign_table(
    reservation_id: int,
    *,
    table_number: str,
    guests_count: int,
    guest_name: str = "",
    guest_phone: str = "",
    event_id: str,
    actor: dict[str, Any],
) -> dict[str, Any]:
    return send_command(
        method="POST",
        path=f"/api/commands/reservations/{int(reservation_id)}/assign-table",
        event_id=event_id,
        payload={
            "table_number": table_number,
            "guests_count": guests_count,
            "guest_name": guest_name,
            "guest_phone": guest_phone,
            "actor": actor,
        },
    )


def clear_table(reservation_id: int, *, event_id: str, actor: dict[str, Any]) -> dict[str, Any]:
    return send_command(
        method="POST",
        path=f"/api/commands/reservations/{int(reservation_id)}/clear-table",
        event_id=event_id,
        payload={"actor": actor},
    )


def set_deposit(
    reservation_id: int,
    *,
    amount: int,
    comment: str = "",
    event_id: str,
    actor: dict[str, Any],
) -> dict[str, Any]:
    return send_command(
        method="POST",
        path=f"/api/commands/reservations/{int(reservation_id)}/deposit",
        event_id=event_id,
        payload={"deposit_amount": amount, "deposit_comment": comment, "actor": actor},
    )


def clear_deposit(reservation_id: int, *, event_id: str, actor: dict[str, Any]) -> dict[str, Any]:
    return send_command(
        method="DELETE",
        path=f"/api/commands/reservations/{int(reservation_id)}/deposit",
        event_id=event_id,
        payload={"actor": actor},
    )


def restrict_table(
    table_number: str,
    *,
    restricted_until: str,
    event_id: str,
    actor: dict[str, Any],
    reservation_id: int = 0,
    comment: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "restricted_until": restricted_until,
        "table_comment": comment,
        "actor": actor,
    }
    if reservation_id:
        payload["reservation_id"] = int(reservation_id)
    return send_command(
        method="POST",
        path=f"/api/commands/tables/{table_number}/restriction",
        event_id=event_id,
        payload=payload,
    )


def clear_table_restriction(table_number: str, *, event_id: str, actor: dict[str, Any]) -> dict[str, Any]:
    return send_command(
        method="DELETE",
        path=f"/api/commands/tables/{table_number}/restriction",
        event_id=event_id,
        payload={"actor": actor},
    )


def restrict_reservation_table(
    reservation_id: int,
    *,
    table_number: str,
    restricted_until: str,
    event_id: str,
    actor: dict[str, Any],
    force_override: bool = False,
    comment: str = "",
) -> dict[str, Any]:
    return send_command(
        method="POST",
        path=f"/api/commands/reservations/{int(reservation_id)}/restriction",
        event_id=event_id,
        payload={
            "table_number": table_number,
            "restricted_until": restricted_until,
            "table_comment": comment,
            "force_override": bool(force_override),
            "actor": actor,
        },
    )


def clear_reservation_table_restriction(
    reservation_id: int,
    *,
    table_number: str = "",
    event_id: str,
    actor: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {"actor": actor}
    if table_number:
        payload["table_number"] = table_number
    return send_command(
        method="DELETE",
        path=f"/api/commands/reservations/{int(reservation_id)}/restriction",
        event_id=event_id,
        payload=payload,
    )
