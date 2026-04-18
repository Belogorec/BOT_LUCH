import json
from datetime import datetime, timedelta
from typing import Any, Optional

from core_sync import sync_booking_state_to_core, sync_booking_to_core
from booking_service import (
    assign_table_to_booking,
    clear_booking_deposit,
    clear_table_assignment,
    ensure_visit_from_confirmed_booking,
    get_table_assignment_conflicts,
    log_booking_event,
    mark_booking_cancelled,
    normalize_table_number,
    parse_restriction_until,
    set_booking_deposit,
    set_table_label,
)
from crm_sync import send_booking_event, send_table_event
from vk_api import vk_send_message
from waiter_notify import notify_waiters_about_deposit_booking
from integration_service import record_inbound_event


def _vk_actor_id(from_id: object) -> str:
    return f"vk:{str(from_id or '').strip() or 'unknown'}"


def _vk_actor_name(from_id: object) -> str:
    return f"vk:{str(from_id or '').strip() or 'unknown'}"


def parse_vk_message_payload(message: dict[str, Any]) -> dict[str, Any]:
    raw = message.get("payload")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def parse_vk_event_payload(event_object: dict[str, Any]) -> dict[str, Any]:
    raw = event_object.get("payload")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _vk_button(label: str, payload: dict[str, Any], color: str = "secondary") -> dict[str, Any]:
    return {
        "action": {
            "type": "callback",
            "label": label,
            "payload": json.dumps(payload, ensure_ascii=False),
        },
        "color": color,
    }


def build_vk_booking_keyboard(booking_id: int) -> dict[str, Any]:
    return {
        "inline": True,
        "buttons": [
            [
                _vk_button("Подтвердить", {"kind": "booking_action", "action": "confirm", "booking_id": booking_id}, "positive"),
                _vk_button("Отменить", {"kind": "booking_action", "action": "cancel", "booking_id": booking_id}, "negative"),
            ],
            [
                _vk_button("Назначить стол", {"kind": "booking_action", "action": "prompt_assign_table", "booking_id": booking_id}),
                _vk_button("Снять стол", {"kind": "booking_action", "action": "clear_table", "booking_id": booking_id}),
            ],
            [
                _vk_button("Депозит", {"kind": "booking_action", "action": "prompt_set_deposit", "booking_id": booking_id}),
                _vk_button("Снять депозит", {"kind": "booking_action", "action": "clear_deposit", "booking_id": booking_id}),
            ],
            [
                _vk_button("Ограничить стол", {"kind": "booking_action", "action": "prompt_restrict_table", "booking_id": booking_id}),
            ],
        ],
    }


def render_vk_booking_message(conn, booking_id: int) -> str:
    row = conn.execute(
        """
        SELECT
            id, status, formname, name, phone_e164, phone_raw,
            reservation_date, reservation_time, guests_count, comment,
            assigned_table_number, deposit_amount, deposit_comment
        FROM bookings
        WHERE id = ?
        """,
        (int(booking_id),),
    ).fetchone()
    if not row:
        return "Бронь не найдена."

    dt_value = " ".join(
        part for part in [str(row["reservation_date"] or "").strip(), str(row["reservation_time"] or "").strip()] if part
    ).strip() or "—"
    guest_name = str(row["name"] or "").strip() or "—"
    guest_phone = str(row["phone_e164"] or row["phone_raw"] or "").strip() or "—"
    comment = str(row["comment"] or "").strip()
    table_number = str(row["assigned_table_number"] or "").strip()
    deposit_amount = row["deposit_amount"]
    deposit_comment = str(row["deposit_comment"] or "").strip()

    lines = [
        "Новая бронь",
        f"ID: #{int(row['id'])}",
        f"Статус: {str(row['status'] or 'WAITING').strip() or 'WAITING'}",
        f"Дата/время: {dt_value}",
        f"Гость: {guest_name}",
        f"Телефон: {guest_phone}",
        f"Гостей: {row['guests_count'] if row['guests_count'] is not None else '—'}",
        f"Источник: {str(row['formname'] or 'booking').strip() or 'booking'}",
    ]
    if table_number:
        lines.append(f"Стол: #{table_number}")
    if deposit_amount:
        deposit_line = f"Депозит: {int(deposit_amount)}"
        if deposit_comment:
            deposit_line += f" ({deposit_comment})"
        lines.append(deposit_line)
    if comment:
        lines.append(f"Комментарий: {comment}")
    lines.append("")
    lines.append("Кнопки ниже управляют бронью прямо из рабочего VK-чата.")
    return "\n".join(lines)


def send_vk_booking_card(conn, peer_id: int, booking_id: int) -> None:
    vk_send_message(int(peer_id), render_vk_booking_message(conn, booking_id), keyboard=build_vk_booking_keyboard(booking_id))


def _save_vk_pending_action(conn, *, peer_id: object, from_id: object, booking_id: int, mode: str, extra: Optional[dict[str, Any]] = None) -> None:
    payload = {"mode": mode, "booking_id": int(booking_id), **(extra or {})}
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO pending_replies (kind, booking_id, phone_e164, chat_id, actor_tg_id, prompt_message_id, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "vk_staff_flow",
            int(booking_id),
            json.dumps(payload, ensure_ascii=False),
            str(peer_id or ""),
            str(from_id or ""),
            f"vk:{mode}",
            expires_at,
        ),
    )


def _load_vk_pending_action(conn, *, peer_id: object, from_id: object):
    row = conn.execute(
        """
        SELECT id, booking_id, phone_e164, expires_at
        FROM pending_replies
        WHERE kind='vk_staff_flow' AND chat_id=? AND actor_tg_id=?
        ORDER BY id DESC LIMIT 1
        """,
        (str(peer_id or ""), str(from_id or "")),
    ).fetchone()
    if not row:
        return None, {}
    try:
        exp = datetime.fromisoformat(str(row["expires_at"]))
        if datetime.utcnow() > exp:
            conn.execute("DELETE FROM pending_replies WHERE id=?", (row["id"],))
            return None, {}
    except Exception:
        pass
    try:
        payload = json.loads(row["phone_e164"] or "{}")
    except Exception:
        payload = {}
    return row, payload if isinstance(payload, dict) else {}


def _clear_vk_pending_action(conn, row_id: int) -> None:
    conn.execute("DELETE FROM pending_replies WHERE id=?", (int(row_id),))


def _booking_action_message(prefix: str, body: str) -> str:
    return f"{prefix}\n{body}".strip()


def process_vk_booking_payload(conn, *, peer_id: object, from_id: object, payload: dict[str, Any]) -> bool:
    if str(payload.get("kind") or "") != "booking_action":
        return False

    action = str(payload.get("action") or "").strip()
    booking_id = int(payload.get("booking_id") or 0)
    peer = int(peer_id)
    actor_id = _vk_actor_id(from_id)
    actor_name = _vk_actor_name(from_id)

    if not booking_id:
        vk_send_message(peer, "Не удалось определить бронь для действия.")
        return True

    if action == "confirm":
        core_reservation_id = sync_booking_to_core(conn, booking_id)
        record_inbound_event(
            conn,
            platform="vk",
            bot_scope="hostess",
            event_type="booking_confirm",
            payload={"booking_id": booking_id, "source": "vk_staff"},
            actor_external_id=str(actor_id or ""),
            actor_display_name=str(actor_name or ""),
            peer_external_id=str(peer or ""),
            reservation_id=core_reservation_id,
        )
        conn.execute("UPDATE bookings SET status='CONFIRMED', updated_at=datetime('now') WHERE id=?", (booking_id,))
        log_booking_event(conn, booking_id, "CONFIRMED", actor_id, actor_name, {"source": "vk_staff"})
        ensure_visit_from_confirmed_booking(conn, booking_id, actor_id, actor_name)
        sync_booking_state_to_core(conn, booking_id)
        try:
            send_booking_event(conn, booking_id, "BOOKING_CONFIRMED", {"actor_tg_id": actor_id, "actor_name": actor_name, "payload": {"source": "vk_staff"}})
        except Exception:
            pass
        vk_send_message(peer, _booking_action_message("Бронь подтверждена.", f"ID: #{booking_id}"))
        return True

    if action == "cancel":
        mark_booking_cancelled(conn, booking_id, actor_id, actor_name)
        try:
            send_booking_event(conn, booking_id, "BOOKING_CANCELLED", {"actor_tg_id": actor_id, "actor_name": actor_name, "payload": {"source": "vk_staff"}})
        except Exception:
            pass
        vk_send_message(peer, _booking_action_message("Бронь отменена.", f"ID: #{booking_id}"))
        return True

    if action == "clear_table":
        clear_table_assignment(conn, booking_id, actor_id, actor_name)
        try:
            send_booking_event(conn, booking_id, "BOOKING_TABLE_UPDATED", {"actor_tg_id": actor_id, "actor_name": actor_name, "payload": {"action": "clear_table", "source": "vk_staff"}})
        except Exception:
            pass
        vk_send_message(peer, _booking_action_message("Стол снят.", f"Бронь #{booking_id}"))
        return True

    if action == "clear_deposit":
        clear_booking_deposit(conn, booking_id, actor_id, actor_name)
        try:
            send_booking_event(conn, booking_id, "BOOKING_DEPOSIT_CLEARED", {"actor_tg_id": actor_id, "actor_name": actor_name, "payload": {"action": "clear_deposit", "source": "vk_staff"}})
        except Exception:
            pass
        vk_send_message(peer, _booking_action_message("Депозит снят.", f"Бронь #{booking_id}"))
        return True

    if action == "prompt_assign_table":
        _save_vk_pending_action(conn, peer_id=peer_id, from_id=from_id, booking_id=booking_id, mode="assign_table")
        vk_send_message(peer, f"Бронь #{booking_id}\nНапиши номер стола одним сообщением.")
        return True

    if action == "prompt_set_deposit":
        _save_vk_pending_action(conn, peer_id=peer_id, from_id=from_id, booking_id=booking_id, mode="set_deposit")
        vk_send_message(peer, f"Бронь #{booking_id}\nНапиши сумму депозита одним сообщением.")
        return True

    if action == "prompt_restrict_table":
        booking_row = conn.execute("SELECT assigned_table_number FROM bookings WHERE id=?", (booking_id,)).fetchone()
        table_number = normalize_table_number(booking_row["assigned_table_number"] if booking_row else None)
        if not table_number:
            _save_vk_pending_action(conn, peer_id=peer_id, from_id=from_id, booking_id=booking_id, mode="restrict_table_number")
            vk_send_message(peer, f"Бронь #{booking_id}\nСначала напиши номер стола для ограничения.")
        else:
            _save_vk_pending_action(
                conn,
                peer_id=peer_id,
                from_id=from_id,
                booking_id=booking_id,
                mode="restrict_table_hours",
                extra={"table_number": table_number},
            )
            vk_send_message(peer, f"Бронь #{booking_id}\nСтол #{table_number}\nНапиши количество часов ограничения.")
        return True

    return False


def process_vk_pending_text(conn, *, peer_id: object, from_id: object, text: str) -> bool:
    pending_row, flow = _load_vk_pending_action(conn, peer_id=peer_id, from_id=from_id)
    if not pending_row:
        return False

    mode = str(flow.get("mode") or "").strip()
    booking_id = int(flow.get("booking_id") or pending_row["booking_id"] or 0)
    peer = int(peer_id)
    actor_id = _vk_actor_id(from_id)
    actor_name = _vk_actor_name(from_id)
    message_text = str(text or "").strip()

    if not message_text:
        return False

    if mode == "assign_table":
        table_number = normalize_table_number(message_text)
        if not table_number:
            vk_send_message(peer, "Номер стола должен быть корректным. Например: 221 или 221.1.")
            return True
        booking_row = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
        if not booking_row:
            _clear_vk_pending_action(conn, pending_row["id"])
            vk_send_message(peer, "Бронь не найдена.")
            return True
        conflicts = get_table_assignment_conflicts(conn, booking_row, table_number, exclude_booking_id=booking_id)
        if conflicts["booking_conflicts"] or conflicts["restricted"]:
            vk_send_message(peer, f"Стол #{table_number} занят или ограничен. Выбери другой номер или используй CRM для override.")
            return True
        result = assign_table_to_booking(conn, booking_id, table_number, actor_id, actor_name)
        _clear_vk_pending_action(conn, pending_row["id"])
        try:
            send_booking_event(conn, booking_id, "BOOKING_TABLE_UPDATED", {"actor_tg_id": actor_id, "actor_name": actor_name, "payload": {"action": "assign_table", "table_number": result["table_number"], "source": "vk_staff"}})
        except Exception:
            pass
        try:
            notify_waiters_about_deposit_booking(conn, booking_id)
        except Exception:
            pass
        vk_send_message(peer, f"Стол #{result['table_number']} назначен к брони #{booking_id}.")
        return True

    if mode == "set_deposit":
        try:
            result = set_booking_deposit(conn, booking_id, message_text, actor_id, actor_name)
        except ValueError:
            vk_send_message(peer, "Сумма депозита должна быть положительным целым числом.")
            return True
        _clear_vk_pending_action(conn, pending_row["id"])
        try:
            send_booking_event(conn, booking_id, "BOOKING_DEPOSIT_SET", {"actor_tg_id": actor_id, "actor_name": actor_name, "payload": {"action": "set_deposit", "deposit_amount": result["deposit_amount"], "source": "vk_staff"}})
        except Exception:
            pass
        try:
            notify_waiters_about_deposit_booking(conn, booking_id)
        except Exception:
            pass
        booking_state = conn.execute("SELECT assigned_table_number FROM bookings WHERE id = ?", (booking_id,)).fetchone()
        if booking_state and not booking_state["assigned_table_number"]:
            _save_vk_pending_action(conn, peer_id=peer_id, from_id=from_id, booking_id=booking_id, mode="assign_table")
            vk_send_message(peer, f"Депозит {result['deposit_amount']} сохранён для брони #{booking_id}.\nТеперь напиши номер стола, чтобы информация ушла официантам.")
            return True
        vk_send_message(peer, f"Депозит {result['deposit_amount']} сохранён для брони #{booking_id}.")
        return True

    if mode == "restrict_table_number":
        table_number = normalize_table_number(message_text)
        if not table_number:
            vk_send_message(peer, "Номер стола должен быть корректным. Например: 221 или 221.1.")
            return True
        flow["mode"] = "restrict_table_hours"
        flow["table_number"] = table_number
        conn.execute(
            "UPDATE pending_replies SET phone_e164=?, expires_at=? WHERE id=?",
            (json.dumps(flow, ensure_ascii=False), (datetime.utcnow() + timedelta(minutes=10)).isoformat(timespec="seconds"), pending_row["id"]),
        )
        vk_send_message(peer, f"Стол #{table_number}\nНапиши количество часов ограничения.")
        return True

    if mode == "restrict_table_hours":
        table_number = normalize_table_number(flow.get("table_number"))
        restricted_until = parse_restriction_until(message_text)
        if not table_number:
            _clear_vk_pending_action(conn, pending_row["id"])
            vk_send_message(peer, "Не удалось определить стол. Начни заново.")
            return True
        if not restricted_until:
            vk_send_message(peer, "Нужно указать положительное число часов. Пример: 3")
            return True
        try:
            result = set_table_label(conn, table_number, "RESTRICTED", actor_id, actor_name, restricted_until=restricted_until, booking_id=booking_id or None)
        except ValueError as exc:
            if str(exc) == "table_conflict":
                vk_send_message(peer, f"Для стола #{table_number} есть конфликтующая бронь. Для override пока используй CRM.")
                _clear_vk_pending_action(conn, pending_row["id"])
                return True
            raise
        _clear_vk_pending_action(conn, pending_row["id"])
        try:
            if booking_id:
                send_booking_event(conn, booking_id, "BOOKING_TABLE_RESTRICTED", {"actor_tg_id": actor_id, "actor_name": actor_name, "payload": {"action": "restrict_table", "table_number": result["table_number"], "restricted_until": result["restricted_until"], "source": "vk_staff"}})
            else:
                send_table_event(conn, result["table_number"], "TABLE_RESTRICTED", {"actor_tg_id": actor_id, "actor_name": actor_name, "payload": {"action": "restrict_table", "table_number": result["table_number"], "restricted_until": result["restricted_until"], "source": "vk_staff"}})
        except Exception:
            pass
        vk_send_message(peer, f"Стол #{result['table_number']} ограничен до {str(result['restricted_until'] or '')[11:16]}.")
        return True

    return False
