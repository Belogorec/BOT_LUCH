import json
import re
import secrets
from typing import Optional
from datetime import datetime, timedelta

from core_sync import (
    sync_booking_assignment_to_core,
    sync_booking_state_to_core,
    sync_booking_to_core,
    sync_table_state_to_core,
)
from db import get_tags, set_tags
from config import BUSINESS_TZ_OFFSET_HOURS, LEGACY_MIRROR_ENABLED
from domain import AssignTable, ClearDeposit, ClearTable, CreateReservation, DomainValidationError, SetDeposit

TABLE_LABELS = {"NONE", "DEPOSIT", "RESTRICTED"}
INACTIVE_BOOKING_STATUSES = {"DECLINED", "CANCELLED", "NO_SHOW"}
BUSINESS_NOW_SQL = f"{BUSINESS_TZ_OFFSET_HOURS:+d} hours"
TABLE_NUMBER_RE = re.compile(r"^\d+(?:\.\d+)?$")
LEGACY_BOOKING_SOURCE = "legacy_booking"
LEGACY_TO_CORE_STATUS = {
    "NEW": "pending",
    "WAITING": "pending",
    "CONFIRMED": "confirmed",
    "DECLINED": "declined",
    "CANCELLED": "cancelled",
    "NO_SHOW": "no_show",
    "COMPLETED": "completed",
}


def now_iso_seconds_utc() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def business_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=BUSINESS_TZ_OFFSET_HOURS)


def compute_segment(visits_count: int, tags: list[str] = None) -> str:
    """
    Возвращает display-статус гостя на основе количества визитов.
    Новая бизнес-логика (русские названия):
    - 0-1 визит → Новый
    - 2-4 визита → Бывалый
    - 5+ визитов → Постоянный
    """
    visits = int(visits_count or 0)
    if visits == 0 or visits == 1:
        return "Новый"
    elif 2 <= visits <= 4:
        return "Бывалый"
    else:
        return "Постоянный"


def get_guest_visits_full(conn, phone_e164: str):
    """
    Возвращает полную историю визитов гостя, упорядоченную от новых к старым.
    Используется для отдельного сообщения 'История визитов'.
    """
    visits = conn.execute(
        """
        SELECT reservation_dt, formname, source
        FROM guest_visits
        WHERE phone_e164=?
        ORDER BY reservation_dt DESC, id DESC
        """,
        (phone_e164,),
    ).fetchall()
    return visits


def upsert_guest_if_missing(conn, phone_e164: str, name_last: str, overwrite_name: bool = False):
    row = conn.execute("SELECT phone_e164 FROM guests WHERE phone_e164=?", (phone_e164,)).fetchone()
    if row:
        if overwrite_name and (name_last or "").strip():
            conn.execute(
                """
                UPDATE guests
                SET name_last = ?,
                    updated_at=datetime('now')
                WHERE phone_e164=?
                """,
                (name_last.strip(), phone_e164),
            )
        else:
            conn.execute(
                """
                UPDATE guests
                SET name_last = CASE WHEN (name_last IS NULL OR trim(name_last)='') THEN ? ELSE name_last END,
                    updated_at=datetime('now')
                WHERE phone_e164=?
                """,
                (name_last, phone_e164),
            )
        return

    conn.execute(
        """
        INSERT INTO guests (phone_e164, name_last, visits_count, first_visit_dt, last_visit_dt, tags_json)
        VALUES (?, ?, 0, NULL, NULL, '[]')
        """,
        (phone_e164, (name_last or "").strip()),
    )


def log_booking_event(conn, booking_id: int, event_type: str, actor_id: str, actor_name: str, payload: Optional[dict] = None):
    reservation_id = _ensure_core_reservation_id_for_booking(conn, int(booking_id))
    _append_reservation_event(conn, reservation_id, event_type, actor_id, actor_name, payload)
    if not _legacy_mirror_enabled():
        return
    conn.execute(
        """
        INSERT INTO booking_events (booking_id, event_type, actor_tg_id, actor_name, payload_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (booking_id, event_type, actor_id, actor_name, json.dumps(payload or {}, ensure_ascii=False)),
    )


def log_table_event(
    conn,
    table_number: str,
    event_type: str,
    actor_id: str,
    actor_name: str,
    payload: Optional[dict] = None,
    booking_id: Optional[int] = None,
):
    event_payload = dict(payload or {})
    event_payload.setdefault("table_number", str(table_number or "").strip() or None)
    if booking_id:
        reservation_id = _ensure_core_reservation_id_for_booking(conn, int(booking_id))
        _append_reservation_event(conn, reservation_id, event_type, actor_id, actor_name, event_payload)
    if not _legacy_mirror_enabled():
        return
    conn.execute(
        """
        INSERT INTO table_events (table_number, booking_id, event_type, actor_tg_id, actor_name, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            table_number,
            booking_id,
            event_type,
            actor_id,
            actor_name,
            json.dumps(payload or {}, ensure_ascii=False),
        ),
    )


def log_guest_event(conn, phone_e164: str, event_type: str, actor_id: str, actor_name: str, payload: Optional[dict] = None):
    conn.execute(
        """
        INSERT INTO guest_events (phone_e164, event_type, actor_tg_id, actor_name, payload_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (phone_e164, event_type, actor_id, actor_name, json.dumps(payload or {}, ensure_ascii=False)),
    )


def add_guest_note(conn, phone_e164: str, note: str, actor_id: str, actor_name: str):
    note = (note or "").strip()
    if not note:
        return

    conn.execute(
        """
        INSERT INTO guest_notes (phone_e164, note, actor_tg_id, actor_name)
        VALUES (?, ?, ?, ?)
        """,
        (phone_e164, note, actor_id, actor_name),
    )
    log_guest_event(conn, phone_e164, "NOTE_ADD", actor_id, actor_name, {"note": note})


def toggle_guest_tag(conn, phone_e164: str, tag: str) -> tuple[list[str], str]:
    tag = (tag or "").strip().upper()
    tags = get_tags(conn, phone_e164)
    s = set(tags)

    if tag in s:
        s.remove(tag)
        action = "TAG_REMOVE"
    else:
        s.add(tag)
        action = "TAG_ADD"

    tags2 = sorted(s)
    set_tags(conn, phone_e164, tags2)
    return tags2, action


def normalize_table_number(value) -> Optional[str]:
    raw = str(value or "").strip().replace(",", ".")
    if not raw or not TABLE_NUMBER_RE.fullmatch(raw):
        return None
    parts = raw.split(".", 1)
    head = str(int(parts[0]))
    if len(parts) == 1:
        return head
    tail = parts[1].rstrip("0")
    return f"{head}.{tail}" if tail else head


def _booking_reservation_dt(booking_row) -> str:
    res_dt = str((booking_row["reservation_dt"] or "")).strip()
    if res_dt:
        return res_dt
    rd = str((booking_row["reservation_date"] or "")).strip()
    rt = str((booking_row["reservation_time"] or "")).strip()
    if rd and rt:
        return f"{rd}T{rt}"
    return ""


def _legacy_status_to_core(status: str) -> str:
    return LEGACY_TO_CORE_STATUS.get(str(status or "").strip().upper(), "pending")


def _core_status_to_legacy(status: str) -> str:
    normalized = str(status or "").strip().lower()
    return {
        "pending": "WAITING",
        "confirmed": "CONFIRMED",
        "declined": "DECLINED",
        "cancelled": "CANCELLED",
        "no_show": "NO_SHOW",
        "completed": "COMPLETED",
    }.get(normalized, "WAITING")


def _ensure_core_reservation_id_for_booking(conn, booking_id: int) -> int:
    row = conn.execute(
        "SELECT id FROM reservations WHERE source=? AND external_ref=?",
        (LEGACY_BOOKING_SOURCE, str(int(booking_id))),
    ).fetchone()
    if row:
        return int(row["id"])
    return int(sync_booking_to_core(conn, int(booking_id)))


def _append_reservation_event(
    conn,
    reservation_id: int,
    event_type: str,
    actor_id: str,
    actor_name: str,
    payload: Optional[dict] = None,
) -> None:
    actor = (actor_name or actor_id or "system").strip() or "system"
    conn.execute(
        """
        INSERT INTO reservation_events (reservation_id, event_type, actor, payload_json)
        VALUES (?, ?, ?, ?)
        """,
        (int(reservation_id), str(event_type or "").strip() or "EVENT", actor, json.dumps(payload or {}, ensure_ascii=False)),
    )


def _legacy_mirror_enabled() -> bool:
    return bool(LEGACY_MIRROR_ENABLED)


def ensure_public_reservation_token(
    conn,
    *,
    reservation_id: int,
    public_token: str,
    token_kind: str = "guest_access",
) -> str:
    token = str(public_token or "").strip()
    if not token:
        return ""

    normalized_kind = str(token_kind or "").strip() or "guest_access"
    conn.execute(
        """
        INSERT INTO public_reservation_tokens (reservation_id, public_token, token_kind, status, expires_at)
        VALUES (?, ?, ?, 'active', NULL)
        ON CONFLICT(public_token) DO UPDATE SET
            reservation_id = excluded.reservation_id,
            token_kind = excluded.token_kind,
            status = 'active',
            expires_at = NULL
        """,
        (int(reservation_id), token, normalized_kind),
    )
    conn.execute(
        """
        UPDATE public_reservation_tokens
        SET status='replaced',
            used_at=COALESCE(used_at, datetime('now'))
        WHERE reservation_id=?
          AND token_kind=?
          AND public_token <> ?
          AND status='active'
        """,
        (int(reservation_id), normalized_kind, token),
    )
    return token


def load_booking_read_model(conn, booking_id: int) -> Optional[dict]:
    legacy_row = conn.execute("SELECT * FROM bookings WHERE id=?", (int(booking_id),)).fetchone()
    core_row = conn.execute(
        """
        SELECT
            r.id AS reservation_id,
            r.guest_name,
            r.guest_phone,
            r.reservation_at,
            r.party_size,
            r.comment,
            r.deposit_amount,
            r.deposit_comment,
            r.status,
            tc.code AS assigned_table_number
        FROM reservations r
        LEFT JOIN reservation_tables rt
          ON rt.reservation_id = r.id
         AND rt.released_at IS NULL
        LEFT JOIN tables_core tc
          ON tc.id = rt.table_id
        WHERE r.source = ? AND r.external_ref = ?
        ORDER BY rt.id DESC
        LIMIT 1
        """,
        (LEGACY_BOOKING_SOURCE, str(int(booking_id))),
    ).fetchone()

    if not legacy_row and not core_row:
        return None

    model = {
        "id": int(booking_id),
        "name": None,
        "phone_e164": None,
        "phone_raw": None,
        "reservation_date": None,
        "reservation_time": None,
        "reservation_dt": None,
        "guests_count": None,
        "comment": None,
        "assigned_table_number": None,
        "deposit_amount": None,
        "deposit_comment": None,
        "status": None,
        "formname": None,
        "tranid": None,
        "guest_segment": None,
        "reservation_token": None,
        "raw_payload_json": None,
        "utm_source": None,
        "utm_medium": None,
        "utm_campaign": None,
        "utm_content": None,
        "utm_term": None,
        "created_at": None,
        "updated_at": None,
    }

    if core_row:
        reservation_at = str(core_row["reservation_at"] or "").strip()
        model.update(
            {
                "name": core_row["guest_name"],
                "phone_e164": core_row["guest_phone"],
                "reservation_dt": reservation_at or None,
                "reservation_date": reservation_at[:10] if reservation_at else None,
                "reservation_time": reservation_at[11:16] if len(reservation_at) >= 16 else None,
                "guests_count": core_row["party_size"],
                "comment": core_row["comment"],
                "assigned_table_number": core_row["assigned_table_number"],
                "deposit_amount": core_row["deposit_amount"],
                "deposit_comment": core_row["deposit_comment"],
                "status": _core_status_to_legacy(core_row["status"]),
            }
        )

    if legacy_row:
        for key in model.keys():
            if model.get(key) in (None, "", 0):
                model[key] = legacy_row[key] if key in legacy_row.keys() else model.get(key)

    return model


def load_table_read_model(conn, table_number: str) -> Optional[dict]:
    normalized_table = normalize_table_number(table_number)
    if not normalized_table:
        return None

    core_row = conn.execute(
        """
        SELECT
            tc.code AS table_number,
            tb.ends_at AS restricted_until,
            tb.reason AS restriction_comment,
            tb.created_at
        FROM tables_core tc
        LEFT JOIN table_blocks tb
          ON tb.table_id = tc.id
         AND datetime(tb.ends_at) > datetime('now')
        WHERE tc.code = ?
        ORDER BY tb.id DESC
        LIMIT 1
        """,
        (normalized_table,),
    ).fetchone()
    legacy_row = conn.execute(
        """
        SELECT table_number, label, restricted_until, restriction_comment, updated_by, updated_at, created_at
        FROM venue_tables
        WHERE table_number = ?
        """,
        (normalized_table,),
    ).fetchone()

    if not core_row and not legacy_row:
        return None

    model = {
        "table_number": normalized_table,
        "label": "NONE",
        "restricted_until": None,
        "restriction_comment": None,
        "updated_by": None,
        "updated_at": None,
        "created_at": None,
    }

    if core_row:
        has_block = str(core_row["restricted_until"] or "").strip()
        model.update(
            {
                "label": "RESTRICTED" if has_block else "NONE",
                "restricted_until": core_row["restricted_until"],
                "restriction_comment": core_row["restriction_comment"],
                "created_at": core_row["created_at"],
            }
        )

    if legacy_row:
        for key in model.keys():
            if model.get(key) in (None, "", 0, "NONE"):
                model[key] = legacy_row[key] if key in legacy_row.keys() else model.get(key)

    return model


def _ensure_table_core_id(conn, table_code: str) -> int:
    code = str(table_code or "").strip()
    if not code:
        raise ValueError("table_code_required")
    conn.execute(
        """
        INSERT INTO tables_core (code, title, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(code) DO UPDATE SET
          updated_at = datetime('now')
        """,
        (code, f"Table {code}"),
    )
    row = conn.execute("SELECT id FROM tables_core WHERE code = ?", (code,)).fetchone()
    if not row:
        raise ValueError("table_not_found_after_upsert")
    return int(row["id"])


def _set_core_table_assignment(conn, reservation_id: int, table_code: str, actor_id: str) -> None:
    table_id = _ensure_table_core_id(conn, table_code)
    active = conn.execute(
        """
        SELECT id, table_id
        FROM reservation_tables
        WHERE reservation_id = ? AND released_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(reservation_id),),
    ).fetchone()
    if active and int(active["table_id"]) == table_id:
        return
    if active:
        conn.execute("UPDATE reservation_tables SET released_at = datetime('now') WHERE id = ?", (int(active["id"]),))
    conn.execute(
        """
        INSERT INTO reservation_tables (reservation_id, table_id, assigned_by)
        VALUES (?, ?, ?)
        """,
        (int(reservation_id), table_id, str(actor_id or "").strip() or "system"),
    )


def _clear_core_table_assignment(conn, reservation_id: int) -> None:
    conn.execute(
        """
        UPDATE reservation_tables
        SET released_at = datetime('now')
        WHERE reservation_id = ? AND released_at IS NULL
        """,
        (int(reservation_id),),
    )


def _set_core_table_restriction(
    conn,
    table_code: str,
    restricted_until: Optional[str],
    reason: str = "",
    reservation_id: Optional[int] = None,
) -> None:
    table_id = _ensure_table_core_id(conn, table_code)
    conn.execute(
        """
        DELETE FROM table_blocks
        WHERE table_id = ?
          AND datetime(ends_at) > datetime('now')
        """,
        (table_id,),
    )
    ends_at = str(restricted_until or "").strip()
    if not ends_at:
        return
    conn.execute(
        """
        INSERT INTO table_blocks (
          table_id, starts_at, ends_at, reason, block_type, reservation_id, created_by
        ) VALUES (?, datetime('now'), ?, ?, 'manual', ?, 'system')
        """,
        (
            table_id,
            ends_at.replace(" ", "T") if "T" not in ends_at and " " in ends_at else ends_at,
            (reason or "").strip() or None,
            int(reservation_id) if reservation_id else None,
        ),
    )


def parse_restriction_until(value: str) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None

    try:
        hours = int(raw)
    except (TypeError, ValueError):
        hours = 0

    if hours > 0:
        base = business_now().replace(second=0, microsecond=0)
        return (base + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")

    candidates = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%d.%m %H:%M",
        "%d.%m.%Y %H:%M",
    ]

    parsed_dt = None
    now = business_now()
    for fmt in candidates:
        try:
            parsed_dt = datetime.strptime(raw, fmt)
            if fmt == "%d.%m %H:%M":
                parsed_dt = parsed_dt.replace(year=now.year)
                if parsed_dt <= now:
                    parsed_dt = parsed_dt.replace(year=now.year + 1)
            break
        except ValueError:
            continue

    if not parsed_dt or parsed_dt <= now:
        return None

    return parsed_dt.strftime("%Y-%m-%d %H:%M:%S")


def get_table_state(conn, table_number: str):
    return conn.execute(
        """
        SELECT table_number, label, restricted_until, restriction_comment, updated_by, updated_at, created_at
        FROM venue_tables
        WHERE table_number = ?
        """,
        (table_number,),
    ).fetchone()


def get_active_table_restrictions(conn):
    return conn.execute(
        """
        SELECT table_number, label, restricted_until, restriction_comment, updated_by, updated_at
        FROM venue_tables
        WHERE label = 'RESTRICTED'
          AND restricted_until IS NOT NULL
          AND datetime(restricted_until) > datetime('now', '{BUSINESS_NOW_SQL}')
        ORDER BY datetime(restricted_until) ASC, table_number ASC
        """.format(BUSINESS_NOW_SQL=BUSINESS_NOW_SQL)
    ).fetchall()


def get_table_booking_conflicts(conn, table_number: str, reservation_dt: str, exclude_booking_id: int = 0):
    if not reservation_dt:
        return []
    return conn.execute(
        """
        SELECT id, name, reservation_dt, status
        FROM bookings
        WHERE assigned_table_number = ?
          AND COALESCE(reservation_dt, '') = ?
          AND id != ?
          AND COALESCE(status, 'WAITING') NOT IN ('DECLINED', 'CANCELLED', 'NO_SHOW')
        ORDER BY id ASC
        """,
        (table_number, reservation_dt, int(exclude_booking_id or 0)),
    ).fetchall()


def get_table_assignment_conflicts(conn, booking_row, table_number: str, exclude_booking_id: int = 0) -> dict:
    conflicts = [dict(r) for r in get_table_booking_conflicts(conn, table_number, _booking_reservation_dt(booking_row), exclude_booking_id)]
    restricted_row = conn.execute(
        """
        SELECT table_number, restricted_until, restriction_comment
        FROM venue_tables
        WHERE table_number = ?
          AND label = 'RESTRICTED'
          AND restricted_until IS NOT NULL
          AND datetime(restricted_until) > datetime('now', '{BUSINESS_NOW_SQL}')
        LIMIT 1
        """.format(BUSINESS_NOW_SQL=BUSINESS_NOW_SQL),
        (table_number,),
    ).fetchone()
    return {
        "booking_conflicts": conflicts,
        "restricted": dict(restricted_row) if restricted_row else None,
    }


def _get_active_restriction_state(conn, table_number: str) -> Optional[dict]:
    if not normalize_table_number(table_number):
        return None
    row = conn.execute(
        """
        SELECT table_number, label, restricted_until, restriction_comment, updated_by, updated_at, created_at
        FROM venue_tables
        WHERE table_number = ?
          AND label = 'RESTRICTED'
          AND restricted_until IS NOT NULL
          AND datetime(restricted_until) > datetime('now', '{BUSINESS_NOW_SQL}')
        LIMIT 1
        """.format(BUSINESS_NOW_SQL=BUSINESS_NOW_SQL),
        (table_number,),
    ).fetchone()
    return dict(row) if row else None


def assign_table_to_booking(
    conn,
    booking_id: int,
    table_number: str,
    actor_id: str,
    actor_name: str,
    force_override: bool = False,
):
    _validate_booking_action_command(
        action="assign_table",
        booking_id=booking_id,
        actor_id=actor_id,
        actor_name=actor_name,
        table_number=table_number,
    )
    normalized_table = normalize_table_number(table_number)
    if not normalized_table:
        raise ValueError("invalid_table_number")

    booking_row = conn.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,)).fetchone()
    if not booking_row:
        raise ValueError("booking_not_found")
    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id)

    conflicts = get_table_assignment_conflicts(conn, booking_row, normalized_table, exclude_booking_id=booking_id)
    if not force_override and (conflicts["booking_conflicts"] or conflicts["restricted"]):
        raise ValueError("table_conflict")

    prev_table = booking_row["assigned_table_number"]
    moved_restriction = _get_active_restriction_state(conn, prev_table) if prev_table and prev_table != normalized_table else None
    _set_core_table_assignment(conn, reservation_id, normalized_table, actor_id)
    if moved_restriction and prev_table:
        _set_core_table_restriction(conn, prev_table, None, "", reservation_id=reservation_id)
        _set_core_table_restriction(
            conn,
            normalized_table,
            moved_restriction.get("restricted_until"),
            moved_restriction.get("restriction_comment") or "",
            reservation_id=reservation_id,
        )

    if _legacy_mirror_enabled():
        conn.execute(
            """
            UPDATE bookings
            SET assigned_table_number = ?, updated_at = datetime('now')
            WHERE id = ?
            """,
            (normalized_table, booking_id),
        )
        conn.execute(
            """
            INSERT INTO venue_tables (table_number, label, updated_by, updated_at, created_at)
            VALUES (?, 'NONE', ?, datetime('now'), datetime('now'))
            ON CONFLICT(table_number) DO UPDATE SET
                updated_by = excluded.updated_by,
                updated_at = datetime('now')
            """,
            (normalized_table, actor_id),
        )
        if moved_restriction:
            conn.execute(
                """
                UPDATE venue_tables
                SET label = 'NONE',
                    restricted_until = NULL,
                    restriction_comment = NULL,
                    updated_by = ?,
                    updated_at = datetime('now')
                WHERE table_number = ?
                """,
                (actor_id, prev_table),
            )
            conn.execute(
                """
                INSERT INTO venue_tables (
                    table_number, label, restricted_until, restriction_comment, updated_by, updated_at, created_at
                ) VALUES (?, 'RESTRICTED', ?, ?, ?, datetime('now'), datetime('now'))
                ON CONFLICT(table_number) DO UPDATE SET
                    label = 'RESTRICTED',
                    restricted_until = excluded.restricted_until,
                    restriction_comment = excluded.restriction_comment,
                    updated_by = excluded.updated_by,
                    updated_at = datetime('now')
                """,
                (
                    normalized_table,
                    moved_restriction.get("restricted_until"),
                    moved_restriction.get("restriction_comment"),
                    actor_id,
                ),
            )

    payload = {
        "old_table_number": prev_table,
        "new_table_number": normalized_table,
        "force_override": bool(force_override),
        "conflicts": conflicts,
        "moved_restriction": moved_restriction,
    }
    event_type = "TABLE_ASSIGNED" if prev_table in (None, "") else "TABLE_REASSIGNED"
    log_booking_event(conn, booking_id, event_type, actor_id, actor_name, payload)
    log_table_event(conn, normalized_table, event_type, actor_id, actor_name, payload, booking_id=booking_id)
    if moved_restriction and prev_table:
        log_table_event(conn, prev_table, "TABLE_LABEL_CLEARED", actor_id, actor_name, payload, booking_id=booking_id)
        log_table_event(conn, normalized_table, "TABLE_RESTRICTED", actor_id, actor_name, payload, booking_id=booking_id)
        sync_table_state_to_core(conn, prev_table)
    sync_booking_state_to_core(conn, booking_id)
    sync_table_state_to_core(conn, normalized_table)
    return {"table_number": normalized_table, "conflicts": conflicts, "previous_table_number": prev_table}


def clear_table_assignment(conn, booking_id: int, actor_id: str, actor_name: str):
    _validate_booking_action_command(
        action="clear_table",
        booking_id=booking_id,
        actor_id=actor_id,
        actor_name=actor_name,
    )
    booking_row = conn.execute("SELECT assigned_table_number FROM bookings WHERE id = ?", (booking_id,)).fetchone()
    if not booking_row:
        raise ValueError("booking_not_found")
    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id)

    prev_table = booking_row["assigned_table_number"]
    _clear_core_table_assignment(conn, reservation_id)
    if _legacy_mirror_enabled():
        conn.execute(
            """
            UPDATE bookings
            SET assigned_table_number = NULL, updated_at = datetime('now')
            WHERE id = ?
            """,
            (booking_id,),
        )
    payload = {"old_table_number": prev_table, "new_table_number": None}
    log_booking_event(conn, booking_id, "TABLE_CLEARED", actor_id, actor_name, payload)
    if prev_table:
        log_table_event(conn, prev_table, "TABLE_CLEARED", actor_id, actor_name, payload, booking_id=booking_id)
        sync_table_state_to_core(conn, prev_table)
    sync_booking_state_to_core(conn, booking_id)
    return {"previous_table_number": prev_table}


def set_booking_deposit(
    conn,
    booking_id: int,
    amount: int,
    actor_id: str,
    actor_name: str,
    comment: str = "",
):
    _validate_booking_action_command(
        action="set_deposit",
        booking_id=booking_id,
        actor_id=actor_id,
        actor_name=actor_name,
        amount=amount,
        comment=comment,
    )
    try:
        deposit_amount = int(str(amount).strip())
    except (TypeError, ValueError):
        raise ValueError("invalid_deposit_amount")

    if deposit_amount <= 0:
        raise ValueError("invalid_deposit_amount")

    booking_row = conn.execute("SELECT id FROM bookings WHERE id = ?", (booking_id,)).fetchone()
    if not booking_row:
        raise ValueError("booking_not_found")

    actor_display = (actor_name or actor_id or "").strip() or "telegram"
    deposit_comment = (comment or "").strip()
    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id)
    conn.execute(
        """
        UPDATE reservations
        SET deposit_amount = ?,
            deposit_comment = ?,
            deposit_set_at = datetime('now'),
            deposit_set_by = ?,
            updated_at = datetime('now')
        WHERE id = ?
        """,
        (deposit_amount, deposit_comment or None, actor_display, reservation_id),
    )
    if _legacy_mirror_enabled():
        conn.execute(
            """
            UPDATE bookings
            SET deposit_amount = ?,
                deposit_comment = ?,
                deposit_set_at = datetime('now'),
                deposit_set_by = ?,
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (deposit_amount, deposit_comment or None, actor_display, booking_id),
        )

    payload = {
        "deposit_amount": deposit_amount,
        "deposit_comment": deposit_comment,
    }
    log_booking_event(conn, booking_id, "DEPOSIT_SET", actor_id, actor_name, payload)
    return payload


def clear_booking_deposit(
    conn,
    booking_id: int,
    actor_id: str,
    actor_name: str,
):
    _validate_booking_action_command(
        action="clear_deposit",
        booking_id=booking_id,
        actor_id=actor_id,
        actor_name=actor_name,
    )
    booking_row = conn.execute(
        "SELECT id, deposit_amount, deposit_comment FROM bookings WHERE id = ?",
        (booking_id,),
    ).fetchone()
    if not booking_row:
        raise ValueError("booking_not_found")

    payload = {
        "old_deposit_amount": booking_row["deposit_amount"],
        "old_deposit_comment": booking_row["deposit_comment"],
    }
    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id)
    conn.execute(
        """
        UPDATE reservations
        SET deposit_amount = NULL,
            deposit_comment = NULL,
            deposit_set_at = NULL,
            deposit_set_by = NULL,
            updated_at = datetime('now')
        WHERE id = ?
        """,
        (reservation_id,),
    )
    if _legacy_mirror_enabled():
        conn.execute(
            """
            UPDATE bookings
            SET deposit_amount = NULL,
                deposit_comment = NULL,
                deposit_set_at = NULL,
                deposit_set_by = NULL,
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (booking_id,),
        )
    log_booking_event(conn, booking_id, "DEPOSIT_CLEARED", actor_id, actor_name, payload)
    return payload


def _validate_booking_action_command(
    action: str,
    booking_id: int,
    actor_id: str,
    actor_name: str,
    table_number: str = "",
    amount: int = 0,
    comment: str = "",
) -> None:
    actor = (actor_name or actor_id or "bot").strip() or "bot"
    try:
        if action == "assign_table":
            if not normalize_table_number(table_number):
                raise DomainValidationError("invalid_table_number")
            AssignTable(reservation_id=int(booking_id), table_id=1, assigned_by=actor)
            return
        if action == "clear_table":
            ClearTable(reservation_id=int(booking_id), released_by=actor)
            return
        if action == "set_deposit":
            SetDeposit(
                reservation_id=int(booking_id),
                amount=int(str(amount).strip()),
                comment=(comment or "").strip() or None,
                set_by=actor,
            )
            return
        if action == "clear_deposit":
            ClearDeposit(reservation_id=int(booking_id), cleared_by=actor)
            return
    except (ValueError, DomainValidationError) as exc:
        raise ValueError(str(exc)) from exc


def set_table_label(
    conn,
    table_number: str,
    label: str,
    actor_id: str,
    actor_name: str,
    restricted_until: Optional[str] = None,
    restriction_comment: str = "",
    booking_id: Optional[int] = None,
    force_override: bool = False,
):
    normalized_table = normalize_table_number(table_number)
    if not normalized_table:
        raise ValueError("invalid_table_number")

    normalized_label = str(label or "").strip().upper()
    if normalized_label not in TABLE_LABELS:
        raise ValueError("invalid_table_label")

    normalized_until = None
    if normalized_label == "RESTRICTED":
        normalized_until = parse_restriction_until(restricted_until or "")
        if not normalized_until:
            raise ValueError("invalid_restricted_until")

        if not force_override:
            row = conn.execute(
                """
                SELECT id
                FROM bookings
                WHERE assigned_table_number = ?
                  AND COALESCE(status, 'WAITING') NOT IN ('DECLINED', 'CANCELLED', 'NO_SHOW')
                  AND COALESCE(reservation_dt, '') <> ''
                  AND datetime(replace(reservation_dt, 'T', ' ')) <= datetime(?)
                LIMIT 1
                """,
                (normalized_table, normalized_until),
            ).fetchone()
            if row:
                raise ValueError("table_conflict")

    if normalized_label != "RESTRICTED":
        normalized_until = None
        restriction_comment = ""

    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id) if booking_id else None
    _set_core_table_restriction(
        conn,
        normalized_table,
        normalized_until,
        (restriction_comment or "").strip(),
        reservation_id=reservation_id,
    )

    prev_state = get_table_state(conn, normalized_table) if _legacy_mirror_enabled() else None
    if _legacy_mirror_enabled():
        conn.execute(
            """
            INSERT INTO venue_tables (
                table_number, label, restricted_until, restriction_comment, updated_by, updated_at, created_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(table_number) DO UPDATE SET
                label = excluded.label,
                restricted_until = excluded.restricted_until,
                restriction_comment = excluded.restriction_comment,
                updated_by = excluded.updated_by,
                updated_at = datetime('now')
            """,
            (
                normalized_table,
                normalized_label,
                normalized_until,
                (restriction_comment or "").strip() or None,
                actor_id,
            ),
        )

    payload = {
        "table_number": normalized_table,
        "old_label": prev_state["label"] if prev_state else None,
        "new_label": normalized_label,
        "old_restricted_until": prev_state["restricted_until"] if prev_state else None,
        "new_restricted_until": normalized_until,
        "comment": (restriction_comment or "").strip(),
        "force_override": bool(force_override),
    }
    event_type = {
        "RESTRICTED": "TABLE_RESTRICTED",
        "DEPOSIT": "TABLE_MARKED_DEPOSIT",
        "NONE": "TABLE_LABEL_CLEARED",
    }[normalized_label]
    log_table_event(conn, normalized_table, event_type, actor_id, actor_name, payload, booking_id=booking_id)
    if booking_id:
        log_booking_event(conn, booking_id, event_type, actor_id, actor_name, payload)
    sync_table_state_to_core(conn, normalized_table)
    return {
        "table_number": normalized_table,
        "label": normalized_label,
        "restricted_until": normalized_until,
    }


def ensure_visit_from_confirmed_booking(conn, booking_id: int, actor_id: str, actor_name: str):
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b:
        return

    phone = b["phone_e164"] or ""
    if not phone:
        return

    reservation_dt = b["reservation_dt"] or ""
    if not reservation_dt:
        rd = (b["reservation_date"] or "").strip()
        rt = (b["reservation_time"] or "").strip()
        if rd and rt:
            reservation_dt = f"{rd}T{rt}"

    if not reservation_dt:
        return

    exists = conn.execute(
        """
        SELECT 1
        FROM guest_visits
        WHERE phone_e164=? AND reservation_dt=? AND source='confirmed_booking'
        LIMIT 1
        """,
        (phone, reservation_dt),
    ).fetchone()

    if not exists:
        conn.execute(
            """
            INSERT INTO guest_visits
              (phone_e164, name, reservation_dt, date_form, time_form, formname, created_dt, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'confirmed_booking')
            """,
            (
                phone,
                b["name"] or "",
                reservation_dt,
                b["reservation_date"] or None,
                b["reservation_time"] or None,
                b["formname"] or None,
                now_iso_seconds_utc(),
            ),
        )
        log_guest_event(
            conn,
            phone,
            "VISIT_FROM_BOOKING",
            actor_id,
            actor_name,
            {"booking_id": booking_id, "reservation_dt": reservation_dt},
        )

    agg = conn.execute(
        """
        SELECT COUNT(*) AS c, MIN(reservation_dt) AS mindt, MAX(reservation_dt) AS maxdt
        FROM guest_visits
        WHERE phone_e164=?
        """,
        (phone,),
    ).fetchone()

    visits_count = int(agg["c"] or 0)
    first_dt = agg["mindt"]
    last_dt = agg["maxdt"]

    name_last_row = conn.execute(
        """
        SELECT name
        FROM guest_visits
        WHERE phone_e164=?
        ORDER BY reservation_dt DESC, id DESC
        LIMIT 1
        """,
        (phone,),
    ).fetchone()
    name_last = (name_last_row["name"] if name_last_row else "") or (b["name"] or "")

    conn.execute(
        """
        UPDATE guests
        SET visits_count=?,
            first_visit_dt=?,
            last_visit_dt=?,
            name_last=CASE WHEN (name_last IS NULL OR trim(name_last)='') THEN ? ELSE name_last END,
            updated_at=datetime('now')
        WHERE phone_e164=?
        """,
        (visits_count, first_dt, last_dt, name_last, phone),
    )


def mark_booking_cancelled(conn, booking_id: int, actor_id: str, actor_name: str):
    b = conn.execute(
        "SELECT phone_e164, reservation_dt FROM bookings WHERE id=?",
        (booking_id,),
    ).fetchone()
    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id)

    conn.execute(
        "UPDATE reservations SET status='cancelled', updated_at=datetime('now') WHERE id=?",
        (reservation_id,),
    )
    if _legacy_mirror_enabled():
        conn.execute(
            "UPDATE bookings SET status='CANCELLED', updated_at=datetime('now') WHERE id=?",
            (booking_id,),
        )
    log_booking_event(conn, booking_id, "CANCELLED", actor_id, actor_name, {})

    if b and b["phone_e164"]:
        log_guest_event(
            conn,
            b["phone_e164"],
            "CANCELLED_BOOKING",
            actor_id,
            actor_name,
            {"booking_id": booking_id, "reservation_dt": b["reservation_dt"]},
        )


def set_booking_status(conn, booking_id: int, status: str, actor_id: str, actor_name: str, source: str = "") -> str:
    normalized = str(status or "").strip().upper()
    if not normalized:
        raise ValueError("invalid_status")

    if normalized == "CANCELLED":
        mark_booking_cancelled(conn, booking_id, actor_id, actor_name)
        return normalized

    event_type = {
        "CONFIRMED": "CONFIRMED",
        "DECLINED": "DECLINED",
        "NO_SHOW": "NO_SHOW",
        "COMPLETED": "COMPLETED",
        "WAITING": "WAITING",
    }.get(normalized, "STATUS_CHANGED")
    payload = {"source": source} if source else {}
    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id)

    conn.execute(
        "UPDATE reservations SET status=?, updated_at=datetime('now') WHERE id=?",
        (_legacy_status_to_core(normalized), reservation_id),
    )
    if _legacy_mirror_enabled():
        conn.execute(
            "UPDATE bookings SET status=?, updated_at=datetime('now') WHERE id=?",
            (normalized, booking_id),
        )
    log_booking_event(conn, booking_id, event_type, actor_id, actor_name, payload)

    if normalized == "CONFIRMED":
        ensure_visit_from_confirmed_booking(conn, booking_id, actor_id, actor_name)

    return normalized


def reschedule_booking(
    conn,
    booking_id: int,
    reservation_date: str,
    reservation_time: str,
    actor_id: str,
    actor_name: str,
    source: str = "",
) -> dict:
    date_value = str(reservation_date or "").strip()
    time_value = str(reservation_time or "").strip()
    if not (date_value and time_value):
        raise ValueError("reservation_date_and_time_required")

    reservation_dt = f"{date_value}T{time_value}"
    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id)
    conn.execute(
        """
        UPDATE reservations
        SET reservation_at=?, updated_at=datetime('now')
        WHERE id=?
        """,
        (reservation_dt, reservation_id),
    )
    if _legacy_mirror_enabled():
        conn.execute(
            """
            UPDATE bookings
            SET reservation_date=?, reservation_time=?, reservation_dt=?, updated_at=datetime('now')
            WHERE id=?
            """,
            (date_value, time_value, reservation_dt, booking_id),
        )
    payload = {"source": source} if source else {}
    log_booking_event(conn, booking_id, "RESCHEDULED", actor_id, actor_name, payload)
    return {
        "booking_id": int(booking_id),
        "reservation_date": date_value,
        "reservation_time": time_value,
        "reservation_dt": reservation_dt,
    }


def update_booking_guests_count(
    conn,
    booking_id: int,
    guests_count: int,
    actor_id: str,
    actor_name: str,
    source: str = "",
) -> dict:
    try:
        party_size = int(str(guests_count).strip())
    except (TypeError, ValueError):
        raise ValueError("invalid_guests_count")
    if party_size <= 0:
        raise ValueError("invalid_guests_count")
    reservation_id = _ensure_core_reservation_id_for_booking(conn, booking_id)
    conn.execute(
        """
        UPDATE reservations
        SET party_size=?, updated_at=datetime('now')
        WHERE id=?
        """,
        (party_size, reservation_id),
    )

    if _legacy_mirror_enabled():
        conn.execute(
            "UPDATE bookings SET guests_count=?, updated_at=datetime('now') WHERE id=?",
            (party_size, booking_id),
        )
    payload = {"source": source} if source else {}
    log_booking_event(conn, booking_id, "GUESTS_UPDATED", actor_id, actor_name, payload)
    return {"booking_id": int(booking_id), "guests_count": party_size}


def create_manual_booking(
    conn,
    *,
    guest_name: str,
    guest_phone: Optional[str],
    reservation_date: str,
    reservation_time: str,
    guests_count: int,
    comment: str,
    actor_id: str,
    actor_name: str,
    table_number: Optional[str] = None,
    session_mode: str = "",
    deposit_amount: Optional[int] = None,
    deposit_comment: str = "",
) -> dict:
    date_value = str(reservation_date or "").strip()
    time_value = str(reservation_time or "").strip()
    party_size = int(str(guests_count).strip())
    if party_size <= 0:
        raise ValueError("invalid_guests_count")

    actor = (actor_name or actor_id or "crm").strip() or "crm"
    try:
        CreateReservation(
            reservation_at=f"{date_value}T{time_value}",
            party_size=party_size,
            source="crm_manual",
            guest_name=(guest_name or "").strip() or "CRM",
            guest_phone=(str(guest_phone or "").strip() or None),
            comment=(comment or "").strip() or None,
            actor=actor,
        )
    except DomainValidationError as exc:
        raise ValueError(str(exc)) from exc

    raw_payload = json.dumps(
        {
            "source": "crm_manual",
            "session_mode": str(session_mode or "").strip().lower(),
            "guest_name": (guest_name or "").strip() or "CRM",
            "guest_phone": str(guest_phone or "").strip() or None,
            "guests_count": party_size,
            "table_number": table_number,
            "comment": (comment or "").strip(),
        },
        ensure_ascii=False,
    )
    cur = conn.execute(
        """
        INSERT INTO bookings
        (
          tranid, formname, name, phone_e164, phone_raw, user_chat_id,
          reservation_date, reservation_time, reservation_dt,
          guests_count, comment,
          utm_source, utm_medium, utm_campaign, utm_content, utm_term,
          status, guest_segment, reservation_token, raw_payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'CONFIRMED', ?, ?, ?)
        """,
        (
            None,
            "crm_manual",
            (guest_name or "").strip() or "CRM",
            str(guest_phone or "").strip() or None,
            str(guest_phone or "").strip() or None,
            None,
            date_value,
            time_value,
            f"{date_value}T{time_value}",
            party_size,
            (comment or "").strip() or None,
            "crm",
            "manual",
            None,
            None,
            None,
            "manual",
            None,
            raw_payload,
        ),
    )
    booking_id = int(cur.lastrowid)
    log_booking_event(conn, booking_id, "CREATED", actor_id, actor_name, {"source": "crm_manual"})

    normalized_table = normalize_table_number(table_number) if table_number else None
    if normalized_table:
        assign_table_to_booking(conn, booking_id, normalized_table, actor_id, actor_name, force_override=True)

    notify_waiters = False
    if str(session_mode or "").strip().lower() == "deposit":
        set_booking_deposit(
            conn,
            booking_id,
            deposit_amount,
            actor_id,
            actor_name,
            comment=str(deposit_comment or comment or "").strip(),
        )
        notify_waiters = True

    reservation_id = int(sync_booking_to_core(conn, booking_id))
    sync_booking_assignment_to_core(conn, booking_id)
    return {"booking_id": booking_id, "notify_waiters": notify_waiters}


def create_telegram_miniapp_booking_record(
    conn,
    *,
    tg_user_id: str,
    date_value: str,
    time_value: str,
    guests_count: int,
    comment_value: str,
    reservation_token: str,
    phone_e164: Optional[str],
    display_name: str,
    raw_payload_json: str,
    source: str = "telegram_miniapp_api",
) -> dict:
    existing = conn.execute(
        "SELECT id FROM bookings WHERE reservation_token=?",
        (reservation_token,),
    ).fetchone()
    if existing:
        booking_id = int(existing["id"])
        reservation_id = int(sync_booking_to_core(conn, booking_id))
        ensure_public_reservation_token(
            conn,
            reservation_id=reservation_id,
            public_token=reservation_token,
        )
        return {"booking_id": booking_id, "duplicate": True}

    cur = conn.execute(
        """
        INSERT INTO bookings
        (tranid, formname, name, phone_e164, phone_raw, user_chat_id,
         reservation_date, reservation_time, reservation_dt,
         guests_count, comment,
         utm_source, utm_medium, utm_campaign, utm_content, utm_term,
         status, guest_segment, reservation_token, raw_payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WAITING', ?, ?, ?)
        """,
        (
            None,
            "telegram_miniapp",
            display_name or "Telegram",
            phone_e164,
            phone_e164,
            tg_user_id,
            date_value,
            time_value,
            f"{date_value} {time_value}:00",
            guests_count,
            comment_value,
            "telegram",
            "miniapp",
            None,
            None,
            None,
            "NEW",
            reservation_token,
            raw_payload_json,
        ),
    )
    booking_id = int(cur.lastrowid)
    log_booking_event(conn, booking_id, "CREATED", tg_user_id, "", {"source": source})
    reservation_id = int(sync_booking_to_core(conn, booking_id))
    ensure_public_reservation_token(
        conn,
        reservation_id=reservation_id,
        public_token=reservation_token,
    )
    sync_booking_assignment_to_core(conn, booking_id)
    return {"booking_id": booking_id, "duplicate": False}


def upsert_tilda_booking_record(
    conn,
    *,
    payload_json: str,
    name: str,
    phone_e164: str,
    phone_raw: str,
    date_raw: str,
    time_raw: str,
    reservation_dt: str,
    guests_count: Optional[int],
    comment: str,
    tranid: str,
    formname: str,
    utm_source: str,
    utm_medium: str,
    utm_campaign: str,
    utm_content: str,
    utm_term: str,
    guest_segment: str,
    source: str = "tilda",
) -> dict:
    existing = None
    if tranid:
        existing = conn.execute(
            "SELECT id FROM bookings WHERE tranid=?",
            (tranid,),
        ).fetchone()

    if existing:
        booking_id = int(existing["id"])
        existing_token_row = conn.execute("SELECT reservation_token FROM bookings WHERE id=?", (booking_id,)).fetchone()
        reservation_token = str(existing_token_row["reservation_token"] or "").strip() if existing_token_row else ""
        if not reservation_token:
            reservation_token = secrets.token_urlsafe(24)
        conn.execute(
            """
            UPDATE bookings
            SET name=?,
                phone_e164=?,
                phone_raw=?,
                reservation_date=?,
                reservation_time=?,
                reservation_dt=?,
                guests_count=?,
                comment=?,
                utm_source=?,
                utm_medium=?,
                utm_campaign=?,
                utm_content=?,
                utm_term=?,
                formname=?,
                guest_segment=?,
                reservation_token=?,
                raw_payload_json=?,
                updated_at=datetime('now')
            WHERE id=?
            """,
            (
                name,
                phone_e164,
                phone_raw,
                date_raw,
                time_raw,
                reservation_dt,
                guests_count,
                comment,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_content,
                utm_term,
                formname,
                guest_segment,
                reservation_token,
                payload_json,
                booking_id,
            ),
        )
        log_booking_event(
            conn,
            booking_id,
            "UPDATED",
            "system",
            "system",
            {"source": source},
        )
        reservation_id = int(sync_booking_to_core(conn, booking_id))
        ensure_public_reservation_token(
            conn,
            reservation_id=reservation_id,
            public_token=reservation_token,
        )
        sync_booking_assignment_to_core(conn, booking_id)
        return {
            "booking_id": booking_id,
            "reservation_token": reservation_token,
            "existing": True,
        }

    reservation_token = secrets.token_urlsafe(24)
    cur = conn.execute(
        """
        INSERT INTO bookings
          (tranid, formname, name, phone_e164, phone_raw, reservation_date, reservation_time, reservation_dt,
           guests_count, comment, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
           status, guest_segment, reservation_token, raw_payload_json)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WAITING', ?, ?, ?)
        """,
        (
            tranid or None,
            formname,
            name,
            phone_e164,
            phone_raw,
            date_raw,
            time_raw,
            reservation_dt,
            guests_count,
            comment,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term,
            guest_segment,
            reservation_token,
            payload_json,
        ),
    )
    booking_id = int(cur.lastrowid)
    log_booking_event(conn, booking_id, "CREATED", "system", "system", {"source": source})
    reservation_id = int(sync_booking_to_core(conn, booking_id))
    ensure_public_reservation_token(
        conn,
        reservation_id=reservation_id,
        public_token=reservation_token,
    )
    sync_booking_assignment_to_core(conn, booking_id)
    return {
        "booking_id": booking_id,
        "reservation_token": reservation_token,
        "existing": False,
    }


def get_guest_summary(conn, phone_e164: str):
    g = conn.execute(
        """
        SELECT phone_e164, name_last, visits_count, last_visit_dt, first_visit_dt, tags_json
        FROM guests
        WHERE phone_e164=?
        """,
        (phone_e164,),
    ).fetchone()

    if not g:
        return None

    tags = get_tags(conn, phone_e164)

    visits = conn.execute(
        """
        SELECT reservation_dt, formname, source
        FROM guest_visits
        WHERE phone_e164=?
        ORDER BY reservation_dt DESC, id DESC
        LIMIT 3
        """,
        (phone_e164,),
    ).fetchall()

    notes = conn.execute(
        """
        SELECT note, actor_name, created_at
        FROM guest_notes
        WHERE phone_e164=?
        ORDER BY id DESC
        LIMIT 2
        """,
        (phone_e164,),
    ).fetchall()

    cancels_90 = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM guest_events
        WHERE phone_e164=? AND event_type='CANCELLED_BOOKING'
          AND datetime(created_at) >= datetime('now', '-90 day')
        """,
        (phone_e164,),
    ).fetchone()
    cancels_90 = int(cancels_90["c"] or 0) if cancels_90 else 0

    return g, tags, visits, notes, cancels_90
