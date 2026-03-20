import json
from typing import Optional
from datetime import datetime, timedelta

from db import get_tags, set_tags
from config import BUSINESS_TZ_OFFSET_HOURS

TABLE_LABELS = {"NONE", "DEPOSIT", "RESTRICTED"}
INACTIVE_BOOKING_STATUSES = {"DECLINED", "CANCELLED", "NO_SHOW"}
BUSINESS_NOW_SQL = f"{BUSINESS_TZ_OFFSET_HOURS:+d} hours"


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
    conn.execute(
        """
        INSERT INTO booking_events (booking_id, event_type, actor_tg_id, actor_name, payload_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (booking_id, event_type, actor_id, actor_name, json.dumps(payload or {}, ensure_ascii=False)),
    )


def log_table_event(
    conn,
    table_number: int,
    event_type: str,
    actor_id: str,
    actor_name: str,
    payload: Optional[dict] = None,
    booking_id: Optional[int] = None,
):
    conn.execute(
        """
        INSERT INTO table_events (table_number, booking_id, event_type, actor_tg_id, actor_name, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            int(table_number),
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


def normalize_table_number(value) -> Optional[int]:
    try:
        table_number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return table_number if table_number > 0 else None


def _booking_reservation_dt(booking_row) -> str:
    res_dt = str((booking_row["reservation_dt"] or "")).strip()
    if res_dt:
        return res_dt
    rd = str((booking_row["reservation_date"] or "")).strip()
    rt = str((booking_row["reservation_time"] or "")).strip()
    if rd and rt:
        return f"{rd}T{rt}"
    return ""


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


def get_table_state(conn, table_number: int):
    return conn.execute(
        """
        SELECT table_number, label, restricted_until, restriction_comment, updated_by, updated_at, created_at
        FROM venue_tables
        WHERE table_number = ?
        """,
        (int(table_number),),
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


def get_table_booking_conflicts(conn, table_number: int, reservation_dt: str, exclude_booking_id: int = 0):
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
        (int(table_number), reservation_dt, int(exclude_booking_id or 0)),
    ).fetchall()


def get_table_assignment_conflicts(conn, booking_row, table_number: int, exclude_booking_id: int = 0) -> dict:
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
        (int(table_number),),
    ).fetchone()
    return {
        "booking_conflicts": conflicts,
        "restricted": dict(restricted_row) if restricted_row else None,
    }


def _get_active_restriction_state(conn, table_number: int) -> Optional[dict]:
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
        (int(table_number),),
    ).fetchone()
    return dict(row) if row else None


def assign_table_to_booking(
    conn,
    booking_id: int,
    table_number: int,
    actor_id: str,
    actor_name: str,
    force_override: bool = False,
):
    normalized_table = normalize_table_number(table_number)
    if not normalized_table:
        raise ValueError("invalid_table_number")

    booking_row = conn.execute("SELECT * FROM bookings WHERE id = ?", (booking_id,)).fetchone()
    if not booking_row:
        raise ValueError("booking_not_found")

    conflicts = get_table_assignment_conflicts(conn, booking_row, normalized_table, exclude_booking_id=booking_id)
    if not force_override and (conflicts["booking_conflicts"] or conflicts["restricted"]):
        raise ValueError("table_conflict")

    prev_table = booking_row["assigned_table_number"]
    moved_restriction = _get_active_restriction_state(conn, prev_table) if prev_table and int(prev_table) != normalized_table else None
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
            (actor_id, int(prev_table)),
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
        log_table_event(conn, int(prev_table), "TABLE_LABEL_CLEARED", actor_id, actor_name, payload, booking_id=booking_id)
        log_table_event(conn, normalized_table, "TABLE_RESTRICTED", actor_id, actor_name, payload, booking_id=booking_id)
    return {"table_number": normalized_table, "conflicts": conflicts, "previous_table_number": prev_table}


def clear_table_assignment(conn, booking_id: int, actor_id: str, actor_name: str):
    booking_row = conn.execute("SELECT assigned_table_number FROM bookings WHERE id = ?", (booking_id,)).fetchone()
    if not booking_row:
        raise ValueError("booking_not_found")

    prev_table = booking_row["assigned_table_number"]
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
        log_table_event(conn, int(prev_table), "TABLE_CLEARED", actor_id, actor_name, payload, booking_id=booking_id)
    return {"previous_table_number": prev_table}


def set_booking_deposit(
    conn,
    booking_id: int,
    amount: int,
    actor_id: str,
    actor_name: str,
    comment: str = "",
):
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


def set_table_label(
    conn,
    table_number: int,
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

    prev_state = get_table_state(conn, normalized_table)
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
