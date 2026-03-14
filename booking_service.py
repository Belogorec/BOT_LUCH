import json
from typing import Optional
from datetime import datetime

from db import get_tags, set_tags


def now_iso_seconds_utc() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


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