from datetime import datetime, timedelta

from flask import request, abort

from config import DASHBOARD_SECRET, ANALYTICS_TZ_OFFSET_HOURS
from db import connect


def ensure_db():
    return connect()


def require_dashboard_key():
    key = (request.args.get("key") or "").strip()
    if not DASHBOARD_SECRET or key != DASHBOARD_SECRET:
        abort(403)


def sqlite_hours_modifier(hours: int) -> str:
    return f"{hours:+d} hours"


def analytics_now_local() -> datetime:
    return datetime.utcnow() + timedelta(hours=ANALYTICS_TZ_OFFSET_HOURS)


def period_to_range(period: str):
    now_local = analytics_now_local()

    today0 = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    if period == "today":
        start_floor = today0
        end_excl = start_floor + timedelta(days=1)
        gran = "hour"
    elif period == "yesterday":
        start_floor = today0 - timedelta(days=1)
        end_excl = today0
        gran = "hour"
    elif period == "week":
        start_floor = today0 - timedelta(days=6)
        end_excl = today0 + timedelta(days=1)
        gran = "day"
    elif period == "30d":
        start_floor = today0 - timedelta(days=29)
        end_excl = today0 + timedelta(days=1)
        gran = "day"
    else:
        start_floor = today0
        end_excl = start_floor + timedelta(days=1)
        gran = "hour"

    return start_floor, end_excl, gran


def iter_labels(start_floor: datetime, end_excl: datetime, gran: str) -> list[str]:
    labels: list[str] = []
    cur = start_floor

    if gran == "day":
        while cur < end_excl:
            labels.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return labels

    while cur < end_excl:
        labels.append(cur.strftime("%Y-%m-%d %H:00"))
        cur += timedelta(hours=1)
    return labels


def fill_series(labels: list[str], rows: list[object], key_label: str, key_count: str) -> list[int]:
    def _get(row: object, key: str):
        if row is None:
            return None
        if isinstance(row, dict):
            return row.get(key)
        try:
            return row[key]
        except Exception:
            return None

    idx = {lab: i for i, lab in enumerate(labels)}
    out = [0] * len(labels)

    for r in rows or []:
        lab = (str(_get(r, key_label) or "")).strip()
        if not lab:
            continue
        i = idx.get(lab)
        if i is None:
            continue
        try:
            out[i] = int(_get(r, key_count) or 0)
        except Exception:
            out[i] = 0

    return out


def admin_api_segments_impl():
    require_dashboard_key()

    lookback_days = 730
    since_local = analytics_now_local() - timedelta(days=lookback_days)
    since_sql = since_local.strftime("%Y-%m-%d %H:%M:%S")

    conn = ensure_db()
    try:
        rows = conn.execute(
            """
            WITH active AS (
              SELECT DISTINCT phone_e164
              FROM guest_visits
              WHERE phone_e164 IS NOT NULL AND trim(phone_e164) <> ''
                AND reservation_dt IS NOT NULL AND trim(reservation_dt) <> ''
                AND datetime(replace(reservation_dt, 'T', ' ')) >= datetime(?)
            )
            SELECT
              CASE
                WHEN instr(COALESCE(g.tags_json, '[]'), '"VIP"') > 0 THEN 'VIP'
                WHEN COALESCE(g.visits_count, 0) <= 0 THEN '0'
                WHEN g.visits_count = 1 THEN '1'
                WHEN g.visits_count BETWEEN 2 AND 3 THEN '2-3'
                WHEN g.visits_count BETWEEN 4 AND 7 THEN '4-7'
                ELSE '8+'
              END AS bucket,
              COUNT(*) AS c
            FROM guests g
            JOIN active a ON a.phone_e164 = g.phone_e164
            GROUP BY bucket
            """,
            (since_sql,),
        ).fetchall()

        m = {r["bucket"]: int(r["c"] or 0) for r in rows} if rows else {}

        order = [
            ("VIP", "VIP"),
            ("0", "0 визитов"),
            ("1", "1 визит"),
            ("2-3", "2–3 визита"),
            ("4-7", "4–7 визитов"),
            ("8+", "8+ визитов"),
        ]

        total = sum(m.get(k, 0) for k, _ in order)
        buckets = []
        for k, title in order:
            c = m.get(k, 0)
            pct = (c * 100.0 / total) if total > 0 else 0.0
            buckets.append({"key": k, "label": title, "count": c, "pct": round(pct, 2)})

        return {
            "ok": True,
            "window": {"since": since_sql, "days": lookback_days},
            "total_guests": total,
            "buckets": buckets,
        }
    finally:
        conn.close()


def admin_api_load_impl(period: str):
    require_dashboard_key()

    period = (period or "today").strip().lower()
    start_floor, end_excl, gran = period_to_range(period)

    start_sql = start_floor.strftime("%Y-%m-%d %H:%M:%S")
    end_sql = end_excl.strftime("%Y-%m-%d %H:%M:%S")

    tzmod = sqlite_hours_modifier(ANALYTICS_TZ_OFFSET_HOURS)
    labels = iter_labels(start_floor, end_excl, gran)

    conn = ensure_db()
    try:
        fmt = "%Y-%m-%d" if gran == "day" else "%Y-%m-%d %H:00"

        created_rows = conn.execute(
            """
            SELECT
              strftime(?, datetime(re.created_at, ?)) AS b,
              COUNT(*) AS c
            FROM reservation_events re
            WHERE re.event_type='CREATED'
              AND datetime(re.created_at, ?) >= datetime(?)
              AND datetime(re.created_at, ?) <  datetime(?)
            GROUP BY b
            ORDER BY b
            """,
            (fmt, tzmod, tzmod, start_sql, tzmod, end_sql),
        ).fetchall()

        reserved_rows = conn.execute(
            """
            SELECT
              strftime(?, datetime(replace(r.reservation_at, 'T', ' '))) AS b,
              COUNT(*) AS c
            FROM reservations r
            WHERE r.reservation_at IS NOT NULL AND trim(r.reservation_at) <> ''
              AND datetime(replace(r.reservation_at, 'T', ' ')) >= datetime(?)
              AND datetime(replace(r.reservation_at, 'T', ' ')) <  datetime(?)
            GROUP BY b
            ORDER BY b
            """,
            (fmt, start_sql, end_sql),
        ).fetchall()

        created_series = fill_series(labels, created_rows, "b", "c")
        reserved_series = fill_series(labels, reserved_rows, "b", "c")

        hours = [f"{h:02d}" for h in range(24)]

        created_hour_rows = conn.execute(
            """
            SELECT strftime('%H', datetime(re.created_at, ?)) AS h, COUNT(*) AS c
            FROM reservation_events re
            WHERE re.event_type='CREATED'
              AND datetime(re.created_at, ?) >= datetime(?)
              AND datetime(re.created_at, ?) <  datetime(?)
            GROUP BY h
            ORDER BY h
            """,
            (tzmod, tzmod, start_sql, tzmod, end_sql),
        ).fetchall()

        reserved_hour_rows = conn.execute(
            """
            SELECT strftime('%H', datetime(replace(r.reservation_at, 'T', ' '))) AS h, COUNT(*) AS c
            FROM reservations r
            WHERE r.reservation_at IS NOT NULL AND trim(r.reservation_at) <> ''
              AND datetime(replace(r.reservation_at, 'T', ' ')) >= datetime(?)
              AND datetime(replace(r.reservation_at, 'T', ' ')) <  datetime(?)
            GROUP BY h
            ORDER BY h
            """,
            (start_sql, end_sql),
        ).fetchall()

        created_hour = fill_series(hours, created_hour_rows, "h", "c")
        reserved_hour = fill_series(hours, reserved_hour_rows, "h", "c")

        return {
            "ok": True,
            "period": {
                "key": period,
                "granularity": gran,
                "from": start_sql,
                "to_exclusive": end_sql,
                "tz_offset_hours": ANALYTICS_TZ_OFFSET_HOURS,
            },
            "timeseries": {"labels": labels, "created": created_series, "reserved": reserved_series},
            "hourly": {"labels": hours, "created": created_hour, "reserved": reserved_hour},
            "totals": {
                "created_total": int(sum(created_series)),
                "reserved_total": int(sum(reserved_series)),
            },
        }
    finally:
        conn.close()
