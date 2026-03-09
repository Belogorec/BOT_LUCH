import html
from datetime import datetime

from booking_service import compute_segment, get_guest_summary


def _h(s: str) -> str:
    return html.escape(s or "", quote=False)


def render_booking_card(conn, booking_id: int) -> tuple[str, dict]:
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b:
        return ("Бронь не найдена.", {"inline_keyboard": []})

    phone = b["phone_e164"] or ""
    guest_data = get_guest_summary(conn, phone) if phone else None

    if guest_data:
        g, tags, visits, notes, cancels_90 = guest_data
        visits_count = int(g["visits_count"] or 0)
        seg = compute_segment(visits_count, tags)
    else:
        tags, visits, notes, cancels_90 = [], [], [], 0
        visits_count = 0
        seg = "NEW"

    bookings_count = 0
    if phone:
        r = conn.execute("SELECT COUNT(*) AS c FROM bookings WHERE phone_e164=?", (phone,)).fetchone()
        bookings_count = int(r["c"] or 0) if r else 0

    title = f"<b>LUCHBAR_reserve</b>\n<b>Бронь #{b['id']}</b>"
    status = f"<b>Статус:</b> {_h(b['status'] or 'WAITING')}"

    dt_str = f"{(b['reservation_date'] or '').strip()} {(b['reservation_time'] or '').strip()}".strip()
    dt_line = f"<b>Дата/время:</b> {_h(dt_str) if dt_str else '—'}"
    guests_line = f"<b>Гостей:</b> {_h(str(b['guests_count'] if b['guests_count'] is not None else '—'))}"

    name_line = f"<b>Имя:</b> {_h(b['name'] or '—')}"
    phone_disp = phone or (b["phone_raw"] or "—")
    phone_link = f"<a href=\"tel:{_h(phone)}\">{_h(phone_disp)}</a>" if phone else _h(phone_disp)
    phone_line = f"<b>Телефон:</b> {phone_link}"

    seg_line = f"<b>Гость:</b> {_h(seg)}"
    vc_line = f"<b>Визитов (история):</b> {_h(str(visits_count))}"
    bc_line = f"<b>Броней (в системе):</b> {_h(str(bookings_count))}"

    tags_line = "<b>Теги:</b> " + (_h(", ".join(tags)) if tags else "—")
    if cancels_90 > 0:
        tags_line += f"\n<b>Отмен за 90 дней:</b> {_h(str(cancels_90))}"

    comment = (b["comment"] or "").strip()
    comment_line = f"<b>Комментарий к брони:</b> {_h(comment) if comment else '—'}"

    history_block = ""
    if visits:
        parts = []
        for v in visits:
            vd = v["reservation_dt"]
            fn = v["formname"] or ""
            src = (v["source"] or "").strip()
            src_mark = " (подтв.)" if src == "confirmed_booking" else ""
            if fn:
                parts.append(f"• {_h(vd)} — {_h(fn)}{src_mark}")
            else:
                parts.append(f"• {_h(vd)}{src_mark}")
        history_block = "<b>Последние визиты:</b>\n" + "\n".join(parts)

    notes_block = ""
    if notes:
        parts = []
        for n in notes:
            who = (n["actor_name"] or "").strip()
            when = (n["created_at"] or "").strip()
            txt = (n["note"] or "").strip()
            head = f"{_h(when)}" + (f" — {_h(who)}" if who else "")
            parts.append(f"• {head}\n  {_h(txt)}")
        notes_block = "<b>Заметки по гостю:</b>\n" + "\n".join(parts)

    text_parts = [
        title,
        status,
        dt_line,
        guests_line,
        "",
        name_line,
        phone_line,
        "",
        seg_line,
        vc_line,
        bc_line,
        tags_line,
        "",
        comment_line,
    ]
    if history_block:
        text_parts.extend(["", history_block])
    if notes_block:
        text_parts.extend(["", notes_block])

    text_parts.extend(["", f"<i>Обновлено: {_h(datetime.now().strftime('%H:%M:%S'))}</i>"])
    text = "\n".join([p for p in text_parts if p is not None])

    def btn(text_, data_):
        return {"text": text_, "callback_data": data_}

    def mark(label: str, tag: str) -> str:
        return f"{'✅' if tag in set(tags) else '❌'} {label}"

    kb = {
        "inline_keyboard": [
            [
                btn("✅ Подтвердить бронь", f"b:{booking_id}:booking:confirm"),
                btn("❌ Отменили бронь", f"b:{booking_id}:booking:cancel"),
            ],
            [
                btn(mark("VIP", "VIP"), f"b:{booking_id}:tag:VIP"),
                btn(mark("НЕ ЗВОНИТЬ", "NOCALL"), f"b:{booking_id}:tag:NOCALL"),
            ],
            [
                btn(mark("АЛЛЕРГИИ", "ALLERGY"), f"b:{booking_id}:tag:ALLERGY"),
                btn(mark("ДЕТИ", "KIDS"), f"b:{booking_id}:tag:KIDS"),
            ],
            [
                btn("✍️ Комментарий к гостю", f"b:{booking_id}:note"),
            ],
        ]
    }

    return text, kb