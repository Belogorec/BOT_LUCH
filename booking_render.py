import html
from datetime import datetime

from booking_service import compute_segment, get_guest_summary


def _h(s: str) -> str:
    return html.escape(s or "", quote=False)


def render_booking_card(conn, booking_id: int) -> tuple[str, dict]:
    """
    Рендерит компактную карточку брони для админов.
    Содержит: номер, статус, дата/время, гостей, имя, телефон, статус гостя (на русском),
    количество визитов, комментарий к брони, заметки о госте.
    Кнопки: Подтвердить, Отменить, Комментарий к гостю, История визитов.
    """
    b = conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
    if not b:
        return ("Бронь не найдена.", {"inline_keyboard": []})

    phone = b["phone_e164"] or ""
    guest_data = get_guest_summary(conn, phone) if phone else None

    if guest_data:
        g, tags, visits, notes, cancels_90 = guest_data
        visits_count = int(g["visits_count"] or 0)
        seg_display = compute_segment(visits_count)
    else:
        notes = []
        visits_count = 0
        seg_display = "Новый"

    # Построение текста карточки
    title = f"<b>LUCHBAR</b>\n<b>Бронь #{b['id']}</b>"
    status = f"<b>Статус:</b> {_h(b['status'] or 'WAITING')}"

    dt_str = f"{(b['reservation_date'] or '').strip()} {(b['reservation_time'] or '').strip()}".strip()
    dt_line = f"<b>Дата/время:</b> {_h(dt_str) if dt_str else '—'}"
    guests_line = f"<b>Гостей:</b> {_h(str(b['guests_count'] if b['guests_count'] is not None else '—'))}"

    name_line = f"<b>Имя:</b> {_h(b['name'] or '—')}"
    phone_disp = phone or (b["phone_raw"] or "—")
    phone_link = f"<a href=\"tel:{_h(phone)}\">{_h(phone_disp)}</a>" if phone else _h(phone_disp)
    phone_line = f"<b>Телефон:</b> {phone_link}"

    seg_line = f"<b>Статус гостя:</b> {_h(seg_display)}"
    vc_line = f"<b>Визитов:</b> {_h(str(visits_count))}"

    comment = (b["comment"] or "").strip()
    comment_line = f"<b>Комментарий к брони:</b> {_h(comment) if comment else '—'}"
    table_line = f"<b>Стол:</b> {_h(str(b['assigned_table_number'])) if b['assigned_table_number'] else '—'}"

    # Блок заметок о госте (если есть)
    notes_block = ""
    if notes:
        parts = []
        for n in notes:
            who = (n["actor_name"] or "").strip()
            when = (n["created_at"] or "").strip()
            txt = (n["note"] or "").strip()
            head = f"{_h(when)}" + (f" — {_h(who)}" if who else "")
            parts.append(f"• {head}\n  {_h(txt)}")
        notes_block = "<b>Заметки о госте:</b>\n" + "\n".join(parts)

    # Собираем текст
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
        "",
        table_line,
        "",
        comment_line,
    ]
    
    # Добавляем заметки внизу, если есть
    if notes_block:
        text_parts.extend(["", notes_block])
    
    # Техническая строка
    text_parts.extend(["", f"<i>Обновлено: {_h(datetime.now().strftime('%H:%M:%S'))}</i>"])
    text = "\n".join([p for p in text_parts if p is not None])

    def btn(text_, data_):
        return {"text": text_, "callback_data": data_}

    kb = {
        "inline_keyboard": [
            [
                btn("✅ Подтвердить бронь", f"b:{booking_id}:booking:confirm"),
                btn("❌ Отменить бронь", f"b:{booking_id}:booking:cancel"),
            ],
            [
                btn("🪑 Назначить стол", f"b:{booking_id}:table:assign"),
                btn("🧹 Снять стол", f"b:{booking_id}:table:clear"),
            ],
            [
                btn("⛔ Ограничить стол", f"b:{booking_id}:table:restrict"),
                btn("📋 История визитов", f"b:{booking_id}:visits"),
            ],
            [
                btn("✍️ Комментарий к гостю", f"b:{booking_id}:note"),
            ],
        ]
    }

    return text, kb


def render_guest_visits_message(conn, phone_e164: str) -> str:
    """
    Рендерит отдельное сообщение с полной историей визитов гостя.
    Вызывается при нажатии на кнопку 'История визитов'.
    """
    from booking_service import get_guest_visits_full
    
    guest_data = get_guest_summary(conn, phone_e164) if phone_e164 else None
    
    if guest_data:
        g, tags, visits_3, notes, cancels_90 = guest_data
        visits_count = int(g["visits_count"] or 0)
        name = g["name_last"] or "—"
    else:
        visits_count = 0
        name = "—"

    seg_display = compute_segment(visits_count)

    # Получаем полную историю
    visits_full = get_guest_visits_full(conn, phone_e164) if phone_e164 else []

    title = f"<b>История визитов</b>\n"
    title += f"<b>Гость:</b> {_h(name)}\n"
    title += f"<b>Телефон:</b> <code>{_h(phone_e164)}</code>\n"
    title += f"<b>Статус:</b> {_h(seg_display)}\n"
    title += f"<b>Всего визитов:</b> {visits_count}"

    if not visits_full:
        return title + "\n\n<i>История визитов пуста</i>"

    parts = [title, "<b>Последние визиты:</b>"]
    for v in visits_full:
        vd = v["reservation_dt"] or "—"
        fn = v["formname"] or ""
        src = (v["source"] or "").strip()
        src_mark = " (подтв.)" if src == "confirmed_booking" else ""
        
        line_parts = [_h(vd)]
        if fn:
            line_parts.append(_h(fn))
        if src_mark:
            line_parts.append(src_mark)
        
        parts.append(f"• {' — '.join(line_parts)}")

    return "\n".join(parts)
