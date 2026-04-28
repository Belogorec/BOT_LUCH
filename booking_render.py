import html
from datetime import datetime

from booking_service import compute_segment, get_guest_summary, load_booking_read_model


def _h(s: str) -> str:
    return html.escape(s or "", quote=False)


def _booking_card_keyboard(booking_id: int) -> dict:
    def btn(text_, data_):
        return {"text": text_, "callback_data": data_}

    return {
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
                btn("📋 Ограничения", f"b:{booking_id}:table:show_restrictions"),
            ],
            [
                btn("💰 Депозит", f"b:{booking_id}:deposit:set"),
                btn("📋 История визитов", f"b:{booking_id}:visits"),
            ],
            [
                btn("✍️ Комментарий к гостю", f"b:{booking_id}:note"),
            ],
        ]
    }


def render_booking_card_from_reservation(reservation: dict) -> tuple[str, dict]:
    booking_id = int(reservation.get("booking_id") or reservation.get("reservation_id") or 0)
    if booking_id <= 0:
        return ("Бронь не найдена.", {"inline_keyboard": []})
    reservation_id = int(reservation.get("reservation_id") or booking_id)
    status = str(reservation.get("status") or "pending").upper()
    reservation_date = str(reservation.get("reservation_date") or "").strip()
    reservation_time = str(reservation.get("reservation_time") or "").strip()
    if not (reservation_date or reservation_time):
        reservation_at = str(reservation.get("reservation_at") or "").replace("T", " ")
        reservation_date = reservation_at[:10] if len(reservation_at) >= 10 else ""
        reservation_time = reservation_at[11:16] if len(reservation_at) >= 16 else ""
    dt_str = f"{reservation_date} {reservation_time}".strip()
    guest_name = str(reservation.get("guest_name") or "—")
    guest_phone = str(reservation.get("guest_phone") or "").strip()
    phone_line_value = (
        f"<a href=\"tel:{_h(guest_phone)}\">{_h(guest_phone)}</a>" if guest_phone else "—"
    )
    table_number = str(reservation.get("table_number") or "").strip()
    deposit_amount = reservation.get("deposit_amount")
    deposit_comment = str(reservation.get("deposit_comment") or "").strip()
    if deposit_amount:
        deposit_text = f"{int(deposit_amount)}" if str(deposit_amount).isdigit() else _h(str(deposit_amount))
        if deposit_comment:
            deposit_text += f" ({_h(deposit_comment)})"
    else:
        deposit_text = "—"
    comment = str(reservation.get("comment") or "").strip()
    restricted_until = str(reservation.get("restricted_until") or "").strip()
    restriction_line = ""
    if restricted_until:
        restriction_time = restricted_until[11:16] if len(restricted_until) >= 16 else restricted_until
        restriction_line = f"<b>Ограничение:</b> до <code>{_h(restriction_time)}</code>"
    text_parts = [
        "<b>LUCHBAR</b>",
        f"<b>Бронь #{booking_id}</b>",
        f"<b>CRM ID:</b> {_h(str(reservation_id))}",
        f"<b>Статус:</b> {_h(status)}",
        f"<b>Дата/время:</b> {_h(dt_str) if dt_str else '—'}",
        f"<b>Гостей:</b> {_h(str(reservation.get('party_size') or '—'))}",
        "",
        f"<b>Имя:</b> {_h(guest_name)}",
        f"<b>Телефон:</b> {phone_line_value}",
        "",
        f"<b>Стол:</b> {_h(table_number) if table_number else '—'}",
        f"<b>Депозит:</b> {deposit_text}",
    ]
    if restriction_line:
        text_parts.append(restriction_line)
    text_parts.extend([
        "",
        f"<b>Комментарий к брони:</b> {_h(comment) if comment else '—'}",
        "",
        f"<i>Обновлено из CRM: {_h(datetime.now().strftime('%H:%M:%S'))}</i>",
    ])
    return "\n".join(text_parts), _booking_card_keyboard(booking_id)


def render_booking_card(conn, booking_id: int) -> tuple[str, dict]:
    """
    Рендерит компактную карточку брони для админов.
    Содержит: номер, статус, дата/время, гостей, имя, телефон, статус гостя (на русском),
    количество визитов, комментарий к брони, заметки о госте.
    Кнопки: Подтвердить, Отменить, Комментарий к гостю, История визитов.
    """
    b = load_booking_read_model(conn, booking_id)
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
    deposit_value = b["deposit_amount"]
    deposit_comment = (b["deposit_comment"] or "").strip()
    if deposit_value:
        deposit_text = f"{int(deposit_value)}"
        if deposit_comment:
            deposit_text += f" ({_h(deposit_comment)})"
        deposit_line = f"<b>Депозит:</b> {deposit_text}"
    else:
        deposit_line = "<b>Депозит:</b> —"

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
        deposit_line,
        "",
        comment_line,
    ]
    
    # Добавляем заметки внизу, если есть
    if notes_block:
        text_parts.extend(["", notes_block])
    
    # Техническая строка
    text_parts.extend(["", f"<i>Обновлено: {_h(datetime.now().strftime('%H:%M:%S'))}</i>"])
    text = "\n".join([p for p in text_parts if p is not None])

    kb = _booking_card_keyboard(booking_id)

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
