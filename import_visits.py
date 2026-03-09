import csv
import sys
import re
from datetime import datetime

from db import connect, init_schema, rebuild_guests_from_visits

try:
    import phonenumbers  # type: ignore
except Exception:
    phonenumbers = None


DATE_ONLY_FORMATS = ("%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y")
DT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M")
TIME_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*$")


def norm_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_phone_e164(raw: str, default_region="RU") -> str:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("Empty phone")

    # сначала приведём популярные RU-форматы вручную (самый надёжный слой)
    digits = re.sub(r"\D+", "", raw)
    if digits:
        if len(digits) == 11 and digits.startswith("8"):
            raw = "+7" + digits[1:]
        elif len(digits) == 11 and digits.startswith("7"):
            raw = "+7" + digits[1:]
        elif len(digits) == 10 and digits.startswith("9"):
            raw = "+7" + digits

    if phonenumbers is None:
        # fallback: если смогли привести к +7XXXXXXXXXX
        if raw.startswith("+") and len(re.sub(r"\D+", "", raw)) >= 11:
            return raw
        raise ValueError(f"phonenumbers not installed and phone not normalized: {raw}")

    num = phonenumbers.parse(raw, default_region)
    if not phonenumbers.is_possible_number(num):
        raise ValueError(f"Impossible phone: {raw}")
    return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)


def parse_date_only_iso(s: str) -> str:
    s = (s or "").strip()
    for fmt in DATE_ONLY_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    raise ValueError(f"Bad date_form: {s}")


def parse_created_dt(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    for fmt in DT_FORMATS:
        try:
            # храним в исходном виде ISO с секундами если есть
            dt = datetime.strptime(s, fmt)
            return dt.isoformat(sep=" ", timespec="seconds")
        except Exception:
            pass
    # если не распарсили — оставим как строку, но не валим импорт
    return s


def normalize_time_hhmm(s: str) -> str:
    s = (s or "").strip()
    m = TIME_RE.match(s)
    if not m:
        return ""
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return ""
    return f"{hh:02d}:{mm:02d}"


def build_reservation_dt(date_iso: str, time_hhmm: str) -> str:
    # date_iso: YYYY-MM-DD
    # time: HH:MM
    if not date_iso or not time_hhmm:
        raise ValueError("Need date_form and Time to build reservation_dt")
    return f"{date_iso}T{time_hhmm}"


def sniff_delimiter(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
    return ";" if sample.count(";") > sample.count(",") else ","


def get_any(row: dict, *keys: str) -> str:
    for k in keys:
        if k in row and str(row.get(k) or "").strip() != "":
            return str(row.get(k) or "")
    return ""


def main():
    if len(sys.argv) < 2:
        print("Usage: python import_visits.py /path/to/visits.csv")
        sys.exit(1)

    path = sys.argv[1]
    delim = sniff_delimiter(path)

    conn = connect()
    init_schema(conn)

    inserted = 0
    skipped = 0

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)

        for row in reader:
            try:
                # По твоему примеру: Name, Phone, Date, date_form, Time, formname
                name = norm_name(get_any(row, "Name", "name", "Имя"))
                phone_raw = get_any(row, "Phone", "phone", "Телефон")
                created_dt = parse_created_dt(get_any(row, "Date", "created", "created_at"))
                date_form_raw = get_any(row, "date_form", "Date_form", "date", "Дата")
                time_raw = get_any(row, "Time", "time", "Время")
                formname = norm_name(get_any(row, "formname", "FormName", "form", "Форма"))

                phone = norm_phone_e164(phone_raw)
                date_form_iso = parse_date_only_iso(date_form_raw)
                time_hhmm = normalize_time_hhmm(time_raw)

                # если вдруг Time пустой/кривой — попробуем вытащить из created_dt
                if not time_hhmm and created_dt:
                    # created_dt может быть "YYYY-MM-DD HH:MM:SS"
                    m = re.search(r"\b(\d{2}:\d{2})\b", created_dt)
                    if m:
                        time_hhmm = normalize_time_hhmm(m.group(1))

                reservation_dt = build_reservation_dt(date_form_iso, time_hhmm)

                conn.execute(
                    """
                    INSERT INTO guest_visits
                      (phone_e164, name, reservation_dt, date_form, time_form, formname, created_dt, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'import')
                    """,
                    (phone, name, reservation_dt, date_form_iso, time_hhmm, formname, created_dt),
                )

                inserted += 1
            except Exception:
                skipped += 1
                continue

    rebuild_guests_from_visits(conn)
    conn.commit()
    conn.close()

    print(f"OK: inserted={inserted}, skipped={skipped}")


if __name__ == "__main__":
    main()
