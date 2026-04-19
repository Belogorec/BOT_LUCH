import unittest

from integration.inbound.tilda_parser import parse_tilda_booking_payload


def _normalize_name(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_phone_e164(raw: str, default_region: str = "RU") -> str:  # noqa: ARG001
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits
    return f"+{digits}" if digits else ""


def _normalize_time_hhmm(value: str) -> str:
    value = str(value or "").strip()
    if ":" not in value:
        return ""
    hh_raw, mm_raw = value.split(":", 1)
    if not (hh_raw.isdigit() and mm_raw.isdigit()):
        return ""
    hh = int(hh_raw)
    mm = int(mm_raw)
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return ""
    return f"{hh:02d}:{mm:02d}"


class TildaParserTests(unittest.TestCase):
    def _parse(self, payload: dict):
        return parse_tilda_booking_payload(
            payload,
            normalize_name=_normalize_name,
            normalize_phone_e164=_normalize_phone_e164,
            normalize_time_hhmm=_normalize_time_hhmm,
        )

    def test_payload_shape_basic_english_keys(self):
        parsed = self._parse(
            {
                "Name": "  Ivan   Ivanov ",
                "Phone": "+7 (999) 123-45-67",
                "date": "2026-05-01",
                "time": "18:30",
                "guests_count": "4",
                "comment": "window table",
                "tranid": "txn-1",
                "formname": "Booking Form",
                "utm_source": "instagram",
            }
        )
        self.assertEqual(parsed["name"], "Ivan Ivanov")
        self.assertEqual(parsed["phone_e164"], "+79991234567")
        self.assertEqual(parsed["date_raw"], "2026-05-01")
        self.assertEqual(parsed["time_raw"], "18:30")
        self.assertEqual(parsed["guests_count"], 4)
        self.assertEqual(parsed["comment"], "window table")
        self.assertEqual(parsed["tranid"], "txn-1")
        self.assertEqual(parsed["formname"], "Booking Form")
        self.assertEqual(parsed["utm_source"], "instagram")

    def test_payload_shape_russian_and_normalized_keys(self):
        parsed = self._parse(
            {
                "Имя": "Анна",
                "Телефон": "8 (901) 000-11-22",
                "Дата": "2026-06-15",
                "Время": "9:05",
                "Количество гостей": "Гостей: 3",
                "Комментарий к бронированию": "день рождения",
                "UTM Source": "ads",
            }
        )
        self.assertEqual(parsed["name"], "Анна")
        self.assertEqual(parsed["phone_e164"], "+79010001122")
        self.assertEqual(parsed["time_raw"], "09:05")
        self.assertEqual(parsed["guests_count"], 3)
        self.assertEqual(parsed["comment"], "день рождения")
        self.assertEqual(parsed["utm_source"], "ads")

    def test_tilda_parser_normalizes_dash_date_to_iso(self):
        parsed = self._parse(
            {
                "Name": "Test",
                "Phone": "+7 (999) 000-00-00",
                "date": "20-04-2026",
                "time": "22:22",
            }
        )
        self.assertEqual(parsed["date_raw"], "2026-04-20")
        self.assertEqual(parsed["time_raw"], "22:22")

    def test_invalid_inputs_keep_safe_defaults(self):
        parsed = self._parse(
            {
                "Name": "Test",
                "Phone": "abc",
                "date": "2026-07-20",
                "time": "25:99",
                "guests_count": "100 guests",
            }
        )
        self.assertEqual(parsed["phone_e164"], "")
        self.assertEqual(parsed["time_raw"], "")
        self.assertIsNone(parsed["guests_count"])
        self.assertEqual(parsed["comment"], "")
        self.assertEqual(parsed["tranid"], "")
        self.assertEqual(parsed["formname"], "Бронь стола")


if __name__ == "__main__":
    unittest.main()
