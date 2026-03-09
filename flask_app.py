import re

from flask import Flask, request

from dashboard_api import (
    admin_api_segments_impl,
    admin_api_load_impl,
)
from db import connect, init_schema
from tg_handlers import tg_webhook_impl
from tilda_api import tilda_webhook_impl

app = Flask(__name__)

# phone normalization
try:
    import phonenumbers  # type: ignore
except Exception:
    phonenumbers = None

TIME_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*$")


# =========================
# Helpers
# =========================
def normalize_name(v: str) -> str:
    v = (v or "").strip()
    v = re.sub(r"\s+", " ", v)
    return v


def normalize_time_hhmm(v: str) -> str:
    m = TIME_RE.match(str(v or "").strip())
    if not m:
        return ""
    hh, mm = int(m.group(1)), int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return ""
    return f"{hh:02d}:{mm:02d}"


def normalize_phone_e164(raw: str, default_region: str = "RU") -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""

    digits = re.sub(r"\D+", "", raw)
    if digits:
        if len(digits) == 11 and digits.startswith("8"):
            raw = "+7" + digits[1:]
        elif len(digits) == 11 and digits.startswith("7"):
            raw = "+7" + digits[1:]
        elif len(digits) == 10 and digits.startswith("9"):
            raw = "+7" + digits

    if phonenumbers is None:
        return raw if raw.startswith("+") else ("+" + digits if digits else "")

    try:
        num = phonenumbers.parse(raw, default_region)
        if not phonenumbers.is_possible_number(num):
            return ""
        return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return ""


def ensure_db():
    conn = connect()
    init_schema(conn)
    conn.commit()
    return conn


# =========================
# HTTP endpoints
# =========================
@app.get("/health")
def health():
    return {"ok": True}


@app.route("/public/api/guest", methods=["GET", "OPTIONS"])
def public_api_guest_lookup():
    if request.method == "OPTIONS":
        return ("", 204)

    phone_raw = (request.args.get("phone") or "").strip()
    phone_e164 = normalize_phone_e164(phone_raw, default_region="RU")
    if not phone_e164:
        return {"ok": True, "found": False}

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT name_last FROM guests WHERE phone_e164=?",
            (phone_e164,),
        ).fetchone()
        name_last = (row["name_last"] or "").strip() if row else ""
        if name_last:
            return {
                "ok": True,
                "found": True,
                "phone_e164": phone_e164,
                "name": name_last,
            }
        return {"ok": True, "found": False, "phone_e164": phone_e164}
    finally:
        conn.close()


@app.after_request
def _admin_api_cors(resp):
    if request.path.startswith("/admin/api/") or request.path.startswith("/public/api/"):
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


@app.route("/admin/api/segments", methods=["GET", "OPTIONS"])
def admin_api_segments():
    if request.method == "OPTIONS":
        return ("", 204)
    return admin_api_segments_impl()


@app.route("/admin/api/load", methods=["GET", "OPTIONS"])
def admin_api_load():
    if request.method == "OPTIONS":
        return ("", 204)
    period = (request.args.get("period") or "today").strip().lower()
    return admin_api_load_impl(period)


@app.post("/webhook/tilda")
def tilda_webhook():
    return tilda_webhook_impl(
        normalize_name=normalize_name,
        normalize_phone_e164=normalize_phone_e164,
        normalize_time_hhmm=normalize_time_hhmm,
    )


@app.post("/tg/webhook")
def tg_webhook():
    return tg_webhook_impl()


if __name__ == "__main__":
    app.run(debug=True)