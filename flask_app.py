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


@app.get("/miniapp/reserve")
def miniapp_reserve():
    return """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
  <title>LUCHBAR • Бронирование</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    :root{
      --bg:#0f0f10;
      --card:#1b1b1d;
      --line:#2e2e33;
      --text:#f5f5f5;
      --muted:#a3a3ad;
      --accent:#ff8562;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      background:var(--bg);
      color:var(--text);
      font:16px/1.4 -apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
    }
    .wrap{
      max-width:720px;
      margin:0 auto;
      padding:20px 16px 28px;
    }
    .card{
      background:var(--card);
      border:1px solid var(--line);
      border-radius:18px;
      padding:16px;
      box-shadow:0 10px 30px rgba(0,0,0,.22);
    }
    h1{
      margin:0 0 8px;
      font-size:24px;
      line-height:1.15;
    }
    .sub{
      margin:0 0 18px;
      color:var(--muted);
      font-size:14px;
    }
    label{
      display:block;
      margin:14px 0 6px;
      font-size:14px;
      color:var(--muted);
    }
    input,select,textarea,button{
      width:100%;
      border-radius:12px;
      border:1px solid var(--line);
      background:#111214;
      color:var(--text);
      padding:14px 12px;
      font-size:16px;
      outline:none;
    }
    textarea{
      min-height:110px;
      resize:vertical;
    }
    .row{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:12px;
    }
    .btn{
      margin-top:18px;
      background:var(--accent);
      color:#fff;
      border:none;
      font-weight:700;
      cursor:pointer;
    }
    .hint{
      margin-top:12px;
      font-size:13px;
      color:var(--muted);
    }
    @media (max-width:640px){
      .row{grid-template-columns:1fr}
      .wrap{padding:14px 12px 22px}
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Бронирование LUCHBAR</h1>
      <p class="sub">Выберите дату, время и количество гостей. После отправки заявка сразу прилетит в Telegram.</p>

      <label for="date">Дата</label>
      <input id="date" type="date">

      <div class="row">
        <div>
          <label for="time">Время</label>
          <input id="time" type="time" step="1800">
        </div>
        <div>
          <label for="guests">Гостей</label>
          <select id="guests">
            <option value="1">1 гость</option>
            <option value="2" selected>2 гостя</option>
            <option value="3">3 гостя</option>
            <option value="4">4 гостя</option>
            <option value="5">5 гостей</option>
            <option value="6">6 гостей</option>
            <option value="7">7 гостей</option>
            <option value="8">8 гостей</option>
            <option value="9">9 гостей</option>
            <option value="10">10 гостей</option>
          </select>
        </div>
      </div>

      <label for="comment">Комментарий</label>
      <textarea id="comment" placeholder="Например: день рождения, нужен тихий стол, опаздываем на 15 минут"></textarea>

      <button id="submitBtn" class="btn" type="button">Отправить заявку</button>
      <div class="hint">Это базовая версия Mini App. Следующим шагом можно добавить автоподтягивание свободных слотов и телефона гостя.</div>
    </div>
  </div>

  <script>
    const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
    if (tg) {
      tg.ready();
      tg.expand();
    }

    const dateInput = document.getElementById("date");
    const timeInput = document.getElementById("time");
    const guestsInput = document.getElementById("guests");
    const commentInput = document.getElementById("comment");
    const submitBtn = document.getElementById("submitBtn");

    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, "0");
    const dd = String(now.getDate()).padStart(2, "0");
    dateInput.min = `${yyyy}-${mm}-${dd}`;
    dateInput.value = `${yyyy}-${mm}-${dd}`;
    timeInput.value = "19:00";

    submitBtn.addEventListener("click", function () {
      const payload = {
        source: "miniapp_reserve",
        date: dateInput.value || "",
        time: timeInput.value || "",
        guests: guestsInput.value || "",
        comment: (commentInput.value || "").trim()
      };

      if (!payload.date || !payload.time || !payload.guests) {
        alert("Заполните дату, время и количество гостей.");
        return;
      }

      if (!tg) {
        alert("Эту форму нужно открывать внутри Telegram.");
        return;
      }

      tg.sendData(JSON.stringify(payload));
      tg.close();
    });
  </script>
</body>
</html>
    """, 200, {"Content-Type": "text/html; charset=utf-8"}


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