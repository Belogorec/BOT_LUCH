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
  <meta
    name="viewport"
    content="width=device-width, initial-scale=1, maximum-scale=1, viewport-fit=cover"
  />
  <title>LUCHBAR • Бронирование</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    :root{
      --bg:#0a0a0a;
      --card:#121212;
      --stroke:rgba(255,255,255,.08);
      --text:#f5f5f5;
      --muted:rgba(255,255,255,.62);
      --accent:#c79a2b;
      --accent-2:#e0b54b;
      --danger:#ff6b6b;
      --ok:#61d095;
      --radius:18px;
      --shadow:0 18px 50px rgba(0,0,0,.28);
    }

    *{ box-sizing:border-box; }
    html,body{
      margin:0;
      padding:0;
      width:100%;
      max-width:100%;
      overflow-x:hidden;
      background:var(--bg);
      color:var(--text);
      font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
      -webkit-text-size-adjust:100%;
    }

    body{
      min-height:100vh;
      min-height:100dvh;
    }

    .page{
      width:100%;
      max-width:100%;
      min-height:100vh;
      min-height:100dvh;
      padding:
        max(16px, env(safe-area-inset-top))
        14px
        max(20px, env(safe-area-inset-bottom))
        14px;
      display:flex;
      align-items:flex-start;
      justify-content:center;
      overflow-x:hidden;
    }

    .card{
      width:min(100%, 560px);
      min-width:0;
      background:linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.01));
      border:1px solid var(--stroke);
      border-radius:var(--radius);
      box-shadow:var(--shadow);
      padding:18px;
    }

    .eyebrow{
      color:var(--accent);
      font-size:12px;
      line-height:1.2;
      letter-spacing:.16em;
      text-transform:uppercase;
      margin:0 0 8px 0;
    }

    h1{
      margin:0 0 8px 0;
      font-size:28px;
      line-height:1.05;
      letter-spacing:.01em;
    }

    .sub{
      margin:0 0 18px 0;
      color:var(--muted);
      font-size:14px;
      line-height:1.45;
    }

    .grid{
      display:grid;
      grid-template-columns:repeat(3, minmax(0, 1fr));
      gap:12px;
      min-width:0;
    }

    .field{
      display:flex;
      flex-direction:column;
      gap:8px;
      min-width:0;
      width:100%;
    }

    .field--full{
      grid-column:1 / -1;
    }

    .label{
      font-size:12px;
      line-height:1.2;
      color:var(--muted);
      letter-spacing:.06em;
      text-transform:uppercase;
    }

    input,
    select,
    textarea,
    button{
      font:inherit;
    }

    input,
    select,
    textarea{
      width:100%;
      min-width:0;
      max-width:100%;
      border:1px solid rgba(255,255,255,.10);
      background:#111;
      color:var(--text);
      border-radius:14px;
      outline:none;
      box-shadow:none;
      appearance:none;
      -webkit-appearance:none;
    }

    input,
    select{
      height:52px;
      padding:0 14px;
    }

    textarea{
      min-height:112px;
      resize:vertical;
      padding:14px;
    }

    input:focus,
    select:focus,
    textarea:focus{
      border-color:rgba(199,154,43,.9);
      box-shadow:0 0 0 3px rgba(199,154,43,.16);
    }

    .hint{
      margin-top:14px;
      color:var(--muted);
      font-size:13px;
      line-height:1.45;
    }

    .error{
      min-height:20px;
      margin-top:12px;
      color:var(--danger);
      font-size:13px;
      line-height:1.35;
    }

    .actions{
      margin-top:16px;
      display:grid;
      grid-template-columns:1fr;
      gap:10px;
    }

    .btn{
      width:100%;
      min-height:54px;
      border:none;
      border-radius:14px;
      background:var(--accent);
      color:#111;
      font-weight:700;
      font-size:16px;
      letter-spacing:.02em;
      cursor:pointer;
      transition:transform .15s ease, opacity .15s ease, background .15s ease;
    }

    .btn:active{
      transform:translateY(1px);
    }

    .btn[disabled]{
      opacity:.6;
      cursor:default;
    }

    .ghost{
      background:transparent;
      color:var(--text);
      border:1px solid rgba(255,255,255,.12);
    }

    .status{
      margin-top:12px;
      padding:12px 14px;
      border-radius:14px;
      font-size:14px;
      line-height:1.4;
      display:none;
    }

    .status.show{ display:block; }
    .status.ok{
      background:rgba(97,208,149,.10);
      border:1px solid rgba(97,208,149,.24);
      color:#baf0cf;
    }
    .status.bad{
      background:rgba(255,107,107,.10);
      border:1px solid rgba(255,107,107,.24);
      color:#ffc4c4;
    }

    @media (max-width: 640px){
      .page{
        padding:
          max(12px, env(safe-area-inset-top))
          10px
          max(16px, env(safe-area-inset-bottom))
          10px;
      }

      .card{
        width:100%;
        padding:14px;
        border-radius:16px;
      }

      h1{
        font-size:24px;
      }

      .grid{
        grid-template-columns:1fr;
        gap:10px;
      }

      input,
      select{
        height:50px;
      }

      textarea{
        min-height:104px;
      }
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="card">
      <p class="eyebrow">LUCHBAR</p>
      <h1>Бронирование</h1>
      <p class="sub">
        Выберите дату, время и количество гостей. После отправки заявка уйдёт администраторам в Telegram.
      </p>

      <form id="reserveForm" novalidate>
        <div class="grid">
          <label class="field" for="date">
            <span class="label">Дата</span>
            <input type="date" id="date" name="date" required />
          </label>

          <label class="field" for="time">
            <span class="label">Время</span>
            <input type="time" id="time" name="time" required step="300" />
          </label>

          <label class="field" for="guests">
            <span class="label">Гостей</span>
            <select id="guests" name="guests" required>
              <option value="">Выберите</option>
              <option>1</option>
              <option>2</option>
              <option>3</option>
              <option>4</option>
              <option>5</option>
              <option>6</option>
              <option>7</option>
              <option>8</option>
              <option>9</option>
              <option>10</option>
            </select>
          </label>

          <label class="field field--full" for="comment">
            <span class="label">Комментарий</span>
            <textarea
              id="comment"
              name="comment"
              placeholder="Например: стол у окна, день рождения, детский стул"
            ></textarea>
          </label>
        </div>

        <div id="error" class="error"></div>

        <div class="actions">
          <button id="submitBtn" class="btn" type="submit">Отправить заявку</button>
          <button id="closeBtn" class="btn ghost" type="button">Закрыть</button>
        </div>

        <div class="hint">
          После отправки менеджер свяжется с гостем для подтверждения брони.
        </div>

        <div id="statusOk" class="status ok"></div>
        <div id="statusBad" class="status bad"></div>
      </form>
    </div>
  </div>

  <script>
    (function () {
      const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;

      if (tg) {
        tg.ready();
        tg.expand();
        try {
          tg.setHeaderColor("#0a0a0a");
          tg.setBackgroundColor("#0a0a0a");
        } catch (_) {}
      }

      const form = document.getElementById("reserveForm");
      const dateInput = document.getElementById("date");
      const timeInput = document.getElementById("time");
      const guestsInput = document.getElementById("guests");
      const commentInput = document.getElementById("comment");
      const submitBtn = document.getElementById("submitBtn");
      const closeBtn = document.getElementById("closeBtn");
      const errorBox = document.getElementById("error");
      const statusOk = document.getElementById("statusOk");
      const statusBad = document.getElementById("statusBad");

      function pad(n) {
        return String(n).padStart(2, "0");
      }

      function toDateISO(d) {
        return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate());
      }

      function toTimeHM(d) {
        return pad(d.getHours()) + ":" + pad(d.getMinutes());
      }

      function ceilToStepMinutes(date, step) {
        const ms = step * 60 * 1000;
        return new Date(Math.ceil(date.getTime() / ms) * ms);
      }

      function showError(text) {
        errorBox.textContent = text || "";
        statusBad.className = "status bad";
        statusBad.textContent = "";
        statusOk.className = "status ok";
        statusOk.textContent = "";
      }

      function showOk(text) {
        errorBox.textContent = "";
        statusBad.className = "status bad";
        statusBad.textContent = "";
        statusOk.className = "status ok show";
        statusOk.textContent = text;
      }

      function showBad(text) {
        errorBox.textContent = "";
        statusOk.className = "status ok";
        statusOk.textContent = "";
        statusBad.className = "status bad show";
        statusBad.textContent = text;
      }

      function applyConstraints() {
        const now = new Date();
        const todayISO = toDateISO(now);
        const minDt = ceilToStepMinutes(new Date(now.getTime() + 20 * 60 * 1000), 5);

        dateInput.min = todayISO;
        if (!dateInput.value) {
          dateInput.value = todayISO;
        }

        if (!timeInput.value) {
          timeInput.value = toTimeHM(minDt);
        }

        if (dateInput.value === todayISO) {
          const minTime = toTimeHM(minDt);
          timeInput.min = minTime;
          if (timeInput.value && timeInput.value < minTime) {
            timeInput.value = minTime;
          }
        } else {
          timeInput.min = "";
        }
      }

      function validate() {
        showError("");

        if (!dateInput.value) {
          showError("Выберите дату.");
          return false;
        }

        if (!timeInput.value) {
          showError("Выберите время.");
          return false;
        }

        if (!guestsInput.value) {
          showError("Выберите количество гостей.");
          return false;
        }

        const now = new Date();
        const minAllowed = new Date(now.getTime() + 20 * 60 * 1000);

        const d = dateInput.value.split("-").map(Number);
        const t = timeInput.value.split(":").map(Number);
        const chosen = new Date(d[0], d[1] - 1, d[2], t[0], t[1], 0, 0);

        if (chosen < minAllowed) {
          showError("Для ближайшей брони выберите время не раньше чем через 20 минут.");
          return false;
        }

        return true;
      }

      applyConstraints();
      dateInput.addEventListener("change", applyConstraints);
      timeInput.addEventListener("change", applyConstraints);

      form.addEventListener("submit", function (e) {
        e.preventDefault();

        if (!validate()) {
          return;
        }

        const payload = {
          date: dateInput.value,
          time: timeInput.value,
          guests: guestsInput.value,
          comment: (commentInput.value || "").trim()
        };

        submitBtn.disabled = true;

        try {
          if (tg && typeof tg.sendData === "function") {
            tg.sendData(JSON.stringify(payload));
            showOk("Заявка отправлена. Сейчас можно закрыть окно.");
          } else {
            showBad("Mini App открыт вне Telegram WebView.");
          }
        } catch (err) {
          showBad("Не удалось отправить заявку. Попробуйте ещё раз.");
        } finally {
          setTimeout(function () {
            submitBtn.disabled = false;
          }, 800);
        }
      });

      closeBtn.addEventListener("click", function () {
        if (tg && typeof tg.close === "function") {
          tg.close();
        }
      });
    })();
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