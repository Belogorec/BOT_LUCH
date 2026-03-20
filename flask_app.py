import re
import hashlib
import hmac

import json
import urllib.parse

from flask import Flask, request

from dashboard_api import (
    admin_api_segments_impl,
    admin_api_load_impl,
)
from config import BOT_TOKEN
from db import connect, run_migrations, seed_discount_codes_from_csv
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
  return connect()


def validate_telegram_init_data(init_data_str: str) -> tuple[bool, dict]:
    raw = (init_data_str or "").strip()
    if not raw or not BOT_TOKEN:
        return False, {}

    pairs = urllib.parse.parse_qsl(raw, keep_blank_values=True)
    data: dict[str, str] = {}
    received_hash = ""

    for key, value in pairs:
        if key == "hash":
            received_hash = value
            continue
        data[key] = value

    if not received_hash:
        return False, {}

    data_check_string = "\n".join(f"{key}={data[key]}" for key in sorted(data))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        return False, {}

    user_obj = {}
    try:
        parsed_user = json.loads(data.get("user") or "{}")
        if isinstance(parsed_user, dict):
            user_obj = parsed_user
    except Exception:
        user_obj = {}

    return True, {"fields": data, "user": user_obj}


def bootstrap_schema():
  conn = connect()
  try:
    run_migrations(conn)
    seed_discount_codes_from_csv(conn)
    conn.commit()
  finally:
    conn.close()


bootstrap_schema()


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
      --bg:#0b0b0b;
      --card:#131313;
      --stroke:rgba(255,255,255,.08);
      --text:#f5f5f5;
      --muted:rgba(255,255,255,.62);
      --accent:#cc9933;
      --accent-2:#dd9933;
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
      padding:20px;
    }

    .eyebrow{
      color:var(--accent);
      font-size:11px;
      line-height:1.2;
      letter-spacing:.18em;
      text-transform:uppercase;
      margin:0 0 8px 0;
      font-weight:600;
    }

    h1{
      margin:0 0 6px 0;
      font-size:28px;
      line-height:1.05;
      letter-spacing:.01em;
      font-weight:700;
    }

    .sub{
      margin:0 0 20px 0;
      color:var(--muted);
      font-size:14px;
      line-height:1.45;
    }

    .grid{
      display:grid;
      grid-template-columns:repeat(3, minmax(0, 1fr));
      gap:12px;
      min-width:0;
      margin-bottom:6px;
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
      font-size:11px;
      line-height:1.2;
      color:var(--muted);
      letter-spacing:.08em;
      text-transform:uppercase;
      font-weight:500;
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
      border:1px solid rgba(255,255,255,.12);
      background:rgba(20,20,20,.6);
      color:var(--text);
      border-radius:12px;
      outline:none;
      box-shadow:none;
      appearance:none;
      -webkit-appearance:none;
      transition:border-color .2s ease, box-shadow .2s ease;
    }

    input,
    select{
      height:50px;
      padding:0 14px;
    }

    textarea{
      min-height:108px;
      resize:vertical;
      padding:12px 14px;
      font-size:14px;
    }

    input:focus,
    select:focus,
    textarea:focus{
      border-color:var(--accent);
      box-shadow:0 0 0 3px rgba(204,153,51,.12);
    }

    select{
      background-image:url("data:image/svg+xml;charset=UTF-8,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='rgba(245,245,245,.7)' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3e%3cpolyline points='6 9 12 15 18 9'%3e%3c/polyline%3e%3c/svg%3e");
      background-repeat:no-repeat;
      background-position:right 12px center;
      background-size:18px;
      padding-right:40px;
    }

    .hint{
      margin-top:14px;
      color:var(--muted);
      font-size:12px;
      line-height:1.45;
    }

    .error{
      min-height:20px;
      margin:12px 0 0 0;
      color:var(--danger);
      font-size:13px;
      line-height:1.35;
    }

    .actions{
      margin-top:18px;
      display:grid;
      grid-template-columns:1fr;
      gap:10px;
    }

    .btn{
      width:100%;
      min-height:54px;
      border:none;
      border-radius:12px;
      background:var(--accent);
      color:#0b0b0b;
      font-weight:700;
      font-size:15px;
      letter-spacing:.03em;
      cursor:pointer;
      transition:transform .12s ease, opacity .12s ease, background .12s ease;
    }

    .btn:active{
      transform:translateY(1px);
    }

    .btn[disabled]{
      opacity:.5;
      cursor:not-allowed;
    }

    .status{
      margin-top:12px;
      padding:12px 14px;
      border-radius:12px;
      font-size:14px;
      line-height:1.4;
      display:none;
    }

    .status.show{ display:block; }
    .status.ok{
      background:rgba(97,208,149,.12);
      border:1px solid rgba(97,208,149,.24);
      color:#a8f0d8;
    }
    .status.bad{
      background:rgba(255,107,107,.12);
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
        padding:16px;
        border-radius:14px;
      }

      h1{
        font-size:24px;
        margin-bottom:4px;
      }

      .grid{
        grid-template-columns:1fr;
        gap:10px;
      }

      input,
      select{
        height:48px;
      }

      textarea{
        min-height:100px;
      }

      .btn{
        min-height:50px;
        font-size:14px;
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
              <option>11</option>
              <option>12</option>
              <option>15</option>
              <option>20</option>
              <option>25</option>
              <option>30</option>
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
          tg.setHeaderColor("#0b0b0b");
          tg.setBackgroundColor("#0b0b0b");
        } catch (_) {}
      }

      const form = document.getElementById("reserveForm");
      const dateInput = document.getElementById("date");
      const timeInput = document.getElementById("time");
      const guestsInput = document.getElementById("guests");
      const commentInput = document.getElementById("comment");
      const submitBtn = document.getElementById("submitBtn");
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

      function makeReservationToken() {
        if (window.crypto && typeof window.crypto.randomUUID === "function") {
          return window.crypto.randomUUID();
        }
        return "req-" + Date.now() + "-" + Math.random().toString(16).slice(2);
      }

      function getReservationToken() {
        const key = "luch_reservation_token";
        let token = "";
        try {
          token = sessionStorage.getItem(key) || "";
          if (!token) {
            token = makeReservationToken();
            sessionStorage.setItem(key, token);
          }
        } catch (_) {
          token = makeReservationToken();
        }
        return token;
      }

      form.addEventListener("submit", function (e) {
        e.preventDefault();

        if (!validate()) {
          return;
        }

        const payload = {
          date: dateInput.value,
          time: timeInput.value,
          guests: guestsInput.value,
          comment: (commentInput.value || "").trim(),
          reservation_token: getReservationToken(),
          initData: (tg && tg.initData) ? tg.initData : ""
        };

        submitBtn.disabled = true;

        fetch("/api/booking", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(payload)
        })
        .then(function (r) { return r.json(); })
        .then(function (result) {
          if (result.ok) {
            try { sessionStorage.removeItem("luch_reservation_token"); } catch (_) {}
            showOk("Заявка отправлена ✓");
            setTimeout(function () {
              if (tg && typeof tg.close === "function") {
                tg.close();
              }
            }, 900);
          } else {
            showBad(result.error || "Ошибка отправки. Попробуйте ещё раз.");
            submitBtn.disabled = false;
          }
        })
        .catch(function () {
          showBad("Не удалось отправить заявку. Попробуйте ещё раз.");
          submitBtn.disabled = false;
        });
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



@app.route("/api/booking", methods=["POST", "OPTIONS"])
def api_submit_booking():
  if request.method == "OPTIONS":
    return ("", 204)

  data = request.get_json(silent=True) or {}

  init_data_str = (data.get("initData") or "").strip()
  init_ok, init_payload = validate_telegram_init_data(init_data_str)
  if not init_ok:
    return {"ok": False, "error": "Некорректные данные Telegram Mini App"}, 403

  tg_user_id = str(((init_payload.get("user") or {}).get("id")) or "").strip()
  if not tg_user_id:
    return {"ok": False, "error": "Не удалось определить пользователя Telegram"}, 400

  date_value = str(data.get("date") or "").strip()
  time_value = str(data.get("time") or "").strip()
  guests_value = str(data.get("guests") or "").strip()
  comment_value = str(data.get("comment") or "").strip()
  reservation_token = str(
    data.get("reservation_token")
    or data.get("request_id")
    or data.get("reservationRequestId")
    or ""
  ).strip()
  if not reservation_token:
    reservation_token = hashlib.sha256(
      f"{tg_user_id}|{date_value}|{time_value}|{guests_value}|{comment_value}".encode("utf-8")
    ).hexdigest()

  if not date_value or not time_value or not guests_value:
    return {"ok": False, "error": "Форма заполнена не полностью"}, 400

  try:
    guests_count = int(guests_value)
  except Exception:
    guests_count = 0
  if guests_count <= 0:
    return {"ok": False, "error": "Некорректное количество гостей"}, 400

  from booking_service import log_booking_event
  from booking_render import render_booking_card
  from telegram_api import tg_send_message as _tg_send
  from config import TG_CHAT_ID
  from crm_sync import send_booking_event

  conn = ensure_db()
  try:
    user_row = None
    if tg_user_id:
      user_row = conn.execute(
        "SELECT phone_e164, first_name FROM tg_bot_users WHERE tg_user_id=? AND has_shared_phone=1",
        (tg_user_id,),
      ).fetchone()
    phone_e164 = user_row["phone_e164"] if user_row else None
    saved_name = (user_row["first_name"] if user_row else None) or ""

    raw_payload = json.dumps({
      "source": "telegram_miniapp_api",
      "requester_tg_user_id": tg_user_id,
      "requester_chat_id": tg_user_id,
      "requester_name": saved_name,
      "reservation_token": reservation_token,
      "date": date_value,
      "time": time_value,
      "guests": guests_count,
      "comment": comment_value,
    }, ensure_ascii=False)

    existing = conn.execute(
      "SELECT id FROM bookings WHERE reservation_token=?",
      (reservation_token,),
    ).fetchone()
    if existing:
      existing_id = int(existing["id"])
      return {"ok": True, "booking_id": existing_id, "duplicate": True}

    cur = conn.execute(
      """
      INSERT INTO bookings
      (tranid, formname, name, phone_e164, phone_raw, user_chat_id,
       reservation_date, reservation_time, reservation_dt,
       guests_count, comment,
       utm_source, utm_medium, utm_campaign, utm_content, utm_term,
       status, guest_segment, reservation_token, raw_payload_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'WAITING', ?, ?, ?)
      """,
      (
        None,
        "telegram_miniapp",
        saved_name or "Telegram",
        phone_e164,
        phone_e164,
        tg_user_id,
        date_value,
        time_value,
        f"{date_value} {time_value}:00",
        guests_count,
        comment_value,
        "telegram", "miniapp", None, None, None,
        "NEW",
        reservation_token,
        raw_payload,
      ),
    )
    booking_id = int(cur.lastrowid)
    log_booking_event(conn, booking_id, "CREATED", tg_user_id, "", {"source": "telegram_miniapp_api"})

    if TG_CHAT_ID:
      card_text, kb = render_booking_card(conn, booking_id)
      try:
        msg_id = _tg_send(str(TG_CHAT_ID), card_text, kb)
        if msg_id:
          conn.execute(
            "UPDATE bookings SET telegram_chat_id=?, telegram_message_id=?, updated_at=datetime('now') WHERE id=?",
            (str(TG_CHAT_ID), str(msg_id), booking_id),
          )
          log_booking_event(conn, booking_id, "TG_SYNC_OK", "system", "system", {"target_chat_id": str(TG_CHAT_ID)})
      except Exception as e:
        log_booking_event(conn, booking_id, "TG_SYNC_FAIL", "system", "system", {"error": str(e)})

    try:
      sync_ok = send_booking_event(
        conn,
        booking_id,
        "BOOKING_UPSERT",
        {
          "actor_tg_id": tg_user_id or "system",
          "actor_name": saved_name or "telegram_miniapp_api",
          "payload": {"source": "telegram_miniapp_api"},
        },
      )
      if not sync_ok:
        log_booking_event(conn, booking_id, "CRM_SYNC_FAIL", "system", "system", {"source": "telegram_miniapp_api", "reason": "send_booking_event_false"})
    except Exception as e:
      log_booking_event(conn, booking_id, "CRM_SYNC_FAIL", "system", "system", {"source": "telegram_miniapp_api", "reason": str(e)})

    conn.commit()
    return {"ok": True, "booking_id": booking_id}
  except Exception as e:
    conn.rollback()
    return {"ok": False, "error": "Ошибка сервера"}, 500
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
