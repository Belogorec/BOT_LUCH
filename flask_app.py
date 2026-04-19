import re
import hashlib
import hmac
from datetime import datetime

import json
import urllib.parse

from flask import Flask, request

from application import execute_telegram_miniapp_booking
from contact_schema import run_contact_schema_migrations
from core_sync import migrate_all_tables_to_core
from core_schema import run_core_schema_migrations
from dashboard_api import (
    admin_api_segments_impl,
    admin_api_load_impl,
)
from integration_schema import run_integration_schema_migrations
from config import BOT_TOKEN, find_vk_bot_config_by_group_id
from db import connect, run_migrations, seed_discount_codes_from_csv
from local_log import log_event, log_exception
from hostess_card_delivery import get_hostess_card_link
from tg_handlers import tg_webhook_impl
from tilda_api import tilda_webhook_impl
from telegram_api import tg_edit_message
from vk_api import vk_answer_message_event, vk_api_enabled, vk_send_message
from vk_staff_flow import parse_vk_event_payload, parse_vk_message_payload, process_vk_booking_payload, process_vk_pending_text
from vk_staff_notify import upsert_vk_staff_peer
from booking_render import render_booking_card
from booking_service import (
    assign_table_to_booking,
    clear_booking_deposit,
    clear_table_assignment,
    create_manual_booking,
    load_booking_read_model,
    normalize_table_number,
    reschedule_booking,
    set_booking_status,
    set_booking_deposit,
    set_table_label,
    update_booking_guests_count,
)
from waiter_notify import notify_waiters_about_deposit_booking

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


def _crm_sync_authorized(req) -> bool:
    payload = req.get_json(silent=True) or {}
    incoming = str(req.headers.get("X-Bot-Token") or payload.get("bot_token") or "").strip()
    return bool(BOT_TOKEN) and incoming == BOT_TOKEN


def _resolve_vk_callback_bot(payload: dict, *, require_secret: bool = True) -> dict:
    incoming_group_id = str(payload.get("group_id") or "").strip()
    bot = find_vk_bot_config_by_group_id(incoming_group_id)
    if not bot:
        return {}

    if not require_secret:
        return bot

    incoming_secret = str(payload.get("secret") or "").strip()
    expected_secret = str(bot.get("callback_secret") or "").strip()

    if expected_secret and incoming_secret != expected_secret:
        return {}
    return bot


def _refresh_admin_booking_card(conn, booking_id: int) -> None:
    reservation_row = conn.execute(
        """
        SELECT id
        FROM reservations
        WHERE source='legacy_booking' AND external_ref=?
        LIMIT 1
        """,
        (str(int(booking_id)),),
    ).fetchone()
    if not reservation_row:
        return
    link = get_hostess_card_link(conn, reservation_id=int(reservation_row["id"]))
    if not link or not link["chat_id"] or not link["message_id"]:
        return
    text, kb = render_booking_card(conn, booking_id)
    tg_edit_message(link["chat_id"], link["message_id"], text, kb)


def _crm_set_booking_status(conn, booking_id: int, status: str, actor_id: str, actor_name: str) -> None:
    set_booking_status(conn, booking_id, status, actor_id, actor_name, source="crm")


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
    run_core_schema_migrations(conn)
    run_integration_schema_migrations(conn)
    run_contact_schema_migrations(conn)
    seed_discount_codes_from_csv(conn)
    migrate_all_tables_to_core(conn)
    conn.commit()
  finally:
    conn.close()


@app.route("/admin/api/crm-sync/booking/<int:booking_id>", methods=["POST"])
def crm_sync_booking(booking_id: int):
    if not _crm_sync_authorized(request):
        return {"ok": False, "error": "forbidden"}, 403

    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "").strip().lower()
    data = payload.get("payload") or {}
    actor_id = str(payload.get("actor_tg_id") or "crm")
    actor_name = str(payload.get("actor_name") or "crm")

    conn = connect()
    should_notify_waiters = False
    try:
        booking = load_booking_read_model(conn, booking_id)
        if not booking:
            return {"ok": False, "error": "booking_not_found"}, 404

        if action == "confirm":
            _crm_set_booking_status(conn, booking_id, "CONFIRMED", actor_id, actor_name)
        elif action == "decline":
            _crm_set_booking_status(conn, booking_id, "DECLINED", actor_id, actor_name)
        elif action == "cancel":
            _crm_set_booking_status(conn, booking_id, "CANCELLED", actor_id, actor_name)
        elif action == "no_show":
            _crm_set_booking_status(conn, booking_id, "NO_SHOW", actor_id, actor_name)
        elif action == "complete":
            _crm_set_booking_status(conn, booking_id, "COMPLETED", actor_id, actor_name)
        elif action == "reschedule":
            reservation_date = str(data.get("reservation_date") or "").strip()
            reservation_time = normalize_time_hhmm(data.get("reservation_time") or "")
            reschedule_booking(
                conn,
                booking_id,
                reservation_date,
                reservation_time,
                actor_id,
                actor_name,
                source="crm",
            )
        elif action == "update_guests":
            update_booking_guests_count(
                conn,
                booking_id,
                data.get("guests_count"),
                actor_id,
                actor_name,
                source="crm",
            )
        elif action == "assign_table":
            table_number = normalize_table_number(data.get("table_number"))
            if not table_number:
                return {"ok": False, "error": "invalid_table_number"}, 400
            assign_table_to_booking(
                conn,
                booking_id,
                table_number,
                actor_id,
                actor_name,
                force_override=str(data.get("force_override") or "").strip() == "1",
            )
            should_notify_waiters = True
        elif action == "clear_table":
            clear_table_assignment(conn, booking_id, actor_id, actor_name)
        elif action == "restrict_table":
            table_number = normalize_table_number(data.get("table_number"))
            if not table_number:
                table_number = normalize_table_number(booking.get("assigned_table_number"))
            if not table_number:
                return {"ok": False, "error": "invalid_table_number"}, 400
            set_table_label(
                conn,
                table_number,
                "RESTRICTED",
                actor_id,
                actor_name,
                restricted_until=str(data.get("restricted_until") or "").strip(),
                restriction_comment=str(data.get("table_comment") or "").strip(),
                booking_id=booking_id,
                force_override=str(data.get("force_override") or "").strip() == "1",
            )
        elif action == "clear_table_restriction":
            table_number = normalize_table_number(data.get("table_number") or booking.get("assigned_table_number"))
            if not table_number:
                return {"ok": False, "error": "invalid_table_number"}, 400
            set_table_label(conn, table_number, "NONE", actor_id, actor_name, booking_id=booking_id)
        elif action == "set_deposit":
            set_booking_deposit(
                conn,
                booking_id,
                data.get("deposit_amount"),
                actor_id,
                actor_name,
                comment=str(data.get("deposit_comment") or "").strip(),
            )
            should_notify_waiters = True
        elif action == "clear_deposit":
            clear_booking_deposit(conn, booking_id, actor_id, actor_name)
        else:
            return {"ok": False, "error": "action_not_supported"}, 400

        conn.commit()
        try:
            _refresh_admin_booking_card(conn, booking_id)
        except Exception:
            pass
        if should_notify_waiters:
            try:
                notify_waiters_about_deposit_booking(conn, booking_id)
            except Exception:
                pass
        return {"ok": True, "booking_id": booking_id}, 200
    except ValueError as exc:
        conn.rollback()
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        conn.rollback()
        return {"ok": False, "error": str(exc)}, 500
    finally:
        conn.close()


@app.route("/admin/api/crm-sync/bookings/recent", methods=["GET"])
def crm_sync_recent_bookings():
    if not _crm_sync_authorized(request):
        return {"ok": False, "error": "forbidden"}, 403

    try:
        limit = max(1, min(int(request.args.get("limit", 200) or 200), 500))
    except (TypeError, ValueError):
        limit = 200

    try:
        days = max(1, min(int(request.args.get("days", 30) or 30), 180))
    except (TypeError, ValueError):
        days = 30

    conn = connect()
    try:
        from crm_sync import build_booking_sync_payload

        rows = conn.execute(
            """
            SELECT external_ref
            FROM reservations
            WHERE source='legacy_booking'
              AND COALESCE(status, 'pending') NOT IN ('declined', 'cancelled', 'no_show', 'completed')
              AND (
                    datetime(updated_at) >= datetime('now', ?)
                    OR datetime(replace(reservation_at, 'T', ' ')) >= datetime('now', ?)
                  )
              AND external_ref IS NOT NULL
              AND trim(external_ref) <> ''
            ORDER BY datetime(updated_at) DESC, id DESC
            LIMIT ?
            """,
            (f"-{int(days)} days", f"-{int(days)} days", int(limit)),
        ).fetchall()

        items = [
            build_booking_sync_payload(
                conn,
                int(row["external_ref"]),
                "BOOKING_UPSERT",
                {"actor_tg_id": "system", "actor_name": "recent_pull", "payload": {"source": "recent_pull"}},
            )
            for row in rows
            if str(row["external_ref"]).strip().isdigit()
        ]
        return {"ok": True, "items": items, "count": len(items)}, 200
    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500
    finally:
        conn.close()


@app.route("/admin/api/crm-sync/manual-booking", methods=["POST"])
def crm_sync_manual_booking():
    if not _crm_sync_authorized(request):
        return {"ok": False, "error": "forbidden"}, 403

    payload = request.get_json(silent=True) or {}
    data = payload.get("payload") or {}
    actor_id = str(payload.get("actor_tg_id") or "crm")
    actor_name = str(payload.get("actor_name") or "crm")

    guest_name = str(data.get("guest_name") or "").strip() or "CRM"
    guest_phone = str(data.get("guest_phone") or "").strip() or None
    reservation_date = str(data.get("reservation_date") or "").strip()
    reservation_time = normalize_time_hhmm(data.get("reservation_time") or "")
    comment = str(data.get("table_comment") or data.get("comment") or "").strip()
    table_number = normalize_table_number(data.get("table_number"))
    session_mode = str(data.get("session_mode") or "").strip().lower()

    try:
        guests_count = int(str(data.get("guests_count") or "").strip())
    except (TypeError, ValueError):
        return {"ok": False, "error": "invalid_guests_count"}, 400
    if guests_count <= 0:
        return {"ok": False, "error": "invalid_guests_count"}, 400

    if not reservation_date:
        reservation_date = datetime.utcnow().strftime("%Y-%m-%d")
    if not reservation_time:
        reservation_time = datetime.utcnow().strftime("%H:%M")

    conn = connect()
    try:
        result = create_manual_booking(
            conn,
            guest_name=guest_name,
            guest_phone=guest_phone,
            reservation_date=reservation_date,
            reservation_time=reservation_time,
            guests_count=guests_count,
            comment=comment,
            actor_id=actor_id,
            actor_name=actor_name,
            table_number=table_number,
            session_mode=session_mode,
            deposit_amount=data.get("deposit_amount"),
            deposit_comment=str(data.get("deposit_comment") or "").strip(),
        )
        booking_id = int(result["booking_id"])
        conn.commit()

        try:
            _refresh_admin_booking_card(conn, booking_id)
        except Exception:
            pass
        if result.get("notify_waiters"):
            try:
                notify_waiters_about_deposit_booking(conn, booking_id)
            except Exception:
                pass
        return {"ok": True, "booking_id": booking_id}, 200
    except ValueError as exc:
        conn.rollback()
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        conn.rollback()
        return {"ok": False, "error": str(exc)}, 500
    finally:
        conn.close()


@app.route("/admin/api/crm-sync/table", methods=["POST"])
def crm_sync_table():
    if not _crm_sync_authorized(request):
        return {"ok": False, "error": "forbidden"}, 403

    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "").strip().lower()
    data = payload.get("payload") or {}
    actor_id = str(payload.get("actor_tg_id") or "crm")
    actor_name = str(payload.get("actor_name") or "crm")
    table_number = normalize_table_number(data.get("table_number"))
    if not table_number:
        return {"ok": False, "error": "invalid_table_number"}, 400

    conn = connect()
    try:
        if action == "clear_table_restriction":
            set_table_label(conn, table_number, "NONE", actor_id, actor_name)
        elif action == "set_table_label":
            set_table_label(
                conn,
                table_number,
                str(data.get("table_label") or "").strip().upper(),
                actor_id,
                actor_name,
                restricted_until=str(data.get("restricted_until") or "").strip(),
                restriction_comment=str(data.get("table_comment") or "").strip(),
                force_override=str(data.get("force_override") or "").strip() == "1",
            )
        else:
            return {"ok": False, "error": "action_not_supported"}, 400
        conn.commit()
        return {"ok": True, "table_number": table_number}, 200
    except ValueError as exc:
        conn.rollback()
        return {"ok": False, "error": str(exc)}, 400
    except Exception as exc:
        conn.rollback()
        return {"ok": False, "error": str(exc)}, 500
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

  conn = ensure_db()
  try:
    result = execute_telegram_miniapp_booking(
      conn,
      tg_user_id=tg_user_id,
      date_value=date_value,
      time_value=time_value,
      guests_count=guests_count,
      comment_value=comment_value,
      reservation_token=reservation_token,
    )
    conn.commit()
    return result
  except Exception:
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


@app.post("/vk/callback")
def vk_callback():
    payload = request.get_json(silent=True) or {}
    event_type = str(payload.get("type") or "").strip()
    incoming_group_id = str(payload.get("group_id") or "").strip()
    event_object = payload.get("object") or {}
    vk_bot = _resolve_vk_callback_bot(payload, require_secret=event_type != "confirmation")
    bot_key = str(vk_bot.get("bot_key") or "").strip() or "unknown"
    role_hint = str(vk_bot.get("role_hint") or "").strip() or bot_key

    log_event(
        "VK-CALLBACK",
        status="incoming",
        event_type=event_type or "-",
        group_id=incoming_group_id or "-",
        bot_key=bot_key,
    )

    if event_type == "confirmation":
        if not vk_bot:
            log_event("VK-CALLBACK", status="confirmation_forbidden", group_id=incoming_group_id or "-")
            return ("forbidden", 403)
        confirmation_token = str(vk_bot.get("confirmation_token") or "").strip()
        if not confirmation_token:
            log_event("VK-CALLBACK", status="confirmation_missing_token")
            return ("VK confirmation token missing", 500)
        log_event("VK-CALLBACK", status="confirmation_ok", bot_key=bot_key)
        return confirmation_token

    if not vk_bot:
        log_event("VK-CALLBACK", status="forbidden", event_type=event_type or "-", group_id=incoming_group_id or "-", bot_key=bot_key)
        return ("forbidden", 403)

    if event_type == "message_new":
        message = event_object.get("message") or {}
        peer_id = message.get("peer_id")
        from_id = message.get("from_id")
        text = str(message.get("text") or "").strip()
        payload_data = parse_vk_message_payload(message)
        conn = connect()
        try:
            is_new_peer = upsert_vk_staff_peer(
                conn,
                bot_key=bot_key,
                role_hint=role_hint,
                peer_id=peer_id,
                from_id=from_id,
                message_text=text,
            )
            handled = False
            if bot_key == "hostess" and peer_id and payload_data:
                handled = process_vk_booking_payload(conn, peer_id=peer_id, from_id=from_id, payload=payload_data)
            if bot_key == "hostess" and peer_id and text and not handled:
                handled = process_vk_pending_text(conn, peer_id=peer_id, from_id=from_id, text=text)
            conn.commit()
        finally:
            conn.close()
        log_event(
            "VK-CALLBACK",
            status="message_new",
            peer_id=peer_id or "-",
            from_id=from_id or "-",
            text=(text[:120] if text else "-"),
            bot_key=bot_key,
        )
        if handled:
            log_event("VK-CALLBACK", status="message_handled", peer_id=peer_id or "-", from_id=from_id or "-", bot_key=bot_key)
            return "ok"
        normalized_text = text.lower()
        should_reply = bool(is_new_peer or normalized_text in {"start", "/start", "старт", "help", "/help", "меню", "menu"})
        if peer_id and vk_api_enabled(bot_key) and should_reply:
            try:
                reply_text = (
                    "Рабочий чат LUCH подключен.\nСюда будут приходить новые брони из действующих webhook-источников.\nСледующим этапом добавим управление бронями прямо из VK."
                    if bot_key == "hostess"
                    else "Чат официантов LUCH подключен.\nСюда будут приходить служебные уведомления по столам и депозитам."
                )
                vk_send_message(
                    int(peer_id),
                    reply_text,
                    bot_key=bot_key,
                )
                log_event("VK-CALLBACK", status="message_replied", peer_id=peer_id, bot_key=bot_key)
            except Exception as exc:
                log_exception("VK-CALLBACK", status="message_reply_failed", peer_id=peer_id, bot_key=bot_key, error=exc)
        elif peer_id and not should_reply:
            log_event("VK-CALLBACK", status="message_recorded", peer_id=peer_id, bot_key=bot_key)
        elif peer_id:
            log_event("VK-CALLBACK", status="message_reply_skipped", reason="vk_api_disabled", peer_id=peer_id, bot_key=bot_key)

    if event_type == "message_event":
        peer_id = event_object.get("peer_id")
        from_id = event_object.get("user_id")
        event_id = str(event_object.get("event_id") or "").strip()
        payload_data = parse_vk_event_payload(event_object)

        handled = False
        conn = connect()
        try:
            if bot_key == "hostess" and peer_id and from_id and payload_data:
                handled = process_vk_booking_payload(conn, peer_id=peer_id, from_id=from_id, payload=payload_data)
            conn.commit()
        finally:
            conn.close()

        log_event(
            "VK-CALLBACK",
            status="message_event",
            peer_id=peer_id or "-",
            from_id=from_id or "-",
            bot_key=bot_key,
            handled=handled,
        )

        if event_id and peer_id and from_id and vk_api_enabled(bot_key):
            action_name = str(payload_data.get("action") or "").strip()
            snackbar_text = (
                "Напишите ответ в чат."
                if action_name in {"prompt_assign_table", "prompt_set_deposit", "prompt_restrict_table"}
                else ("Команда принята." if handled else "Действие недоступно.")
            )
            try:
                vk_answer_message_event(
                    event_id=event_id,
                    user_id=int(from_id),
                    peer_id=int(peer_id),
                    event_data={"type": "show_snackbar", "text": snackbar_text},
                    bot_key=bot_key,
                )
                log_event("VK-CALLBACK", status="message_event_answered", peer_id=peer_id or "-", from_id=from_id or "-", bot_key=bot_key)
            except Exception as exc:
                log_exception("VK-CALLBACK", status="message_event_answer_failed", peer_id=peer_id or "-", from_id=from_id or "-", bot_key=bot_key, error=exc)

    return "ok"


if __name__ == "__main__":
    app.run(debug=True)
