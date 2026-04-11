import os
import json
import csv
import sqlite3
from contextlib import contextmanager
from typing import Optional

DB_PATH = os.environ.get("DB_PATH", "./data/luchbar.db").strip()


def connect():
    # Ensure parent directory exists for file-based SQLite DB path.
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def db():
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, ddl: str):
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not table_exists:
        return
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    if col not in cols:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise


def init_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        -- ===== guests (агрегаты по гостям) =====
        CREATE TABLE IF NOT EXISTS guests (
          phone_e164        TEXT PRIMARY KEY,
          name_last         TEXT,
          visits_count      INTEGER NOT NULL DEFAULT 0,
          first_visit_dt    TEXT,
          last_visit_dt     TEXT,
          created_at        TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_guests_last_visit_dt ON guests(last_visit_dt);

        -- ===== guest_visits (сырой лог визитов/истории) =====
        CREATE TABLE IF NOT EXISTS guest_visits (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          phone_e164     TEXT NOT NULL,
          name           TEXT,
          reservation_dt TEXT NOT NULL,
          date_form      TEXT,
          time_form      TEXT,
          formname       TEXT,
          created_dt     TEXT,
          source         TEXT NOT NULL DEFAULT 'import',
          created_at     TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_guest_visits_phone ON guest_visits(phone_e164);
        CREATE INDEX IF NOT EXISTS idx_guest_visits_resdt ON guest_visits(reservation_dt);
        CREATE INDEX IF NOT EXISTS idx_guest_visits_form  ON guest_visits(formname);

        -- ===== bookings (приходящие брони) =====
        CREATE TABLE IF NOT EXISTS bookings (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          tranid              TEXT UNIQUE,
          formname            TEXT,
          name                TEXT,
          phone_e164          TEXT,
          phone_raw           TEXT,
          reservation_date    TEXT,
          reservation_time    TEXT,
          reservation_dt      TEXT,
          guests_count        INTEGER,
          comment             TEXT,
          utm_source          TEXT,
          utm_medium          TEXT,
          utm_campaign        TEXT,
          utm_content         TEXT,
          utm_term            TEXT,
          status              TEXT NOT NULL DEFAULT 'WAITING',
          guest_segment       TEXT,
          reservation_token   TEXT,
          assigned_table_number INTEGER,
          deposit_amount      INTEGER,
          deposit_comment     TEXT,
          deposit_set_at      TEXT,
          deposit_set_by      TEXT,
          telegram_chat_id    TEXT,
          telegram_message_id TEXT,
          raw_payload_json    TEXT,
          created_at          TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_bookings_phone ON bookings(phone_e164);
        CREATE INDEX IF NOT EXISTS idx_bookings_resdt ON bookings(reservation_dt);

        -- ===== booking_events =====
        CREATE TABLE IF NOT EXISTS booking_events (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          booking_id     INTEGER NOT NULL,
          event_type     TEXT NOT NULL,
          actor_tg_id    TEXT,
          actor_name     TEXT,
          payload_json   TEXT,
          created_at     TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (booking_id) REFERENCES bookings(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_booking_events_booking ON booking_events(booking_id);

        -- ===== venue_tables =====
        CREATE TABLE IF NOT EXISTS venue_tables (
          table_number       INTEGER PRIMARY KEY,
          label              TEXT NOT NULL DEFAULT 'NONE',
          restricted_until   TEXT,
          restriction_comment TEXT,
          updated_by         TEXT,
          updated_at         TEXT NOT NULL DEFAULT (datetime('now')),
          created_at         TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_venue_tables_label ON venue_tables(label);
        CREATE INDEX IF NOT EXISTS idx_venue_tables_restricted_until ON venue_tables(restricted_until);

        -- ===== table_events =====
        CREATE TABLE IF NOT EXISTS table_events (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          table_number   INTEGER NOT NULL,
          booking_id     INTEGER,
          event_type     TEXT NOT NULL,
          actor_tg_id    TEXT,
          actor_name     TEXT,
          payload_json   TEXT,
          created_at     TEXT NOT NULL DEFAULT (datetime('now')),
          FOREIGN KEY (booking_id) REFERENCES bookings(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_table_events_table ON table_events(table_number, created_at);
        CREATE INDEX IF NOT EXISTS idx_table_events_booking ON table_events(booking_id, created_at);

        -- ===== guest_notes =====
        CREATE TABLE IF NOT EXISTS guest_notes (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          phone_e164  TEXT NOT NULL,
          note        TEXT NOT NULL,
          actor_tg_id TEXT,
          actor_name  TEXT,
          created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_guest_notes_phone ON guest_notes(phone_e164);

        -- ===== guest_events =====
        CREATE TABLE IF NOT EXISTS guest_events (
          id           INTEGER PRIMARY KEY AUTOINCREMENT,
          phone_e164   TEXT NOT NULL,
          event_type   TEXT NOT NULL,
          actor_tg_id  TEXT,
          actor_name   TEXT,
          payload_json TEXT,
          created_at   TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_guest_events_phone ON guest_events(phone_e164);

        -- ===== pending_replies =====
        CREATE TABLE IF NOT EXISTS pending_replies (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          kind              TEXT NOT NULL,
          booking_id        INTEGER NOT NULL,
          phone_e164        TEXT NOT NULL,
          chat_id           TEXT NOT NULL,
          actor_tg_id       TEXT NOT NULL,
          prompt_message_id TEXT NOT NULL,
          expires_at        TEXT NOT NULL,
          created_at        TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_pending_chat_prompt ON pending_replies(chat_id, prompt_message_id);

        -- ===== discount_codes =====
        CREATE TABLE IF NOT EXISTS discount_codes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT UNIQUE NOT NULL,
          discount_percent INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'ACTIVE',
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          redeemed_at TEXT,
          redeemed_by_tg_id TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_discount_codes_code ON discount_codes(code);

        -- ===== tg_bot_users =====
        CREATE TABLE IF NOT EXISTS tg_bot_users (
          tg_user_id        TEXT PRIMARY KEY,
          username          TEXT,
          first_name        TEXT,
          last_name         TEXT,
          first_started_at  TEXT NOT NULL DEFAULT (datetime('now')),
          last_started_at   TEXT NOT NULL DEFAULT (datetime('now')),
          start_count       INTEGER NOT NULL DEFAULT 1,
          last_start_param  TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_tg_bot_users_last_started_at ON tg_bot_users(last_started_at);

        -- ===== lineup_posters =====
        CREATE TABLE IF NOT EXISTS lineup_posters (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          file_id TEXT NOT NULL,
          caption TEXT,
          uploaded_by TEXT,
          uploaded_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- ===== processed_tg_updates =====
        CREATE TABLE IF NOT EXISTS processed_tg_updates (
          update_id          INTEGER PRIMARY KEY,
          update_type        TEXT,
          chat_id            TEXT,
          message_id         TEXT,
          callback_query_id  TEXT,
          created_at         TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_processed_tg_updates_created_at
          ON processed_tg_updates(created_at);

        -- ===== vk_staff_peers =====
        CREATE TABLE IF NOT EXISTS vk_staff_peers (
          peer_id           TEXT PRIMARY KEY,
          peer_external_id  TEXT,
          bot_key           TEXT NOT NULL DEFAULT 'hostess',
          from_id           TEXT,
          is_active         INTEGER NOT NULL DEFAULT 1,
          role_hint         TEXT,
          last_message_text TEXT,
          last_seen_at      TEXT NOT NULL DEFAULT (datetime('now')),
          created_at        TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_vk_staff_peers_active ON vk_staff_peers(is_active, updated_at);
        """
    )

    _ensure_column(conn, "guests", "tags_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(conn, "bookings", "user_chat_id", "TEXT")
    _ensure_column(conn, "bookings", "reservation_token", "TEXT")
    _ensure_column(conn, "bookings", "assigned_table_number", "INTEGER")
    _ensure_column(conn, "bookings", "deposit_amount", "INTEGER")
    _ensure_column(conn, "bookings", "deposit_comment", "TEXT")
    _ensure_column(conn, "bookings", "deposit_set_at", "TEXT")
    _ensure_column(conn, "bookings", "deposit_set_by", "TEXT")
    _ensure_column(conn, "tg_bot_users", "has_shared_phone", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "tg_bot_users", "phone_e164", "TEXT")
    _ensure_column(conn, "vk_staff_peers", "peer_external_id", "TEXT")
    _ensure_column(conn, "vk_staff_peers", "bot_key", "TEXT NOT NULL DEFAULT 'hostess'")
    conn.execute(
        """
        UPDATE vk_staff_peers
        SET peer_external_id = peer_id
        WHERE peer_external_id IS NULL OR trim(peer_external_id) = ''
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_vk_staff_peers_bot_active
          ON vk_staff_peers(bot_key, is_active, updated_at)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_reservation_token
          ON bookings(reservation_token)
          WHERE reservation_token IS NOT NULL
        """
    )


def run_migrations(conn: sqlite3.Connection):
    row = conn.execute("PRAGMA user_version").fetchone()
    version = int(row[0]) if row else 0

    if version < 1:
        init_schema(conn)
        conn.execute("PRAGMA user_version = 1")
        version = 1

    if version < 2:
        _ensure_column(conn, "bookings", "reservation_token", "TEXT")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_reservation_token
              ON bookings(reservation_token)
              WHERE reservation_token IS NOT NULL
            """
        )
        conn.execute("PRAGMA user_version = 2")

    if version < 3:
        _ensure_column(conn, "vk_staff_peers", "peer_external_id", "TEXT")
        _ensure_column(conn, "vk_staff_peers", "bot_key", "TEXT NOT NULL DEFAULT 'hostess'")
        conn.execute(
            """
            UPDATE vk_staff_peers
            SET peer_external_id = peer_id
            WHERE peer_external_id IS NULL OR trim(peer_external_id) = ''
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_vk_staff_peers_bot_active
              ON vk_staff_peers(bot_key, is_active, updated_at)
            """
        )
        conn.execute("PRAGMA user_version = 3")

    # Defensive idempotent step for environments with inconsistent user_version.
    init_schema(conn)
    conn.commit()


def rebuild_guests_from_visits(conn: sqlite3.Connection):
    """
    Пересчитывает guests из guest_visits:
    - visits_count = count(*)
    - first_visit_dt = min(reservation_dt)
    - last_visit_dt  = max(reservation_dt)
    - name_last = name из записи с last_visit_dt (если есть)
    """
    conn.execute(
        """
        INSERT INTO guests (phone_e164, visits_count, first_visit_dt, last_visit_dt, updated_at)
        SELECT
          phone_e164,
          COUNT(*) AS visits_count,
          MIN(reservation_dt) AS first_visit_dt,
          MAX(reservation_dt) AS last_visit_dt,
          datetime('now')
        FROM guest_visits
        GROUP BY phone_e164
        ON CONFLICT(phone_e164) DO UPDATE SET
          visits_count=excluded.visits_count,
          first_visit_dt=excluded.first_visit_dt,
          last_visit_dt=excluded.last_visit_dt,
          updated_at=datetime('now');
        """
    )

    conn.execute(
        """
        WITH latest AS (
          SELECT v.phone_e164, v.name
          FROM guest_visits v
          JOIN (
            SELECT phone_e164, MAX(reservation_dt) AS maxdt
            FROM guest_visits
            GROUP BY phone_e164
          ) t ON t.phone_e164 = v.phone_e164 AND t.maxdt = v.reservation_dt
          WHERE v.name IS NOT NULL AND trim(v.name) <> ''
        )
        UPDATE guests
        SET name_last = (SELECT latest.name FROM latest WHERE latest.phone_e164 = guests.phone_e164),
            updated_at = datetime('now')
        WHERE phone_e164 IN (SELECT phone_e164 FROM latest);
        """
    )


def get_tags(conn: sqlite3.Connection, phone_e164: str) -> list[str]:
    row = conn.execute("SELECT tags_json FROM guests WHERE phone_e164=?", (phone_e164,)).fetchone()
    if not row:
        return []
    try:
        tags = json.loads(row["tags_json"] or "[]")
        return [str(t) for t in tags if str(t).strip()]
    except Exception:
        return []


def set_tags(conn: sqlite3.Connection, phone_e164: str, tags: list[str]):
    tags_norm = sorted({t.strip().upper() for t in tags if t and str(t).strip()})
    conn.execute(
        "UPDATE guests SET tags_json=?, updated_at=datetime('now') WHERE phone_e164=?",
        (json.dumps(tags_norm, ensure_ascii=False), phone_e164),
    )


def seed_discount_codes_from_csv(conn: sqlite3.Connection, csv_path: Optional[str] = None) -> int:
  """
  Загружает promo-коды из CSV в таблицу discount_codes.
  CSV ожидается в формате: code,qr_link[,discount_percent].
  Возвращает количество добавленных строк.
  """
  if not csv_path:
    csv_path = os.path.join(os.path.dirname(__file__), "discount_qr_codes.csv")

  if not os.path.exists(csv_path):
    return 0

  before = conn.execute("SELECT COUNT(*) AS c FROM discount_codes").fetchone()
  before_count = int(before["c"] or 0) if before else 0

  with open(csv_path, "r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
      code = str((row or {}).get("code") or "").strip().upper()
      if not code:
        continue

      raw_discount = str((row or {}).get("discount_percent") or "").strip()
      discount_percent = int(raw_discount) if raw_discount.isdigit() else 15

      conn.execute(
        """
        INSERT OR IGNORE INTO discount_codes (code, discount_percent, status)
        VALUES (?, ?, 'ACTIVE')
        """,
        (code, discount_percent),
      )

  after = conn.execute("SELECT COUNT(*) AS c FROM discount_codes").fetchone()
  after_count = int(after["c"] or 0) if after else 0
  return max(0, after_count - before_count)
